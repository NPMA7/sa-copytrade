"""Strategy Arena Copy-Trading Bot Daemon.
Menyalin sinyal strategi terpilih (ROC Momentum 12 & EMA Deviation) secara real-time ke akun real MetaTrader 5 Headway.
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from typing import Optional
import aiohttp
import requests

from config import (
    ARENA_BASE_URL,
    ARENA_WS_URL,
    ARENA_API_KEY,
    TARGET_STRATEGIES,
    TARGET_STRATEGIES_SET,
    TARGET_STRATEGIES_QUERY,
    STRATEGY_NAMES_MAP,
    TARGET_STRATEGY,
    TARGET_STRATEGY_NAME,
    TARGET_MODE,
    AUTO_SYNC_PAST_TRADES,
    POLL_INTERVAL_SEC,
    MT5_MAGIC_NUMBER,
    MIN_EQUITY_USD,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    log,
)
from mt5_client import MT5Client
from risk_manager import RiskManager
import trade_storage

# Global state
running = True
mt5 = MT5Client()


def send_telegram_alert(message: str) -> None:
    """Mengirim notifikasi Telegram jika token dan chat_id dikonfigurasi."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
        }
        requests.post(url, json=payload, timeout=5)
    except Exception as exc:
        log.warning("Gagal mengirim Telegram alert: %s", exc)


def handle_shutdown(signum, frame):
    """Menangani sinyal terminasi dengan aman."""
    global running
    log.info("Menerima sinyal shutdown (SIGTERM/SIGINT). Menutup bot...")
    running = False


async def execute_open_signal(sig: dict, acc_info: dict) -> None:
    """Mengeksekusi sinyal pembukaan posisi baru dari Strategy Arena."""
    symbol = sig.get("symbol", "").upper()
    side = sig.get("side", "LONG").upper()
    action = sig.get("action", "")
    strategy_id = sig.get("strategy", TARGET_STRATEGY)

    # Validasi apakah strategi termasuk yang diikuti
    if strategy_id not in TARGET_STRATEGIES_SET:
        log.info("⏩ Mengabaikan sinyal %s %s: Strategi '%s' tidak diikuti.", action, symbol, strategy_id)
        return

    equity = acc_info.get("equity", 1000.0)
    balance = acc_info.get("balance", 1000.0)

    # Cek Circuit Breaker Equity Floor ($10)
    if RiskManager.is_equity_halted(equity):
        log.warning("🛑 Sinyal %s %s dibatalkan: Equity ($%.2f) telah mencapai/di bawah batas stop ($%.2f)!", action, symbol, equity, MIN_EQUITY_USD)
        return

    # 1. Cek database & MT5 apakah posisi sudah terbuka
    existing_db = trade_storage.get_open_position_by_symbol(symbol)
    if existing_db:
        log.info("⏩ Mengabaikan sinyal: Simbol %s sudah aktif di database (Ticket #%s)", symbol, existing_db["ticket"])
        return

    # 2. Risk Management Pre-Checks
    open_positions = mt5.get_open_positions(MT5_MAGIC_NUMBER)
    can_open, reason = RiskManager.can_open_position(symbol, equity, balance, open_positions)
    if not can_open:
        log.warning("🛑 Sinyal %s %s dibatalkan: %s", action, symbol, reason)
        return

    # 3. Hitung lot size & validasi SL / TP
    lot = RiskManager.calculate_lot(equity)
    sym_info = mt5.get_symbol_info(symbol)
    digits = sym_info.get("digits", 5) if sym_info else 5
    point = sym_info.get("point", 0.00001) if sym_info else 0.00001
    pip_size = point * 10 if digits in (3, 5) else point

    entry_price = float(sig.get("price") or (sym_info["ask"] if side == "LONG" else sym_info["bid"]))
    raw_sl = sig.get("stop_loss")
    raw_tp = sig.get("take_profit")

    sl, tp = RiskManager.validate_or_calculate_stops(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        sl=raw_sl,
        tp=raw_tp,
        digits=digits,
        pip_size=pip_size,
    )

    strat_title = STRATEGY_NAMES_MAP.get(strategy_id, strategy_id)
    log.info(
        "⚡ MEMPROSES SINYAL: [%s] %s %s | Lot: %.2f | Entry Est: %.5f | SL: %.5f | TP: %.5f",
        strat_title, action, symbol, lot, entry_price, sl, tp
    )

    # 4. Eksekusi order ke broker Headway
    ok, ticket, fill_price, err = mt5.open_position(
        symbol=symbol,
        side=side,
        lot=lot,
        sl=sl,
        tp=tp,
        comment=f"SA:{strategy_id}",
    )

    if ok:
        trade_storage.record_open(
            ticket=ticket,
            strategy=strategy_id,
            symbol=symbol,
            side=side,
            lot=lot,
            open_price=fill_price,
            sl=sl,
            tp=tp,
        )
        msg = (
            f"🟢 *SA COPY-TRADE: ORDER DIBUKA*\n"
            f"• Strategi: `{strat_title}` (`{strategy_id}`)\n"
            f"• Simbol: `{symbol}` ({side})\n"
            f"• Lot: `{lot}`\n"
            f"• Harga Fill: `{fill_price:.5f}`\n"
            f"• Stop Loss: `{sl:.5f}`\n"
            f"• Take Profit: `{tp:.5f}`\n"
            f"• Tiket MT5: `#{ticket}`\n"
            f"• Equity Akun: `${equity:.2f}`"
        )
        send_telegram_alert(msg)
    else:
        log.error("Gagal mengeksekusi order %s %s: %s", action, symbol, err)


async def execute_close_signal(sig: dict, acc_info: dict) -> None:
    """Mengeksekusi sinyal penutupan posisi dari Strategy Arena."""
    symbol = sig.get("symbol", "").upper()
    action = sig.get("action", "CLOSE_SIGNAL")
    strategy_id = sig.get("strategy")

    # Filter jika strategi disebutkan dan bukan salah satu target strategi
    if strategy_id and strategy_id not in TARGET_STRATEGIES_SET:
        return

    # Cari posisi di database
    db_pos = trade_storage.get_open_position_by_symbol(symbol)
    ticket_to_close = None

    if db_pos:
        ticket_to_close = db_pos["ticket"]
        strategy_id = db_pos.get("strategy", strategy_id or TARGET_STRATEGY)
    else:
        # Cek langsung di MT5 jika tidak ada di database lokal
        open_positions = mt5.get_open_positions(MT5_MAGIC_NUMBER)
        for p in open_positions:
            if symbol in p["symbol"]:
                ticket_to_close = p["ticket"]
                break

    if not ticket_to_close:
        log.info("ℹ️ Sinyal tutup %s diterima, tetapi tidak ada posisi terbuka untuk %s.", action, symbol)
        return

    strat_title = STRATEGY_NAMES_MAP.get(strategy_id, strategy_id) if strategy_id else TARGET_STRATEGY_NAME
    log.info("🏁 MENUTUP POSISI: Ticket #%s (%s - %s) karena sinyal %s", ticket_to_close, symbol, strat_title, action)
    ok, close_price, profit, err = mt5.close_position(ticket_to_close, comment=f"SA:{action}")

    if ok:
        trade_storage.record_close(
            ticket=ticket_to_close,
            close_price=close_price,
            pnl=profit,
            close_reason=action,
        )
        pnl_symbol = "🟢" if profit >= 0 else "🔴"
        msg = (
            f"{pnl_symbol} *SA COPY-TRADE: POSISI DITUTUP*\n"
            f"• Strategi: `{strat_title}`\n"
            f"• Simbol: `{symbol}`\n"
            f"• Harga Tutup: `{close_price:.5f}`\n"
            f"• Realized PnL: *${profit:+.2f}*\n"
            f"• Alasan: `{action}`\n"
            f"• Tiket: `#{ticket_to_close}`"
        )
        send_telegram_alert(msg)
    else:
        log.warning("Gagal close ticket #%s: %s", ticket_to_close, err)


async def poll_reconciliation_task() -> None:
    """Background task yang menyinkronkan posisi Strategy Arena dengan MT5 secara periodik."""
    log.info("Memulai watchdog sinkronisasi posisi periodik (interval: %ss)...", POLL_INTERVAL_SEC)
    api_url = f"{ARENA_BASE_URL}/api/v1/signals/positions"

    while running:
        try:
            await asyncio.sleep(POLL_INTERVAL_SEC)
            acc_info = mt5.get_account_info()
            if not acc_info:
                continue

            equity = acc_info.get("equity", 0.0)

            # EMERGENCY CIRCUIT BREAKER: Stop jika equity <= MIN_EQUITY_USD ($10)
            if RiskManager.is_equity_halted(equity):
                log.critical("🚨 EMERGENCY CIRCUIT BREAKER: Equity ($%.2f) <= batas minimum stop ($%.2f)!", equity, MIN_EQUITY_USD)
                mt5_active = mt5.get_open_positions(MT5_MAGIC_NUMBER)
                if mt5_active:
                    for p in mt5_active:
                        ticket = p["ticket"]
                        sym = p.get("symbol", "")
                        log.warning("🚨 EMERGENCY STOP: Menutup posisi MT5 Ticket #%s (%s) untuk mengamankan sisa equity...", ticket, sym)
                        ok, close_price, profit, err = mt5.close_position(ticket, comment="SA:EQUITY_STOP")
                        if ok:
                            trade_storage.record_close(
                                ticket=ticket,
                                close_price=close_price,
                                pnl=profit,
                                close_reason="EMERGENCY_EQUITY_STOP",
                            )
                    send_telegram_alert(
                        f"🚨 *SA COPY-TRADE: EMERGENCY STOP TRIPPED*\n"
                        f"• Equity Akun: `${equity:.2f}` (Batas Stop: `${MIN_EQUITY_USD:.2f}`)\n"
                        f"• Tindakan: Seluruh ({len(mt5_active)}) posisi aktif telah ditutup darurat.\n"
                        f"• Eksekusi dihentikan untuk melindungi saldo modal tersisa."
                    )
                continue

            # 1. Ambil posisi terbuka riil dari Strategy Arena REST API untuk strategi target
            params = {
                "strategy": TARGET_STRATEGIES_QUERY,
                "mode": TARGET_MODE,
                "api_key": ARENA_API_KEY,
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, params=params, timeout=5) as resp:
                    if resp.status != 200:
                        log.warning("Gagal mengambil posisi aktif dari Strategy Arena API (HTTP %d)", resp.status)
                        continue
                    data = await resp.json()

            raw_positions = data.get("positions", [])
            arena_positions = [p for p in raw_positions if p.get("strategy") in TARGET_STRATEGIES_SET]
            arena_strat_symbols = {(p["symbol"].upper(), p.get("strategy")) for p in arena_positions}
            arena_symbols = {p["symbol"].upper() for p in arena_positions}

            # 2. Ambil posisi yang masih kita catat OPEN di database
            db_open_positions = trade_storage.get_all_open_positions()

            # 3. Jika di Strategy Arena posisi sudah tutup tapi di bot masih buka -> TUTUP!
            for pos in db_open_positions:
                sym = pos["symbol"].upper()
                strat = pos.get("strategy")
                if (strat and (sym, strat) not in arena_strat_symbols) or (not strat and sym not in arena_symbols):
                    log.info("🔍 REKONSILIASI: Posisi %s (%s) sudah ditutup di Strategy Arena. Menutup di MT5...", sym, strat or "all")
                    await execute_close_signal({"symbol": sym, "strategy": strat, "action": "SYNC_CLOSE"}, acc_info)

            # 4. Cek apakah posisi MT5 ditutup oleh broker (misal hit hard SL/TP)
            mt5_active = mt5.get_open_positions(MT5_MAGIC_NUMBER)
            active_tickets = {p["ticket"] for p in mt5_active}

            for pos in db_open_positions:
                if pos["ticket"] not in active_tickets:
                    # Posisi sudah tidak ada di MT5 -> Catat sebagai CLOSED oleh broker SL/TP
                    log.info("🔍 REKONSILIASI: Ticket #%s (%s) telah ditutup di MT5 (Hit SL/TP).", pos["ticket"], pos["symbol"])
                    trade_storage.record_close(
                        ticket=pos["ticket"],
                        close_price=pos["open_price"],
                        pnl=0.0,
                        close_reason="BROKER_SL_TP",
                    )

            # 5. Sinkronisasi posisi terbuka Strategy Arena yang belum ada di MT5
            # PENTING: Dinonaktifkan secara default (AUTO_SYNC_PAST_TRADES=False) agar tidak masuk
            # ke posisi lama yang sudah berjalan dengan harga entry berbeda. Bot hanya menunggu entry sinyal baru!
            if AUTO_SYNC_PAST_TRADES:
                db_symbols = {p["symbol"].upper() for p in db_open_positions}
                mt5_symbols = {p["symbol"].upper() for p in mt5_active}

                for p in arena_positions:
                    sym = p.get("symbol", "").upper()
                    strat = p.get("strategy", TARGET_STRATEGY)
                    if sym not in db_symbols and sym not in mt5_symbols:
                        log.info("🔍 REKONSILIASI: Menemukan posisi aktif '%s' (%s - %s) di Strategy Arena. Menyinkronkan ke MT5...", sym, p.get("side"), strat)
                        sig_to_open = {
                            "symbol": sym,
                            "side": p.get("side", "LONG"),
                            "action": f"OPEN_{p.get('side', 'LONG')}",
                            "price": p.get("current_price"),
                            "stop_loss": p.get("stop_loss"),
                            "take_profit": p.get("take_profit"),
                            "strategy": strat,
                        }
                        await execute_open_signal(sig_to_open, acc_info)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("Exception dalam watchdog rekonsiliasi: %s", exc)


async def websocket_signal_listener() -> None:
    """Menghubungkan ke WebSocket Strategy Arena dan menerima broadcast sinyal instan."""
    ws_endpoint = f"{ARENA_WS_URL}?strategy={TARGET_STRATEGIES_QUERY}&mode={TARGET_MODE}&api_key={ARENA_API_KEY}"
    backoff = 2

    while running:
        try:
            log.info("Menghubungkan ke WebSocket sinyal di %s...", ws_endpoint)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_endpoint, heartbeat=20.0) as ws:
                    log.info("✓ Terhubung ke WebSocket Strategy Arena! Mendengarkan strategi: %s (%s)...", TARGET_STRATEGY_NAME, TARGET_STRATEGIES_QUERY)
                    backoff = 2

                    async for msg in ws:
                        if not running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                msg_type = payload.get("type")

                                if msg_type == "hello":
                                    log.info("📡 Handshake diterima: %s", payload.get("message"))
                                    active_list = payload.get("active_positions", [])
                                    log.info("📊 Posisi aktif saat ini di Strategy Arena: %d pair", len(active_list))

                                elif msg_type == "signal":
                                    sig = payload.get("signal", {})
                                    action = sig.get("action", "")
                                    sig_strat = sig.get("strategy", "")

                                    # Filter agar hanya memproses strategi target yang diikuti
                                    if sig_strat and sig_strat not in TARGET_STRATEGIES_SET:
                                        continue

                                    acc_info = mt5.get_account_info() or {"equity": 1000.0, "balance": 1000.0}

                                    if action.startswith("OPEN_"):
                                        await execute_open_signal(sig, acc_info)
                                    elif action.startswith("CLOSE_") or action == "MARGIN_CALL":
                                        await execute_close_signal(sig, acc_info)

                            except json.JSONDecodeError:
                                pass
                            except Exception as exc:
                                log.error("Error memproses sinyal WebSocket: %s", exc)

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log.warning("WebSocket terputus.")
                            break

        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("Koneksi WebSocket gagal: %s. Reconnecting dalam %ds...", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def main() -> None:
    """Fungsi utama daemon."""
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    trade_storage.init_db()

    log.info("=" * 70)
    log.info("STRATEGY ARENA COPY-TRADING ENGINE v1.1 (Multi-Strategy)")
    log.info("Target Strategi : %s (%s)", TARGET_STRATEGY_NAME, ", ".join(TARGET_STRATEGIES))
    log.info("Mode Pasar      : %s", TARGET_MODE)
    log.info("Emergency Stop  : Equity <= $%.2f USD", MIN_EQUITY_USD)
    log.info("Akun MT5        : %s | Server: %s", mt5.host, mt5.port)
    log.info("=" * 70)

    # Inisialisasi koneksi awal MT5
    connected = False
    for attempt in range(10):
        if mt5.connect():
            connected = True
            break
        log.info("Menunggu terminal MT5 siap (percobaan %d/10)...", attempt + 1)
        await asyncio.sleep(3)

    if connected:
        acc = mt5.get_account_info()
        if acc:
            log.info("💼 INFORMASI AKUN MT5 REAL:")
            log.info("   • Login    : %s (%s)", acc["login"], acc["server"])
            log.info("   • Saldo    : $%.2f %s", acc["balance"], acc["currency"])
            log.info("   • Equity   : $%.2f", acc["equity"])
            log.info("   • Margin   : $%.2f (Free: $%.2f)", acc["margin"], acc["margin_free"])
    else:
        log.warning("⚠️ Terminal MT5 belum siap saat startup. Bot akan tetap mencoba menyambung di background.")

    # Jalankan WebSocket listener dan Polling Watchdog secara konkuren
    tasks = [
        asyncio.create_task(websocket_signal_listener()),
        asyncio.create_task(poll_reconciliation_task()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        log.info("SA Copy-Trade Bot dimatikan secara bersih.")


if __name__ == "__main__":
    asyncio.run(main())


