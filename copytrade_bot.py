"""Strategy Arena Copy-Trading Bot Daemon.
Menyalin sinyal strategi terpilih (ROC Momentum 12 & EMA Deviation) secara real-time ke akun real MetaTrader 5 Headway.
"""

from __future__ import annotations

import asyncio
import datetime
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
    ALLOW_HEDGING,
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


def send_telegram_alert(message: str, chat_id: Optional[str] = None, parse_mode: str = "HTML") -> None:
    """Mengirim notifikasi Telegram jika token dan chat_id dikonfigurasi."""
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target_chat:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": target_chat,
            "text": message,
            "parse_mode": parse_mode,
        }
        res = requests.post(url, json=payload, timeout=5)
        if res.status_code != 200:
            # Fallback tanpa parsing jika ada tag tidak valid
            requests.post(url, json={"chat_id": target_chat, "text": message}, timeout=5)
    except Exception as exc:
        log.warning("Gagal mengirim Telegram alert: %s", exc)


def find_matching_strategies(query: str) -> list[str]:
    """Mencocokkan nama atau kata kunci strategi dari user Telegram ke ID strategi."""
    q = query.strip().lower()
    if not q or q in ("all", "*", "semua"):
        return list(TARGET_STRATEGIES)

    # 1. Cek exact match ID atau Nama
    for sid, sname in STRATEGY_NAMES_MAP.items():
        if q == sid.lower() or q == sname.lower():
            return [sid]

    # 2. Cek substring match
    matches = []
    for sid, sname in STRATEGY_NAMES_MAP.items():
        if q in sid.lower() or q in sname.lower():
            matches.append(sid)

    # 3. Jika tidak ada di STRATEGY_NAMES_MAP, izinkan query langsung (misal ID strategi arena lainnya)
    if not matches and (q.isalnum() or "_" in q):
        matches.append(q)

    return matches


async def handle_posisi_command(arg: str, chat_id: str) -> None:
    """Mengambil posisi aktif dari Strategy Arena dan MT5 untuk strategi yang diminta."""
    matching_strats = find_matching_strategies(arg)
    if not matching_strats:
        avail_strats = "\n".join(f"  • <code>{s}</code> ({STRATEGY_NAMES_MAP.get(s, s)})" for s in TARGET_STRATEGIES)
        msg = (
            f"⚠️ <b>Strategi '{arg}' tidak ditemukan.</b>\n\n"
            f"Strategi yang sedang Anda ikuti:\n{avail_strats}\n\n"
            f"💡 Ketik <code>/posisi</code> untuk melihat semua strategi yang diikuti."
        )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    # Ambil posisi riil dari MT5 dan database lokal
    mt5_open = mt5.get_open_positions(MT5_MAGIC_NUMBER) if mt5 else []
    db_open = trade_storage.get_all_open_positions()

    mt5_pos_map = {p.get("symbol", "").upper(): p for p in mt5_open}
    db_pos_map = {p.get("symbol", "").upper(): p for p in db_open}

    query_strats = ",".join(matching_strats)
    api_url = f"{ARENA_BASE_URL}/api/v1/signals/positions"
    params = {
        "strategy": query_strats,
        "mode": TARGET_MODE,
        "api_key": ARENA_API_KEY,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params, timeout=5) as resp:
                if resp.status != 200:
                    send_telegram_alert(f"⚠️ Gagal menghubungi Strategy Arena API (HTTP {resp.status})", chat_id=chat_id)
                    return
                data = await resp.json()
    except Exception as exc:
        send_telegram_alert(f"⚠️ Error mengambil posisi dari Strategy Arena: {exc}", chat_id=chat_id)
        return

    arena_positions = data.get("positions", [])
    strat_arena_map: dict[str, list[dict]] = {sid: [] for sid in matching_strats}
    for p in arena_positions:
        sid = p.get("strategy")
        if sid in strat_arena_map:
            strat_arena_map[sid].append(p)
        else:
            strat_arena_map[sid] = [p]

    lines = [f"📍 <b>Laporan Posisi Aktif ({len(matching_strats)} Strategi)</b>\n"]

    for sid in matching_strats:
        sname = STRATEGY_NAMES_MAP.get(sid, sid)
        pos_list = strat_arena_map.get(sid, [])
        lines.append(f"━━ <b>{sname}</b> (<code>{sid}</code>) ━━")

        if not pos_list:
            lines.append("  <i>Tidak ada posisi terbuka di Strategy Arena (Standby menunggu sinyal).</i>\n")
            continue

        for p in pos_list:
            sym = p.get("symbol", "").upper()
            side = p.get("side", "LONG")
            side_badge = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
            entry_px = float(p.get("entry_price") or 0.0)
            cur_px = float(p.get("current_price") or 0.0)
            sl = p.get("stop_loss")
            tp = p.get("take_profit")
            pnl_usd = float(p.get("unrealized_pnl_usd") or 0.0)
            open_ts = p.get("opened_at")

            lines.append(f"• <b>{sym}</b> [{side_badge}]")
            if open_ts:
                ts_sec = open_ts / 1000.0 if open_ts > 1e12 else float(open_ts)
                dt_wib = datetime.datetime.fromtimestamp(ts_sec, tz=datetime.timezone(datetime.timedelta(hours=7)))
                lines.append(f"  ├ Waktu Open: <code>{dt_wib.strftime('%H:%M:%S')} WIB</code>")

            lines.append(f"  ├ Entry Sinyal: <code>{entry_px:.5f}</code> | Kini: <code>{cur_px:.5f}</code>")
            if sl and tp:
                lines.append(f"  ├ SL: <code>{float(sl):.5f}</code> | TP: <code>{float(tp):.5f}</code>")
            lines.append(f"  ├ Floating Arena: <code>{pnl_usd:+.2f} USD</code>")

            # Cek status di MT5 Real kita yang spesifik untuk strategi ini
            found_mt5 = None
            for mp in mt5_open:
                if mp.get("symbol", "").upper() == sym:
                    if mp.get("comment") == f"SA:{sid}":
                        found_mt5 = mp
                        break
                    elif not found_mt5 and not ALLOW_HEDGING:
                        found_mt5 = mp

            found_db = None
            for dp in db_open:
                if dp.get("symbol", "").upper() == sym:
                    if dp.get("strategy") == sid:
                        found_db = dp
                        break
                    elif not found_db and not ALLOW_HEDGING:
                        found_db = dp

            if found_mt5 and (found_mt5.get("comment") == f"SA:{sid}" or not ALLOW_HEDGING):
                lines.append(f"  └ <b>Status MT5 Real:</b> 🟢 <b>OPEN</b> (Ticket #{found_mt5.get('ticket')}, Lot: {found_mt5.get('volume'):.2f}, Profit: ${found_mt5.get('profit', 0.0):+.2f})")
            elif found_db and (found_db.get("strategy") == sid or not ALLOW_HEDGING):
                lines.append(f"  └ <b>Status MT5 Real:</b> 🟡 <b>TERCATAT</b> (Ticket #{found_db.get('ticket')})")
            else:
                lines.append(f"  └ <b>Status MT5 Real:</b> ⏳ Standby")

        lines.append("")

    full_msg = "\n".join(lines).strip()
    send_telegram_alert(full_msg, chat_id=chat_id, parse_mode="HTML")


async def handle_my_posisi_command(chat_id: str) -> None:
    """Menampilkan daftar seluruh trade riil di akun MT5 yang sedang aktif."""
    open_positions = mt5.get_open_positions() if mt5 else []
    acc = mt5.get_account_info() if mt5 else None

    if not open_positions:
        msg = (
            "💼 <b>Posisi Aktif Akun MT5 Anda:</b>\n\n"
            "<i>Saat ini tidak ada posisi terbuka (0 trade aktif). Akun dalam kondisi standby.</i>"
        )
        if acc:
            msg += (
                f"\n\n• <b>Saldo:</b> <code>${acc.get('balance', 0.0):.2f} USD</code>\n"
                f"• <b>Equity:</b> <code>${acc.get('equity', 0.0):.2f} USD</code>\n"
                f"• <b>Free Margin:</b> <code>${acc.get('margin_free', 0.0):.2f} USD</code>"
            )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    total_profit = sum(p.get("profit", 0.0) for p in open_positions)
    pnl_sign = "+" if total_profit >= 0 else ""
    lines = [
        f"💼 <b>Posisi Aktif Akun MT5 Anda ({len(open_positions)} Trade)</b>\n"
    ]

    for p in open_positions:
        sym = p.get("symbol", "").upper()
        side = p.get("side", "LONG")
        side_badge = "🟢 BUY" if side == "LONG" else "🔴 SELL"
        ticket = p.get("ticket")
        vol = p.get("volume", 0.01)
        px_open = p.get("price_open", 0.0)
        px_cur = p.get("price_current", 0.0)
        sl = p.get("sl", 0.0)
        tp = p.get("tp", 0.0)
        profit = p.get("profit", 0.0)
        comment = p.get("comment", "")
        db_record = trade_storage.get_open_position_by_ticket(ticket)
        if db_record and db_record.get("open_time"):
            time_sec = float(db_record["open_time"])
        else:
            broker_time = p.get("time", 0)
            time_sec = (broker_time - 7200) if (broker_time - time.time() > 3600) else broker_time

        dt_wib = datetime.datetime.fromtimestamp(time_sec, tz=datetime.timezone(datetime.timedelta(hours=7))) if time_sec else None
        time_str = f"{dt_wib.strftime('%H:%M:%S')} WIB" if dt_wib else "—"

        profit_str = f"+${profit:.2f}" if profit >= 0 else f"-${abs(profit):.2f}"
        profit_emoji = "🟢" if profit >= 0 else "🔴"

        digits = 2 if ("XAU" in sym or "JPY" in sym) else 5

        lines.append(f"• <b>{sym}</b> [{side_badge}] <code>#{ticket}</code>")
        lines.append(f"  ├ Waktu Open: <code>{time_str}</code>")
        lines.append(f"  ├ Lot: <code>{vol:.2f} Lot</code>")
        lines.append(f"  ├ Entry: <code>{px_open:.{digits}f}</code> | Kini: <code>{px_cur:.{digits}f}</code>")
        if sl > 0 or tp > 0:
            sl_s = f"{sl:.{digits}f}" if sl > 0 else "—"
            tp_s = f"{tp:.{digits}f}" if tp > 0 else "—"
            lines.append(f"  ├ SL: <code>{sl_s}</code> | TP: <code>{tp_s}</code>")
        lines.append(f"  ├ Floating PnL: {profit_emoji} <b>{profit_str}</b>")
        if comment:
            lines.append(f"  └ Catatan: <code>{comment}</code>")
        else:
            lines.append(f"  └ Status: 🟢 <b>AKTIF</b>")
        lines.append("")

    if acc:
        lines.append(
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Total Floating PnL:</b> <code>{pnl_sign}${total_profit:.2f} USD</code>\n"
            f"• <b>Equity MT5:</b> <code>${acc.get('equity', 0.0):.2f} USD</code>\n"
            f"• <b>Free Margin:</b> <code>${acc.get('margin_free', 0.0):.2f} USD</code>"
        )

    send_telegram_alert("\n".join(lines).strip(), chat_id=chat_id, parse_mode="HTML")


async def handle_telegram_command(text: str, chat_id: str) -> None:
    """Memproses perintah yang dikirimkan user di Telegram."""
    clean_text = text.strip()
    parts = clean_text.split(maxsplit=1)
    cmd = parts[0].lower().split("@")[0]
    arg = parts[1].strip() if len(parts) > 1 else ""

    log.info("📩 Menerima perintah Telegram: %s dari %s (arg: '%s')", cmd, chat_id, arg)

    if cmd in ("/start", "/help"):
        help_msg = (
            "🤖 <b>SA Copy-Trade Control Bot</b>\n\n"
            "Perintah yang dapat Anda gunakan:\n\n"
            "• <code>/my-posisi</code> (atau <code>/my_posisi</code>)\n"
            "  Melihat seluruh trade riil akun MT5 Anda yang sedang aktif.\n\n"
            "• <code>/posisi</code>\n"
            "  Melihat posisi aktif dari semua strategi yang diikuti.\n\n"
            "• <code>/posisi &lt;nama_strategi&gt;</code>\n"
            "  Melihat posisi aktif strategi tertentu.\n"
            "  <i>Contoh:</i>\n"
            "  - <code>/posisi roc_momentum</code>\n"
            "  - <code>/posisi gap_fill</code>\n\n"
            "• <code>/status</code>\n"
            "  Cek saldo akun MT5, equity, margin, dan status bot.\n\n"
            "• <code>/help</code>\n"
            "  Tampilkan pesan panduan ini."
        )
        send_telegram_alert(help_msg, chat_id=chat_id, parse_mode="HTML")

    elif cmd in ("/my-posisi", "/my_posisi", "/myposisi"):
        await handle_my_posisi_command(chat_id)

    elif cmd == "/status":
        acc = mt5.get_account_info()
        open_pos = mt5.get_open_positions(MT5_MAGIC_NUMBER) if mt5 else []
        db_open = trade_storage.get_all_open_positions()

        if acc:
            stop_str = f"{MIN_EQUITY_USD} {acc['currency']}" if MIN_EQUITY_USD > 0 else "0 (Sampai saldo habis)"
            strats_list = "\n".join(f"  {i+1}. {STRATEGY_NAMES_MAP.get(s, s)} (<code>{s}</code>)" for i, s in enumerate(TARGET_STRATEGIES))
            hedging_str = "🟢 AKTIF (Strategi berbeda boleh buka di pair sama)" if ALLOW_HEDGING else "🔴 NONAKTIF (1 pair 1 posisi)"
            status_msg = (
                "📊 <b>Status Copy-Trade Engine (Headway Real)</b>\n\n"
                f"• <b>Login:</b> <code>{acc['login']}</code> ({acc['server']})\n"
                f"• <b>Saldo:</b> <code>{acc['balance']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Equity:</b> <code>{acc['equity']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Floating PnL:</b> <code>{acc['profit']:+,.2f} {acc['currency']}</code>\n"
                f"• <b>Margin Terpakai:</b> <code>{acc['margin']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Margin Bebas:</b> <code>{acc['margin_free']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Batas Stop:</b> <code>{stop_str}</code>\n"
                f"• <b>Multi-Strategy Hedging:</b> <code>{hedging_str}</code>\n\n"
                "🎯 <b>Strategi yang Diikuti:</b>\n"
                f"{strats_list}\n\n"
                f"📈 <b>Posisi Terbuka MT5:</b> {len(open_pos)} trade (DB: {len(db_open)})"
            )
        else:
            status_msg = "⚠️ Gagal mengambil informasi akun MT5. Terminal sedang offline / reconnecting."

        send_telegram_alert(status_msg, chat_id=chat_id, parse_mode="HTML")

    elif cmd == "/posisi":
        await handle_posisi_command(arg, chat_id)


async def telegram_command_listener() -> None:
    """Mendengarkan perintah interaktif dari chat Telegram (long-polling getUpdates)."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN tidak dikonfigurasi. Telegram listener dinonaktifkan.")
        return

    # Daftarkan daftar perintah ke BotFather / Menu Telegram
    try:
        set_cmd_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
        commands = [
            {"command": "my_posisi", "description": "Cek posisi aktif di akun MT5 Anda"},
            {"command": "posisi", "description": "Cek posisi strategi di Strategy Arena"},
            {"command": "status", "description": "Cek saldo MT5, equity & status engine"},
            {"command": "help", "description": "Panduan perintah bot"},
        ]
        async with aiohttp.ClientSession() as session:
            async with session.post(set_cmd_url, json={"commands": commands}, timeout=5) as resp:
                if resp.status == 200:
                    log.info("✓ Menu perintah bot Telegram berhasil didaftarkan ke Telegram API.")
    except Exception as exc:
        log.warning("Gagal mendaftarkan menu perintah Telegram: %s", exc)

    poll_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = 0

    # Ambil update terakhir untuk membersihkan antrean pesan lama saat startup
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(poll_url, params={"offset": -1, "timeout": 1}, timeout=5) as resp:
                if resp.status == 200:
                    init_data = await resp.json()
                    res = init_data.get("result", [])
                    if res:
                        offset = res[-1]["update_id"] + 1
    except Exception:
        pass

    log.info("✓ Telegram interactive command listener aktif! Siap menerima /posisi & /status...")

    while running:
        try:
            params = {"offset": offset, "timeout": 20}
            async with aiohttp.ClientSession() as session:
                async with session.get(poll_url, params=params, timeout=30) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue
                    data = await resp.json()

            if not data.get("ok"):
                await asyncio.sleep(3)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message") or update.get("edited_message")
                if not msg:
                    continue

                chat = msg.get("chat", {})
                sender_id = str(chat.get("id", ""))
                text = msg.get("text", "").strip()

                # Filter pengirim agar hanya pemilik akun Telegram yang direspons
                if TELEGRAM_CHAT_ID and sender_id != TELEGRAM_CHAT_ID:
                    log.warning("Pesan diabaikan dari chat_id asing: %s", sender_id)
                    continue

                if not text.startswith("/"):
                    continue

                await handle_telegram_command(text, sender_id)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("Exception dalam Telegram listener: %s", exc)
            await asyncio.sleep(3)


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
    if ALLOW_HEDGING:
        existing_db = trade_storage.get_open_position_by_strategy_symbol(strategy_id, symbol)
        if existing_db:
            log.info("⏩ Mengabaikan sinyal: Strategi '%s' sudah memiliki posisi aktif di %s (Ticket #%s)", strategy_id, symbol, existing_db["ticket"])
            return
    else:
        existing_db = trade_storage.get_open_position_by_symbol(symbol)
        if existing_db:
            log.info("⏩ Mengabaikan sinyal: Simbol %s sudah aktif di database (Ticket #%s, ALLOW_HEDGING=false)", symbol, existing_db["ticket"])
            return

    # 2. Risk Management Pre-Checks
    open_positions = mt5.get_open_positions(MT5_MAGIC_NUMBER)
    can_open, reason = RiskManager.can_open_position(symbol, equity, balance, open_positions, strategy=strategy_id)
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
    side = sig.get("side", "").upper()

    # Filter jika strategi disebutkan dan bukan salah satu target strategi
    if strategy_id and strategy_id not in TARGET_STRATEGIES_SET:
        return

    # Cari posisi di database
    db_pos = None
    if strategy_id:
        # Jika ada strategy_id spesifik, WAJIB hanya mencari posisi milik strategi tersebut!
        db_pos = trade_storage.get_open_position_by_strategy_symbol(strategy_id, symbol, side=side if side else None)
    elif not ALLOW_HEDGING:
        # Hanya boleh mencari generic symbol jika HEDGING dimatikan dan tidak ada strategy_id spesifik
        db_pos = trade_storage.get_open_position_by_symbol(symbol)

    ticket_to_close = None
    if db_pos:
        ticket_to_close = db_pos["ticket"]
        strategy_id = db_pos.get("strategy", strategy_id or TARGET_STRATEGY)
    else:
        # Cek langsung di MT5 jika tidak ada di database lokal
        open_positions = mt5.get_open_positions(MT5_MAGIC_NUMBER)
        for p in open_positions:
            if symbol == p.get("symbol"):
                # Jika strategy_id diketahui, WAJIB mencocokkan komentar MT5 (SA:<strategy_id>)
                if strategy_id:
                    if p.get("comment") == f"SA:{strategy_id}":
                        if side and p.get("side", "").upper() != side:
                            continue
                        ticket_to_close = p["ticket"]
                        break
                elif not ALLOW_HEDGING:
                    ticket_to_close = p["ticket"]
                    break

    if not ticket_to_close:
        strat_lbl = f"[{strategy_id}] " if strategy_id else ""
        log.info("ℹ️ Sinyal tutup %s diterima untuk %s%s, tetapi tidak ada posisi aktif yang cocok (sudah tertutup broker SL/TP). Posisi strategi lain TIDAK disentuh.",
                 action, strat_lbl, symbol)
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

            # 2. Ambil posisi yang masih kita catat OPEN di database & posisi aktif di MT5
            db_open_positions = trade_storage.get_all_open_positions()
            mt5_active = mt5.get_open_positions(MT5_MAGIC_NUMBER)
            active_tickets = {p["ticket"] for p in mt5_active}

            # 3. Cek apakah ada posisi MT5 yang sudah ditutup langsung oleh broker (misal hit hard SL/TP)
            for pos in db_open_positions:
                if pos["ticket"] not in active_tickets:
                    log.info("🔍 REKONSILIASI: Ticket #%s (%s - %s) telah ditutup di MT5 (Hit SL/TP).", pos["ticket"], pos["symbol"], pos.get("strategy"))
                    trade_storage.record_close(
                        ticket=pos["ticket"],
                        close_price=pos["open_price"],
                        pnl=0.0,
                        close_reason="BROKER_SL_TP",
                    )

            # Re-fetch setelah update status broker
            db_open_positions = trade_storage.get_all_open_positions()

            # 4. Jika di Strategy Arena posisi sudah tutup tapi di bot masih buka -> TUTUP posisi strategi tersebut!
            for pos in db_open_positions:
                sym = pos["symbol"].upper()
                strat = pos.get("strategy")
                side = pos.get("side", "").upper()
                if (strat and (sym, strat) not in arena_strat_symbols) or (not strat and sym not in arena_symbols):
                    log.info("🔍 REKONSILIASI: Posisi %s (%s) sudah ditutup di Strategy Arena. Menutup di MT5...", sym, strat or "all")
                    await execute_close_signal({"symbol": sym, "strategy": strat, "side": side, "action": "SYNC_CLOSE"}, acc_info)

            # 5. Re-refresh posisi terbuka setelah langkah penutupan sebelum menyinkronkan posisi baru
            db_open_positions = trade_storage.get_all_open_positions()
            mt5_active = mt5.get_open_positions(MT5_MAGIC_NUMBER)

            # 6. Sinkronisasi posisi terbuka Strategy Arena yang belum ada di MT5
            # Tangkap posisi jika:
            # a) AUTO_SYNC_PAST_TRADES = True, ATAU
            # b) Posisi baru saja dibuka (usia <= 15 menit) yang terlewat saat restart server atau reconnect WebSocket
            if ALLOW_HEDGING:
                active_keys = {(p.get("strategy", "").lower(), p.get("symbol", "").upper()) for p in db_open_positions}
                mt5_keys = set()
                db_ticket_map = {p["ticket"]: p for p in db_open_positions}
                for p in mt5_active:
                    sym = p.get("symbol", "").upper()
                    strat = ""
                    if p["ticket"] in db_ticket_map:
                        strat = db_ticket_map[p["ticket"]].get("strategy", "")
                    if not strat:
                        strat = p.get("comment", "").replace("SA:", "")
                    if strat:
                        mt5_keys.add((strat.lower(), sym))
            else:
                db_symbols = {p["symbol"].upper() for p in db_open_positions}
                mt5_symbols = {p["symbol"].upper() for p in mt5_active}

            for p in arena_positions:
                sym = p.get("symbol", "").upper()
                strat = p.get("strategy", TARGET_STRATEGY)
                strat_key = (strat.lower(), sym)
                open_ts = p.get("opened_at", 0)
                open_sec = open_ts / 1000.0 if open_ts > 1e12 else float(open_ts)
                age_sec = max(0, time.time() - open_sec) if open_sec > 0 else 999999

                is_fresh_trade = age_sec <= 900  # Posisi baru dibuka dalam 15 menit terakhir

                needs_sync = False
                if ALLOW_HEDGING:
                    if (AUTO_SYNC_PAST_TRADES or is_fresh_trade) and strat_key not in active_keys and strat_key not in mt5_keys:
                        needs_sync = True
                else:
                    if (AUTO_SYNC_PAST_TRADES or is_fresh_trade) and sym not in db_symbols and sym not in mt5_symbols:
                        needs_sync = True

                if needs_sync:
                    log.info(
                        "🔍 REKONSILIASI: Menemukan posisi aktif '%s' (%s - %s, usia: %.0fs) di Strategy Arena. Menyinkronkan ke MT5...",
                        sym, p.get("side"), strat, age_sec
                    )
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
                    if ALLOW_HEDGING:
                        active_keys.add(strat_key)
                        mt5_keys.add(strat_key)
                    else:
                        db_symbols.add(sym)
                        mt5_symbols.add(sym)

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
    stop_desc = f"Equity <= ${MIN_EQUITY_USD:.2f} USD" if MIN_EQUITY_USD > 0 else "0 (Trading sampai saldo habis)"
    log.info("Emergency Stop  : %s", stop_desc)
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

    # Jalankan WebSocket listener, Polling Watchdog, dan Telegram Command Listener secara konkuren
    tasks = [
        asyncio.create_task(websocket_signal_listener()),
        asyncio.create_task(poll_reconciliation_task()),
        asyncio.create_task(telegram_command_listener()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        log.info("SA Copy-Trade Bot dimatikan secara bersih.")


if __name__ == "__main__":
    asyncio.run(main())


