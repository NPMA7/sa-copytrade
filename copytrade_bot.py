"""Strategy Arena Copy-Trading Bot Daemon.
Menyalin sinyal strategi terpilih (ROC Momentum 12 & EMA Deviation) secara real-time ke akun real MetaTrader 5.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import re
import signal
import socket
import sys
import threading
import time
from typing import Optional
import aiohttp
import requests
import urllib3.util.connection

# Pastikan koneksi ke api.telegram.org di dalam Docker memprioritaskan IPv4 (menghindari hang IPv6)
urllib3.util.connection.allowed_gai_family = lambda: socket.AF_INET

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
    MAX_DAILY_LOSS_USD,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    log,
)
from mt5_client import MT5Client
from risk_manager import RiskManager
import trade_storage
from formatters import format_price

# Global state
running = True
mt5 = MT5Client()

ACTIVE_STRATEGIES: list[str] = []
ACTIVE_STRATEGIES_SET: set[str] = set()
ARENA_STRATEGIES_CACHE: dict[str, dict] = {}
_ARENA_CACHE_UPDATED_AT: float = 0.0
active_ws_session: Optional[aiohttp.ClientWebSocketResponse] = None
ws_reconnect_needed: bool = False
_ALERT_COOLDOWN: dict[str, float] = {}
_RECONCILE_SKIP_LOGGED: set[tuple[str, str, str, int]] = set()
_TG_THREAD_LOCAL = threading.local()


def _get_tg_session() -> requests.Session:
    """Mengembalikan requests.Session khusus per-thread agar TCP/TLS keep-alive ke Telegram tetap hangat."""
    sess = getattr(_TG_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        _TG_THREAD_LOCAL.session = sess
    return sess


def load_initial_strategies() -> list[str]:
    """Memuat strategi awal dari database persistent (SQLite), fallback ke .env jika belum pernah diset."""
    stored = trade_storage.get_setting("target_strategies")
    if stored:
        strats = [s.strip() for s in stored.split(",") if s.strip()]
        if strats:
            log.info("✓ Memuat target strategi persisten dari SQLite: %s", strats)
            return strats
    # Fallback ke konfigurasi .env awal
    initial = list(TARGET_STRATEGIES) if TARGET_STRATEGIES else ["gap_fill"]
    trade_storage.set_setting("target_strategies", ",".join(initial))
    log.info("✓ Menginisialisasi strategi awal ke database SQLite: %s", initial)
    return initial


def get_active_strategies() -> list[str]:
    """Mengembalikan daftar ID strategi yang sedang aktif diikuti."""
    global ACTIVE_STRATEGIES, ACTIVE_STRATEGIES_SET
    if not ACTIVE_STRATEGIES:
        ACTIVE_STRATEGIES = load_initial_strategies()
        ACTIVE_STRATEGIES_SET = set(ACTIVE_STRATEGIES)
    return ACTIVE_STRATEGIES


def set_active_strategies(strats: list[str]) -> None:
    """Mengatur strategi aktif baru, menyimpan ke database persistent, dan trigger reconnect WebSocket."""
    global ACTIVE_STRATEGIES, ACTIVE_STRATEGIES_SET, ws_reconnect_needed
    cleaned = []
    seen = set()
    for s in strats:
        s_clean = s.strip().lower()
        if s_clean and s_clean not in seen:
            seen.add(s_clean)
            cleaned.append(s_clean)
    if not cleaned:
        raise ValueError("Daftar strategi tidak boleh kosong")

    ACTIVE_STRATEGIES = cleaned
    ACTIVE_STRATEGIES_SET = set(cleaned)
    trade_storage.set_setting("target_strategies", ",".join(cleaned))
    log.info("🎯 Target strategi copy-trade diperbarui: %s", cleaned)

    # Trigger reconnect WebSocket agar segera subscribe ke stream strategi baru
    ws_reconnect_needed = True
    if active_ws_session and not active_ws_session.closed:
        try:
            asyncio.create_task(active_ws_session.close())
        except Exception:
            pass


async def refresh_arena_strategies_cache(force: bool = False) -> dict[str, dict]:
    """Mengambil metadata dan ranking strategi dari Strategy Arena API (dengan TTL cache 60 detik)."""
    global ARENA_STRATEGIES_CACHE, _ARENA_CACHE_UPDATED_AT
    if not force and ARENA_STRATEGIES_CACHE and (time.time() - _ARENA_CACHE_UPDATED_AT) < 60.0:
        return ARENA_STRATEGIES_CACHE
    url = f"{ARENA_BASE_URL}/api/v1/signals/strategies"
    params = {"mode": TARGET_MODE, "api_key": ARENA_API_KEY}
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(family=socket.AF_INET), timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    strats = data.get("strategies", [])
                    new_cache = {}
                    for s in strats:
                        sid = s.get("strategy")
                        if sid:
                            new_cache[sid] = s
                    if new_cache:
                        ARENA_STRATEGIES_CACHE = new_cache
                        _ARENA_CACHE_UPDATED_AT = time.time()
                        log.info("✓ Cache strategi Strategy Arena diperbarui: %d strategi ditemukan", len(ARENA_STRATEGIES_CACHE))
    except Exception as exc:
        log.warning("Gagal refresh cache strategi dari Strategy Arena: %r", exc)
    return ARENA_STRATEGIES_CACHE


def get_strategy_display_name(sid: str) -> str:
    """Mendapatkan nama rapi strategi (dari API Strategy Arena atau static map)."""
    if sid in ARENA_STRATEGIES_CACHE:
        return ARENA_STRATEGIES_CACHE[sid].get("name", sid)
    return STRATEGY_NAMES_MAP.get(sid, sid)


def _send_telegram_sync(
    message: str,
    chat_id: Optional[str] = None,
    parse_mode: str = "Markdown",
    reply_markup: Optional[dict] = None,
) -> Optional[int]:
    """Kirim pesan Telegram secara sinkron dengan retry otomatis dan backoff."""
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target_chat:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target_chat,
        "text": message,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            sess = _get_tg_session()
            res = sess.post(url, json=payload, timeout=(20, 25))
            if res.status_code != 200 and res.status_code < 500:
                fallback_payload = {"chat_id": target_chat, "text": message}
                if reply_markup:
                    fallback_payload["reply_markup"] = reply_markup
                res = sess.post(url, json=fallback_payload, timeout=(20, 25))
            if res.status_code == 200:
                return res.json().get("result", {}).get("message_id")
            last_exc = RuntimeError(f"HTTP {res.status_code}: {res.text[:160]}")
        except Exception as exc:
            last_exc = exc
            # Reset session pada thread ini jika koneksi terputus
            _TG_THREAD_LOCAL.session = None
        if attempt < 2:
            time.sleep(1.5 * (attempt + 1))

    safe_err = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot<REDACTED>", repr(last_exc))
    log.warning("Gagal mengirim Telegram alert setelah 3 percobaan: %s", safe_err)
    return None


def send_telegram_alert(
    message: str,
    chat_id: Optional[str] = None,
    parse_mode: str = "Markdown",
    reply_markup: Optional[dict] = None,
) -> Optional[int]:
    """Mengirim notifikasi Telegram tanpa memblokir event loop asyncio (dengan retry otomatis)."""
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target_chat:
        return None
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.to_thread(_send_telegram_sync, message, target_chat, parse_mode, reply_markup))
        return None
    except RuntimeError:
        return _send_telegram_sync(message, target_chat, parse_mode, reply_markup)


def _send_throttled_warning(key: str, message: str, cooldown_sec: float = 900.0) -> None:
    """Mengirim peringatan ke Telegram maksimal sekali per interval cooldown agar tidak spam."""
    now = time.time()
    last = _ALERT_COOLDOWN.get(key, 0.0)
    if (now - last) >= cooldown_sec:
        _ALERT_COOLDOWN[key] = now
        send_telegram_alert(message)


def edit_telegram_message(
    chat_id: str,
    message_id: int,
    text: str,
    parse_mode: str = "HTML",
    reply_markup: Optional[dict] = None,
) -> bool:
    """Mengedit teks dan tombol pesan Telegram secara in-place."""
    if not TELEGRAM_BOT_TOKEN or not chat_id or not message_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    for attempt in range(2):
        try:
            sess = _get_tg_session()
            res = sess.post(url, json=payload, timeout=(20, 20))
            if res.status_code != 200 and res.status_code < 500:
                if "message is not modified" in res.text.lower():
                    return True
                fallback = {"chat_id": chat_id, "message_id": message_id, "text": text}
                if reply_markup is not None:
                    fallback["reply_markup"] = reply_markup
                res = sess.post(url, json=fallback, timeout=(20, 20))
            if res.status_code == 200:
                return True
        except Exception as exc:
            _TG_THREAD_LOCAL.session = None
            if attempt == 1:
                safe_err = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot<REDACTED>", repr(exc))
                log.warning("Gagal mengedit pesan Telegram: %s", safe_err)
    return False


def answer_callback_query(
    callback_query_id: str,
    text: Optional[str] = None,
    show_alert: bool = False,
) -> bool:
    """Menjawab callback query interaktif dari tombol Telegram."""
    if not TELEGRAM_BOT_TOKEN or not callback_query_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
            payload["show_alert"] = show_alert
        res = _get_tg_session().post(url, json=payload, timeout=(20, 15))
        return res.status_code == 200
    except Exception as exc:
        _TG_THREAD_LOCAL.session = None
        safe_err = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot<REDACTED>", repr(exc))
        log.warning("Gagal answerCallbackQuery: %s", safe_err)
        return False


def _get_sorted_arena_strategies() -> list[dict]:
    """Mengambil seluruh strategi Arena terurut berdasar win rate & return (atau fallback cache statis)."""
    if ARENA_STRATEGIES_CACHE:
        return sorted(
            ARENA_STRATEGIES_CACHE.values(),
            key=lambda x: (x.get("win_rate_pct", 0), x.get("return_pct", 0)),
            reverse=True,
        )
    return [{"strategy": sid, "name": sname, "win_rate_pct": 0.0, "trades": 0, "return_pct": 0.0} for sid, sname in STRATEGY_NAMES_MAP.items()]


def build_strategy_menu_keyboard(page: int = 0, per_page: int = 10) -> dict:
    """Membangun inline keyboard interaktif berhalaman untuk toggle seluruh strategi dari Telegram."""
    cur_set = set(get_active_strategies())
    all_strats = _get_sorted_arena_strategies()
    total = len(all_strats)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    page_items = all_strats[page * per_page : (page + 1) * per_page]

    keyboard = []
    row = []
    for s in page_items:
        sid = s.get("strategy", "")
        if not sid:
            continue
        raw_name = s.get("name") or sid
        short_label = raw_name if len(raw_name) <= 18 else (raw_name[:17] + "…")
        icon = "✅" if sid in cur_set else "⬜"
        # Batasi callback_data agar tetap <= 64 byte sesuai limit Telegram Bot API
        cb_data = f"strat:toggle:{sid}:{page}"
        if len(cb_data.encode("utf-8")) > 64:
            cb_data = f"strat:toggle:{sid}"
        row.append({"text": f"{icon} {short_label}", "callback_data": cb_data})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    # Baris paginasi Previous / Next untuk menjelajahi semua strategi
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"strat:page:{page - 1}"})
        nav_row.append({"text": f"📄 {page + 1}/{total_pages} ({total})", "callback_data": "strat:noop"})
        if page < total_pages - 1:
            nav_row.append({"text": "Next ➡️", "callback_data": f"strat:page:{page + 1}"})
        keyboard.append(nav_row)

    # Preset & Navigasi
    keyboard.append([
        {"text": "🏆 Top 3 Win Rate", "callback_data": "strat:preset:top3"},
        {"text": "⭐ Top 5 Performa", "callback_data": "strat:preset:top5"},
    ])
    keyboard.append([
        {"text": "📊 Daftar & Peringkat", "callback_data": "strat:leaderboard"},
        {"text": "🔄 Refresh", "callback_data": f"strat:refresh:{page}"},
    ])
    return {"inline_keyboard": keyboard}


def format_strategy_menu_text() -> str:
    """Membuat pesan panduan dan daftar strategi yang sedang aktif."""
    cur_strats = get_active_strategies()
    lines = [
        "🎯 <b>Pengaturan Strategi yang Diikuti (Copy-Trade)</b>\n",
        f"Bot MT5 Anda saat ini mengikuti <b>{len(cur_strats)} strategi</b>:",
    ]
    for i, s in enumerate(cur_strats, 1):
        sname = get_strategy_display_name(s)
        lines.append(f"  <b>{i}. {sname}</b> (<code>{s}</code>)")

    lines.extend([
        "\n<i>Sentuh tombol di bawah untuk toggle aktif/nonaktif strategi favorit, atau gunakan perintah cepat:</i>\n",
        "• <code>/set_strategi &lt;id&gt;</code>\n"
        "  Mengganti daftar strategi target.\n"
        "  <i>Contoh:</i> <code>/set_strategi gap_fill,roc_momentum</code>\n",
        "• <code>/tambah_strategi &lt;id&gt;</code>\n"
        "  Menambah strategi baru ke pantauan bot.\n"
        "  <i>Contoh:</i> <code>/tambah_strategi zscore_rev</code>\n",
        "• <code>/hapus_strategi &lt;id&gt;</code>\n"
        "  Menghapus strategi dari pantauan bot.\n"
        "  <i>Contoh:</i> <code>/hapus_strategi gap_fill</code>\n",
        "• <code>/daftar_strategi</code>\n"
        "  Melihat daftar lengkap 88 strategi di Strategy Arena beserta performanya.",
    ])
    return "\n".join(lines)


def find_matching_strategies(query: str) -> list[str]:
    """Mencocokkan nama atau kata kunci strategi dari user Telegram ke ID strategi."""
    q = query.strip().lower()
    if not q or q in ("all", "*", "semua"):
        return list(get_active_strategies())

    # 1. Cek cache Strategy Arena
    for sid, sdata in ARENA_STRATEGIES_CACHE.items():
        sname = sdata.get("name", "")
        if q == sid.lower() or q == sname.lower():
            return [sid]

    # 2. Cek STRATEGY_NAMES_MAP
    for sid, sname in STRATEGY_NAMES_MAP.items():
        if q == sid.lower() or q == sname.lower():
            return [sid]

    # 3. Cek substring match
    matches = []
    for sid, sdata in ARENA_STRATEGIES_CACHE.items():
        sname = sdata.get("name", "")
        if q in sid.lower() or q in sname.lower():
            matches.append(sid)

    if not matches:
        for sid, sname in STRATEGY_NAMES_MAP.items():
            if q in sid.lower() or q in sname.lower():
                matches.append(sid)

    # 4. Jika tidak ada di map, izinkan ID langsung
    if not matches and (q.isalnum() or "_" in q):
        matches.append(q)

    return matches


async def handle_posisi_command(arg: str, chat_id: str) -> None:
    """Mengambil posisi aktif dari Strategy Arena dan MT5 untuk strategi yang diminta."""
    matching_strats = find_matching_strategies(arg)
    if not matching_strats:
        avail_strats = "\n".join(f"  • <code>{s}</code> ({get_strategy_display_name(s)})" for s in get_active_strategies())
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
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(family=socket.AF_INET), timeout=aiohttp.ClientTimeout(total=25)) as session:
            async with session.get(api_url, params=params) as resp:
                if resp.status != 200:
                    send_telegram_alert(f"⚠️ Gagal menghubungi Strategy Arena API (HTTP {resp.status})", chat_id=chat_id)
                    return
                data = await resp.json()
    except Exception as exc:
        err_str = str(exc) or type(exc).__name__
        send_telegram_alert(f"⚠️ Error mengambil posisi dari Strategy Arena: {err_str}", chat_id=chat_id)
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
        sname = get_strategy_display_name(sid)
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

            lines.append(f"  ├ Entry Sinyal: <code>{format_price(entry_px, sym)}</code> | Kini: <code>{format_price(cur_px, sym)}</code>")
            if sl and tp:
                lines.append(f"  ├ SL: <code>{format_price(sl, sym)}</code> | TP: <code>{format_price(tp, sym)}</code>")
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

        lines.append(f"• <b>{sym}</b> [{side_badge}] <code>#{ticket}</code>")
        lines.append(f"  ├ Waktu Open: <code>{time_str}</code>")
        lines.append(f"  ├ Lot: <code>{vol:.2f} Lot</code>")
        lines.append(f"  ├ Entry: <code>{format_price(px_open, sym)}</code> | Kini: <code>{format_price(px_cur, sym)}</code>")
        if sl > 0 or tp > 0:
            sl_s = format_price(sl, sym) if sl > 0 else "—"
            tp_s = format_price(tp, sym) if tp > 0 else "—"
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
            f"• <b>Saldo MT5:</b> <code>${acc.get('balance', 0.0):.2f} USD</code>\n"
            f"• <b>Equity MT5:</b> <code>${acc.get('equity', 0.0):.2f} USD</code>\n"
            f"• <b>Free Margin:</b> <code>${acc.get('margin_free', 0.0):.2f} USD</code>"
        )

    send_telegram_alert("\n".join(lines).strip(), chat_id=chat_id, parse_mode="HTML")


async def handle_strategi_command(chat_id: str) -> None:
    """Menampilkan menu interaktif konfigurasi strategi di Telegram."""
    await refresh_arena_strategies_cache()
    text = format_strategy_menu_text()
    keyboard = build_strategy_menu_keyboard()
    send_telegram_alert(text, chat_id=chat_id, parse_mode="HTML", reply_markup=keyboard)


async def handle_set_strategi_command(arg: str, chat_id: str) -> None:
    """Mengganti strategi target secara langsung melalui teks perintah."""
    if not arg:
        msg = (
            "⚠️ <b>Format perintah salah.</b> Masukkan ID strategi yang ingin diikuti.\n\n"
            "<i>Contoh:</i>\n"
            "• <code>/set_strategi gap_fill</code> (1 strategi)\n"
            "• <code>/set_strategi gap_fill,roc_momentum</code> (banyak strategi)\n\n"
            "💡 Ketik <code>/daftar_strategi</code> untuk melihat seluruh ID strategi."
        )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    raw_items = [s.strip() for s in arg.replace(",", " ").split() if s.strip()]
    if not raw_items:
        return

    new_strats = []
    for item in raw_items:
        matched = find_matching_strategies(item)
        if matched:
            new_strats.extend(matched)
        else:
            new_strats.append(item.lower())

    seen = set()
    final_strats = [s for s in new_strats if not (s in seen or seen.add(s))]

    try:
        set_active_strategies(final_strats)
    except Exception as exc:
        send_telegram_alert(f"⚠️ Gagal memperbarui strategi: {exc}", chat_id=chat_id, parse_mode="HTML")
        return

    strats_text = "\n".join(
        f"  {i}. <b>{get_strategy_display_name(s)}</b> (<code>{s}</code>)"
        for i, s in enumerate(final_strats, 1)
    )
    msg = (
        "✅ <b>Target Strategi Berhasil Diperbarui!</b>\n\n"
        f"Bot copy-trade kini aktif mengikuti <b>{len(final_strats)} strategi</b>:\n"
        f"{strats_text}\n\n"
        "⚡ <i>Stream WebSocket & filter sinyal telah disinkronkan langsung ke Strategy Arena.</i>\n"
        "💡 <i>Ketik <code>/strategi</code> untuk membuka menu interaktif tombol.</i>"
    )
    send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")


async def handle_tambah_strategi_command(arg: str, chat_id: str) -> None:
    """Menambahkan strategi baru ke daftar pantauan."""
    if not arg:
        msg = (
            "⚠️ <b>Format perintah salah.</b> Masukkan ID strategi yang ingin ditambahkan.\n\n"
            "<i>Contoh:</i> <code>/tambah_strategi zscore_rev</code>\n"
            "💡 Ketik <code>/daftar_strategi</code> untuk melihat ID strategi yang tersedia."
        )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    matched = find_matching_strategies(arg)
    target_id = matched[0] if matched else arg.strip().lower()

    current = list(get_active_strategies())
    if target_id in current:
        msg = (
            f"ℹ️ Strategi <b>{get_strategy_display_name(target_id)}</b> (<code>{target_id}</code>) "
            "sudah aktif dalam daftar pantauan Anda."
        )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    current.append(target_id)
    set_active_strategies(current)

    msg = (
        f"✅ Berhasil menambahkan strategi <b>{get_strategy_display_name(target_id)}</b> (<code>{target_id}</code>)!\n\n"
        f"Saat ini bot aktif mengikuti <b>{len(current)} strategi</b>.\n"
        "Ketik <code>/strategi</code> untuk melihat menu tombol atau <code>/status</code> untuk cek detail akun."
    )
    send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")


async def handle_hapus_strategi_command(arg: str, chat_id: str) -> None:
    """Menghapus strategi dari daftar pantauan."""
    if not arg:
        msg = (
            "⚠️ <b>Format perintah salah.</b> Masukkan ID strategi yang ingin dihapus.\n\n"
            "<i>Contoh:</i> <code>/hapus_strategi gap_fill</code>"
        )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    matched = find_matching_strategies(arg)
    target_id = matched[0] if matched else arg.strip().lower()

    current = list(get_active_strategies())
    if target_id not in current:
        msg = (
            f"⚠️ Strategi <code>{target_id}</code> tidak ada dalam daftar yang sedang diikuti.\n\n"
            f"Strategi aktif: {', '.join(current)}"
        )
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    if len(current) <= 1:
        msg = "⚠️ <b>Tidak dapat menghapus:</b> Minimal harus ada 1 strategi yang aktif diikuti bot!"
        send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")
        return

    current.remove(target_id)
    set_active_strategies(current)

    msg = (
        f"🗑️ Strategi <b>{get_strategy_display_name(target_id)}</b> (<code>{target_id}</code>) telah dihapus dari pantauan.\n\n"
        f"Sisa strategi aktif: <b>{len(current)}</b> ({', '.join(current)})."
    )
    send_telegram_alert(msg, chat_id=chat_id, parse_mode="HTML")


def format_daftar_strategi_page(page: int = 0, per_page: int = 12) -> tuple[str, dict]:
    """Membuat teks dan inline keyboard berhalaman untuk /daftar_strategi agar seluruh strategi bisa dijelajahi."""
    sorted_strats = _get_sorted_arena_strategies()
    total = len(sorted_strats)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    page_items = sorted_strats[start_idx : start_idx + per_page]

    cur_set = set(get_active_strategies())
    mode_label = TARGET_MODE.capitalize()
    lines = [
        f"🏆 <b>Peringkat Strategi Strategy Arena ({mode_label})</b>\n"
        f"<i>Halaman {page + 1} dari {total_pages} • Total {total} strategi</i>\n"
    ]
    for idx, s in enumerate(page_items, start=start_idx + 1):
        sid = s.get("strategy", "")
        sname = s.get("name", sid)
        win = s.get("win_rate_pct", 0.0)
        trades = s.get("trades", 0)
        ret = s.get("return_pct", 0.0)
        is_active = "✅ " if sid in cur_set else ""
        lines.append(
            f"<b>{idx}. {is_active}{sname}</b> (<code>{sid}</code>)\n"
            f"   • Win Rate: <b>{win:.1f}%</b> ({trades} trade) | Return: {ret:+.2f}%"
        )

    lines.extend([
        "\n💡 <b>Cara Mengikuti:</b>",
        "• Ketik <code>/set_strategi &lt;id&gt;</code> untuk mengganti target.",
        "• Ketik <code>/tambah_strategi &lt;id&gt;</code> untuk menambah strategi.",
        "• Ketik <code>/strategi</code> untuk toggle lewat tombol interaktif.",
    ])

    keyboard = []
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"list:page:{page - 1}"})
        nav_row.append({"text": f"📄 {page + 1}/{total_pages}", "callback_data": "list:noop"})
        if page < total_pages - 1:
            nav_row.append({"text": "Next ➡️", "callback_data": f"list:page:{page + 1}"})
        keyboard.append(nav_row)

    keyboard.append([
        {"text": "🔄 Refresh", "callback_data": f"list:refresh:{page}"},
        {"text": "🎯 Toggle di /strategi", "callback_data": "strat:open:0"},
    ])
    return "\n".join(lines), {"inline_keyboard": keyboard}


async def handle_daftar_strategi_command(chat_id: str, page: int = 0) -> None:
    """Menampilkan daftar seluruh strategi di Strategy Arena dengan paginasi Next/Prev."""
    await refresh_arena_strategies_cache()
    text, keyboard = format_daftar_strategi_page(page=page)
    send_telegram_alert(text, chat_id=chat_id, parse_mode="HTML", reply_markup=keyboard)


async def handle_telegram_callback(cb_query: dict) -> None:
    """Memproses klik tombol interaktif Telegram."""
    cb_id = cb_query.get("id")
    cb_data = cb_query.get("data", "")
    msg = cb_query.get("message", {})
    message_id = msg.get("message_id")
    chat_id = str(msg.get("chat", {}).get("id", ""))

    # 1. Callback paginasi /daftar_strategi
    if cb_data.startswith("list:"):
        parts = cb_data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        page_str = parts[2] if len(parts) > 2 else "0"
        page = int(page_str) if page_str.isdigit() else 0

        if action == "noop":
            await asyncio.to_thread(answer_callback_query, cb_id)
            return
        if action == "refresh":
            await refresh_arena_strategies_cache(force=True)
            await asyncio.to_thread(answer_callback_query, cb_id, text="🔄 Data strategi diperbarui")
        else:
            await refresh_arena_strategies_cache(force=False)
            await asyncio.to_thread(answer_callback_query, cb_id)

        if message_id and chat_id:
            text, keyboard = format_daftar_strategi_page(page=page)
            await asyncio.to_thread(edit_telegram_message, chat_id, message_id, text, "HTML", keyboard)
        return

    if not cb_data.startswith("strat:"):
        await asyncio.to_thread(answer_callback_query, cb_id)
        return

    parts = cb_data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    target = parts[2] if len(parts) > 2 else ""

    if action == "noop":
        await asyncio.to_thread(answer_callback_query, cb_id)
        return

    if action == "open":
        page = int(target) if target.isdigit() else 0
        await asyncio.to_thread(answer_callback_query, cb_id)
        send_telegram_alert(
            format_strategy_menu_text(),
            chat_id=chat_id,
            parse_mode="HTML",
            reply_markup=build_strategy_menu_keyboard(page=page),
        )
        return

    if action == "page":
        page = int(target) if target.isdigit() else 0
        if not ARENA_STRATEGIES_CACHE:
            await refresh_arena_strategies_cache()
        await asyncio.to_thread(answer_callback_query, cb_id)
        if message_id and chat_id:
            await asyncio.to_thread(
                edit_telegram_message,
                chat_id,
                message_id,
                format_strategy_menu_text(),
                "HTML",
                build_strategy_menu_keyboard(page=page),
            )
        return

    if action == "toggle":
        sid = target
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        current = list(get_active_strategies())
        if sid in current:
            if len(current) <= 1:
                await asyncio.to_thread(answer_callback_query, cb_id, text="⚠️ Minimal harus ada 1 strategi aktif!", show_alert=True)
                return
            current.remove(sid)
            set_active_strategies(current)
            await asyncio.to_thread(answer_callback_query, cb_id, text=f"❌ Strategi {get_strategy_display_name(sid)} dinonaktifkan")
        else:
            current.append(sid)
            set_active_strategies(current)
            await asyncio.to_thread(answer_callback_query, cb_id, text=f"✅ Strategi {get_strategy_display_name(sid)} diaktifkan!")

        if message_id and chat_id:
            await asyncio.to_thread(
                edit_telegram_message,
                chat_id,
                message_id,
                format_strategy_menu_text(),
                "HTML",
                build_strategy_menu_keyboard(page=page),
            )

    elif action == "preset":
        await refresh_arena_strategies_cache()
        if not ARENA_STRATEGIES_CACHE:
            await asyncio.to_thread(answer_callback_query, cb_id, text="⚠️ Gagal mengambil peringkat strategi dari Strategy Arena", show_alert=True)
            return

        sorted_strats = _get_sorted_arena_strategies()
        count = 3 if target == "top3" else 5
        top_ids = [s["strategy"] for s in sorted_strats[:count] if s.get("strategy")]
        if top_ids:
            set_active_strategies(top_ids)
            await asyncio.to_thread(answer_callback_query, cb_id, text=f"🏆 Mengikuti {len(top_ids)} strategi terbaik!")
            if message_id and chat_id:
                await asyncio.to_thread(
                    edit_telegram_message,
                    chat_id,
                    message_id,
                    format_strategy_menu_text(),
                    "HTML",
                    build_strategy_menu_keyboard(page=0),
                )

    elif action == "leaderboard":
        await asyncio.to_thread(answer_callback_query, cb_id)
        await handle_daftar_strategi_command(chat_id)

    elif action == "refresh":
        page = int(target) if target.isdigit() else 0
        await refresh_arena_strategies_cache(force=True)
        await asyncio.to_thread(answer_callback_query, cb_id, text="🔄 Data strategi diperbarui")
        if message_id and chat_id:
            await asyncio.to_thread(
                edit_telegram_message,
                chat_id,
                message_id,
                format_strategy_menu_text(),
                "HTML",
                build_strategy_menu_keyboard(page=page),
            )


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
            "• <code>/strategi</code>\n"
            "  Menu interaktif untuk mengatur strategi yang diikuti bot (toggle tombol & setting langsung di Telegram).\n\n"
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
            "• <code>/daftar_strategi</code>\n"
            "  Melihat daftar semua strategi di arena beserta performanya.\n\n"
            "• <code>/help</code>\n"
            "  Tampilkan pesan panduan ini."
        )
        send_telegram_alert(help_msg, chat_id=chat_id, parse_mode="HTML")

    elif cmd in ("/my-posisi", "/my_posisi", "/myposisi"):
        await handle_my_posisi_command(chat_id)

    elif cmd in ("/strategi", "/strategy", "/menu_strategi", "/setting_strategi"):
        await handle_strategi_command(chat_id)

    elif cmd in ("/set_strategi", "/set_strategy", "/setstrategi"):
        await handle_set_strategi_command(arg, chat_id)

    elif cmd in ("/tambah_strategi", "/add_strategy", "/tambahstrategi"):
        await handle_tambah_strategi_command(arg, chat_id)

    elif cmd in ("/hapus_strategi", "/del_strategy", "/hapusstrategi", "/remove_strategy"):
        await handle_hapus_strategi_command(arg, chat_id)

    elif cmd in ("/daftar_strategi", "/list_strategi", "/daftarstrategi", "/strategies"):
        page_num = (int(arg) - 1) if arg.isdigit() and int(arg) >= 1 else 0
        await handle_daftar_strategi_command(chat_id, page=page_num)

    elif cmd == "/status":
        acc = mt5.get_account_info()
        open_pos = mt5.get_open_positions(MT5_MAGIC_NUMBER) if mt5 else []
        db_open = trade_storage.get_all_open_positions()

        if acc:
            cur_strats = get_active_strategies()
            daily_pnl_map = trade_storage.get_daily_realized_pnl_by_strategy()
            stop_str = f"{MIN_EQUITY_USD} {acc['currency']}" if MIN_EQUITY_USD > 0 else "0 (Sampai saldo habis)"
            strat_lines = []
            for i, s in enumerate(cur_strats):
                s_pnl = daily_pnl_map.get(s.lower(), 0.0)
                cb_halted = MAX_DAILY_LOSS_USD > 0 and s_pnl <= -abs(MAX_DAILY_LOSS_USD)
                cb_badge = "🔴 CB HALT" if cb_halted else "🟢 AKTIF"
                strat_lines.append(
                    f"  {i+1}. {get_strategy_display_name(s)} (<code>{s}</code>) — PnL Hari Ini: <code>${s_pnl:+.2f}</code> [{cb_badge}]"
                )
            strats_list = "\n".join(strat_lines)
            hedging_str = "🟢 AKTIF (Strategi berbeda boleh buka di pair sama)" if ALLOW_HEDGING else "🔴 NONAKTIF (1 pair 1 posisi)"
            status_msg = (
                "📊 <b>Status Copy-Trade Engine (Real)</b>\n\n"
                f"• <b>Login:</b> <code>{acc['login']}</code> ({acc['server']})\n"
                f"• <b>Saldo:</b> <code>{acc['balance']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Equity:</b> <code>{acc['equity']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Floating PnL:</b> <code>{acc['profit']:+,.2f} {acc['currency']}</code>\n"
                f"• <b>Margin Terpakai:</b> <code>{acc['margin']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Margin Bebas:</b> <code>{acc['margin_free']:,.2f} {acc['currency']}</code>\n"
                f"• <b>Batas Stop:</b> <code>{stop_str}</code>\n"
                f"• <b>Max Rugi Harian / Strategi:</b> <code>-${MAX_DAILY_LOSS_USD:.2f} USD</code>\n"
                f"• <b>Multi-Strategy Hedging:</b> <code>{hedging_str}</code>\n\n"
                f"🎯 <b>Strategi yang Diikuti ({len(cur_strats)}):</b>\n"
                f"{strats_list}\n\n"
                f"📈 <b>Posisi Terbuka MT5:</b> {len(open_pos)} trade (DB: {len(db_open)})"
            )
        else:
            status_msg = "⚠️ Gagal mengambil informasi akun MT5. Terminal sedang offline / reconnecting."

        send_telegram_alert(status_msg, chat_id=chat_id, parse_mode="HTML")

    elif cmd == "/posisi":
        await handle_posisi_command(arg, chat_id)


async def telegram_command_listener() -> None:
    """Mendengarkan perintah interaktif dari chat Telegram (long-polling getUpdates via thread-safe Session)."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN tidak dikonfigurasi. Telegram listener dinonaktifkan.")
        return

    poll_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    set_cmd_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
    commands = [
        {"command": "strategi", "description": "Menu & pengaturan strategi yang diikuti bot"},
        {"command": "my_posisi", "description": "Cek posisi aktif di akun MT5 Anda"},
        {"command": "posisi", "description": "Cek posisi strategi di Strategy Arena"},
        {"command": "daftar_strategi", "description": "Lihat seluruh 89 strategi Arena berhalaman"},
        {"command": "status", "description": "Cek saldo MT5, equity & status engine"},
        {"command": "help", "description": "Panduan perintah bot"},
    ]

    # Gunakan Session tersendiri khusus thread poller agar keep-alive TCP/TLS tetap hangat (~0.2s)
    poll_sess = requests.Session()

    def _sync_post_cmds() -> tuple[int, str]:
        r = poll_sess.post(set_cmd_url, json={"commands": commands}, timeout=(20, 20))
        return r.status_code, r.text

    def _sync_get_updates(params: dict) -> tuple[int, dict]:
        r = poll_sess.get(poll_url, params=params, timeout=(20, 30))
        return r.status_code, (r.json() if r.status_code == 200 else {})

    try:
        st, _ = await asyncio.to_thread(_sync_post_cmds)
        if st == 200:
            log.info("✓ Menu perintah bot Telegram berhasil didaftarkan ke Telegram API.")
    except Exception as exc:
        safe_err = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot<REDACTED>", repr(exc))
        log.warning("Gagal mendaftarkan menu perintah Telegram: %s", safe_err)

    offset = 0
    try:
        st, init_data = await asyncio.to_thread(_sync_get_updates, {"offset": -1, "timeout": 1})
        if st == 200:
            res = init_data.get("result", [])
            if res:
                offset = res[-1]["update_id"] + 1
    except Exception:
        pass

    log.info("✓ Telegram interactive command listener aktif! Siap menerima /strategi, /posisi & /status...")

    try:
        while running:
            try:
                st, data = await asyncio.to_thread(_sync_get_updates, {"offset": offset, "timeout": 20})
                if st != 200:
                    await asyncio.sleep(3)
                    continue

                if not data.get("ok"):
                    await asyncio.sleep(3)
                    continue

                for update in data.get("result", []):
                    offset = update["update_id"] + 1

                    # 1. Cek Callback Query (Tombol Interaktif)
                    cb_query = update.get("callback_query")
                    if cb_query:
                        from_user = cb_query.get("from", {})
                        sender_id = str(from_user.get("id", ""))
                        if TELEGRAM_CHAT_ID and sender_id != TELEGRAM_CHAT_ID:
                            log.warning("Callback query diabaikan dari chat_id asing: %s", sender_id)
                            continue
                        asyncio.create_task(handle_telegram_callback(cb_query))
                        continue

                    # 2. Cek Pesan Masuk
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

                    asyncio.create_task(handle_telegram_command(text, sender_id))

            except asyncio.CancelledError:
                break
            except Exception as exc:
                safe_err = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot<REDACTED>", repr(exc))
                log.warning("Exception dalam Telegram listener: %s", safe_err)
                poll_sess = requests.Session()
                await asyncio.sleep(2)
    finally:
        poll_sess.close()


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
    strategy_id = sig.get("strategy", ACTIVE_STRATEGIES[0] if ACTIVE_STRATEGIES else "gap_fill")

    # Validasi apakah strategi termasuk yang diikuti
    if strategy_id not in ACTIVE_STRATEGIES_SET:
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
        strat_title = get_strategy_display_name(strategy_id)
        if "Circuit Breaker" in reason:
            _send_throttled_warning(
                f"cb_halt:{strategy_id}",
                f"🛑 *SA COPY-TRADE: CIRCUIT BREAKER AKTIF*\n"
                f"• Sinyal terhenti: `{strat_title}` ({action} `{symbol}`)\n"
                f"• Alasan: {reason}\n"
                f"• Hanya strategi `{strategy_id}` yang dijeda hingga reset harian (00:00 UTC / 07:00 WIB); strategi lain tetap aktif.",
                cooldown_sec=900.0,
            )
        elif "Kapasitas posisi" in reason:
            _send_throttled_warning(
                f"max_pos:{strategy_id}:{symbol}",
                f"⚠️ *SA COPY-TRADE: SLOT POSISI PENUH*\n"
                f"• Sinyal dilewati: `{strat_title}` ({action} `{symbol}`)\n"
                f"• Alasan: {reason}",
                cooldown_sec=300.0,
            )
        return

    # 3. Hitung lot size & validasi SL / TP
    lot = RiskManager.calculate_lot(equity, signal_lot=sig.get("lot"))
    sym_info = mt5.get_symbol_info(symbol)
    if not sym_info or sym_info.get("bid", 0.0) <= 0 or sym_info.get("ask", 0.0) <= 0:
        log.error("Gagal mendapatkan quote MT5 untuk %s saat open_position", symbol)
        _send_throttled_warning(
            f"no_quote:{symbol}",
            f"⚠️ *SA COPY-TRADE: QUOTE MT5 KOSONG*\n• Gagal membuka `{symbol}` ({strategy_id}): quote Ask/Bid broker tidak tersedia.",
            cooldown_sec=300.0,
        )
        return

    digits = sym_info.get("digits", 5)
    point = sym_info.get("point", 0.00001)
    pip_size = point * 10 if digits in (3, 5) else point
    bid_px = float(sym_info["bid"])
    ask_px = float(sym_info["ask"])

    entry_price = ask_px if side == "LONG" else bid_px
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
        bid=bid_px,
        ask=ask_px,
        stops_level=int(sym_info.get("stops_level", 0)),
        spread_points=int(sym_info.get("spread", 0)),
        point=point,
    )

    strat_title = get_strategy_display_name(strategy_id)
    log.info(
        "⚡ MEMPROSES SINYAL: [%s] %s %s | Lot: %.2f | Entry Est: %s | SL: %s | TP: %s",
        strat_title, action, symbol, lot, format_price(entry_price, symbol), format_price(sl, symbol), format_price(tp, symbol)
    )

    # 4. Eksekusi order ke broker 
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
            f"• Harga Fill: `{format_price(fill_price, symbol)}`\n"
            f"• Stop Loss: `{format_price(sl, symbol)}`\n"
            f"• Take Profit: `{format_price(tp, symbol)}`\n"
            f"• Tiket MT5: `#{ticket}`\n"
            f"• Equity Akun: `${equity:.2f}`"
        )
        send_telegram_alert(msg)
    else:
        log.error("Gagal mengeksekusi order %s %s: %s", action, symbol, err)
        _send_throttled_warning(
            f"order_err:{strategy_id}:{symbol}",
            f"❌ *SA COPY-TRADE: EKSEKUSI MT5 GAGAL*\n"
            f"• Strategi: `{strat_title}` (`{strategy_id}`)\n"
            f"• Sinyal: {action} `{symbol}` ({lot} lot)\n"
            f"• Alasan Broker: `{err}`",
            cooldown_sec=300.0,
        )


async def execute_close_signal(sig: dict, acc_info: dict) -> None:
    """Mengeksekusi sinyal penutupan posisi dari Strategy Arena."""
    symbol = sig.get("symbol", "").upper()
    action = sig.get("action", "CLOSE_SIGNAL")
    strategy_id = sig.get("strategy")
    side = sig.get("side", "").upper()

    # Filter jika strategi disebutkan dan bukan salah satu target strategi
    if strategy_id and strategy_id not in ACTIVE_STRATEGIES_SET:
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
        strategy_id = db_pos.get("strategy", strategy_id or (ACTIVE_STRATEGIES[0] if ACTIVE_STRATEGIES else "gap_fill"))
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

    strat_title = get_strategy_display_name(strategy_id) if strategy_id else "Copy-Trade"
    log.info("🏁 MENUTUP POSISI: Ticket #%s (%s - %s) karena sinyal %s", ticket_to_close, symbol, strat_title, action)
    ok, close_price, profit, err = mt5.close_position(ticket_to_close, comment=f"SA:{action}")

    deal_info = None
    if not ok:
        deal_info = mt5.get_closed_deal_info(ticket_to_close)

    if ok or deal_info:
        if not ok and deal_info:
            close_price = deal_info.get("price") or (db_pos.get("open_price", 0.0) if db_pos else 0.0)
            profit = deal_info.get("profit", 0.0)
            err = deal_info.get("comment", "")

        close_reason = action
        if "[sl" in str(err).lower() or action == "CLOSE_SL":
            close_reason = "CLOSE_SL"
        elif "[tp" in str(err).lower() or action == "CLOSE_TP":
            close_reason = "CLOSE_TP"

        trade_storage.record_close(
            ticket=ticket_to_close,
            close_price=close_price,
            pnl=profit,
            close_reason=close_reason,
        )
        pnl_symbol = "🟢" if profit >= 0 else "🔴"
        msg = (
            f"{pnl_symbol} *SA COPY-TRADE: POSISI DITUTUP*\n"
            f"• Strategi: `{strat_title}`\n"
            f"• Simbol: `{symbol}`\n"
            f"• Harga Tutup: `{format_price(close_price, symbol)}`\n"
            f"• Realized PnL: *${profit:+.2f}*\n"
            f"• Alasan: `{close_reason}`\n"
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
            cur_strats = get_active_strategies()
            cur_query = ",".join(cur_strats)
            params = {
                "strategy": cur_query,
                "mode": TARGET_MODE,
                "api_key": ARENA_API_KEY,
            }
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(family=socket.AF_INET), timeout=aiohttp.ClientTimeout(total=25)) as session:
                async with session.get(api_url, params=params) as resp:
                    if resp.status != 200:
                        log.warning("Gagal mengambil posisi aktif dari Strategy Arena API (HTTP %d)", resp.status)
                        continue
                    data = await resp.json()

            raw_positions = data.get("positions", [])
            arena_positions = [p for p in raw_positions if p.get("strategy") in ACTIVE_STRATEGIES_SET]
            arena_strat_symbols = {(p["symbol"].upper(), p.get("strategy")) for p in arena_positions}
            arena_symbols = {p["symbol"].upper() for p in arena_positions}

            # 2. Ambil posisi yang masih kita catat OPEN di database & posisi aktif di MT5
            db_open_positions = trade_storage.get_all_open_positions()
            mt5_active = mt5.get_open_positions(MT5_MAGIC_NUMBER)
            active_tickets = {p["ticket"] for p in mt5_active}

            # 3. Cek apakah ada posisi MT5 yang sudah ditutup langsung oleh broker (misal hit hard SL/TP)
            for pos in db_open_positions:
                if pos["ticket"] not in active_tickets:
                    ticket_num = pos["ticket"]
                    sym = pos["symbol"]
                    strat = pos.get("strategy")
                    strat_title = STRATEGY_NAMES_MAP.get(strat, strat or TARGET_STRATEGY_NAME)
                    log.info("🔍 REKONSILIASI: Ticket #%s (%s - %s) telah ditutup di MT5 (Hit SL/TP).", ticket_num, sym, strat)

                    deal = mt5.get_closed_deal_info(ticket_num)
                    close_price = deal.get("price") if deal else pos.get("open_price", 0.0)
                    profit = deal.get("profit") if deal else 0.0
                    deal_cm = deal.get("comment", "") if deal else ""

                    close_reason = "BROKER_SL_TP"
                    if "[sl" in deal_cm.lower():
                        close_reason = "CLOSE_SL"
                    elif "[tp" in deal_cm.lower():
                        close_reason = "CLOSE_TP"

                    trade_storage.record_close(
                        ticket=ticket_num,
                        close_price=close_price,
                        pnl=profit,
                        close_reason=close_reason,
                    )

                    pnl_symbol = "🟢" if profit >= 0 else "🔴"
                    msg = (
                        f"{pnl_symbol} *SA COPY-TRADE: POSISI DITUTUP (BROKER SL/TP)*\n"
                        f"• Strategi: `{strat_title}`\n"
                        f"• Simbol: `{sym}`\n"
                        f"• Harga Tutup: `{format_price(close_price, sym)}`\n"
                        f"• Realized PnL: *${profit:+.2f}*\n"
                        f"• Alasan: `{close_reason}`\n"
                        f"• Tiket: `#{ticket_num}`"
                    )
                    send_telegram_alert(msg)

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
                strat = p.get("strategy", cur_strats[0] if cur_strats else "gap_fill")
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
                    # Cegah loop buka-ulang jika posisi Arena yang sama (opened_at) sudah pernah disalin & terkena SL/TP/Close di MT5
                    side_str = p.get("side", "LONG")
                    if open_sec > 0 and trade_storage.has_closed_for_arena_trade(strat, sym, side_str, open_sec):
                        skip_key = (strat.lower(), sym, side_str.upper(), int(open_sec))
                        if skip_key not in _RECONCILE_SKIP_LOGGED:
                            _RECONCILE_SKIP_LOGGED.add(skip_key)
                            log.info(
                                "⏩ REKONSILIASI SKIP: '%s' (%s - %s) sudah pernah dieksekusi & ditutup di MT5 (hit SL/TP), tidak dibuka ulang.",
                                sym, side_str, strat
                            )
                        continue

                    log.info(
                        "🔍 REKONSILIASI: Menemukan posisi aktif '%s' (%s - %s, usia: %.0fs) di Strategy Arena. Menyinkronkan ke MT5...",
                        sym, side_str, strat, age_sec
                    )
                    sig_to_open = {
                        "symbol": sym,
                        "side": p.get("side", "LONG"),
                        "action": f"OPEN_{p.get('side', 'LONG')}",
                        "price": p.get("current_price"),
                        "stop_loss": p.get("stop_loss"),
                        "take_profit": p.get("take_profit"),
                        "strategy": strat,
                        "lot": p.get("lot"),
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
            log.warning("Exception dalam watchdog rekonsiliasi: %r", exc)


async def websocket_signal_listener() -> None:
    """Menghubungkan ke WebSocket Strategy Arena dan menerima broadcast sinyal instan."""
    global active_ws_session, ws_reconnect_needed
    backoff = 2

    while running:
        cur_strats = get_active_strategies()
        cur_query = ",".join(cur_strats)
        names = " & ".join(get_strategy_display_name(s) for s in cur_strats)
        ws_endpoint = f"{ARENA_WS_URL}?strategy={cur_query}&mode={TARGET_MODE}&api_key={ARENA_API_KEY}"
        try:
            log.info("Menghubungkan ke WebSocket sinyal di %s...", ws_endpoint)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_endpoint, heartbeat=20.0) as ws:
                    active_ws_session = ws
                    ws_reconnect_needed = False
                    log.info("✓ Terhubung ke WebSocket Strategy Arena! Mendengarkan: %s (%s)...", names, cur_query)
                    backoff = 2

                    async for msg in ws:
                        if not running or ws_reconnect_needed:
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
                                    if sig_strat and sig_strat not in ACTIVE_STRATEGIES_SET:
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
    init_strats = load_initial_strategies()
    set_active_strategies(init_strats)
    await refresh_arena_strategies_cache()

    strats_display = ", ".join(f"{get_strategy_display_name(s)} ({s})" for s in init_strats)
    log.info("=" * 70)
    log.info("STRATEGY ARENA COPY-TRADING ENGINE v1.2 (Interactive Telegram Control)")
    log.info("Target Strategi : %s", strats_display)
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


