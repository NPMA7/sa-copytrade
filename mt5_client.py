"""Klien Komunikasi MetaTrader 5 via RPyC Bridge."""

from __future__ import annotations

import time
from typing import Optional, Tuple
import rpyc
from config import (
    MT5_HOST,
    MT5_PORT,
    MT5_LOGIN,
    MT5_PASSWORD,
    MT5_SERVER,
    MT5_IP_SERVER,
    MT5_MAGIC_NUMBER,
    MT5_ORDER_COMMENT,
    log,
)


class MT5Client:
    """Wrapper profesional MetaTrader 5 untuk eksekusi order pada akun riil Headway."""

    def __init__(self, host: str = MT5_HOST, port: int = MT5_PORT):
        self.host = host
        self.port = port
        self.conn: Optional[rpyc.Connection] = None
        self._connected = False
        self._symbol_cache: dict[str, str] = {}

    def connect(self) -> bool:
        """Membangun koneksi RPyC ke MT5 terminal dan menginisialisasi modul."""
        try:
            log.info("Menghubungi terminal MT5 di %s:%s...", self.host, self.port)
            self.conn = rpyc.classic.connect(self.host, self.port)
            self.conn._config["sync_request_timeout"] = 60

            # Remote setup MT5 di Windows/Wine
            self.conn.execute("""
import sys
if 'C:\\\\mt5libs' not in sys.path:
    sys.path.append('C:\\\\mt5libs')
import MetaTrader5 as mt5

def _client_init(login, password, server, ip_server):
    ok = mt5.initialize(path="C:\\\\MT5\\\\terminal64.exe", portable=True)
    if not ok:
        ok = mt5.initialize()
    if ok and login and password:
        # Coba login dengan server name
        logged_in = mt5.login(login, password=password, server=server)
        if not logged_in and ip_server:
            # Fallback ke IP server langsung
            srv = ip_server if ":" in ip_server else ip_server + ":443"
            logged_in = mt5.login(login, password=password, server=srv)
    return ok
""")
            init_ok = self.conn.eval(
                f"_client_init({MT5_LOGIN}, {repr(MT5_PASSWORD)}, {repr(MT5_SERVER)}, {repr(MT5_IP_SERVER)})"
            )
            if not init_ok:
                err = self.conn.eval("mt5.last_error()")
                log.error("Gagal inisialisasi MT5: %s", err)
                self._connected = False
                return False

            self._connected = True
            log.info("✓ Terhubung ke MetaTrader 5 terminal!")
            return True
        except Exception as exc:
            log.warning("Koneksi ke MT5 terminal gagal (%s:%s): %s", self.host, self.port, exc)
            self.conn = None
            self._connected = False
            return False

    def is_connected(self) -> bool:
        """Memeriksa status koneksi aktif."""
        if not self._connected or self.conn is None:
            return False
        try:
            return not self.conn.closed
        except Exception:
            return False

    def ensure_connected(self) -> bool:
        """Memastikan koneksi aktif atau mencoba menyambung kembali."""
        if self.is_connected():
            return True
        return self.connect()

    def get_account_info(self) -> Optional[dict]:
        """Mengambil data riil saldo, equity, margin, dan server akun."""
        if not self.ensure_connected():
            return None
        try:
            self.conn.execute("""
def _get_acc_dict():
    info = mt5.account_info()
    if not info:
        return None
    return {
        "login": int(info.login),
        "trade_mode": int(info.trade_mode),
        "name": str(info.name),
        "server": str(info.server),
        "currency": str(info.currency),
        "balance": float(info.balance),
        "equity": float(info.equity),
        "margin": float(info.margin),
        "margin_free": float(info.margin_free),
        "margin_level": float(info.margin_level or 0.0),
        "profit": float(info.profit),
    }
""")
            acc = self.conn.eval("_get_acc_dict()")
            return acc
        except Exception as exc:
            log.warning("Gagal mengambil info akun: %s", exc)
            return None

    def resolve_broker_symbol(self, arena_symbol: str) -> Optional[str]:
        """Menemukan nama simbol yang sesuai di broker (contoh: EURUSD atau EURUSD.h)."""
        if arena_symbol in self._symbol_cache:
            return self._symbol_cache[arena_symbol]

        if not self.ensure_connected():
            return arena_symbol

        try:
            self.conn.execute(f"""
def _find_symbol(target):
    # Cek simbol langsung
    t = target.upper()
    info = mt5.symbol_info(t)
    if info:
        mt5.symbol_select(t, True)
        return t
    # Cari variasi nama pair dengan prefix/suffix broker
    all_syms = mt5.symbols_get()
    if all_syms:
        for s in all_syms:
            if t in s.name.upper():
                mt5.symbol_select(s.name, True)
                return s.name
    return t
""")
            resolved = self.conn.eval(f"_find_symbol({repr(arena_symbol)})")
            if resolved:
                self._symbol_cache[arena_symbol] = resolved
                return resolved
        except Exception as exc:
            log.warning("Gagal resolve simbol %s: %s", arena_symbol, exc)

        return arena_symbol

    def get_symbol_info(self, symbol: str) -> Optional[dict]:
        """Mengambil data spesifikasi simbol (digits, spread, bid, ask, point)."""
        if not self.ensure_connected():
            return None
        try:
            broker_sym = self.resolve_broker_symbol(symbol)
            self.conn.execute(f"""
def _get_sym_info(s):
    mt5.symbol_select(s, True)
    info = mt5.symbol_info(s)
    tick = mt5.symbol_info_tick(s)
    if not info:
        return None
    return {{
        "name": info.name,
        "digits": int(info.digits),
        "point": float(info.point),
        "spread": int(info.spread),
        "filling_mode": int(getattr(info, 'filling_mode', 2)),
        "bid": float(tick.bid) if tick else 0.0,
        "ask": float(tick.ask) if tick else 0.0,
    }}
""")
            return self.conn.eval(f"_get_sym_info({repr(broker_sym)})")
        except Exception as exc:
            log.warning("Gagal mengambil info simbol %s: %s", symbol, exc)
            return None

    def get_open_positions(self, magic: int = MT5_MAGIC_NUMBER) -> list[dict]:
        """Mengambil daftar seluruh posisi terbuka bot di terminal MT5."""
        if not self.ensure_connected():
            return []
        try:
            self.conn.execute(f"""
def _get_positions(m):
    positions = mt5.positions_get(magic=m) if m else mt5.positions_get()
    if positions is None:
        return []
    res = []
    for p in positions:
        res.append({{
            "ticket": int(p.ticket),
            "symbol": str(p.symbol),
            "type": int(p.type),  # 0: BUY, 1: SELL
            "side": "LONG" if p.type == 0 else "SHORT",
            "volume": float(p.volume),
            "price_open": float(p.price_open),
            "price_current": float(p.price_current),
            "sl": float(p.sl),
            "tp": float(p.tp),
            "profit": float(p.profit),
            "swap": float(p.swap),
            "magic": int(p.magic),
            "comment": str(p.comment),
            "time": int(p.time),
        }})
    return res
""")
            positions = self.conn.eval(f"_get_positions({magic})")
            return positions or []
        except Exception as exc:
            log.warning("Gagal mengambil posisi terbuka MT5: %s", exc)
            return []

    def open_position(
        self,
        symbol: str,
        side: str,
        lot: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        comment: str = MT5_ORDER_COMMENT,
    ) -> Tuple[bool, int, float, str]:
        """Membuka posisi Market Order (BUY/SELL) dengan Stop Loss dan Take Profit riil."""
        if not self.ensure_connected():
            return False, 0, 0.0, "Koneksi MT5 tidak aktif"

        broker_sym = self.resolve_broker_symbol(symbol)
        is_buy = side.upper() in ("BUY", "LONG")

        sym_info = self.get_symbol_info(broker_sym)
        if not sym_info or sym_info["bid"] <= 0 or sym_info["ask"] <= 0:
            return False, 0, 0.0, f"Gagal mendapatkan harga live untuk simbol {broker_sym}"

        price = sym_info["ask"] if is_buy else sym_info["bid"]
        order_type_str = "mt5.ORDER_TYPE_BUY" if is_buy else "mt5.ORDER_TYPE_SELL"

        # Kirim order dengan fallback filling type
        try:
            self.conn.execute(f"""
def _send_open_order(sym, order_type, vol, price, sl, tp, magic, comment):
    import MetaTrader5 as mt5

    # Coba berbagai mode filling yang didukung broker (IOC, RETURN, FOK)
    filling_modes = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN, mt5.ORDER_FILLING_FOK]
    last_err = None

    for filling in filling_modes:
        req = {{
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": sym,
            "volume": float(vol),
            "type": order_type,
            "price": float(price),
            "sl": float(sl) if sl else 0.0,
            "tp": float(tp) if tp else 0.0,
            "deviation": 20,
            "magic": int(magic),
            "comment": str(comment),
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }}
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            return {{"ok": True, "ticket": int(res.order), "price": float(res.price), "comment": str(res.comment)}}
        elif res:
            last_err = f"retcode={{res.retcode}}: {{res.comment}}"
            # Jika bukan masalah filling type, jangan lanjut loop
            if res.retcode != 10030:
                break
        else:
            last_err = str(mt5.last_error())

    return {{"ok": False, "error": last_err or "Unknown error"}}
""")
            res = self.conn.eval(
                f"_send_open_order({repr(broker_sym)}, {order_type_str}, {lot}, {price}, {sl or 0.0}, {tp or 0.0}, {MT5_MAGIC_NUMBER}, {repr(comment)})"
            )
            if res.get("ok"):
                ticket = res["ticket"]
                fill_price = res.get("price") or price
                log.info(
                    "🚀 EKSEKUSI MT5 SUKSES! Ticket #%s | %s %s %.2f lot @ %.5f | SL: %.5f | TP: %.5f",
                    ticket, side, broker_sym, lot, fill_price, sl or 0.0, tp or 0.0
                )
                return True, ticket, fill_price, "SUCCESS"
            else:
                err_msg = res.get("error", "Eksekusi order ditolak broker")
                log.error("❌ GAGAL BUKA POSISI MT5: %s", err_msg)
                return False, 0, 0.0, err_msg
        except Exception as exc:
            log.error("Exception saat open_position: %s", exc)
            return False, 0, 0.0, str(exc)

    def close_position(
        self,
        ticket: int,
        comment: str = "SA:close",
    ) -> Tuple[bool, float, float, str]:
        """Menutup posisi aktif di MT5 pada harga market."""
        if not self.ensure_connected():
            return False, 0.0, 0.0, "Koneksi MT5 tidak aktif"

        try:
            self.conn.execute(f"""
def _send_close_order(t, comment):
    import MetaTrader5 as mt5
    pos = mt5.positions_get(ticket=t)
    if not pos or len(pos) == 0:
        return {{"ok": False, "error": f"Ticket {{t}} tidak ditemukan atau sudah tertutup"}}

    p = pos[0]
    is_buy = (p.type == mt5.ORDER_TYPE_BUY)
    opp_type = mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY
    
    tick = mt5.symbol_info_tick(p.symbol)
    price = tick.bid if is_buy else tick.ask

    filling_modes = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN, mt5.ORDER_FILLING_FOK]
    last_err = None

    for filling in filling_modes:
        req = {{
            "action": mt5.TRADE_ACTION_DEAL,
            "position": int(p.ticket),
            "symbol": str(p.symbol),
            "volume": float(p.volume),
            "type": opp_type,
            "price": float(price),
            "deviation": 20,
            "magic": int(p.magic),
            "comment": str(comment),
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }}
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            return {{
                "ok": True,
                "price": float(res.price or price),
                "profit": float(p.profit),
            }}
        elif res:
            last_err = f"retcode={{res.retcode}}: {{res.comment}}"
            if res.retcode != 10030:
                break
        else:
            last_err = str(mt5.last_error())

    return {{"ok": False, "error": last_err or "Unknown error"}}
""")
            res = self.conn.eval(f"_send_close_order({ticket}, {repr(comment)})")
            if res.get("ok"):
                close_price = res.get("price", 0.0)
                profit = res.get("profit", 0.0)
                log.info("🏁 POSISI DITUTUP MT5! Ticket #%s @ %.5f | Profit: $%.2f", ticket, close_price, profit)
                return True, close_price, profit, "SUCCESS"
            else:
                err_msg = res.get("error", "Gagal menutup posisi di MT5")
                log.warning("Gagal close ticket #%s: %s", ticket, err_msg)
                return False, 0.0, 0.0, err_msg
        except Exception as exc:
            log.error("Exception saat close_position: %s", exc)
            return False, 0.0, 0.0, str(exc)
