"""Manajemen Resiko & Alokasi Modal Riil ($1,000 USD)."""

from __future__ import annotations

from typing import Tuple, Optional
from config import (
    BASE_LOT,
    MAX_LOT,
    LOT_SIZING_MODE,
    MAX_OPEN_POSITIONS,
    MAX_DAILY_LOSS_USD,
    MIN_EQUITY_USD,
    ENFORCE_SL,
    DEFAULT_SL_PIPS,
    DEFAULT_TP_PIPS,
    ALLOW_HEDGING,
    log,
)
import trade_storage


class RiskManager:
    """Mengawal eksekusi transaksi pada akun riil $1,000 USD agar tahan banting dan terproteksi."""

    @staticmethod
    def calculate_lot(equity: float, sl_points: float = 0.0) -> float:
        """Menghitung lot size yang aman untuk modal $1,000 USD."""
        if LOT_SIZING_MODE == "PROPORTIONAL":
            # Skala lot proporsional terhadap modal awal $1,000
            scale = max(0.5, equity / 1000.0)
            lot = round(BASE_LOT * scale, 2)
        else:
            # Mode "FIXED": Selalu gunakan 0.01 micro-lot (rekomendasi untuk $1k)
            lot = BASE_LOT

        # Clamp ketat ke batas aman
        lot = max(0.01, min(lot, MAX_LOT))
        return lot

    @staticmethod
    def is_equity_halted(equity: float) -> bool:
        """Mengecek apakah equity akun menyentuh atau berada di bawah ambang batas stop."""
        if MIN_EQUITY_USD <= 0:
            return equity <= 0.0
        return equity <= MIN_EQUITY_USD

    @staticmethod
    def can_open_position(
        symbol: str,
        equity: float,
        balance: float,
        current_open_positions: list[dict],
        strategy: str = "",
    ) -> Tuple[bool, str]:
        """Pemeriksaan pra-eksekusi (pre-trade checks) sebelum mengirim order ke broker Headway."""
        # 1. Periksa batas minimum equity (Emergency Floor)
        if MIN_EQUITY_USD > 0 and equity <= MIN_EQUITY_USD:
            msg = f"Equity akun (${equity:.2f}) telah mencapai atau di bawah batas minimum stop (${MIN_EQUITY_USD:.2f}). Order ditolak!"
            log.warning("⚠️ RISK GUARD: %s", msg)
            return False, msg
        elif MIN_EQUITY_USD <= 0 and equity <= 0.0:
            msg = f"Equity akun (${equity:.2f}) telah habis (<= 0). Order ditolak!"
            log.warning("⚠️ RISK GUARD: %s", msg)
            return False, msg

        # 2. Periksa Circuit Breaker: Kerugian harian maksimum ($50 = 5%)
        daily_loss = trade_storage.get_daily_realized_pnl()
        if daily_loss <= -abs(MAX_DAILY_LOSS_USD):
            msg = f"Circuit Breaker aktif! Kerugian harian (-${abs(daily_loss):.2f}) telah menyentuh batas maksimum (-${MAX_DAILY_LOSS_USD:.2f})."
            log.warning("⚠️ RISK GUARD: %s", msg)
            return False, msg

        # 3. Periksa jumlah posisi terbuka maksimum
        if len(current_open_positions) >= MAX_OPEN_POSITIONS:
            msg = f"Kapasitas posisi maksimum ({MAX_OPEN_POSITIONS}) telah penuh. Tidak membuka posisi baru."
            log.info("ℹ️ RISK GUARD: %s", msg)
            return False, msg

        # 4. Periksa apakah sudah ada posisi aktif untuk simbol ini
        clean_sym = symbol.upper()
        if ALLOW_HEDGING:
            # Multi-strategy hedging diizinkan:
            # Strategi berbeda boleh membuka posisi di simbol yang sama (misal ROC SHORT & Gap Fill LONG)
            # Tetapi strategi YANG SAMA dilarang membuka duplikat di simbol yang sama
            for pos in current_open_positions:
                pos_sym = pos.get("symbol", "").upper()
                pos_comment = pos.get("comment", "")
                if pos_sym == clean_sym:
                    if strategy and (pos_comment == f"SA:{strategy}" or pos.get("strategy") == strategy):
                        msg = f"Strategi '{strategy}' sudah memiliki posisi aktif di {symbol}. Menghindari duplikasi internal strategi."
                        log.info("ℹ️ RISK GUARD: %s", msg)
                        return False, msg
        else:
            # Mode Strict: 1 simbol hanya boleh ada 1 posisi terlepas dari strateginya
            for pos in current_open_positions:
                if pos.get("symbol", "").upper() == clean_sym:
                    msg = f"Simbol {symbol} sudah memiliki posisi terbuka di MT5 (ALLOW_HEDGING=false). Menghindari duplikasi eksposur."
                    log.info("ℹ️ RISK GUARD: %s", msg)
                    return False, msg

        return True, "OK"

    @staticmethod
    def validate_or_calculate_stops(
        symbol: str,
        side: str,
        entry_price: float,
        sl: Optional[float],
        tp: Optional[float],
        digits: int = 5,
        pip_size: float = 0.0001,
    ) -> Tuple[float, float]:
        """Memastikan Stop Loss dan Take Profit valid dan terpasang pada order broker."""
        final_sl = float(sl or 0.0)
        final_tp = float(tp or 0.0)

        # Fallback perhitungan jarak pip
        sl_distance = DEFAULT_SL_PIPS * pip_size
        tp_distance = DEFAULT_TP_PIPS * pip_size

        is_buy = side.upper() in ("BUY", "LONG")

        if is_buy:
            if final_sl <= 0.0 or final_sl >= entry_price:
                final_sl = round(entry_price - sl_distance, digits)
            if final_tp <= 0.0 or final_tp <= entry_price:
                final_tp = round(entry_price + tp_distance, digits)
        else:  # Sell
            if final_sl <= 0.0 or final_sl <= entry_price:
                final_sl = round(entry_price + sl_distance, digits)
            if final_tp <= 0.0 or final_tp >= entry_price:
                final_tp = round(entry_price - tp_distance, digits)

        return round(final_sl, digits), round(final_tp, digits)

