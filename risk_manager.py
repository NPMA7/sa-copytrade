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
        """Mengecek apakah equity akun menyentuh atau berada di bawah ambang batas stop ($10)."""
        return equity <= MIN_EQUITY_USD

    @staticmethod
    def can_open_position(
        symbol: str,
        equity: float,
        balance: float,
        current_open_positions: list[dict],
    ) -> Tuple[bool, str]:
        """Pemeriksaan pra-eksekusi (pre-trade checks) sebelum mengirim order ke broker Headway."""
        # 1. Periksa batas minimum equity (Emergency Floor $10 USD)
        if equity <= MIN_EQUITY_USD:
            msg = f"Equity akun (${equity:.2f}) telah mencapai atau di bawah batas minimum stop (${MIN_EQUITY_USD:.2f}). Order ditolak!"
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
        for pos in current_open_positions:
            if pos.get("symbol") == symbol:
                msg = f"Simbol {symbol} sudah memiliki posisi terbuka. Menghindari duplikasi eksposur."
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

