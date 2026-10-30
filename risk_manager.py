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
    def calculate_lot(equity: float, signal_lot: Optional[float] = None) -> float:
        """Menghitung lot size untuk eksekusi order MT5 berdasarkan mode sizing.

        - "PROPORTIONAL": Mengikuti kalkulasi lot dari strategi di Strategy Arena (1:1),
          dibatasi dengan aman oleh BASE_LOT (sebagai batas minimum) dan MAX_LOT (sebagai batas maksimum).
        - "FIXED": Selalu menggunakan BASE_LOT secara konstan (default 0.01 lot).
        - "EQUITY_SCALE": Skala ukuran lot proporsional terhadap rasio equity akun / $1,000.
        """
        strat_lot = None
        if signal_lot is not None:
            try:
                val = float(signal_lot)
                if val > 0:
                    strat_lot = val
            except (ValueError, TypeError):
                strat_lot = None

        if LOT_SIZING_MODE == "FIXED":
            lot = BASE_LOT
        elif LOT_SIZING_MODE == "EQUITY_SCALE":
            target = strat_lot if strat_lot is not None else BASE_LOT
            scale = max(0.1, equity / 1000.0)
            lot = round(target * scale, 2)
        else:
            # Mode "PROPORTIONAL" (Default & Rekomendasi):
            # Salin ukuran lot strategi (misal XTIUSD 0.05 lot, XAUUSD 0.01 lot, EURUSD 0.03 lot)
            lot = strat_lot if strat_lot is not None else BASE_LOT

        # Clamp ketat ke batas aman [BASE_LOT, MAX_LOT]
        min_allowed = min(BASE_LOT, MAX_LOT)
        max_allowed = max(BASE_LOT, MAX_LOT)
        lot = max(min_allowed, min(lot, max_allowed))
        return round(lot, 2)

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
        """Pemeriksaan pra-eksekusi (pre-trade checks) sebelum mengirim order ke broker."""
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

