"""Format helper untuk copytrade bot."""
from __future__ import annotations
from typing import Any


def format_price(price: Any, symbol: str = "") -> str:
    """Format angka harga sesuai standar simbol.
    XAUUSD dan pasangan emas/logam/minyak/JPY menggunakan 3 desimal, forex lainnya 5 desimal.
    """
    if price is None or price == "" or price == "-":
        return "-"
    try:
        p = float(price)
    except (ValueError, TypeError):
        return str(price)

    sym = (symbol or "").upper()
    if any(k in sym for k in ["JPY", "XAU", "GOLD", "XAG", "SILVER", "XTI", "OIL", "USOIL"]):
        return f"{p:.3f}"
    return f"{p:.5f}"
