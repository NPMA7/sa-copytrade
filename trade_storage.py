"""Database Penyimpanan & Tracking Posisi Copy-Trade (SQLite)."""

from __future__ import annotations

import sqlite3
import time
from typing import Optional
from config import DATA_DIR, log

DB_PATH = DATA_DIR / "copytrade.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Inisialisasi tabel SQLite untuk melacak posisi dan riwayat eksekusi."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket INTEGER UNIQUE,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                lot REAL NOT NULL,
                open_price REAL NOT NULL,
                sl REAL,
                tp REAL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                open_time REAL NOT NULL,
                close_time REAL,
                close_price REAL,
                pnl REAL DEFAULT 0.0,
                close_reason TEXT
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_open_time ON trades(open_time);
        """)
        conn.commit()


def record_open(
    ticket: int,
    strategy: str,
    symbol: str,
    side: str,
    lot: float,
    open_price: float,
    sl: Optional[float] = None,
    tp: Optional[float] = None,
) -> None:
    """Menyimpan posisi baru yang berhasil dieksekusi di MT5."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO trades (
                ticket, strategy, symbol, side, lot, open_price, sl, tp, status, open_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
            """,
            (ticket, strategy, symbol, side, lot, open_price, sl, tp, time.time()),
        )
        conn.commit()
    log.info("💾 Posisi tercatat di database: Ticket #%s %s %s @ %.5f", ticket, side, symbol, open_price)


def record_close(
    ticket: int,
    close_price: float,
    pnl: float,
    close_reason: str = "SIGNAL",
) -> None:
    """Memperbarui posisi yang ditutup."""
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE trades
            SET status = 'CLOSED', close_time = ?, close_price = ?, pnl = ?, close_reason = ?
            WHERE ticket = ?
            """,
            (time.time(), close_price, pnl, close_reason, ticket),
        )
        conn.commit()
    log.info("💾 Posisi ditutup di database: Ticket #%s | PnL: $%.2f | Alasan: %s", ticket, pnl, close_reason)


def get_open_position_by_symbol(symbol: str) -> Optional[dict]:
    """Mencari posisi terbuka berdasarkan simbol pair."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM trades WHERE symbol = ? AND status = 'OPEN' ORDER BY id DESC LIMIT 1",
            (symbol,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_open_position_by_strategy_symbol(strategy: str, symbol: str) -> Optional[dict]:
    """Mencari posisi terbuka berdasarkan strategi dan simbol pair."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM trades WHERE strategy = ? AND symbol = ? AND status = 'OPEN' ORDER BY id DESC LIMIT 1",
            (strategy, symbol),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_open_position_by_ticket(ticket: int) -> Optional[dict]:
    """Mencari posisi terbuka berdasarkan nomor tiket MT5."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM trades WHERE ticket = ?", (ticket,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_all_open_positions() -> list[dict]:
    """Mengambil semua posisi terbuka yang aktif."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM trades WHERE status = 'OPEN' ORDER BY open_time ASC")
        return [dict(r) for r in cur.fetchall()]


def get_daily_realized_pnl() -> float:
    """Menghitung total profit/loss realisasi hari ini (UTC 00:00 - sekarang)."""
    # Menghitung awal hari (midnight UTC)
    now = time.time()
    day_start = now - (now % 86400)
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(pnl), 0.0) FROM trades WHERE status = 'CLOSED' AND close_time >= ?",
            (day_start,),
        )
        val = cur.fetchone()[0]
        return float(val or 0.0)


def get_trade_history(limit: int = 20) -> list[dict]:
    """Mengambil riwayat transaksi terbaru."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM trades WHERE status = 'CLOSED' ORDER BY close_time DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

