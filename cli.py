#!/usr/bin/env python3
"""CLI Management Utility untuk SA Copy-Trade (Real)."""

import argparse
import sys
import requests
from tabulate import tabulate
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from config import (
    ARENA_BASE_URL,
    ARENA_API_KEY,
    TARGET_STRATEGIES,
    TARGET_STRATEGIES_QUERY,
    TARGET_STRATEGY,
    TARGET_STRATEGY_NAME,
    TARGET_MODE,
    MT5_MAGIC_NUMBER,
    MT5_HOST,
    MT5_PORT,
    MT5_LOGIN,
    MT5_SERVER,
    MIN_EQUITY_USD,
)
from mt5_client import MT5Client
import trade_storage

console = Console()


def show_status():
    """Menampilkan ringkasan status akun, koneksi broker, dan posisi aktif."""
    mt5 = MT5Client()
    connected = mt5.connect()

    table = Table(title="📊 Status Copy-Trade Engine (Real)", show_header=True, header_style="bold cyan")
    table.add_column("Parameter", style="dim", width=25)
    table.add_column("Nilai / Keterangan", style="bold")

    table.add_row("Target Strategi", f"{TARGET_STRATEGY_NAME} (`{', '.join(TARGET_STRATEGIES)}`)")
    table.add_row("Mode Pasar", TARGET_MODE.upper())
    stop_text = f"${MIN_EQUITY_USD:,.2f}" if MIN_EQUITY_USD > 0 else "$0.00 (Trading sampai saldo habis)"
    table.add_row("Emergency Equity Stop", stop_text)
    table.add_row("Login MT5", str(MT5_LOGIN))
    table.add_row("Server Broker", MT5_SERVER)
    table.add_row("MT5 Terminal Host", f"{MT5_HOST}:{MT5_PORT}")
    table.add_row("Status MT5 Bridge", "[green]TERHUBUNG ✓[/green]" if connected else "[red]TERPUTUS ✗[/red]")

    if connected:
        acc = mt5.get_account_info()
        if acc:
            table.add_row("Saldo (Balance)", f"${acc['balance']:,.2f} {acc['currency']}")
            table.add_row("Equity", f"${acc['equity']:,.2f}")
            table.add_row("Margin Terpakai", f"${acc['margin']:,.2f}")
            table.add_row("Margin Bebas", f"${acc['margin_free']:,.2f}")
            table.add_row("Floating Profit", f"${acc['profit']:+,.2f}")

    daily_pnl = trade_storage.get_daily_realized_pnl()
    pnl_color = "green" if daily_pnl >= 0 else "red"
    table.add_row("Realized PnL Hari Ini", f"[{pnl_color}]${daily_pnl:+,.2f}[/{pnl_color}]")

    console.print(table)

    # Tampilkan Posisi Terbuka
    open_pos = mt5.get_open_positions(MT5_MAGIC_NUMBER) if connected else []
    if open_pos:
        pos_table = Table(title="🟢 Posisi Terbuka Aktif (MetaTrader 5)", show_header=True, header_style="bold green")
        pos_table.add_column("Ticket")
        pos_table.add_column("Simbol")
        pos_table.add_column("Arah")
        pos_table.add_column("Lot")
        pos_table.add_column("Harga Buka")
        pos_table.add_column("Harga Saat Ini")
        pos_table.add_column("Stop Loss")
        pos_table.add_column("Take Profit")
        pos_table.add_column("Profit ($)")

        for p in open_pos:
            prof_color = "green" if p['profit'] >= 0 else "red"
            pos_table.add_row(
                str(p['ticket']),
                p['symbol'],
                p['side'],
                f"{p['volume']:.2f}",
                f"{p['price_open']:.5f}",
                f"{p['price_current']:.5f}",
                f"{p['sl']:.5f}" if p['sl'] else "-",
                f"{p['tp']:.5f}" if p['tp'] else "-",
                f"[{prof_color}]${p['profit']:+,.2f}[/{prof_color}]"
            )
        console.print(pos_table)
    else:
        console.print("[dim]Tidak ada posisi terbuka saat ini.[/dim]\n")


def test_signal():
    """Menguji koneksi feed sinyal ke Strategy Arena."""
    console.print(f"Menguji koneksi ke Strategy Arena ({ARENA_BASE_URL})...")
    url = f"{ARENA_BASE_URL}/api/v1/signals/positions"
    params = {
        "strategy": TARGET_STRATEGIES_QUERY,
        "mode": TARGET_MODE,
        "api_key": ARENA_API_KEY,
    }
    try:
        r = requests.get(url, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json()
            console.print("[green]✓ Sukses terhubung ke Strategy Arena Signal API![/green]")
            console.print(f"• Total posisi aktif target strategi ({TARGET_STRATEGY_NAME}): {data.get('count', 0)}")
            positions = data.get("positions", [])
            for p in positions:
                strat_label = p.get('strategy_name') or p.get('strategy', 'Unknown')
                console.print(
                    f"  - [{strat_label}] {p['symbol']} | {p['action']} | Entry: {p['entry_price']} | SL: {p.get('stop_loss')} | TP: {p.get('take_profit')} | Lot: {p.get('lot')}"
                )
        else:
            console.print(f"[red]Gagal mengambil sinyal: HTTP {r.status_code} - {r.text}[/red]")
    except Exception as exc:
        console.print(f"[red]Error menghubungi Strategy Arena: {exc}[/red]")


def show_history(limit=15):
    """Menampilkan riwayat order copytrade yang sudah ditutup."""
    trades = trade_storage.get_trade_history(limit)
    if not trades:
        console.print("[dim]Belum ada riwayat transaksi yang tersimpan.[/dim]")
        return

    table = Table(title=f"📜 Riwayat {len(trades)} Transaksi Terakhir", show_header=True, header_style="bold yellow")
    table.add_column("Ticket")
    table.add_column("Simbol")
    table.add_column("Arah")
    table.add_column("Lot")
    table.add_column("Harga Buka")
    table.add_column("Harga Tutup")
    table.add_column("PnL ($)")
    table.add_column("Alasan Tutup")

    for t in trades:
        prof_color = "green" if t['pnl'] >= 0 else "red"
        table.add_row(
            str(t['ticket']),
            t['symbol'],
            t['side'],
            f"{t['lot']:.2f}",
            f"{t['open_price']:.5f}",
            f"{t['close_price']:.5f}" if t['close_price'] else "-",
            f"[{prof_color}]${t['pnl']:+,.2f}[/{prof_color}]",
            t['close_reason'] or "SIGNAL"
        )
    console.print(table)


def close_all():
    """Tutup darurat semua posisi yang dibuka oleh bot di MT5."""
    mt5 = MT5Client()
    if not mt5.connect():
        console.print("[red]Gagal terhubung ke MT5 untuk menutup posisi.[/red]")
        return

    open_pos = mt5.get_open_positions(MT5_MAGIC_NUMBER)
    if not open_pos:
        console.print("[yellow]Tidak ada posisi bot yang terbuka di MT5.[/yellow]")
        return

    console.print(f"[bold red]Menutup {len(open_pos)} posisi secara darurat...[/bold red]")
    for p in open_pos:
        ok, price, prof, err = mt5.close_position(p['ticket'], comment="SA:EMERGENCY_CLOSE")
        if ok:
            console.print(f"✓ Ditutup Ticket #{p['ticket']} ({p['symbol']}) @ {price:.5f} | PnL: ${prof:+.2f}")
            trade_storage.record_close(p['ticket'], price, prof, "EMERGENCY_CLOSE")
        else:
            console.print(f"✗ Gagal menutup Ticket #{p['ticket']}: {err}")


def main():
    parser = argparse.ArgumentParser(description="Strategy Arena Copy-Trading CLI")
    parser.add_argument("command", choices=["status", "positions", "test-signal", "history", "close-all"], help="Perintah yang dijalankan")
    parser.add_argument("--limit", type=int, default=15, help="Jumlah transaksi pada history")

    args = parser.parse_args()

    if args.command == "status":
        show_status()
    elif args.command == "positions":
        show_status()
    elif args.command == "test-signal":
        test_signal()
    elif args.command == "history":
        show_history(args.limit)
    elif args.command == "close-all":
        close_all()


if __name__ == "__main__":
    main()

