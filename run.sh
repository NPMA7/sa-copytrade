#!/usr/bin/env bash
# ==============================================================================
# STRATEGY ARENA COPY-TRADE ENGINE CONTROLLER
# Headway Real Account ($1,000 USD) - Target: Gap Fill Reversal
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Helper Functions
print_banner() {
    echo -e "${CYAN}${BOLD}"
    echo "========================================================================"
    echo "       STRATEGY ARENA COPY-TRADE ENGINE — HEADWAY REAL ($1,000)         "
    echo "========================================================================"
    echo -e "${NC}"
}

check_env() {
    if [ ! -f .env ]; then
        echo -e "${YELLOW}File .env belum ditemukan! Membuat .env dari template .env.example...${NC}"
        cp .env.example .env
    fi

    # Cek apakah password masih placeholder
    if grep -q "MASUKKAN_PASSWORD_DISINI" .env; then
        echo -e "${RED}${BOLD}[PERINGATAN KRUSIAL]${NC}"
        echo -e "${YELLOW}Kata sandi akun MetaTrader 5 Headway belum diatur di file .env!${NC}"
        echo -e "Silakan buka dan edit file .env terlebih dahulu:"
        echo -e "  ${BOLD}nano /var/www/sa-copytrade/.env${NC}"
        echo -e "Ubah baris ${BOLD}MT5_PASSWORD=...${NC} dengan kata sandi akun Headway Anda."
        echo ""
        read -p "Apakah Anda ingin melanjutkan sekarang? [y/N]: " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            echo -e "${CYAN}Dibatalkan. Silakan isi password lalu jalankan kembali.${NC}"
            exit 1
        fi
    fi
}

get_server_ip() {
    # Dapatkan IP publik / IP host server
    local ip=$(curl -s -4 ifconfig.me || curl -s -4 icanhazip.com || hostname -I | awk '{print $1}')
    echo "${ip:-localhost}"
}

cmd_start() {
    print_banner
    check_env
    echo -e "${GREEN}▶ Memulai container Copy-Trade Engine (MT5 Real + Python Bot)...${NC}"
    docker compose up -d --build
    echo ""
    echo -e "${GREEN}✓ Layanan berhasil dijalankan di background!${NC}"
    echo ""
    cmd_gui
    echo ""
    echo -e "Gunakan ${BOLD}./run.sh logs${NC} untuk memantau log transaksi secara langsung."
}

cmd_stop() {
    print_banner
    echo -e "${YELLOW}⏹ Menghentikan layanan Copy-Trade...${NC}"
    docker compose down
    echo -e "${GREEN}✓ Semua container copy-trade telah dihentikan.${NC}"
}

cmd_restart() {
    print_banner
    echo -e "${YELLOW}🔄 Me-restart Copy-Trade Engine...${NC}"
    docker compose down
    cmd_start
}

cmd_logs() {
    echo -e "${CYAN}Menampilkan live log bot transaksi (Tekan Ctrl+C untuk keluar)...${NC}"
    docker compose logs -f bot
}

cmd_logs_mt5() {
    echo -e "${CYAN}Menampilkan live log MT5 terminal...${NC}"
    docker compose logs -f mt5-real
}

cmd_status() {
    print_banner
    echo -e "${CYAN}Status Container Docker:${NC}"
    docker compose ps
    echo ""
    # Coba jalankan CLI status di dalam container bot jika sedang jalan
    if docker ps --format '{{.Names}}' | grep -q "sa-copytrade-bot"; then
        echo -e "${CYAN}Status Akun & Posisi Terbuka:${NC}"
        docker compose exec bot python cli.py status || true
    fi
}

cmd_gui() {
    local host_ip=$(get_server_ip)
    echo -e "${CYAN}${BOLD}🖥️  TAMPILAN GRAFIS METATRADER 5 (BROWSER):${NC}"
    echo -e "Anda dapat melihat grafik pair, saldo real, dan eksekusi order MT5 secara langsung di browser:"
    echo -e "  👉 ${BOLD}http://${host_ip}:3002/${NC}"
    echo -e "(Tidak perlu install aplikasi tambahan, cukup buka link di atas pada browser Anda)"
}

# Main routing
case "$1" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    restart)
        cmd_restart
        ;;
    logs)
        cmd_logs
        ;;
    logs-mt5)
        cmd_logs_mt5
        ;;
    status)
        cmd_status
        ;;
    gui)
        cmd_gui
        ;;
    cli)
        shift
        docker compose exec bot python cli.py "$@"
        ;;
    *)
        print_banner
        echo "Penggunaan: ./run.sh [perintah]"
        echo ""
        echo "Perintah yang tersedia:"
        echo "  start     : Membangun dan menjalankan copy-trade engine di background"
        echo "  stop      : Menghentikan seluruh container copy-trade"
        echo "  restart   : Me-restart layanan copy-trade"
        echo "  status    : Melihat status container, saldo akun, dan posisi terbuka"
        echo "  logs      : Menampilkan log eksekusi bot secara real-time"
        echo "  logs-mt5  : Menampilkan log terminal MetaTrader 5"
        echo "  gui       : Menampilkan link web noVNC untuk melihat MT5 di browser"
        echo "  cli       : Menjalankan utilitas CLI (contoh: ./run.sh cli status)"
        echo ""
        ;;
esac

