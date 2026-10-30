# 🚀 Strategy Arena — Real Account Copy-Trade Engine (Headway MT5)

Program Copy-Trading otomatis profesional untuk menyalin sinyal dari **Strategy Arena** secara real-time ke akun **MetaTrader 5 Real (Headway)**.

Khusus dikonfigurasi dan dioptimasi untuk menyalin strategi forex:
> 🎯 **Target Strategi:** `ROC Momentum 12` (`roc_momentum`) & `EMA Deviation` (`price_ema_dev`)  
> 💰 **Modal Riil:** `$350.50 USD` (Batas Stop Equity: `$10.00 USD`)  
> 🏦 **Broker:** Headway (`Headway-Real` / `mt5.real.trade-hw.online`)  
> 🔑 **Nomor Akun (Login):** `17607969`

---

## 📋 Fitur Utama & Proteksi Modal

1. **Sub-Millisecond Multi-Strategi (WebSocket + REST Sync Watchdog)**:
   - Terhubung langsung ke WebSocket feed Strategy Arena (`ws://.../ws/signals?strategy=roc_momentum,price_ema_dev&mode=forex`).
   - Mendukung eksekusi multi-strategi secara simultan dengan alokasi lot dan perlindungan margin independen.
   - Dilengkapi sistem rekonsiliasi audit berkala (setiap 10 detik) via REST API untuk menjamin **tidak ada order atau penutupan posisi yang terlewat**.
2. **Manajemen Resiko Ketat**:
   - **Ukuran Lot Aman:** Default menggunakan mikro lot **`0.01` lot** per trade.
   - **Batas Maksimal Posisi:** Maksimal **3 posisi simultan** untuk menjaga margin aman.
   - **Mandatory Hard Stop Loss:** Setiap order wajib memiliki Stop Loss terpasang langsung di server broker MT5.
   - **Circuit Breaker Harian:** Jika total rugi terealisasi dalam 1 hari mencapai **`-$50.00 USD`**, bot otomatis menolak membuka order baru.
   - **Emergency Equity Floor ($10 USD):** Bot otomatis menghentikan transaksi dan menutup seluruh posisi jika equity menyentuh atau turun ke **`$10.00 USD`**.
3. **Dedicated MT5 Container + Web noVNC (Browser View)**:
   - Menjalankan terminal MT5 Headway-Real secara mandiri dan terisolasi di dalam container Docker.
   - Anda dapat melihat tampilan grafik, saldo riil, dan posisi floating MT5 secara langsung di browser Anda melalui **port 3002** (`http://<ip-server>:3002/`).
4. **Notifikasi Telegram Instan (Opsional)**:
   - Menerima notifikasi langsung ke smartphone Anda setiap kali order dibuka atau ditutup, lengkap dengan PnL dan harga eksekusi.

---

## 📂 Struktur Direktori

```text
/var/www/sa-copytrade/
├── .env.example              # Template konfigurasi environment
├── .env                      # File konfigurasi utama (isi password di sini)
├── docker-compose.yml        # Konfigurasi container MT5 Real + Bot Daemon
├── Dockerfile                # Image container Python bot
├── requirements.txt          # Library Python (rpyc, aiohttp, websockets, dll.)
├── config.py                 # Modul pemuat konfigurasi & logger
├── mt5_client.py             # Client interaksi MetaTrader 5 via RPyC
├── risk_manager.py           # Logika perhitungan lot & circuit breaker
├── trade_storage.py          # Database SQLite penyimpan riwayat transaksi
├── copytrade_bot.py          # Daemon bot copy-trading utama
├── cli.py                    # Tool baris perintah interaktif (status, history, dsb.)
├── run.sh                    # Skrip kontrol cepat satu-perintah
├── data/                     # Database lokal SQLite (copytrade.db)
└── logs/                     # Log harian bot transaksi (copytrade.log)
```

---

## ⚡ Panduan Memulai Cepat (Quick Start)

### 1. Masukkan Password Akun Headway
Buka file `.env` di folder ini dan masukkan kata sandi akun Headway Anda:
```bash
nano /var/www/sa-copytrade/.env
```
Cari baris:
```ini
MT5_PASSWORD=MASUKKAN_PASSWORD_DISINI
```
Ganti `MASUKKAN_PASSWORD_DISINI` dengan kata sandi akun Headway Anda (`17607969`), lalu simpan (`Ctrl+O`, `Enter`, `Ctrl+X`).

### 2. Jalankan Copy-Trade Engine
Cukup jalankan satu perintah berikut:
```bash
cd /var/www/sa-copytrade
./run.sh start
```
Perintah ini akan secara otomatis:
- Membangun container Docker untuk Python Bot.
- Mengaktifkan terminal MetaTrader 5 untuk broker Headway Real.
- Menginisialisasi koneksi sinyal ke Strategy Arena.

---

## 🖥️ Melihat Tampilan MT5 Langsung di Browser

Anda tidak perlu menginstal aplikasi MetaTrader 5 di laptop Anda untuk memantau akun. Cukup buka browser dan akses:

👉 **`http://<IP_SERVER_ANDA>:3002/`**

Tampilan grafis terminal MT5 resmi akan langsung terbuka di browser dengan grafik live, tab trade, saldo $1,000 Anda, dan posisi yang dieksekusi oleh bot.

---

## 🛠️ Perintah Kontrol (`./run.sh`)

| Perintah | Deskripsi |
| :--- | :--- |
| `./run.sh start` | Menjalankan seluruh sistem copy-trade di background |
| `./run.sh stop` | Menghentikan sistem copy-trade secara aman |
| `./run.sh restart` | Me-restart container MT5 dan bot |
| `./run.sh status` | Menampilkan status container, saldo akun MT5, dan posisi aktif |
| `./run.sh logs` | Menampilkan live log aktivitas bot transaksi |
| `./run.sh logs-mt5` | Menampilkan log internal MetaTrader 5 terminal |
| `./run.sh gui` | Menampilkan tautan URL browser untuk melihat tampilan MT5 |
| `./run.sh cli status` | Menampilkan tabel rapi status akun dan posisi terbuka |
| `./run.sh cli history` | Menampilkan riwayat transaksi yang sudah ditutup beserta PnL |
| `./run.sh cli test-signal` | Menguji ketersediaan sinyal strategi 'Gap Fill Reversal' |
| `./run.sh cli close-all` | **Tutup Darurat**: Menutup seluruh posisi aktif bot di MT5 |

---

## 📱 Mengaktifkan Notifikasi Telegram (Opsional)

Jika ingin menerima notifikasi real-time di Telegram saat bot membuka atau menutup order:
1. Dapatkan **Bot Token** dari `@BotFather` di Telegram.
2. Dapatkan **Chat ID** Anda dari `@userinfobot`.
3. Buka `.env` dan masukkan:
   ```ini
   TELEGRAM_BOT_TOKEN=123456789:ABCDefghIJKLmnop...
   TELEGRAM_CHAT_ID=123456789
   ```
4. Restart bot:
   ```bash
   ./run.sh restart
   ```

---

## 🛡️ Catatan Keamanan
- File `.env` berisi data login broker Anda. File ini telah diproteksi dan dikecualikan dari repositori publik.
- Magic Number unik `889901` disematkan pada setiap order, sehingga bot **hanya mengelola order miliknya sendiri** dan tidak akan mengganggu order manual lain yang Anda buka di terminal yang sama.

