# 🚀 Strategy Arena — Real Account Copy-Trade Engine (Headway MT5)

Program Copy-Trading otomatis profesional untuk menyalin sinyal dari **Strategy Arena** secara real-time ke akun **MetaTrader 5 Real**.

Khusus dikonfigurasi dan dioptimasi untuk menyalin strategi kuantitatif forex & komoditas:
> 🎯 **Target Strategi Aktif:** `ROC Momentum 12` (`roc_momentum`) & `Gap Fill Reversal` (`gap_fill`)  
> 🔄 **Mode Pasar:** `FOREX` (28 Pair Forex & Logam Mulia / XAUUSD)  
> 🏦 **Broker:** Headway (`Headway-Real` / `mt5.real.trade-hw.online`)  
> 🔑 **Nomor Akun:** `17607969` (Mendukung Akun Cent & Standard)  
> 🛡️ **Fitur Hedging Multi-Strategi:** Aktif (`ALLOW_HEDGING=true`)  
> 🤖 **Telegram Remote Control:** Mendukung command `/my-posisi`, `/posisi`, `/status`, dan `/help`  

---

## 📋 Fitur Utama & Proteksi Modal

### 1. Eksekusi Sub-Milidetik Multi-Strategi & Hedging Cerdas
- **WebSocket Feed Instan**: Terhubung langsung ke WebSocket stream Strategy Arena (`ws://.../ws/signals?strategy=roc_momentum,gap_fill&mode=forex`).
- **Multi-Strategy Hedging (`ALLOW_HEDGING=true`)**:
  - Mengizinkan strategi yang berbeda membuka posisi berlawanan pada pair yang sama tanpa konflik (contoh: `roc_momentum` membuka **SHORT XAUUSD**, sedangkan `gap_fill` membuka **LONG XAUUSD**).
  - Terminal MT5 mengeksekusi kedua order secara independen dengan nomor tiket terpisah.
  - Sinyal penutupan (CLOSE) dipetakan spesifik berdasarkan pasangan `(strategy_id, symbol)`, sehingga exit dari satu strategi tidak akan menutup posisi milik strategi lain.
- **Smart Fresh-Trade Reconciliation**:
  - Saat bot pertama kali dijalankan atau setelah rekoneksi WebSocket, sistem *watchdog* secara otomatis memindai trade aktif di Strategy Arena yang berumur $\le$ 900 detik (15 menit).
  - Trade yang belum ada di MT5 akan disinkronkan dan dieksekusi seketika, mencegah hilangnya momentum sinyal akibat restart jaringan atau server.

### 2. Remote Bot Telegram Interaktif
Bot dilengkapi antarmuka Telegram dua arah (notifikasi otomatis + command interaktif):
- **Notifikasi Real-Time**: Menerima laporan instan setiap pembukaan order, penutupan posisi, level SL/TP, floating profit, dan waktu open terstandarisasi dalam format **WIB (Asia/Jakarta)**.
- **Daftar Perintah Telegram**:
  | Perintah | Fungsi |
  | :--- | :--- |
  | `/my-posisi` (atau `/my_posisi`) | Menampilkan seluruh posisi riil yang sedang terbuka di MT5 beserta nomor tiket, lot, floating profit/loss, dan waktu open WIB. |
  | `/posisi` | Menampilkan posisi aktif semua strategi yang sedang dipantau di Strategy Arena. |
  | `/posisi <nama_strategi>` | Menampilkan posisi aktif strategi tertentu (contoh: `/posisi roc_momentum` atau `/posisi gap_fill`). |
  | `/status` | Menampilkan saldo akun broker MT5, equity, margin bebas, status Algo Trading, dan ringkasan posisi aktif. |
  | `/help` | Menampilkan panduan bantuan perintah bot. |

### 3. Manajemen Risiko & Proteksi Saldo
- **Ukuran Lot Aman:** Default micro-lot **`0.01` lot** per posisi.
- **Batas Emergency Stop Terkonfigurasi:** Parameter `MIN_EQUITY_USD` dapat diatur ke `0` (trading hingga batas margin/saldo akun cent) atau nominal tertentu sebagai batas proteksi modal.
- **Hard Stop Loss Terpasang:** Setiap transaksi wajib memasang Stop Loss langsung di level server broker MT5 untuk mengantisipasi lonjakan spread atau slippage ekstrem.
- **Circuit Breaker Harian:** Bot menolak order baru jika batas kerugian harian (`MAX_DAILY_LOSS_USD`) tercapai.
- **Magic Number Terisolasi (`889901`):** Bot hanya mengelola posisi miliknya sendiri, aman jika Anda membuka order manual lain di terminal yang sama.

### 4. Dedicated MT5 Container + Web noVNC (Browser View)
- Terminal MetaTrader 5 resmi berjalan terisolasi di dalam container Docker.
- Anda dapat memantau grafik live, saldo, dan posisi floating langsung melalui browser pada **port 3002**:
  👉 **`http://<IP_SERVER>:3002/`**

---

## 📂 Struktur Direktori

```text
/var/www/sa-copytrade/
├── .env.example              # Template konfigurasi environment
├── .env                      # File konfigurasi aktif (kredensial broker & Telegram)
├── docker-compose.yml        # Definisi service Docker MT5 Real + Python Bot Daemon
├── Dockerfile                # Image container Python runtime
├── requirements.txt          # Library dependencies (rpyc, aiohttp, websockets, python-dotenv)
├── config.py                 # Pemuat konfigurasi environment terintegrasi
├── mt5_client.py             # Client interaksi MetaTrader 5 via bridge RPyC
├── risk_manager.py           # Logika validasi margin, lot sizing, & hedging filter
├── trade_storage.py          # Database SQLite penyimpan tiket & status order
├── copytrade_bot.py          # Daemon bot utama (WebSocket, Watchdog, & Telegram Bot)
├── cli.py                    # Tool baris perintah interaktif (status, history, close-all)
├── run.sh                    # Skrip kontrol cepat satu-perintah
├── data/                     # Database lokal SQLite (copytrade.db)
└── logs/                     # Log aktivitas transaksi (copytrade.log)
```

---

## ⚙️ Variabel Konfigurasi (`.env`)

```ini
# --- Strategy Arena Feed ---
ARENA_HOST=172.24.30.4:8080
ARENA_PUBLIC_KEY=pk_live_strategyarena_public_feed
ARENA_MARKET_MODE=forex

# --- Target Strategi ---
TARGET_STRATEGIES=roc_momentum,gap_fill

# --- Fitur Hedging & Sinkronisasi ---
ALLOW_HEDGING=true
AUTO_SYNC_PAST_TRADES=true

# --- Manajemen Risiko ---
LOT_SIZE=0.01
MAX_OPEN_POSITIONS=5
MAX_DAILY_LOSS_USD=50.0
MIN_EQUITY_USD=0.0

# --- Kredensial Broker MetaTrader 5 ---
MT5_LOGIN=17607969
MT5_PASSWORD=******
MT5_SERVER=Headway-Real

# --- Notifikasi & Remote Control Telegram ---
TELEGRAM_BOT_TOKEN=8427773229:AAH...
TELEGRAM_CHAT_ID=6058983995
```

---

## ⚡ Panduan Operasional (`./run.sh`)

| Perintah | Deskripsi |
| :--- | :--- |
| `./run.sh start` | Menjalankan terminal MT5 dan daemon bot di latar belakang |
| `./run.sh stop` | Menghentikan sistem copy-trade secara aman |
| `./run.sh restart` | Memuat ulang seluruh container dan konfigurasi |
| `./run.sh status` | Menampilkan status container, saldo akun MT5, dan posisi aktif |
| `./run.sh logs` | Menampilkan live log aktivitas bot transaksi |
| `./run.sh logs-mt5` | Menampilkan log terminal internal MetaTrader 5 |
| `./run.sh gui` | Menampilkan tautan URL browser noVNC MT5 |
| `./run.sh cli status` | Menampilkan tabel status akun dan posisi terbuka di konsol |
| `./run.sh cli history` | Menampilkan riwayat transaksi yang telah ditutup beserta PnL |
| `./run.sh cli close-all` | **Tutup Darurat**: Menutup seluruh posisi aktif bot di MT5 |

---

## 🛡️ Catatan Keamanan
- Kredensial akun MT5 dan token Telegram tersimpan secara privat di file `.env` lokal dan dikecualikan dari repositori Git (`.gitignore`).
- Semua koneksi sinyal diverifikasi melalui public key Strategy Arena dengan protokol fail-safe otomatis.
