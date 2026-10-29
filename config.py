"""Konfigurasi SA Copy-Trade Engine (Headway Real - $1,000)."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env dari root folder
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# MetaTrader 5 Configuration
MT5_LOGIN = int(os.getenv("MT5_LOGIN", "17607969"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER = os.getenv("MT5_SERVER", "Headway-Real")
MT5_IP_SERVER = os.getenv("MT5_IP_SERVER", "mt5.real.trade-hw.online:443")
MT5_HOST = os.getenv("MT5_HOST", "localhost")
MT5_PORT = int(os.getenv("MT5_PORT", "18813"))
MT5_MAGIC_NUMBER = int(os.getenv("MT5_MAGIC_NUMBER", "889901"))
MT5_ORDER_COMMENT = os.getenv("MT5_ORDER_COMMENT", "SA:gap_fill")

# Strategy Arena Signal Sources
ARENA_BASE_URL = os.getenv("ARENA_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
ARENA_WS_URL = os.getenv("ARENA_WS_URL", "ws://127.0.0.1:8080/ws/signals")
ARENA_API_KEY = os.getenv("ARENA_API_KEY", "pk_live_strategyarena_public_feed")

# Target Strategy
TARGET_STRATEGY = os.getenv("TARGET_STRATEGY", "gap_fill")
TARGET_STRATEGY_NAME = os.getenv("TARGET_STRATEGY_NAME", "Gap Fill Reversal")
TARGET_MODE = os.getenv("TARGET_MODE", "forex")

# Risk & Money Management (Dirancang untuk modal $1,000 USD)
LOT_SIZING_MODE = os.getenv("LOT_SIZING_MODE", "FIXED").upper()  # "FIXED" | "PROPORTIONAL"
BASE_LOT = float(os.getenv("BASE_LOT", "0.01"))
MAX_LOT = float(os.getenv("MAX_LOT", "0.05"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))

# Circuit Breakers & Proteksi Akun
MAX_DAILY_LOSS_USD = float(os.getenv("MAX_DAILY_LOSS_USD", "50.0"))
MIN_EQUITY_USD = float(os.getenv("MIN_EQUITY_USD", "800.0"))
ENFORCE_SL = os.getenv("ENFORCE_SL", "true").lower() in ("true", "1", "yes")
DEFAULT_SL_PIPS = float(os.getenv("DEFAULT_SL_PIPS", "35.0"))
DEFAULT_TP_PIPS = float(os.getenv("DEFAULT_TP_PIPS", "70.0"))

# Telegram Notification
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# General
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "10.0"))
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Logger Setup
def setup_logger(name: str = "sa-copytrade") -> logging.Logger:
    logger = logging.getLogger(name)
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logger.setLevel(log_level)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Console Handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File Handler (Daily rotating log)
        fh = logging.FileHandler(LOGS_DIR / "copytrade.log", encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

log = setup_logger()

