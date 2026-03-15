# ─── Ponch Signal System — Config ───────────────────────────────
import os
from dotenv import load_dotenv

load_dotenv()

# ─── TELEGRAM ─────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("CHAT_ID", "")

# ─── SYMBOL ───────────────────────────────────
SYMBOL = "BTCUSDT"

# ─── TIMEFRAMES ───────────────────────────────
# Scalp timeframes for momentum system
SCALP_TIMEFRAMES = ["5m", "15m", "1h"]
# Data timeframe for levels/channels
ANALYSIS_TIMEFRAME = "1h"
# How much history to fetch per timeframe
KLINE_LIMITS = {
    "5m":  500,
    "15m": 500,
    "1h":  500,
    "1d":  120,
    "1w":  52,
    "1M":  12,
}

# ─── EMA CHANNELS ─────────────────────────────
EMA1_LEN = 9       # Inner channel EMA
EMA2_LEN = 21      # Mid channel EMA
EMA3_LEN = 55      # Outer channel EMA
ATR_LEN  = 14      # ATR period

# Channel multipliers
MULT_INNER = 1.0   # Inner band = EMA9 ± ATR × 1.0
MULT_MID   = 2.5   # Mid band   = EMA21 ± ATR × 2.5
MULT_OUTER = 5.0   # Outer band = EMA55 ± ATR × 5.0

# ─── ADR / VOLATILITY ────────────────────────
ADR_LEN = 14       # Average Daily Range lookback

# ─── MOMENTUM ────────────────────────────────
MOMENTUM_RSI_LEN  = 14    # RSI length for momentum
MOMENTUM_SMOOTH   = 5     # Smoothing EMA for momentum
MOMENTUM_OB       = 70    # Overbought threshold
MOMENTUM_OS       = 30    # Oversold threshold

# ─── SCALP PARAMETERS ────────────────────────
# ATR multipliers for SL/TP calculation
SL_ATR_MULT  = 0.7
TP1_ATR_MULT = 0.7   # 30% allocation
TP2_ATR_MULT = 1.4   # 40% allocation
TP3_ATR_MULT = 2.1   # 30% allocation

# Strength & Sizing per timeframe
TIMEFRAME_PROFILES = {
    "5m":  {"strength": "Weak",   "emoji": "⚡️", "size": 0.5},
    "15m": {"strength": "Medium", "emoji": "⚡️", "size": 1.0},
    "1h":  {"strength": "Strong", "emoji": "🚀", "size": 2.0},
}

# ─── SIGNAL POINTS ────────────────────────────
# Trade signal points by entry level
SIGNAL_POINTS = {
    "L":   1, "S":   1,   # Basic
    "L+":  2, "S+":  2,   # Inner channel
    "L++": 3, "S++": 3,   # Mid channel
    "L+++": 4, "S+++": 4, # Outer channel
}

# Liquidity sweep points by level
SWEEP_POINTS = {
    "PDL": 1, "PDH": 1,   # Previous Day = Low
    "PWL": 2, "PWH": 2,   # Previous Week = Medium
    "PML": 3, "PMH": 3,   # Previous Month = Strong
}

SWEEP_STRENGTH = {1: "Low", 2: "Medium", 3: "Strong"}

# Volatility zone points
VOL_ZONE_POINTS = {
    "DUMP": 1, "PUMP": 1,          # Weak
    "DUMPMAX": 3, "PUMPMAX": 3,    # Strong
}

VOL_ZONE_STRENGTH = {1: "Low", 3: "Strong"}

# ─── CONFIRMATION THRESHOLDS ─────────────────
STRONG_THRESHOLD  = 2    # 2 confirmations = STRONG
EXTREME_THRESHOLD = 3    # 3+ confirmations = EXTREME

# ─── TIMING ──────────────────────────────────
POLL_INTERVAL = 60       # Seconds between data fetches
CONFIRMATION_WINDOW = 300  # 5 min window to aggregate signals

# ─── FUNDING RATE ALERTS ─────────────────────
FUNDING_THRESHOLD = 0.0005       # 0.05% — trigger alert above this
FUNDING_CHECK_INTERVAL = 300     # Check every 5 min
FUNDING_COOLDOWN = 3600          # 1 hour cooldown between alerts

# ─── VOLUME SPIKE DETECTION ──────────────────
VOLUME_SPIKE_MULT = 3.0                   # Alert when vol > 3x average
VOLUME_SPIKE_TIMEFRAMES = ["15m", "1h"]   # Skip 5m (too noisy)
VOLUME_AVG_PERIOD = 20                    # 20-candle volume SMA

# ─── PRICE APPROACHING ALERTS ────────────────
APPROACH_THRESHOLD = 0.003       # 0.3% distance from level
APPROACH_COOLDOWN = 900          # 15 min cooldown per level
APPROACH_LEVELS = ["Pump", "Dump", "PumpMax", "DumpMax"]  # Which levels to watch

# ─── SESSION TIMES (UTC hours) ───────────────
SESSIONS = {
    "ASIA":   {"open": 0,  "close": 8},
    "LONDON": {"open": 8,  "close": 16},
    "NY":     {"open": 13, "close": 21},
}

# ─── TELEGRAM COMMANDS ───────────────────────
COMMAND_POLL_INTERVAL = 5   # Seconds between getUpdates polls
ALERT_BATCH_WINDOW = 10     # Seconds to wait for multiple alerts before batching
