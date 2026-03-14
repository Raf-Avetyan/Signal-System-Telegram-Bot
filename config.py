# ─── CTLT Signal System Configuration ────────────────────────

# ─── TELEGRAM ─────────────────────────────────
BOT_TOKEN = "8312618850:AAHM7Sjrgjcz0S895xQlZvOYeMpn6vWwjZc"
CHAT_ID   = "@ponch_alerts"

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
    "L":  1, "S":  1,   # Basic
    "L1": 2, "S1": 2,   # Inner channel
    "L2": 3, "S2": 3,   # Mid channel
    "L3": 4, "S3": 4,   # Outer channel
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
