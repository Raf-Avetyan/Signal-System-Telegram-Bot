# ─── Ponch Signal System — Config ───────────────────────────────
import os
from dotenv import load_dotenv

load_dotenv()

# ─── TELEGRAM ─────────────────────────────────
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
PRIVATE_CHAT_ID = os.getenv("PRIVATE_CHAT_ID", "")
CHAT_ID         = PRIVATE_CHAT_ID

# ─── SYMBOL ───────────────────────────────────
SYMBOL = "BTCUSDT"

# ─── TIMEFRAMES ───────────────────────────────
# Signal timeframes for momentum system
SIGNAL_TIMEFRAMES = ["5m", "15m", "1h", "4h"]
# Data timeframe for levels/channels
ANALYSIS_TIMEFRAME = "1h"
# How much history to fetch per timeframe
KLINE_LIMITS = {
    "5m":  500,
    "15m": 500,
    "1h":  500,
    "4h":  500,
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
MOMENTUM_SMOOTH   = 3     # Smoothing EMA for momentum (Fast)
MOMENTUM_OB       = 66    # Overbought threshold
MOMENTUM_OS       = 34    # Oversold threshold

# ─── SCALP PARAMETERS ────────────────────────
# ATR multipliers for SL/TP calculation
SL_ATR_MULT  = 1.3
TP1_ATR_MULT = 0.7   # 30% allocation
TP2_ATR_MULT = 1.4   # 40% allocation
TP3_ATR_MULT = 2.1   # 30% allocation

# Per-timeframe risk model (overrides global multipliers above when present)
TIMEFRAME_RISK_MULTIPLIERS = {
    "5m":  {"sl": 2.2, "tp1": 0.25, "tp2": 0.35, "tp3": 0.6},
    "15m": {"sl": 2.2, "tp1": 0.45, "tp2": 0.6, "tp3": 0.75},
    "1h":  {"sl": 2.0, "tp1": 0.8, "tp2": 1.2, "tp3": 1.8},
    "4h":  {"sl": 2.0, "tp1": 0.9, "tp2": 1.4, "tp3": 2.0},
}

# Strength & Sizing per timeframe
TIMEFRAME_PROFILES = {
    "5m":  {"strength": "Weak",   "emoji": "⚡️", "size": 0.5},
    "15m": {"strength": "Medium", "emoji": "⚡️", "size": 1.0},
    "1h":  {"strength": "Strong", "emoji": "🚀", "size": 2.0},
    "4h":  {"strength": "Ultra",  "emoji": "💎", "size": 3.0},
}

# Scalp confirmation buffer (RSI points beyond zone edge):
# LONG confirm when RSI > MOMENTUM_OS + buffer
# SHORT confirm when RSI < MOMENTUM_OB - buffer
SCALP_CONFIRM_RSI_BUFFER = 2
TIMEFRAME_CONFIRM_RSI_BUFFER = {
    "5m": 3,
    "15m": 1,
    "1h": 0,
    "4h": 0,
}

# Min seconds between repeated OPEN alerts for same timeframe+side.
# Helps reduce alert spam when RSI repeatedly tags OB/OS.
SCALP_OPEN_ALERT_COOLDOWN = 1800

# Optional live relaxation layer for scalp filters.
# Keeps core logic unchanged, but softens entry gates when enabled.
SCALP_RELAXED_FILTERS = True
SCALP_RELAX_MIN_SCORE_DELTA = 2
SCALP_RELAX_VOL_MIN_MULT = 0.75
SCALP_RELAX_VOL_MAX_MULT = 1.25
SCALP_RELAX_COUNTERTREND_EXTRA = 2
SCALP_RELAX_ALLOW_OFFSESSION = True

# Scalp trend filter:
# - "hard": block all counter-trend scalp confirms
# - "soft": allow counter-trend only if score >= SCALP_COUNTERTREND_MIN_SCORE
# - "off": no trend gating for scalp confirms
SCALP_TREND_FILTER_MODE = "off"
SCALP_COUNTERTREND_MIN_SCORE = 6
SCALP_COUNTERTREND_MAX_PER_WINDOW = 3
SCALP_COUNTERTREND_WINDOW_SEC = 21600  # 6h
SCALP_TREND_FILTER_MODE_BY_TF = {
    "4h": "soft",
}
SCALP_COUNTERTREND_MIN_SCORE_BY_TF = {
    "4h": 6,
}

# Win-rate-first quality gates
SCALP_MIN_SCORE_BY_TF = {
    "5m": 4,
    "15m": 4,
    "1h": 3,
    "4h": 1,
}
SCALP_ALLOWED_SESSIONS_BY_TF = {
    "5m": ["LONDON", "NY"],
    "15m": ["ASIA", "LONDON", "NY"],
    "1h": ["ASIA", "LONDON", "NY"],
    "4h": ["ASIA", "LONDON", "NY"],
}

# Market regime switcher for scalp logic.
# - score_delta: adjusts min score gate
# - vol_min_mult / vol_max_mult: reshapes acceptable ATR% band
# - size_mult: scales displayed position size
SCALP_REGIME_SWITCHING = True
SCALP_REGIME_PROFILES = {
    "TREND":    {"score_delta": -1, "vol_min_mult": 0.9, "vol_max_mult": 1.15, "size_mult": 1.10},
    "RANGE":    {"score_delta": 0,  "vol_min_mult": 1.0, "vol_max_mult": 1.0,  "size_mult": 1.00},
    "HIGH_VOL": {"score_delta": 0,  "vol_min_mult": 1.0, "vol_max_mult": 0.95, "size_mult": 0.90},
}

# Rolling self-tuning for scalp quality.
SCALP_SELF_TUNING_ENABLED = True
SCALP_SELF_TUNE_LOOKBACK = 25
SCALP_SELF_TUNE_MIN_CLOSED = 12
SCALP_SELF_TUNE_LOW_WR = 54.0
SCALP_SELF_TUNE_HIGH_WR = 75.0
SCALP_SELF_TUNE_LOW_AVGR = -0.08
SCALP_SELF_TUNE_HIGH_AVGR = 0.10

# Exposure control for overlapping scalp positions.
SCALP_EXPOSURE_ENABLED = True
SCALP_MAX_OPEN_TOTAL = 8
SCALP_MAX_OPEN_PER_SIDE = 5
SCALP_MAX_OPEN_PER_TF = {
    "5m": 3,
    "15m": 2,
    "1h": 2,
    "4h": 1,
}

# Lose-streak protection
SCALP_LOSS_STREAK_LIMIT = 3
SCALP_LOSS_COOLDOWN_SEC = 1800

# Volatility regime filter by timeframe (ATR/Close % bounds)
VOLATILITY_FILTER_ENABLED = True
VOLATILITY_MIN_ATR_PCT = {
    "5m": 0.03,
    "15m": 0.04,
    "1h": 0.06,
    "4h": 0.08,
}
VOLATILITY_MAX_ATR_PCT = {
    "5m": 1.50,
    "15m": 1.60,
    "1h": 1.80,
    "4h": 2.20,
}

# Session-aware scalp mode tuning
SESSION_SCALP_MODE = {
    "ASIA":   {"countertrend_max": 1, "score_boost": 1},
    "LONDON": {"countertrend_max": 2, "score_boost": 0},
    "NY":     {"countertrend_max": 2, "score_boost": 0},
}

# Order-flow safety filter for scalp confirmations
ORDERFLOW_SAFETY_ENABLED = True
ORDERFLOW_ANOMALY_SCORE_MIN = 8
ORDERFLOW_OI_PCT_ANOMALY = 1.5      # absolute OI change (%)
ORDERFLOW_LIQ_ANOMALY_USD = 1200000 # liquidation spike threshold

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
STRONG_THRESHOLD  = 3    # Confirmed Confluence (3+ systems)
EXTREME_THRESHOLD = 4    # High-Alpha Confluence (4+ systems)

# ─── TIMING ──────────────────────────────────
POLL_INTERVAL = 1       # Seconds between data fetches
CONFIRMATION_WINDOW = 1800  # 30 min window to aggregate signals
CONFIRMATION_FRESH_WINDOW = 480  # 8 min: confirmations must be recent to count
CONFIRMATION_RSI_EXHAUSTION_BUFFER = 6  # block SHORT if RSI too low, LONG if RSI too high
CONFLUENCE_OPPOSITE_LOCK_SEC = 900  # 15 min opposite-side confluence lock after a fire

# ─── FUNDING RATE ALERTS ─────────────────────
FUNDING_THRESHOLD = 0.0005       # 0.05% — trigger alert above this
FUNDING_CHECK_INTERVAL = 300     # Check every 5 min
FUNDING_COOLDOWN = 3600          # 1 hour cooldown between alerts

# ─── VOLUME SPIKE DETECTION ──────────────────
VOLUME_SPIKE_MULT = 3.0                   # Alert when vol > 3x average
VOLUME_SPIKE_TIMEFRAMES = ["15m", "1h", "4h"]   # Skip 5m (too noisy)
VOLUME_AVG_PERIOD = 20                    # 20-candle volume SMA

# ─── PRICE APPROACHING ALERTS ────────────────
APPROACH_THRESHOLD = 0.002       # 0.2% distance from level
APPROACH_COOLDOWN = 10800          # 3 hour cooldown per level
APPROACH_LEVELS = ["PDH", "PDL", "DO", "PWH", "PWL", "PMH", "PML", "Pump", "Dump", "PumpMax", "DumpMax"]  # Which levels to watch

# ─── ADVANCED ALERTS (NEW) ───────────────────
OI_CHANGE_THRESHOLD = 0.015      # 1.5% change in OI to trigger divergence check
LIQ_SQUEEZE_THRESHOLD = 500000   # $500k in liquidations to trigger squeeze alert
LIQ_ALERT_COOLDOWN = 600         # 10 min cooldown for squeeze alerts

# ─── MARKET ALERTS (FAST MOVE) ────────────────
FAST_MOVE_THRESHOLD = 0.03       # 3% move
FAST_MOVE_WINDOW    = 4          # 4 hours
FAST_MOVE_COOLDOWN  = 14400      # 4 hours

SESSIONS = {
    "ASIA":   {"open": 0.0,  "close": 8.0},
    "LONDON": {"open": 8.0,  "close": 16.0},
    "NY":     {"open": 13.5, "close": 20.0}, # Stock Market: 9:30 AM - 4:00 PM ET
}

def get_adjusted_sessions(dt):
    """Returns adjusted SESSIONS mapping for a given UTC datetime (handles NY DST)."""
    import copy
    from datetime import datetime
    
    adj = copy.deepcopy(SESSIONS)
    
    # DST Check for USA (Second Sunday of March to First Sunday of November)
    try:
        dst_start = datetime(dt.year, 3, 8 + (6 - datetime(dt.year, 3, 1).weekday() + 7) % 7)
        dst_end = datetime(dt.year, 11, 1 + (6 - datetime(dt.year, 11, 1).weekday() + 7) % 7)
        is_dst = dst_start <= dt.replace(tzinfo=None) < dst_end
        
        # NY Stock Open: 9:30 AM ET -> 13:30 (DST) or 14:30 (No DST) UTC
        offset = -4 if is_dst else -5
        adj["NY"]["open"] = 9.5 - offset
        adj["NY"]["close"] = 16.0 - offset
    except:
        pass # Fallback to SESSIONS
        
    return adj

# ─── TELEGRAM COMMANDS ───────────────────────
COMMAND_POLL_INTERVAL = 2   # Seconds between getUpdates polls
ALERT_BATCH_WINDOW = 10     # Seconds to wait for multiple alerts before batching

# ─── REGISTRATION & ONBOARDING ───────────────
BITUNIX_REG_LINK = "https://www.bitunix.com/register?vipCode=mrponch"
INVITE_LINK = "https://t.me/+Z0rG8WJK58RlN2Ey"
