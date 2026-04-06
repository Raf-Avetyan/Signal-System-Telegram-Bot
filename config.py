# Ponch Signal System - Config
import os
from dotenv import load_dotenv

load_dotenv()

# --- TELEGRAM -------------------------------------------------------
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
PRIVATE_CHAT_ID = os.getenv("PRIVATE_CHAT_ID", "")
CHAT_ID         = PRIVATE_CHAT_ID
PRIVATE_EXEC_CHAT_ID = os.getenv("PRIVATE_EXEC_CHAT_ID", "").strip()
EXECUTION_UPDATES_PRIVATE_ONLY = os.getenv("EXECUTION_UPDATES_PRIVATE_ONLY", "true").strip().lower() == "true"
PRIVATE_EXEC_AI_CONTROL_ENABLED = os.getenv("PRIVATE_EXEC_AI_CONTROL_ENABLED", "true").strip().lower() == "true"
PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC = int(os.getenv("PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC", "180"))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip()

# Live Bitunix futures trading
BITUNIX_FAPI_KEY = os.getenv("BITUNIX_FAPI_KEY", "")
BITUNIX_FAPI_SECRET = os.getenv("BITUNIX_FAPI_SECRET", "")
BITUNIX_FAPI_BASE_URL = os.getenv("BITUNIX_FAPI_BASE_URL", "https://fapi.bitunix.com")
BITUNIX_TRADING_ENABLED = os.getenv("BITUNIX_TRADING_ENABLED", "false").strip().lower() == "true"
BITUNIX_TRADING_MODE = os.getenv("BITUNIX_TRADING_MODE", "off").strip().lower()  # off | demo | live
BITUNIX_MARGIN_COIN = os.getenv("BITUNIX_MARGIN_COIN", "USDT")
BITUNIX_REQUIRED_MARGIN_MODE = os.getenv("BITUNIX_REQUIRED_MARGIN_MODE", "ISOLATION").strip().upper()
BITUNIX_DEFAULT_LEVERAGE = int(os.getenv("BITUNIX_DEFAULT_LEVERAGE", "3"))
BITUNIX_POSITION_MODE = os.getenv("BITUNIX_POSITION_MODE", "ONE_WAY").strip().upper()
BITUNIX_MAX_OPEN_POSITIONS = int(os.getenv("BITUNIX_MAX_OPEN_POSITIONS", "3"))
BITUNIX_MAX_RISK_USD = float(os.getenv("BITUNIX_MAX_RISK_USD", "25"))
BITUNIX_RISK_CAP_PCT = float(os.getenv("BITUNIX_RISK_CAP_PCT", "0.01"))
BITUNIX_MIN_NOTIONAL_USD = float(os.getenv("BITUNIX_MIN_NOTIONAL_USD", "25"))
BITUNIX_TP_SPLITS = (0.30, 0.40, 0.30)
BITUNIX_TPSL_TRIGGER_TYPE = os.getenv("BITUNIX_TPSL_TRIGGER_TYPE", "MARK_PRICE").strip().upper()
BITUNIX_MIN_BASE_QTY = float(os.getenv("BITUNIX_MIN_BASE_QTY", "0.0001"))
BITUNIX_QTY_STEP = float(os.getenv("BITUNIX_QTY_STEP", "0.0001"))
BITUNIX_FETCH_SYMBOL_RULES = os.getenv("BITUNIX_FETCH_SYMBOL_RULES", "true").strip().lower() == "true"

# --- SYMBOL ---------------------------------------------------------
SYMBOL = "BTCUSDT"

# --- TIMEFRAMES -----------------------------------------------------
# Signal timeframes for momentum system
SIGNAL_TIMEFRAMES = ["5m", "15m", "1h", "4h"]
# Data timeframe for levels/channels
ANALYSIS_TIMEFRAME = "1h"
# How much history to fetch per timeframe
KLINE_LIMITS = {
    "1m":  720,
    "5m":  500,
    "15m": 500,
    "1h":  500,
    "4h":  500,
    "1d":  120,
    "1w":  52,
    "1M":  12,
}

# --- EMA CHANNELS ---------------------------------------------------
EMA1_LEN = 9       # Inner channel EMA
EMA2_LEN = 21      # Mid channel EMA
EMA3_LEN = 55      # Outer channel EMA
ATR_LEN  = 14      # ATR period

# Channel multipliers
# Channel multipliers
MULT_INNER = 1.0   # Inner band = EMA9 +/- ATR x 1.0
MULT_MID   = 2.5   # Mid band   = EMA21 +/- ATR x 2.5
MULT_OUTER = 5.0   # Outer band = EMA55 +/- ATR x 5.0
# --- ADR / VOLATILITY -----------------------------------------------
ADR_LEN = 14       # Average Daily Range lookback

# --- MOMENTUM -------------------------------------------------------
MOMENTUM_RSI_LEN  = 14    # RSI length for momentum
MOMENTUM_SMOOTH   = 3     # Smoothing EMA for momentum (Fast)
MOMENTUM_OB       = 66    # Overbought threshold
MOMENTUM_OS       = 34    # Oversold threshold
TIMEFRAME_MOMENTUM_THRESHOLDS = {
    "5m": {"ob": 66, "os": 34},
    "15m": {"ob": 62, "os": 38},
    "1h": {"ob": 66, "os": 34},
    "4h": {"ob": 64, "os": 36},
}

# RSI divergence confirmation
RSI_DIVERGENCE_ENABLED = True
RSI_DIVERGENCE_LOOKBACK = 60
RSI_DIVERGENCE_SEGMENT = 20
RSI_DIVERGENCE_MIN_PRICE_DELTA_PCT = 0.20
RSI_DIVERGENCE_MIN_RSI_DELTA = 4.0
RSI_DIVERGENCE_POINTS = 2
RSI_DIVERGENCE_MAX_AGE_CANDLES_BY_TF = {
    "5m": 8,
    "15m": 6,
    "1h": 4,
    "4h": 3,
}

# Metric rule: count breakeven exits as wins once this TP threshold was reached.
# 1 = TP1 hit then breakeven counts as win
# 2 = TP2 hit then breakeven counts as win
BREAKEVEN_WIN_MIN_TP = max(1, min(3, int(os.getenv("BREAKEVEN_WIN_MIN_TP", "1"))))

# --- SCALP PARAMETERS -----------------------------------------------
# ATR multipliers for SL/TP calculation
SL_ATR_MULT  = 1.3
TP1_ATR_MULT = 0.7   # 30% allocation
TP2_ATR_MULT = 1.4   # 40% allocation
TP3_ATR_MULT = 2.1   # 30% allocation

# Per-timeframe risk model (overrides global multipliers above when present)
TIMEFRAME_RISK_MULTIPLIERS = {
    "5m":  {"sl": 3.2, "tp1": 1.10, "tp2": 1.90, "tp3": 2.80},
    "15m": {"sl": 3.0, "tp1": 1.25, "tp2": 2.10, "tp3": 3.00},
    "1h":  {"sl": 2.3, "tp1": 1.10, "tp2": 1.75, "tp3": 2.45},
    "4h":  {"sl": 2.1, "tp1": 1.25, "tp2": 1.95, "tp3": 2.70},
}

BITUNIX_LIQUIDATION_SAFETY_ENABLED = os.getenv("BITUNIX_LIQUIDATION_SAFETY_ENABLED", "true").strip().lower() == "true"
BITUNIX_LIQUIDATION_SAFETY_BUFFER_R = float(os.getenv("BITUNIX_LIQUIDATION_SAFETY_BUFFER_R", "0.50"))
BITUNIX_LIQUIDATION_MAX_LEVERAGE_BY_TF = {
    "5m": int(os.getenv("BITUNIX_MAX_LEVERAGE_5M", str(BITUNIX_DEFAULT_LEVERAGE))),
    "15m": int(os.getenv("BITUNIX_MAX_LEVERAGE_15M", str(BITUNIX_DEFAULT_LEVERAGE))),
    "1h": int(os.getenv("BITUNIX_MAX_LEVERAGE_1H", str(BITUNIX_DEFAULT_LEVERAGE))),
    "4h": int(os.getenv("BITUNIX_MAX_LEVERAGE_4H", str(BITUNIX_DEFAULT_LEVERAGE))),
}

# Smart Money Liquidity
SMART_MONEY_ENABLED = True
SMART_MONEY_EXECUTION_TFS = ["5m", "15m"]
SMART_MONEY_RISK_PCT = 1.0
SMART_MONEY_MAX_TRADES_PER_DAY = 3
SMART_MONEY_ALLOWED_SESSIONS = ["LONDON", "NY"]
SMART_MONEY_HTF_SWING_LOOKBACK = 40
SMART_MONEY_SWING_PIVOT_BARS = 2
SMART_MONEY_KEYLEVEL_TOLERANCE_PCT = 0.35
SMART_MONEY_DEALING_RANGE_BUFFER_PCT = 0.10
SMART_MONEY_LTF_SWEEP_LOOKBACK = 12
SMART_MONEY_FVG_LOOKBACK = 30
SMART_MONEY_OB_LOOKBACK = 24
SMART_MONEY_DISPLACEMENT_BODY_ATR = 0.60
SMART_MONEY_SL_BUFFER_ATR = 0.20
SMART_MONEY_MIN_RR = 2.0
SMART_MONEY_BOS_SWING_LOOKBACK = 4
SMART_MONEY_POST_SWEEP_CONFIRM_BARS = 8
SMART_MONEY_MAX_EXECUTION_CANDLE_RISK_RATIO_BY_TF = {
    "15m": 1.30,
}

# Strength & Sizing per timeframe
TIMEFRAME_PROFILES = {
    "5m":  {"strength": "Weak",   "emoji": "⚡️", "size": 5.0},
    "15m": {"strength": "Medium", "emoji": "⚡️", "size": 6.0},
    "1h":  {"strength": "Strong", "emoji": "🚀", "size": 7.5},
    "4h":  {"strength": "Ultra",  "emoji": "💎", "size": 9.0},
}
MIN_SIGNAL_SIZE_PCT = float(os.getenv("MIN_SIGNAL_SIZE_PCT", "5.0"))

# Scalp confirmation buffer (RSI points beyond zone edge):
# LONG confirm when RSI > MOMENTUM_OS + buffer
# SHORT confirm when RSI < MOMENTUM_OB - buffer
SCALP_CONFIRM_RSI_BUFFER = 2
TIMEFRAME_CONFIRM_RSI_BUFFER = {
    "5m": 2,
    "15m": 0,
    "1h": 0,
    "4h": 0,
}
TIMEFRAME_ZONE_TIMEOUT_CANDLES = {
    "5m": 10,
    "15m": 12,
    "1h": 16,
    "4h": 20,
}
TIMEFRAME_DEEPER_RSI_DELTA = {
    "5m": 5,
    "15m": 6,
    "1h": 8,
    "4h": 10,
}
TIMEFRAME_FLAT_MAX_CANDLES = {
    "5m": 5,
    "15m": 6,
    "1h": 8,
    "4h": 10,
}
TIMEFRAME_RESTING_RESET_RSI = {
    "5m": {"long": 32, "short": 68},
    "15m": {"long": 31, "short": 69},
    "1h": {"long": 30, "short": 70},
    "4h": {"long": 30, "short": 70},
}
BASE_MOMENTUM_ENABLED_TFS = ["5m", "15m", "1h", "4h"]
HTF_PULLBACK_ENABLED_TFS = ["4h"]
HTF_PULLBACK_LOOKBACK = {
    "1h": 8,
    "4h": 5,
}
HTF_PULLBACK_RSI_LONG_MAX = {
    "1h": 45,
    "4h": 44,
}
HTF_PULLBACK_RSI_SHORT_MIN = {
    "1h": 55,
    "4h": 56,
}
HTF_PULLBACK_RSI_CONFIRM = 52
ONE_H_RECLAIM_ENABLED = False
ONE_H_RECLAIM_LOOKBACK = 10
ONE_H_RECLAIM_RSI_LONG_MAX = 44
ONE_H_RECLAIM_RSI_SHORT_MIN = 56
ONE_H_RECLAIM_RSI_CONFIRM = 50

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
    "1h": "off",
    "4h": "off",
}
SCALP_COUNTERTREND_MIN_SCORE_BY_TF = {
    "4h": 6,
}

# Win-rate-first quality gates
SCALP_MIN_SCORE_BY_TF = {
    "5m": 3,
    "15m": 3,
    "1h": 2,
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

# Falling-knife / blow-off safety filter for confluence entries
FALLING_KNIFE_FILTER_ENABLED = True
FALLING_KNIFE_LOOKBACK_5M = 6      # 30 min window
FALLING_KNIFE_LOOKBACK_15M = 4     # 60 min window
FALLING_KNIFE_MOVE_PCT_5M = 0.7    # block longs if <= -0.7% without stabilization (shorts vice versa)
FALLING_KNIFE_MOVE_PCT_15M = 1.0   # block longs if <= -1.0% without stabilization (shorts vice versa)

# --- SIGNAL POINTS --------------------------------------------------
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

# --- CONFIRMATION THRESHOLDS ----------------------------------------
STRONG_THRESHOLD  = 3    # Confirmed Confluence (3+ systems)
EXTREME_THRESHOLD = 4    # High-Alpha Confluence (4+ systems)

# --- TIMING ---------------------------------------------------------
POLL_INTERVAL = 1       # Seconds between data fetches
CONFIRMATION_WINDOW = 1800  # 30 min window to aggregate signals
CONFIRMATION_FRESH_WINDOW = 480  # 8 min: confirmations must be recent to count
CONFIRMATION_RSI_EXHAUSTION_BUFFER = 6  # block SHORT if RSI too low, LONG if RSI too high
CONFLUENCE_OPPOSITE_LOCK_SEC = 900  # 15 min opposite-side confluence lock after a fire

# --- FUNDING RATE ALERTS --------------------------------------------
FUNDING_THRESHOLD = 0.0005       # 0.05% - trigger alert above this
FUNDING_CHECK_INTERVAL = 300     # Check every 5 min
FUNDING_COOLDOWN = 3600          # 1 hour cooldown between alerts

# --- VOLUME SPIKE DETECTION -----------------------------------------
VOLUME_SPIKE_MULT = 3.0                   # Alert when vol > 3x average
VOLUME_SPIKE_TIMEFRAMES = ["15m", "1h", "4h"]   # Skip 5m (too noisy)
VOLUME_AVG_PERIOD = 20                    # 20-candle volume SMA

# --- PRICE APPROACHING ALERTS ---------------------------------------
APPROACH_THRESHOLD = 0.002       # 0.2% distance from level
APPROACH_COOLDOWN = 10800          # 3 hour cooldown per level
APPROACH_LEVELS = ["PDH", "PDL", "DO", "PWH", "PWL", "PMH", "PML", "Pump", "Dump", "PumpMax", "DumpMax"]  # Which levels to watch

# --- ADVANCED ALERTS ------------------------------------------------
OI_CHANGE_THRESHOLD = 0.015      # 1.5% change in OI to trigger divergence check
LIQ_SQUEEZE_THRESHOLD = 500000   # $500k in liquidations to trigger squeeze alert
LIQ_ALERT_COOLDOWN = 600         # 10 min cooldown for squeeze alerts

# Order-book liquidity pool alerts (4h/1d context)
LIQ_POOL_ALERT_ENABLED = False
LIQ_POOL_MIN_USD = 100000000           # legacy/global fallback
LIQ_POOL_HUGE_USD_OVERRIDE = 0          # 0 disables near-distance override
LIQ_POOL_MIN_DISTANCE_PCT = 0.25       # legacy/global fallback
LIQ_POOL_ALERT_COOLDOWN = 3600         # legacy/global fallback
LIQ_POOL_BIAS_SCORE_BONUS = 1          # small score tilt toward liquidity pull side
LIQ_POOL_MAX_DISTANCE_ATR_MULT = {
    "5m": 1.0,
    "15m": 1.2,
    "1h": 1.4,
    "4h": 1.5,
    "1d": 2.5,
}

# Multi-timeframe liquidity report controls
LIQ_POOL_REPORT_TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]
LIQ_POOL_MIN_USD_BY_TF = {
    "5m": 30000000,
    "15m": 50000000,
    "1h": 80000000,
    "4h": 120000000,
    "1d": 180000000,
}
LIQ_POOL_MIN_DISTANCE_PCT_BY_TF = {
    "5m": 0.10,
    "15m": 0.15,
    "1h": 0.25,
    "4h": 0.40,
    "1d": 0.70,
}
LIQ_POOL_TARGET_DISTANCE_PCT_BY_TF = {
    "5m": 0.15,
    "15m": 0.30,
    "1h": 0.60,
    "4h": 1.00,
    "1d": 1.80,
}
LIQ_POOL_PROGRESSIVE_MIN_STEP_PCT = 0.10
LIQ_POOL_LEVEL_DEDUP_GAP_PCT = 0.01
LIQ_POOL_AGG_WINDOW_PCT_BY_TF = {
    "5m": 0.02,
    "15m": 0.05,
    "1h": 0.12,
    "4h": 0.25,
    "1d": 0.50,
}
LIQ_POOL_NO_MOVE_RANGE_PCT_1H = 0.8
LIQ_POOL_EXPANSION_PRICE_MOVE_PCT_1H = 1.0
LIQ_POOL_EXPANSION_VOLUME_MULT = 1.8
LIQ_POOL_EXPANSION_BOOK_MULT = 1.12
LIQ_POOL_EXPANSION_COOLDOWN = 1800

# TP-near liquidity confidence for entry messages
TP_LIQUIDITY_MIN_USD = 25000000   # minimum near-TP liquidity to annotate probability
TP_LIQUIDITY_BAND_PCT = 0.12      # "near TP" band size (% around TP level)

# --- MARKET ALERTS (FAST MOVE) --------------------------------------
FAST_MOVE_THRESHOLD = 0.03       # 3% move
FAST_MOVE_WINDOW    = 4          # 4 hours
FAST_MOVE_COOLDOWN  = 14400      # 4 hours

SESSIONS = {
    "ASIA":   {"open": 0.0,  "close": 8.0},
    "LONDON": {"open": 8.0,  "close": 16.0},
    "NY":     {"open": 13.5, "close": 20.0}, # Stock Market: 9:30 AM - 4:00 PM ET
}

NEWS_FILTER_ENABLED = os.getenv("NEWS_FILTER_ENABLED", "true").strip().lower() == "true"
NY_HOLIDAY_FILTER_ENABLED = os.getenv("NY_HOLIDAY_FILTER_ENABLED", "true").strip().lower() == "true"
HIGH_IMPACT_NEWS_BLACKOUT_WINDOWS_UTC = os.getenv("HIGH_IMPACT_NEWS_BLACKOUT_WINDOWS_UTC", "").strip()
TRADING_ECONOMICS_NEWS_ENABLED = os.getenv("TRADING_ECONOMICS_NEWS_ENABLED", "true").strip().lower() == "true"
TRADING_ECONOMICS_API_KEY = os.getenv("TRADING_ECONOMICS_API_KEY", "guest:guest").strip()
TRADING_ECONOMICS_COUNTRIES = os.getenv("TRADING_ECONOMICS_COUNTRIES", "united states").strip()
TRADING_ECONOMICS_MIN_IMPORTANCE = int(os.getenv("TRADING_ECONOMICS_MIN_IMPORTANCE", "3"))
TRADING_ECONOMICS_REFRESH_SEC = int(os.getenv("TRADING_ECONOMICS_REFRESH_SEC", "600"))
TRADING_ECONOMICS_BLOCK_BEFORE_MIN = int(os.getenv("TRADING_ECONOMICS_BLOCK_BEFORE_MIN", "60"))
TRADING_ECONOMICS_BLOCK_AFTER_MIN = int(os.getenv("TRADING_ECONOMICS_BLOCK_AFTER_MIN", "30"))


def _observed_date(year, month, day):
    from datetime import datetime, timedelta
    dt = datetime(year, month, day)
    if dt.weekday() == 5:
        return (dt - timedelta(days=1)).date()
    if dt.weekday() == 6:
        return (dt + timedelta(days=1)).date()
    return dt.date()


def _nth_weekday_of_month(year, month, weekday, n):
    from datetime import datetime, timedelta
    dt = datetime(year, month, 1)
    while dt.weekday() != weekday:
        dt += timedelta(days=1)
    dt += timedelta(days=7 * (n - 1))
    return dt.date()


def _last_weekday_of_month(year, month, weekday):
    from datetime import datetime, timedelta
    if month == 12:
        dt = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        dt = datetime(year, month + 1, 1) - timedelta(days=1)
    while dt.weekday() != weekday:
        dt -= timedelta(days=1)
    return dt.date()


def _easter_date(year):
    # Anonymous Gregorian algorithm.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    from datetime import date
    return date(year, month, day)


def get_us_market_holidays(year):
    from datetime import timedelta
    easter = _easter_date(year)
    return {
        _observed_date(year, 1, 1),                          # New Year
        _nth_weekday_of_month(year, 1, 0, 3),               # MLK
        _nth_weekday_of_month(year, 2, 0, 3),               # Presidents Day
        easter - timedelta(days=2),                         # Good Friday
        _last_weekday_of_month(year, 5, 0),                 # Memorial Day
        _observed_date(year, 6, 19),                        # Juneteenth
        _observed_date(year, 7, 4),                         # Independence Day
        _nth_weekday_of_month(year, 9, 0, 1),               # Labor Day
        _nth_weekday_of_month(year, 11, 3, 4),              # Thanksgiving
        _observed_date(year, 12, 25),                       # Christmas
    }


def is_ny_market_holiday(dt):
    if not NY_HOLIDAY_FILTER_ENABLED:
        return False
    d = dt.date()
    return d in get_us_market_holidays(dt.year)


def get_manual_news_blackouts():
    windows = []
    raw = HIGH_IMPACT_NEWS_BLACKOUT_WINDOWS_UTC
    if not raw:
        return windows
    from datetime import datetime, timezone
    for chunk in raw.split(";"):
        parts = [p.strip() for p in chunk.split("|") if p.strip()]
        if len(parts) < 2:
            continue
        try:
            start = datetime.fromisoformat(parts[0].replace("Z", "+00:00"))
            end = datetime.fromisoformat(parts[1].replace("Z", "+00:00"))
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            else:
                start = start.astimezone(timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            else:
                end = end.astimezone(timezone.utc)
            windows.append({
                "start": start,
                "end": end,
                "label": parts[2] if len(parts) > 2 else "High Impact News",
            })
        except Exception:
            continue
    return windows


def get_active_news_blackout(dt):
    if not NEWS_FILTER_ENABLED:
        return None
    from datetime import timezone
    check_dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    check_dt = check_dt.astimezone(timezone.utc)
    for window in get_manual_news_blackouts():
        if window["start"] <= check_dt <= window["end"]:
            return window
    return None

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

# --- TELEGRAM COMMANDS ----------------------------------------------
COMMAND_POLL_INTERVAL = 2   # Seconds between getUpdates polls
ALERT_BATCH_WINDOW = 10     # Seconds to wait for multiple alerts before batching

# --- REGISTRATION & ONBOARDING --------------------------------------
BITUNIX_REG_LINK = "https://www.bitunix.com/register?vipCode=mrponch"
INVITE_LINK = "https://t.me/+Z0rG8WJK58RlN2Ey"
