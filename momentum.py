# ─── Ponch Momentum Scalp System ──────────────────────────────

import pandas as pd
import numpy as np
from config import (
    MOMENTUM_RSI_LEN, MOMENTUM_SMOOTH,
    MOMENTUM_OB, MOMENTUM_OS,
    TIMEFRAME_MOMENTUM_THRESHOLDS,
    SCALP_CONFIRM_RSI_BUFFER,
    TIMEFRAME_CONFIRM_RSI_BUFFER,
    TIMEFRAME_ZONE_TIMEOUT_CANDLES,
    TIMEFRAME_DEEPER_RSI_DELTA,
    TIMEFRAME_FLAT_MAX_CANDLES,
    TIMEFRAME_RESTING_RESET_RSI,
    BASE_MOMENTUM_ENABLED_TFS,
    HTF_PULLBACK_ENABLED_TFS,
    HTF_PULLBACK_LOOKBACK,
    HTF_PULLBACK_RSI_LONG_MAX,
    HTF_PULLBACK_RSI_SHORT_MIN,
    HTF_PULLBACK_RSI_CONFIRM,
    ONE_H_RECLAIM_ENABLED,
    ONE_H_RECLAIM_LOOKBACK,
    ONE_H_RECLAIM_RSI_LONG_MAX,
    ONE_H_RECLAIM_RSI_SHORT_MIN,
    ONE_H_RECLAIM_RSI_CONFIRM,
    SL_ATR_MULT, TP1_ATR_MULT, TP2_ATR_MULT, TP3_ATR_MULT,
    TIMEFRAME_RISK_MULTIPLIERS,
    TIMEFRAME_PROFILES,
)


def rsi(series, length):
    """Relative Strength Index."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/length, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1/length, min_periods=length).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_momentum(df):
    """
    Calculate momentum oscillator for a given timeframe DataFrame.

    Adds columns:
        RSI, MomentumSmooth, MomentumZone ('OB', 'OS', 'NEUTRAL')
    """
    df = df.copy()

    df["RSI"] = rsi(df["Close"], MOMENTUM_RSI_LEN)
    df["MomentumSmooth"] = df["RSI"].ewm(span=MOMENTUM_SMOOTH, adjust=False).mean()

    # Zone classification
    def classify(val):
        if pd.isna(val):
            return "NEUTRAL"
        if val >= MOMENTUM_OB:
            return "OB"
        elif val <= MOMENTUM_OS:
            return "OS"
        return "NEUTRAL"

    df["MomentumZone"] = df["MomentumSmooth"].apply(classify)

    return df


def get_momentum_thresholds(timeframe):
    cfg = TIMEFRAME_MOMENTUM_THRESHOLDS.get(timeframe, {})
    return float(cfg.get("ob", MOMENTUM_OB)), float(cfg.get("os", MOMENTUM_OS))


def classify_momentum_zone(value, timeframe):
    ob, os = get_momentum_thresholds(timeframe)
    if pd.isna(value):
        return "NEUTRAL"
    if value >= ob:
        return "OB"
    if value <= os:
        return "OS"
    return "NEUTRAL"


def check_htf_pullback_entry(df, timeframe):
    if timeframe not in HTF_PULLBACK_ENABLED_TFS:
        return None
    if df is None or df.empty or len(df) < 20:
        return None
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if "RSI" not in df.columns or "EMA2" not in df.columns or "EMA3" not in df.columns:
        return None

    lookback = int(HTF_PULLBACK_LOOKBACK.get(timeframe, 6))
    confirm_rsi = float(HTF_PULLBACK_RSI_CONFIRM)
    long_max = float(HTF_PULLBACK_RSI_LONG_MAX.get(timeframe, 45))
    short_min = float(HTF_PULLBACK_RSI_SHORT_MIN.get(timeframe, 55))

    recent = df.iloc[-lookback:]
    curr_rsi = float(curr["RSI"])
    prev_rsi = float(prev["RSI"])
    close = float(curr["Close"])
    ema2 = float(curr["EMA2"])
    ema3 = float(curr["EMA3"])
    atr_val = float(curr.get("ATR", 0) or 0)
    if atr_val <= 0:
        return None

    bullish_trend = close > ema2 > ema3
    bearish_trend = close < ema2 < ema3

    if bullish_trend:
        dipped = float(recent["RSI"].min()) <= long_max
        recovered = prev_rsi <= confirm_rsi and curr_rsi > confirm_rsi and close >= ema2
        if dipped and recovered:
            tracker = ScalpTracker(timeframe)
            calc = tracker._calc_sl_tp(close, atr_val, "LONG")
            return {"type": "CONFIRMED", "side": "LONG", "entry": close, **calc, "trigger": "HTF_PULLBACK"}

    if bearish_trend:
        popped = float(recent["RSI"].max()) >= short_min
        rejected = prev_rsi >= confirm_rsi and curr_rsi < confirm_rsi and close <= ema2
        if popped and rejected:
            tracker = ScalpTracker(timeframe)
            calc = tracker._calc_sl_tp(close, atr_val, "SHORT")
            return {"type": "CONFIRMED", "side": "SHORT", "entry": close, **calc, "trigger": "HTF_PULLBACK"}

    return None


def check_one_h_reclaim_entry(df, timeframe):
    if timeframe != "1h" or not ONE_H_RECLAIM_ENABLED:
        return None
    if df is None or df.empty or len(df) < max(20, ONE_H_RECLAIM_LOOKBACK + 2):
        return None
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if "RSI" not in df.columns or "EMA2" not in df.columns or "EMA3" not in df.columns:
        return None

    lookback = int(ONE_H_RECLAIM_LOOKBACK)
    recent = df.iloc[-lookback:]
    curr_rsi = float(curr["RSI"])
    prev_rsi = float(prev["RSI"])
    close = float(curr["Close"])
    prev_close = float(prev["Close"])
    ema2 = float(curr["EMA2"])
    ema3 = float(curr["EMA3"])
    atr_val = float(curr.get("ATR", 0) or 0)
    if atr_val <= 0:
        return None

    bullish_trend = ema2 > ema3
    bearish_trend = ema2 < ema3

    if bullish_trend:
        dipped = float(recent["RSI"].min()) <= float(ONE_H_RECLAIM_RSI_LONG_MAX)
        reclaim = prev_close <= float(prev["EMA2"]) and close > ema2
        recover = prev_rsi <= float(ONE_H_RECLAIM_RSI_CONFIRM) and curr_rsi > float(ONE_H_RECLAIM_RSI_CONFIRM)
        if dipped and reclaim and recover:
            tracker = ScalpTracker(timeframe)
            calc = tracker._calc_sl_tp(close, atr_val, "LONG")
            return {"type": "CONFIRMED", "side": "LONG", "entry": close, **calc, "trigger": "ONE_H_RECLAIM"}

    if bearish_trend:
        popped = float(recent["RSI"].max()) >= float(ONE_H_RECLAIM_RSI_SHORT_MIN)
        reject = prev_close >= float(prev["EMA2"]) and close < ema2
        recover = prev_rsi >= float(ONE_H_RECLAIM_RSI_CONFIRM) and curr_rsi < float(ONE_H_RECLAIM_RSI_CONFIRM)
        if popped and reject and recover:
            tracker = ScalpTracker(timeframe)
            calc = tracker._calc_sl_tp(close, atr_val, "SHORT")
            return {"type": "CONFIRMED", "side": "SHORT", "entry": close, **calc, "trigger": "ONE_H_RECLAIM"}

    return None


class ScalpTracker:
    """
    Tracks the momentum scalp state machine for one timeframe.

    States:
        IDLE       → Waiting for zone entry
        ZONE_ENTRY → Inside momentum zone (OPEN)
        PREPARED   → Prepare message sent, waiting for exit
        CONFIRMED  → Entry confirmed on zone exit
        CLOSED     → Window closed without confirmation
        RESTING    → Cooldown period, waiting for RSI buffer (65/35)
    """

    def __init__(self, timeframe):
        self.timeframe = timeframe
        self.state = "IDLE"
        self.side = None
        self.entry_price = None
        self.last_processed_ts = None     # Cooldown timestamp
        self.last_side = None             # Remember last side for resting buffer
        self.zone_entry_candles = 0       # Candles spent in ZONE_ENTRY (for timeout)
        self.zone_entry_candle_ts = None  # Candle that triggered ZONE_ENTRY
        self.last_zone_candle_ts = None   # Last candle processed in ZONE_ENTRY (dedup)
        self.prepare_sent = False         # PREPARE: RSI reversing while still in zone
        self.prev_raw_rsi = None          # Previous candle's raw RSI (for reversal detection)
        self.entry_rsi = None             # RSI at zone entry (for deeper-into-zone detection)
        self.flat_candles = 0             # Consecutive candles with < 2pt RSI movement

    def update(self, zone, close, atr_value, candle_ts=None, rsi_raw=None, rsi_smooth=None):
        """
        Process new candle data. Returns list of events to act on.

        Events: [{"type": "OPEN"|"PREPARE"|"CONFIRMED"|"CLOSED",
                  "side": ..., "price": ..., ...}]
        """
        events = []

        # Current smoothed RSI value (used for Entry/Noise Filtering)
        mom_rsi = rsi_smooth if rsi_smooth is not None else 50
        # Current raw RSI value (used for Fast Confirmation/Reset)
        raw_rsi = rsi_raw if rsi_raw is not None else mom_rsi
        tf_ob, tf_os = get_momentum_thresholds(self.timeframe)
        timeout_candles = int(TIMEFRAME_ZONE_TIMEOUT_CANDLES.get(self.timeframe, 10))
        deeper_delta = float(TIMEFRAME_DEEPER_RSI_DELTA.get(self.timeframe, 5))
        flat_limit = int(TIMEFRAME_FLAT_MAX_CANDLES.get(self.timeframe, 5))
        rest_cfg = TIMEFRAME_RESTING_RESET_RSI.get(self.timeframe, {})
        rest_long = float(rest_cfg.get("long", tf_os + 2))
        rest_short = float(rest_cfg.get("short", tf_ob - 2))

        if self.state == "IDLE":
            # Cooldown check: don't restart on the same candle
            if candle_ts and candle_ts == self.last_processed_ts:
                return []

            # Check for zone entry (Use Smoothed for Entry to filter noise)
            # OS (RSI < 30, oversold) → LONG  |  OB (RSI > 70, overbought) → SHORT
            if zone == "OS":
                self.state = "ZONE_ENTRY"
                self.side = "LONG"
                self.entry_price = close
                self.zone_entry_candles = 0
                self.zone_entry_candle_ts = candle_ts
                self.last_zone_candle_ts = candle_ts
                self.prepare_sent = False
                self.prev_raw_rsi = raw_rsi
                self.entry_rsi = raw_rsi
                self.flat_candles = 0
                events.append({"type": "OPEN", "side": "LONG", "price": close})

            elif zone == "OB":
                self.state = "ZONE_ENTRY"
                self.side = "SHORT"
                self.entry_price = close
                self.zone_entry_candles = 0
                self.zone_entry_candle_ts = candle_ts
                self.last_zone_candle_ts = candle_ts
                self.prepare_sent = False
                self.prev_raw_rsi = raw_rsi
                self.entry_rsi = raw_rsi
                self.flat_candles = 0
                events.append({"type": "OPEN", "side": "SHORT", "price": close})

        elif self.state == "ZONE_ENTRY":
            # LONG was in OS (RSI < 30), opposite zone is OB
            # SHORT was in OB (RSI > 70), opposite zone is OS
            opposite_zone = "OB" if self.side == "LONG" else "OS"

            # Don't evaluate on the same candle that triggered zone entry
            if candle_ts and candle_ts == self.zone_entry_candle_ts:
                return events

            # Dedup: only process each new candle once (bot polls every 5s)
            if candle_ts and candle_ts == self.last_zone_candle_ts:
                return events
            self.last_zone_candle_ts = candle_ts
            self.zone_entry_candles += 1

            # --- Helper to reset state ---
            def _reset(next_state="RESTING"):
                self.last_side = self.side
                self.last_processed_ts = candle_ts
                self.state = next_state
                self.side = None
                self.entry_price = None
                self.zone_entry_candles = 0
                self.zone_entry_candle_ts = None
                self.last_zone_candle_ts = None
                self.prepare_sent = False
                self.prev_raw_rsi = None
                self.entry_rsi = None
                self.flat_candles = 0

            confirm_buffer = TIMEFRAME_CONFIRM_RSI_BUFFER.get(self.timeframe, SCALP_CONFIRM_RSI_BUFFER)

            # 1.5 FAST CONFIRM: if reversal happens immediately after OPEN,
            # confirm on first follow-up candle without extra buffer.
            # This prevents missing valid quick bounces/rejections.
            if self.zone_entry_candles <= 1:
                fast_confirm = (
                    (self.side == "LONG" and raw_rsi > tf_os) or
                    (self.side == "SHORT" and raw_rsi < tf_ob)
                )
                if fast_confirm:
                    entry = close
                    calc = self._calc_sl_tp(entry, atr_value, self.side)
                    events.append({"type": "CONFIRMED", "side": self.side, "entry": entry, **calc})
                    _reset()

            # 1. TIMEOUT: close stale window after 10 candles
            if self.state == "ZONE_ENTRY" and self.zone_entry_candles > timeout_candles:
                events.append({"type": "CLOSED", "side": self.side, "price": close})
                _reset()

            # 2. CONFIRMED: RSI exits zone with buffer for stronger confirmation
            #    LONG (was OS < 30): RSI rises above (30 + buffer)
            #    SHORT (was OB > 70): RSI drops below (70 - buffer)
            elif self.state == "ZONE_ENTRY" and (
                (self.side == "LONG" and raw_rsi > (tf_os + confirm_buffer)) or
                (self.side == "SHORT" and raw_rsi < (tf_ob - confirm_buffer))
            ):
                entry = close
                calc = self._calc_sl_tp(entry, atr_value, self.side)
                events.append({"type": "CONFIRMED", "side": self.side, "entry": entry, **calc})
                _reset()

            # 3. OPPOSITE ZONE: crossed directly → CLOSED
            elif self.state == "ZONE_ENTRY" and zone == opposite_zone:
                events.append({"type": "CLOSED", "side": self.side, "price": close})
                _reset()

            else:
                # 4. CLOSE: RSI going DEEPER into zone (getting worse)
                #    LONG (OS): RSI dropped 5+ pts below entry RSI (e.g. 25 → 20)
                #    SHORT (OB): RSI rose 5+ pts above entry RSI (e.g. 75 → 80)
                if self.entry_rsi is not None:
                    deeper = False
                    if self.side == "LONG" and raw_rsi <= self.entry_rsi - deeper_delta:
                        deeper = True
                    elif self.side == "SHORT" and raw_rsi >= self.entry_rsi + deeper_delta:
                        deeper = True

                    if deeper:
                        events.append({"type": "CLOSED", "side": self.side, "price": close})
                        _reset()
                        self.prev_raw_rsi = raw_rsi
                        return events

                # 5. CLOSE: RSI flat too long (< 2pt movement for 5 candles)
                if self.prev_raw_rsi is not None:
                    if abs(raw_rsi - self.prev_raw_rsi) < 2:
                        self.flat_candles += 1
                    else:
                        self.flat_candles = 0

                    if self.flat_candles >= flat_limit:
                        events.append({"type": "CLOSED", "side": self.side, "price": close})
                        _reset()
                        self.prev_raw_rsi = raw_rsi
                        return events

                # 6. PREPARE: RSI starting to reverse but STILL inside zone
                #    LONG (OS): RSI going UP (raw > prev) while still ≤ 30
                #    SHORT (OB): RSI going DOWN (raw < prev) while still ≥ 70
                if not self.prepare_sent and self.prev_raw_rsi is not None:
                    reversing = False
                    if self.side == "LONG" and raw_rsi > self.prev_raw_rsi and raw_rsi <= tf_os:
                        reversing = True
                    elif self.side == "SHORT" and raw_rsi < self.prev_raw_rsi and raw_rsi >= tf_ob:
                        reversing = True

                    if reversing:
                        self.prepare_sent = True
                        events.append({"type": "PREPARE", "side": self.side, "price": close})

                # Update prev RSI for next candle comparison
                self.prev_raw_rsi = raw_rsi

        elif self.state == "RESTING":
            # Buffer check (Use Raw for Fast Reset)
            # LONG was in OS (RSI < 30): rest until RSI recovers to neutral (≥ 32)
            # SHORT was in OB (RSI > 70): rest until RSI drops to neutral (≤ 68)
            if self.last_side == "LONG":
                if raw_rsi >= rest_long:
                    self.state = "IDLE"
                    self.last_side = None
            else:  # SHORT
                if raw_rsi <= rest_short:
                    self.state = "IDLE"
                    self.last_side = None

        return events

    def to_dict(self):
        return {
            "state": self.state,
            "side": self.side,
            "entry_price": self.entry_price,
            "last_processed_ts": self.last_processed_ts,
            "last_side": self.last_side,
            "zone_entry_candles": self.zone_entry_candles,
            "zone_entry_candle_ts": self.zone_entry_candle_ts,
            "last_zone_candle_ts": self.last_zone_candle_ts,
            "prepare_sent": self.prepare_sent,
            "prev_raw_rsi": self.prev_raw_rsi,
            "entry_rsi": self.entry_rsi,
            "flat_candles": self.flat_candles,
        }

    def from_dict(self, data):
        if not data: return
        self.state = data.get("state", "IDLE")
        self.side = data.get("side")
        self.entry_price = data.get("entry_price")
        self.last_processed_ts = data.get("last_processed_ts")
        self.last_side = data.get("last_side")
        self.zone_entry_candles = data.get("zone_entry_candles", 0)
        self.zone_entry_candle_ts = data.get("zone_entry_candle_ts")
        self.last_zone_candle_ts = data.get("last_zone_candle_ts")
        self.prepare_sent = data.get("prepare_sent", False)
        self.prev_raw_rsi = data.get("prev_raw_rsi")
        self.entry_rsi = data.get("entry_rsi")
        self.flat_candles = data.get("flat_candles", 0)

    def _calc_sl_tp(self, entry, atr_val, side):
        """Calculate SL and 3 TP levels based on ATR."""
        if pd.isna(atr_val) or atr_val == 0:
            atr_val = entry * 0.007  # Fallback: 0.7% of price

        tf_cfg = TIMEFRAME_RISK_MULTIPLIERS.get(self.timeframe, {})
        sl_mult = tf_cfg.get("sl", SL_ATR_MULT)
        tp1_mult = tf_cfg.get("tp1", TP1_ATR_MULT)
        tp2_mult = tf_cfg.get("tp2", TP2_ATR_MULT)
        tp3_mult = tf_cfg.get("tp3", TP3_ATR_MULT)

        if side == "LONG":
            sl  = entry - atr_val * sl_mult
            tp1 = entry + atr_val * tp1_mult
            tp2 = entry + atr_val * tp2_mult
            tp3 = entry + atr_val * tp3_mult
        else:  # SHORT
            sl  = entry + atr_val * sl_mult
            tp1 = entry - atr_val * tp1_mult
            tp2 = entry - atr_val * tp2_mult
            tp3 = entry - atr_val * tp3_mult

        return {"sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3}

def detect_trend(df_1h):
    """
    Detect trend bias from EMA structure.
    Returns one of:
      - "Trending Bullish" / "Trending Bearish" for fully stacked trends
      - "Bullish" / "Bearish" for partial directional bias
      - "Ranging" otherwise
    """
    if len(df_1h) < 200:
        return "Ranging"

    close = df_1h["Close"]
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema100 = close.ewm(span=100, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()

    curr_p = close.iloc[-1]
    e50 = ema50.iloc[-1]
    e100 = ema100.iloc[-1]
    e200 = ema200.iloc[-1]

    # Bullish: Price > all EMAs and EMAs are stacked (50 > 100 > 200)
    if curr_p > e50 and e50 > e100 and e100 > e200:
        return "Trending Bullish"

    # Bearish: Price < all EMAs and EMAs are stacked (50 < 100 < 200)
    if curr_p < e50 and e50 < e100 and e100 < e200:
        return "Trending Bearish"

    # Partial directional bias: enough to block counter-trend confluence/scalp entries.
    if curr_p > e50 and e50 > e100:
        return "Bullish"
    if curr_p < e50 and e50 < e100:
        return "Bearish"

    # Otherwise Ranging
    return "Ranging"
