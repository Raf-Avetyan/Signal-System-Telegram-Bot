# ─── Ponch Momentum Scalp System ──────────────────────────────

import pandas as pd
import numpy as np
from config import (
    MOMENTUM_RSI_LEN, MOMENTUM_SMOOTH,
    MOMENTUM_OB, MOMENTUM_OS,
    SL_ATR_MULT, TP1_ATR_MULT, TP2_ATR_MULT, TP3_ATR_MULT,
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

            # Increment candle counter only on new candles (for timeout)
            if candle_ts and candle_ts != self.last_zone_candle_ts:
                self.last_zone_candle_ts = candle_ts
                self.zone_entry_candles += 1

                # Update prev_raw_rsi and flat counter once per new candle
                if self.prev_raw_rsi is not None:
                    if abs(raw_rsi - self.prev_raw_rsi) < 2:
                        self.flat_candles += 1
                    else:
                        self.flat_candles = 0
                self.prev_raw_rsi = raw_rsi

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

            # 1. TIMEOUT: close stale window after 10 candles
            if self.zone_entry_candles > 10:
                events.append({"type": "CLOSED", "side": self.side, "price": close})
                _reset()

            # 2. CONFIRMED: RSI exits the zone
            #    LONG (was OS < 30): RSI rises above 30
            #    SHORT (was OB > 70): RSI drops below 70
            elif (self.side == "LONG" and raw_rsi > MOMENTUM_OS) or \
                 (self.side == "SHORT" and raw_rsi < MOMENTUM_OB):
                entry = close
                calc = self._calc_sl_tp(entry, atr_value, self.side)
                events.append({"type": "CONFIRMED", "side": self.side, "entry": entry, **calc})
                _reset()

            # 3. OPPOSITE ZONE: crossed directly → CLOSED
            elif zone == opposite_zone:
                events.append({"type": "CLOSED", "side": self.side, "price": close})
                _reset()

            else:
                # 4. CLOSE: RSI going DEEPER into zone (getting worse)
                #    LONG (OS): RSI dropped 5+ pts below entry RSI (e.g. 25 → 20)
                #    SHORT (OB): RSI rose 5+ pts above entry RSI (e.g. 75 → 80)
                if self.entry_rsi is not None:
                    deeper = False
                    if self.side == "LONG" and raw_rsi <= self.entry_rsi - 5:
                        deeper = True
                    elif self.side == "SHORT" and raw_rsi >= self.entry_rsi + 5:
                        deeper = True

                    if deeper:
                        events.append({"type": "CLOSED", "side": self.side, "price": close})
                        _reset()
                        return events

                # 5. CLOSE: RSI flat too long (< 2pt movement for 5 consecutive candles)
                if self.flat_candles >= 5:
                    events.append({"type": "CLOSED", "side": self.side, "price": close})
                    _reset()
                    return events

                # 6. PREPARE: RSI starting to reverse but STILL inside zone
                #    LONG (OS): RSI going UP while still ≤ 30
                #    SHORT (OB): RSI going DOWN while still ≥ 70
                #    Uses prev_raw_rsi (updated once per candle above)
                if not self.prepare_sent and self.prev_raw_rsi is not None:
                    reversing = False
                    if self.side == "LONG" and raw_rsi > self.prev_raw_rsi and raw_rsi <= MOMENTUM_OS:
                        reversing = True
                    elif self.side == "SHORT" and raw_rsi < self.prev_raw_rsi and raw_rsi >= MOMENTUM_OB:
                        reversing = True

                    if reversing:
                        self.prepare_sent = True
                        events.append({"type": "PREPARE", "side": self.side, "price": close})

        elif self.state == "RESTING":
            # Buffer check (Use Raw for Fast Reset)
            # LONG was in OS (RSI < 30): rest until RSI recovers to neutral (≥ 32)
            # SHORT was in OB (RSI > 70): rest until RSI drops to neutral (≤ 68)
            if self.last_side == "LONG":
                if raw_rsi >= 32:
                    self.state = "IDLE"
                    self.last_side = None
            else:  # SHORT
                if raw_rsi <= 68:
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

        if side == "LONG":
            sl  = entry - atr_val * SL_ATR_MULT
            tp1 = entry + atr_val * TP1_ATR_MULT
            tp2 = entry + atr_val * TP2_ATR_MULT
            tp3 = entry + atr_val * TP3_ATR_MULT
        else:  # SHORT
            sl  = entry + atr_val * SL_ATR_MULT
            tp1 = entry - atr_val * TP1_ATR_MULT
            tp2 = entry - atr_val * TP2_ATR_MULT
            tp3 = entry - atr_val * TP3_ATR_MULT

        return {"sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3}

def detect_trend(df_1h):
    """
    Detect the macro trend based on 1h timeframe prices relative to EMAs.
    Returns: "Trending Bullish", "Trending Bearish", or "Ranging"
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
    elif curr_p < e50 and e50 < e100 and e100 < e200:
        return "Trending Bearish"

    # Otherwise Ranging
    return "Ranging"
