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
        self.last_processed_ts = None # Cooldown timestamp
        self.last_side = None         # Remember last side for resting buffer

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
            if zone == "OS":
                self.state = "ZONE_ENTRY"
                self.side = "LONG"
                self.entry_price = close
                events.append({"type": "OPEN", "side": "LONG", "price": close})
                events.append({"type": "PREPARE", "side": "LONG", "price": close})

            elif zone == "OB":
                self.state = "ZONE_ENTRY"
                self.side = "SHORT"
                self.entry_price = close
                events.append({"type": "OPEN", "side": "SHORT", "price": close})
                events.append({"type": "PREPARE", "side": "SHORT", "price": close})

        elif self.state == "ZONE_ENTRY":
            # Waiting for zone exit (confirmation) or timeout
            opposite_zone = "OB" if self.side == "LONG" else "OS"

            # FAST CONFIRMATION: Use Raw RSI to detect zone exit immediately
            is_exit = False
            if self.side == "LONG" and raw_rsi > 30:
                is_exit = True
            elif self.side == "SHORT" and raw_rsi < 70:
                is_exit = True

            if is_exit:
                # Zone exit → CONFIRMED
                entry = close
                calc = self._calc_sl_tp(entry, atr_value, self.side)
                events.append({
                    "type":     "CONFIRMED",
                    "side":     self.side,
                    "entry":    entry,
                    **calc,
                })
                
                self.last_side = self.side # Store for RESTING logic
                self.state = "RESTING"    # Move to RESTING to prevent immediate re-entry
                self.last_processed_ts = candle_ts # Update cooldown timestamp
                self.side = None
                self.entry_price = None

            elif zone == opposite_zone:
                # Crossed directly to opposite zone → CLOSED without confirmation
                events.append({
                    "type":  "CLOSED",
                    "side":  self.side,
                    "price": close,
                })
                
                self.last_side = self.side
                self.state = "RESTING" 
                self.last_processed_ts = candle_ts # Update cooldown timestamp
                
                # Switch directly to the new zone's entry if no cooldown on same candle
                if not (candle_ts and candle_ts == self.last_processed_ts):
                    self.side = "SHORT" if opposite_zone == "OB" else "LONG"
                    self.entry_price = close
                    self.state = "ZONE_ENTRY"
                    events.append({"type": "OPEN", "side": self.side, "price": close})
                    events.append({"type": "PREPARE", "side": self.side, "price": close})
                else:
                    self.side = None
                    self.entry_price = None

        elif self.state == "RESTING":
            # Buffer check (Use Raw for Fast Reset)
            # Make it more reactive: exit resting at 32/68 instead of 35/65
            if self.last_side == "SHORT":
                if raw_rsi <= 68:
                    self.state = "IDLE"
                    self.last_side = None
            else: # LONG
                if raw_rsi >= 32:
                    self.state = "IDLE"
                    self.last_side = None

        return events

    def to_dict(self):
        return {
            "state": self.state,
            "side": self.side,
            "entry_price": self.entry_price,
            "last_processed_ts": self.last_processed_ts,
            "last_side": self.last_side
        }

    def from_dict(self, data):
        if not data: return
        self.state = data.get("state", "IDLE")
        self.side = data.get("side")
        self.entry_price = data.get("entry_price")
        self.last_processed_ts = data.get("last_processed_ts")
        self.last_side = data.get("last_side")

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
