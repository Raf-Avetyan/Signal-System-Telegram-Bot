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
    """

    def __init__(self, timeframe):
        self.timeframe = timeframe
        self.state = "IDLE"
        self.side = None
        self.entry_price = None
        self.last_processed_ts = None # Cooldown timestamp

    def update(self, zone, close, atr_value, candle_ts=None):
        """
        Process new candle data. Returns list of events to act on.

        Events: [{"type": "OPEN"|"PREPARE"|"CONFIRMED"|"CLOSED",
                  "side": ..., "price": ..., ...}]
        """
        events = []

        if self.state == "IDLE":
            # Cooldown check: don't restart on the same candle
            if candle_ts and candle_ts == self.last_processed_ts:
                return []
                
            # Check for zone entry
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
            expected_zone = "OS" if self.side == "LONG" else "OB"
            opposite_zone = "OB" if self.side == "LONG" else "OS"

            if zone == "NEUTRAL":
                # Zone exit → CONFIRMED
                self.state = "IDLE"
                self.last_processed_ts = candle_ts # Update cooldown timestamp
                entry = close
                calc = self._calc_sl_tp(entry, atr_value, self.side)
                events.append({
                    "type":     "CONFIRMED",
                    "side":     self.side,
                    "entry":    entry,
                    **calc,
                })
                self.side = None
                self.entry_price = None

            elif zone == opposite_zone:
                # Crossed directly to opposite zone → CLOSED without confirmation
                self.state = "IDLE"
                self.last_processed_ts = candle_ts # Update cooldown timestamp
                events.append({
                    "type":  "CLOSED",
                    "side":  self.side,
                    "price": close,
                })
                
                # Cooldown check before switching
                if candle_ts and candle_ts == self.last_processed_ts:
                    self.side = None
                    self.entry_price = None
                else:
                    # Switch directly to the new zone's entry
                    self.side = "SHORT" if opposite_zone == "OB" else "LONG"
                    self.entry_price = close
                    self.state = "ZONE_ENTRY"
                    events.append({"type": "OPEN", "side": self.side, "price": close})
                    events.append({"type": "PREPARE", "side": self.side, "price": close})

        return events

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
