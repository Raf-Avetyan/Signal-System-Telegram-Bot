# ─── CTLT Trade Signal Detection ─────────────────────────────

"""
Detects individual trade signals from multiple "virtual" indicators:
  - CTLT_Trader: Channel crossings (L1-L3, S1-S3) — from channels.py
  - CTLT_Momentum_Confirm: Momentum RSI confirmation (1pt)
  - CTLT_RangeTrader_Confirm: Level-based range confirmation (3pts)
  - CTLT_Flow_Confirm: Volume-flow alignment (1pt)
"""

import pandas as pd
import numpy as np
from config import SIGNAL_POINTS


def check_momentum_confirm(df):
    """
    CTLT_Momentum_Confirm — Generates confirmation when RSI crosses
    back from extreme zones, confirming the direction.

    Returns list of signal dicts.
    """
    if len(df) < 3 or "RSI" not in df.columns:
        return []

    signals = []
    curr_rsi = df["RSI"].iloc[-1]
    prev_rsi = df["RSI"].iloc[-2]

    # LONG confirmation: RSI was oversold and now crossing back up above OS
    if prev_rsi <= 30 and curr_rsi > 30:
        signals.append({
            "side":      "LONG",
            "signal":    "L",
            "points":    SIGNAL_POINTS["L"],
            "strength":  "Low",
            "price":     float(df["Close"].iloc[-1]),
            "indicator": "CTLT_Momentum_Confirm",
            "note":      "Confirm",
        })

    # SHORT confirmation: RSI was overbought and now crossing back down below OB
    if prev_rsi >= 70 and curr_rsi < 70:
        signals.append({
            "side":      "SHORT",
            "signal":    "S",
            "points":    SIGNAL_POINTS["S"],
            "strength":  "Low",
            "price":     float(df["Close"].iloc[-1]),
            "indicator": "CTLT_Momentum_Confirm",
            "note":      "Confirm",
        })

    return signals


def check_range_confirm(df, levels):
    """
    CTLT_RangeTrader_Confirm — Range-based confirmation when price
    interacts with key levels while in trend direction.

    Returns list of signal dicts.
    """
    if len(df) < 2 or not levels:
        return []

    signals = []
    close = float(df["Close"].iloc[-1])
    prev_close = float(df["Close"].iloc[-2])

    do = levels.get("DO", 0)
    pdl = levels.get("PDL", 0)
    pdh = levels.get("PDH", 0)

    # LONG confirmation: Close bounces off support zone (near PDL or below DO)
    if close < do and prev_close >= do:
        # Price dropped below DO — range trader sees short confirmation
        signals.append({
            "side":      "SHORT",
            "signal":    "S2",
            "points":    SIGNAL_POINTS["S2"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "CTLT_RangeTrader_Confirm",
            "note":      "Confirm",
        })

    if close > do and prev_close <= do:
        # Price reclaimed DO — range trader sees long confirmation
        signals.append({
            "side":      "LONG",
            "signal":    "L2",
            "points":    SIGNAL_POINTS["L2"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "CTLT_RangeTrader_Confirm",
            "note":      "Confirm",
        })

    return signals


def check_flow_confirm(df):
    """
    CTLT_Flow_Confirm — Volume-weighted flow confirmation.
    Triggers when volume spike aligns with price direction.

    Returns list of signal dicts.
    """
    if len(df) < 10 or "Volume" not in df.columns:
        return []

    signals = []
    curr = df.iloc[-1]
    avg_vol = df["Volume"].iloc[-10:].mean()

    # Volume spike: current volume > 2x average
    if curr["Volume"] < avg_vol * 2.0:
        return signals

    close = float(curr["Close"])
    open_ = float(curr["Open"])

    # Bullish volume spike (close > open with high volume)
    if close > open_:
        signals.append({
            "side":      "LONG",
            "signal":    "L",
            "points":    SIGNAL_POINTS["L"],
            "strength":  "Low",
            "price":     close,
            "indicator": "CTLT_Flow_Confirm",
            "note":      "Confirm",
        })

    # Bearish volume spike (close < open with high volume)
    elif close < open_:
        signals.append({
            "side":      "SHORT",
            "signal":    "S",
            "points":    SIGNAL_POINTS["S"],
            "strength":  "Low",
            "price":     close,
            "indicator": "CTLT_Flow_Confirm",
            "note":      "Confirm",
        })

    return signals
