# ─── Ponch Trade Signal Detection ─────────────────────────────

"""
Detects individual trade signals from multiple "virtual" indicators:
  - Ponch_Trader: Channel crossings (L1-L3, S1-S3) — from channels.py
  - Ponch_Momentum_Confirm: Momentum RSI confirmation (1pt)
  - Ponch_RangeTrader_Confirm: Level-based range confirmation (3pts)
  - Ponch_Flow_Confirm: Volume-flow alignment (1pt)
"""

import pandas as pd
import numpy as np
from config import SIGNAL_POINTS, MOMENTUM_OB, MOMENTUM_OS


def check_momentum_confirm(df):
    """
    Ponch_Momentum_Confirm — Generates confirmation when RSI crosses
    back from extreme zones, confirming the direction.

    Returns list of signal dicts.
    """
    if len(df) < 3 or "RSI" not in df.columns:
        return []

    signals = []
    curr_rsi = df["RSI"].iloc[-1]
    prev_rsi = df["RSI"].iloc[-2]

    # LONG confirmation: RSI was oversold and now crossing back up above OS
    if prev_rsi <= MOMENTUM_OS and curr_rsi > MOMENTUM_OS:
        signals.append({
            "side":      "LONG",
            "signal":    "L",
            "points":    SIGNAL_POINTS["L"],
            "strength":  "Low",
            "price":     float(df["Close"].iloc[-1]),
            "indicator": "Ponch_Momentum_Confirm",
            "note":      "Confirm",
        })

    # SHORT confirmation: RSI was overbought and now crossing back down below OB
    if prev_rsi >= MOMENTUM_OB and curr_rsi < MOMENTUM_OB:
        signals.append({
            "side":      "SHORT",
            "signal":    "S",
            "points":    SIGNAL_POINTS["S"],
            "strength":  "Low",
            "price":     float(df["Close"].iloc[-1]),
            "indicator": "Ponch_Momentum_Confirm",
            "note":      "Confirm",
        })

    return signals


def check_range_confirm(df, levels):
    """
    Ponch_RangeTrader_Confirm — Range-based confirmation when price
    interacts with key levels while in trend direction.

    Returns list of signal dicts.
    """
    if len(df) < 2 or not levels:
        return []

    signals = []
    close = float(df["Close"].iloc[-1])
    prev_close = float(df["Close"].iloc[-2])

    do = levels.get("DO", 0)

    # LONG confirmation: Close reclaim DO
    if close > do and prev_close <= do:
        signals.append({
            "side":      "LONG",
            "signal":    "L++",
            "points":    SIGNAL_POINTS["L++"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "Ponch_RangeTrader_Confirm",
            "note":      "Confirm",
        })

    # SHORT confirmation: Close drop below DO
    if close < do and prev_close >= do:
        signals.append({
            "side":      "SHORT",
            "signal":    "S++",
            "points":    SIGNAL_POINTS["S++"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "Ponch_RangeTrader_Confirm",
            "note":      "Confirm",
        })

    return signals


def check_flow_confirm(df):
    """
    Ponch_Flow_Confirm — Volume-weighted flow confirmation.
    Triggers when volume spike aligns with price direction.

    Returns list of signal dicts.
    """
    if len(df) < 10 or "Volume" not in df.columns:
        return []

    signals = []
    curr = df.iloc[-1]
    # Volume spike: current volume > 2x average (exclude current candle from average)
    avg_vol = df["Volume"].iloc[-11:-1].mean()
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
            "indicator": "Ponch_Flow_Confirm",
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
            "indicator": "Ponch_Flow_Confirm",
            "note":      "Confirm",
        })

    return signals
