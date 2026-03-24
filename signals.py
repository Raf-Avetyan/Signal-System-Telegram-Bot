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
from config import (
    SIGNAL_POINTS,
    MOMENTUM_OB,
    MOMENTUM_OS,
    RSI_DIVERGENCE_ENABLED,
    RSI_DIVERGENCE_LOOKBACK,
    RSI_DIVERGENCE_SEGMENT,
    RSI_DIVERGENCE_MIN_PRICE_DELTA_PCT,
    RSI_DIVERGENCE_MIN_RSI_DELTA,
    RSI_DIVERGENCE_POINTS,
)


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


def check_rsi_divergence(df):
    """
    RSI divergence confirmation:
    - Bullish: price makes lower low, RSI makes higher low.
    - Bearish: price makes higher high, RSI makes lower high.
    """
    if not RSI_DIVERGENCE_ENABLED:
        return []
    if len(df) < max(20, RSI_DIVERGENCE_LOOKBACK) or "RSI" not in df.columns:
        return []

    lookback = int(max(20, RSI_DIVERGENCE_LOOKBACK))
    seg = int(max(6, RSI_DIVERGENCE_SEGMENT))
    min_rsi_delta = float(RSI_DIVERGENCE_MIN_RSI_DELTA)
    min_price_delta_pct = float(RSI_DIVERGENCE_MIN_PRICE_DELTA_PCT)

    sub = df.iloc[-lookback:].copy()
    if len(sub) < seg * 2:
        return []

    older = sub.iloc[:-seg]
    newer = sub.iloc[-seg:]
    if older.empty or newer.empty:
        return []

    signals = []

    # Bullish divergence: lower low in price, higher low in RSI.
    old_low_idx = older["Low"].idxmin()
    new_low_idx = newer["Low"].idxmin()
    old_low_price = float(sub.loc[old_low_idx, "Low"])
    new_low_price = float(sub.loc[new_low_idx, "Low"])
    old_low_rsi = float(sub.loc[old_low_idx, "RSI"])
    new_low_rsi = float(sub.loc[new_low_idx, "RSI"])
    low_drop_pct = ((old_low_price - new_low_price) / old_low_price * 100.0) if old_low_price > 0 else 0.0
    if low_drop_pct >= min_price_delta_pct and (new_low_rsi - old_low_rsi) >= min_rsi_delta:
        signals.append({
            "side": "LONG",
            "signal": "L+",
            "points": int(RSI_DIVERGENCE_POINTS),
            "strength": "Medium",
            "price": float(sub["Close"].iloc[-1]),
            "indicator": "Ponch_RSI_Divergence",
            "note": "Bullish divergence",
        })

    # Bearish divergence: higher high in price, lower high in RSI.
    old_high_idx = older["High"].idxmax()
    new_high_idx = newer["High"].idxmax()
    old_high_price = float(sub.loc[old_high_idx, "High"])
    new_high_price = float(sub.loc[new_high_idx, "High"])
    old_high_rsi = float(sub.loc[old_high_idx, "RSI"])
    new_high_rsi = float(sub.loc[new_high_idx, "RSI"])
    high_rise_pct = ((new_high_price - old_high_price) / old_high_price * 100.0) if old_high_price > 0 else 0.0
    if high_rise_pct >= min_price_delta_pct and (old_high_rsi - new_high_rsi) >= min_rsi_delta:
        signals.append({
            "side": "SHORT",
            "signal": "S+",
            "points": int(RSI_DIVERGENCE_POINTS),
            "strength": "Medium",
            "price": float(sub["Close"].iloc[-1]),
            "indicator": "Ponch_RSI_Divergence",
            "note": "Bearish divergence",
        })

    return signals
