# ─── CTLT EMA Channels ───────────────────────────────────────

import pandas as pd
import numpy as np
from config import (
    EMA1_LEN, EMA2_LEN, EMA3_LEN,
    ATR_LEN,
    MULT_INNER, MULT_MID, MULT_OUTER,
    SIGNAL_POINTS,
)


def ema(series, length):
    """Exponential Moving Average."""
    return series.ewm(span=length, adjust=False).mean()


def atr(df, length):
    """Average True Range."""
    high_low   = df["High"] - df["Low"]
    high_close = np.abs(df["High"] - df["Close"].shift(1))
    low_close  = np.abs(df["Low"] - df["Close"].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(length).mean()


def calculate_channels(df):
    """
    Calculate EMA channels with ATR-based bands.

    Adds columns to df:
        EMA1, EMA2, EMA3, ATR,
        InnerUp, InnerDn,
        MidUp, MidDn,
        OuterUp, OuterDn
    """
    df = df.copy()

    df["EMA1"] = ema(df["Close"], EMA1_LEN)
    df["EMA2"] = ema(df["Close"], EMA2_LEN)
    df["EMA3"] = ema(df["Close"], EMA3_LEN)
    df["ATR"]  = atr(df, ATR_LEN)

    # Inner Channel (EMA9 ± ATR × 1.0)
    df["InnerUp"] = df["EMA1"] + df["ATR"] * MULT_INNER
    df["InnerDn"] = df["EMA1"] - df["ATR"] * MULT_INNER

    # Mid Channel (EMA21 ± ATR × 2.5)
    df["MidUp"] = df["EMA2"] + df["ATR"] * MULT_MID
    df["MidDn"] = df["EMA2"] - df["ATR"] * MULT_MID

    # Outer Channel (EMA55 ± ATR × 5.0)
    df["OuterUp"] = df["EMA3"] + df["ATR"] * MULT_OUTER
    df["OuterDn"] = df["EMA3"] - df["ATR"] * MULT_OUTER

    return df


def check_channel_signals(df):
    """
    Check for trade signals based on channel crossings on the latest candle.

    Returns list of signal events:
    [{"side": "LONG", "signal": "L2", "points": 3, "strength": ..., "price": ...}, ...]
    """
    if len(df) < 2:
        return []

    signals = []
    curr = df.iloc[-1]
    prev = df.iloc[-2]

    close = curr["Close"]
    prev_close = prev["Close"]

    # ─── LONG signals (price crossing DOWN through bands) ────
    # L1: Close crosses below InnerDn
    if close <= curr["InnerDn"] and prev_close > prev["InnerDn"]:
        signals.append({
            "side":      "LONG",
            "signal":    "L1",
            "points":    SIGNAL_POINTS["L1"],
            "strength":  "Medium",
            "price":     close,
            "indicator": "CTLT_Trader",
            "note":      "L1 confirmation",
        })

    # L2: Close crosses below MidDn
    if close <= curr["MidDn"] and prev_close > prev["MidDn"]:
        signals.append({
            "side":      "LONG",
            "signal":    "L2",
            "points":    SIGNAL_POINTS["L2"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "CTLT_Trader",
            "note":      "L2 confirmation",
        })

    # L3: Close crosses below OuterDn
    if close <= curr["OuterDn"] and prev_close > prev["OuterDn"]:
        signals.append({
            "side":      "LONG",
            "signal":    "L3",
            "points":    SIGNAL_POINTS["L3"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "CTLT_Trader",
            "note":      "L3",
        })

    # ─── SHORT signals (price crossing UP through bands) ─────
    # S1: Close crosses above InnerUp
    if close >= curr["InnerUp"] and prev_close < prev["InnerUp"]:
        signals.append({
            "side":      "SHORT",
            "signal":    "S1",
            "points":    SIGNAL_POINTS["S1"],
            "strength":  "Medium",
            "price":     close,
            "indicator": "CTLT_Trader",
            "note":      "S1 confirmation",
        })

    # S2: Close crosses above MidUp
    if close >= curr["MidUp"] and prev_close < prev["MidUp"]:
        signals.append({
            "side":      "SHORT",
            "signal":    "S2",
            "points":    SIGNAL_POINTS["S2"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "CTLT_Trader",
            "note":      "S2 confirmation",
        })

    # S3: Close crosses above OuterUp
    if close >= curr["OuterUp"] and prev_close < prev["OuterUp"]:
        signals.append({
            "side":      "SHORT",
            "signal":    "S3",
            "points":    SIGNAL_POINTS["S3"],
            "strength":  "Strong",
            "price":     close,
            "indicator": "CTLT_Trader",
            "note":      "S3",
        })

    return signals
