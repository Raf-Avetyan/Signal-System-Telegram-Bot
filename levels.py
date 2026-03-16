# ─── Ponch Levels & Volatility Zones ──────────────────────────

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from config import ADR_LEN, SWEEP_POINTS, SWEEP_STRENGTH, VOL_ZONE_POINTS, VOL_ZONE_STRENGTH


def calculate_levels(daily_df, weekly_df=None, monthly_df=None):
    """
    Calculate key price levels from daily/weekly/monthly data.

    Returns dict with all levels:
        DO, PDH, PDL, PWH, PWL, PMH, PML, WO, MO,
        Pump, Dump, PumpMax, DumpMax,
        Resistance, Support, Volatility, CriticalHigh, CriticalLow
    """
    levels = {}

    if daily_df.empty:
        return levels

    # Get current UTC date to anchor "Today" vs "Yesterday"
    now_utc = datetime.now(timezone.utc).date()

    # 1. DAILY LEVELS (DO, PDH, PDL)
    # Find today's candle
    today_df = daily_df[daily_df.index.date == now_utc]
    if not today_df.empty:
        # Today exists in data
        levels["DO"] = float(today_df["Open"].iloc[0])
        # History is everything BEFORE today
        history = daily_df[daily_df.index.date < now_utc]
        if not history.empty:
            prev_day = history.iloc[-1]
            levels["PDH"] = float(prev_day["High"])
            levels["PDL"] = float(prev_day["Low"])
            levels["PD_Date"] = prev_day.name.strftime("%d.%m.%Y")
        else:
            levels["PDH"] = levels["PDL"] = levels["DO"]
    else:
        # Fallback to simple iloc if today's candle is not yet started or missing
        levels["DO"] = float(daily_df["Open"].iloc[-1])
        if len(daily_df) >= 2:
            levels["PDH"] = float(daily_df["High"].iloc[-2])
            levels["PDL"] = float(daily_df["Low"].iloc[-2])
        else:
            levels["PDH"] = levels["PDL"] = levels["DO"]

    # 2. WEEKLY LEVELS (WO, PWH, PWL)
    if weekly_df is not None and not weekly_df.empty:
        # Start of current week (last row usually)
        levels["WO"] = float(weekly_df["Open"].iloc[-1])
        # Previous Week
        if len(weekly_df) >= 2:
            prev_week = weekly_df.iloc[-2]
            levels["PWH"] = float(prev_week["High"])
            levels["PWL"] = float(prev_week["Low"])
        else:
            levels["PWH"] = levels["PWL"] = levels["WO"]
    else:
        # Approximate from daily
        levels["WO"] = levels["DO"]
        history = daily_df[daily_df.index.date < now_utc]
        if len(history) >= 7:
            last_7 = history.iloc[-7:]
            levels["PWH"] = float(last_7["High"].max())
            levels["PWL"] = float(last_7["Low"].min())
        else:
            levels["PWH"] = levels["PDH"]
            levels["PWL"] = levels["PDL"]

    # 3. MONTHLY LEVELS (MO, PMH, PML)
    if monthly_df is not None and not monthly_df.empty:
        levels["MO"] = float(monthly_df["Open"].iloc[-1])
        if len(monthly_df) >= 2:
            prev_month = monthly_df.iloc[-2]
            levels["PMH"] = float(prev_month["High"])
            levels["PML"] = float(prev_month["Low"])
        else:
            levels["PMH"] = levels["PML"] = levels["MO"]
    else:
        levels["MO"] = levels["DO"]
        history = daily_df[daily_df.index.date < now_utc]
        if len(history) >= 30:
            last_30 = history.iloc[-30:]
            levels["PMH"] = float(last_30["High"].max())
            levels["PML"] = float(last_30["Low"].min())
        else:
            levels["PMH"] = levels["PWH"]
            levels["PML"] = levels["PWL"]

    # Volatility Zones (ADR-based) - EXCLUDE today's live candle to match Pine Script [1] logic
    lookback = min(ADR_LEN, len(daily_df) - 1)
    if lookback < 1:
        avg_pump = avg_dump = max_pump = max_dump = 0
    else:
        # Use only completed candles for the average (excluding today)
        recent = daily_df.iloc[-lookback-1:-1]

        # Average pump/dump
        pumps = recent["High"] - recent["Open"]
        dumps = recent["Open"] - recent["Low"]

        avg_pump = float(pumps.mean())
        avg_dump = float(dumps.mean())
        max_pump = float(pumps.max())
        max_dump = float(dumps.max())

    do = levels["DO"]

    levels["Pump"]     = do + avg_pump
    levels["Dump"]     = do - avg_dump
    levels["PumpMax"]  = do + max_pump
    levels["DumpMax"]  = do - max_dump

    # Daily levels report values
    levels["Resistance"]   = do + avg_pump
    levels["Support"]      = do - avg_dump
    levels["Volatility"]   = avg_pump + avg_dump
    levels["CriticalHigh"] = do + max_pump
    levels["CriticalLow"]  = do - max_dump

    # Percentages
    levels["ResistancePct"] = (avg_pump / do) * 100 if do else 0
    levels["SupportPct"]    = (avg_dump / do) * 100 if do else 0
    levels["VolatilityPct"] = ((avg_pump + avg_dump) / do) * 100 if do else 0

    # Raw values for reference
    levels["AvgPump"] = avg_pump
    levels["AvgDump"] = avg_dump

    return levels


def check_liquidity_sweep(price_high, price_low, levels, prev_high=None, prev_low=None):
    """
    Check if current candle sweeps any liquidity levels.

    Returns list of sweep events:
    [{"side": "LONG", "level": "PDL", "price": ..., "points": ..., "strength": ...}, ...]
    """
    sweeps = []

    # Levels to check for LONG sweeps (price goes BELOW these)
    long_levels = {
        "PDL": levels.get("PDL"),
        "PWL": levels.get("PWL"),
        "PML": levels.get("PML"),
    }

    # Levels to check for SHORT sweeps (price goes ABOVE these)
    short_levels = {
        "PDH": levels.get("PDH"),
        "PWH": levels.get("PWH"),
        "PMH": levels.get("PMH"),
    }

    for name, value in long_levels.items():
        if value is None:
            continue
        # Low sweeps below the level
        if price_low <= value:
            # Check it's a new cross (prev candle was above)
            if prev_low is None or prev_low > value:
                pts = SWEEP_POINTS.get(name, 1)
                sweeps.append({
                    "side":     "LONG",
                    "level":    name,
                    "price":    value,
                    "points":   pts,
                    "strength": SWEEP_STRENGTH.get(pts, "Low"),
                    "note":     f"Liquidity cross {name} ({SWEEP_STRENGTH.get(pts, 'weak').lower()})",
                })

    for name, value in short_levels.items():
        if value is None:
            continue
        # High sweeps above the level
        if price_high >= value:
            if prev_high is None or prev_high < value:
                pts = SWEEP_POINTS.get(name, 1)
                sweeps.append({
                    "side":     "SHORT",
                    "level":    name,
                    "price":    value,
                    "points":   pts,
                    "strength": SWEEP_STRENGTH.get(pts, "Low"),
                    "note":     f"Liquidity cross {name} ({SWEEP_STRENGTH.get(pts, 'weak').lower()})",
                })

    return sweeps


def check_volatility_touch(price_high, price_low, levels, prev_high=None, prev_low=None):
    """
    Check if current candle touches volatility zones.

    Returns list of touch events.
    """
    touches = []

    # LONG touches (price drops into dump zones)
    dump_levels = {
        "DUMP":    levels.get("Dump"),
        "DUMPMAX": levels.get("DumpMax"),
    }

    # SHORT touches (price rises into pump zones)
    pump_levels = {
        "PUMP":    levels.get("Pump"),
        "PUMPMAX": levels.get("PumpMax"),
    }

    for name, value in dump_levels.items():
        if value is None:
            continue
        if price_low <= value:
            if prev_low is None or prev_low > value:
                pts = VOL_ZONE_POINTS.get(name, 1)
                strength = VOL_ZONE_STRENGTH.get(pts, "Low")
                note_suffix = "(weak)" if pts == 1 else "(strong/anomaly)" if "MAX" in name else "(strong)"
                touches.append({
                    "side":     "LONG",
                    "level":    name,
                    "price":    value,
                    "points":   pts,
                    "strength": strength,
                    "note":     f"Vol line cross {name.replace('MAX', ' Max')} {note_suffix}",
                })

    for name, value in pump_levels.items():
        if value is None:
            continue
        if price_high >= value:
            if prev_high is None or prev_high < value:
                pts = VOL_ZONE_POINTS.get(name, 1)
                strength = VOL_ZONE_STRENGTH.get(pts, "Low")
                note_suffix = "(weak)" if pts == 1 else "(strong/anomaly)" if "MAX" in name else "(strong)"
                touches.append({
                    "side":     "SHORT",
                    "level":    name,
                    "price":    value,
                    "points":   pts,
                    "strength": strength,
                    "note":     f"Vol line cross {name.replace('MAX', ' Max')} {note_suffix}",
                })

    return touches
