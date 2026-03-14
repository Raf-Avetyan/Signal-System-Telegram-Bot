# ─── Ponch Levels & Volatility Zones ──────────────────────────

import pandas as pd
import numpy as np
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

    # ─── Daily Open (today's open) ────────────────────
    levels["DO"] = float(daily_df["Open"].iloc[-1])

    # ─── Previous Day High / Low ──────────────────────
    if len(daily_df) >= 2:
        levels["PDH"] = float(daily_df["High"].iloc[-2])
        levels["PDL"] = float(daily_df["Low"].iloc[-2])
    else:
        levels["PDH"] = levels["PDL"] = levels["DO"]

    # ─── Previous Week High / Low ─────────────────────
    if weekly_df is not None and len(weekly_df) >= 2:
        levels["PWH"] = float(weekly_df["High"].iloc[-2])
        levels["PWL"] = float(weekly_df["Low"].iloc[-2])
        levels["WO"]  = float(weekly_df["Open"].iloc[-1])
    else:
        # Approximate from daily data
        if len(daily_df) >= 7:
            last_week = daily_df.iloc[-7:-1]
            levels["PWH"] = float(last_week["High"].max())
            levels["PWL"] = float(last_week["Low"].min())
        else:
            levels["PWH"] = levels.get("PDH", levels["DO"])
            levels["PWL"] = levels.get("PDL", levels["DO"])
        levels["WO"] = levels["DO"]

    # ─── Previous Month High / Low ────────────────────
    if monthly_df is not None and len(monthly_df) >= 2:
        levels["PMH"] = float(monthly_df["High"].iloc[-2])
        levels["PML"] = float(monthly_df["Low"].iloc[-2])
        levels["MO"]  = float(monthly_df["Open"].iloc[-1])
    else:
        # Approximate from daily data
        if len(daily_df) >= 30:
            last_month = daily_df.iloc[-30:-1]
            levels["PMH"] = float(last_month["High"].max())
            levels["PML"] = float(last_month["Low"].min())
        else:
            levels["PMH"] = levels.get("PWH", levels["DO"])
            levels["PML"] = levels.get("PWL", levels["DO"])
        levels["MO"] = levels["DO"]

    # ─── Volatility Zones (ADR-based) ─────────────────
    lookback = min(ADR_LEN, len(daily_df))
    recent = daily_df.iloc[-lookback:]

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
