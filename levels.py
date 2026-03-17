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
    # Most reliable method for 24/7 markets:
    # iloc[-1] is the current LIVE candle (Today)
    # iloc[-2] is the most recent CLOSED candle (Yesterday)
    
    if len(daily_df) >= 2:
        today_candle = daily_df.iloc[-1]
        prev_day_candle = daily_df.iloc[-2]
        
        levels["DO"] = float(today_candle["Open"])
        levels["PDH"] = float(prev_day_candle["High"])
        levels["PDL"] = float(prev_day_candle["Low"])
        levels["PD_Date"] = prev_day_candle.name.strftime("%d.%m.%Y")
    else:
        # Emergency fallback for very short history
        levels["DO"] = float(daily_df["Open"].iloc[-1])
        levels["PDH"] = levels["PDL"] = levels["DO"]
        levels["PD_Date"] = "N/A"

    # 2. WEEKLY LEVELS (WO, PWH, PWL)
    if weekly_df is not None and len(weekly_df) >= 2:
        levels["WO"] = float(weekly_df["Open"].iloc[-1])
        levels["PWH"] = float(weekly_df["High"].iloc[-2])
        levels["PWL"] = float(weekly_df["Low"].iloc[-2])
    else:
        # Fallback to daily estimation
        levels["WO"] = levels["DO"]
        if len(daily_df) >= 8:
            # Last 7 days EXCLUDING today
            hist_7 = daily_df.iloc[-8:-1]
            levels["PWH"] = float(hist_7["High"].max())
            levels["PWL"] = float(hist_7["Low"].min())
        else:
            levels["PWH"] = levels["PDH"]
            levels["PWL"] = levels["PDL"]

    # 3. MONTHLY LEVELS (MO, PMH, PML)
    if monthly_df is not None and len(monthly_df) >= 2:
        levels["MO"] = float(monthly_df["Open"].iloc[-1])
        levels["PMH"] = float(monthly_df["High"].iloc[-2])
        levels["PML"] = float(monthly_df["Low"].iloc[-2])
    else:
        levels["MO"] = levels["DO"]
        if len(daily_df) >= 31:
            # Last 30 days EXCLUDING today
            hist_30 = daily_df.iloc[-31:-1]
            levels["PMH"] = float(hist_30["High"].max())
            levels["PML"] = float(hist_30["Low"].min())
        else:
            levels["PMH"] = levels.get("PWH", levels["PDH"])
            levels["PML"] = levels.get("PWL", levels["PDL"])

    # Volatility Zones (ADR-based)
    # Use only COMPLETED candles (excluding the current live candle at iloc[-1])
    lookback = min(ADR_LEN, len(daily_df) - 1)
    if lookback < 1:
        avg_pump = avg_dump = max_pump = max_dump = 0
    else:
        # Take lookback number of candles UP TO the last one
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
