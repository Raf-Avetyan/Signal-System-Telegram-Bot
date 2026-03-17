# ─── Ponch Levels & Volatility Zones ──────────────────────────

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from config import ADR_LEN, SWEEP_POINTS, SWEEP_STRENGTH, VOL_ZONE_POINTS, VOL_ZONE_STRENGTH


def calculate_levels(daily_df, weekly_df=None, monthly_df=None, hourly_df=None):
    """
    Calculate key price levels. 
    Ideally, hourly_df (last 200+ hours) is provided for UTC-accurate D1 levels.
    """
    levels = {}

    if daily_df.empty and (hourly_df is None or hourly_df.empty):
        return levels

    # Get current UTC date elements
    now_utc_dt = datetime.now(timezone.utc)
    today_utc = now_utc_dt.date()
    yesterday_utc = (now_utc_dt - timedelta(days=1)).date()

    # Time boundaries for Weekly/Monthly
    curr_week_start = (now_utc_dt - timedelta(days=now_utc_dt.weekday())).date()
    prev_week_start = curr_week_start - timedelta(days=7)
    prev_week_end   = curr_week_start - timedelta(days=1)
    
    first_day_curr_month = now_utc_dt.replace(day=1).date()
    last_day_prev_month  = first_day_curr_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)

    # --- 1. HOURLY RECONSTRUCTION (PREFERRED) ---
    # Construct "Real UTC Daily" candles from 1H data if available
    if hourly_df is not None and not hourly_df.empty:
        # Group by date part of index
        h_df = hourly_df.copy()
        h_df['date'] = h_df.index.date
        
        # High, Low, Open (first of day), Close (last of day)
        daily_agg = h_df.groupby('date').agg({
            'High': 'max',
            'Low': 'min',
            'Open': 'first',
            'Close': 'last'
        })
        
        # Find Yesterday
        if yesterday_utc in daily_agg.index:
            y_candle = daily_agg.loc[yesterday_utc]
            levels["PDH"] = float(y_candle["High"])
            levels["PDL"] = float(y_candle["Low"])
            levels["PD_Date"] = yesterday_utc.strftime("%d.%m.%Y")
        
        # Find Today's Open (DO)
        if today_utc in daily_agg.index:
            levels["DO"] = float(daily_agg.loc[today_utc]["Open"])

        # Find Weekly Open (WO) - Filter for current week's candles
        curr_week_start_dt = datetime.combine(curr_week_start, datetime.min.time(), tzinfo=timezone.utc)
        week_candles = h_df[h_df.index >= curr_week_start_dt]
        if not week_candles.empty:
            levels["WO"] = float(week_candles["Open"].iloc[0])

        # Find Monthly Open (MO) - Filter for current month's candles
        curr_month_start_dt = datetime.combine(first_day_curr_month, datetime.min.time(), tzinfo=timezone.utc)
        month_candles = h_df[h_df.index >= curr_month_start_dt]
        if not month_candles.empty:
            levels["MO"] = float(month_candles["Open"].iloc[0])

    # --- 2. FALLBACK/ENHANCE WITH DAILY DATA ---
    if "PDH" not in levels or "DO" not in levels:
        df_dates = daily_df.index.date
        
        if "PDH" not in levels:
            yesterday_df = daily_df[df_dates == yesterday_utc]
            if not yesterday_df.empty:
                levels["PDH"] = float(yesterday_df["High"].max())
                levels["PDL"] = float(yesterday_df["Low"].min())
                levels["PD_Date"] = yesterday_utc.strftime("%d.%m.%Y")
            else:
                history = daily_df[df_dates < today_utc]
                if not history.empty:
                    prev_day = history.iloc[-1]
                    levels["PDH"] = float(prev_day["High"])
                    levels["PDL"] = float(prev_day["Low"])
                    levels["PD_Date"] = prev_day.name.strftime("%d.%m.%Y")
        
        if "DO" not in levels:
            today_df = daily_df[df_dates == today_utc]
            levels["DO"] = float(today_df["Open"].iloc[0]) if not today_df.empty else float(daily_df["Open"].iloc[-1])

    # 3. WEEKLY/MONTHLY PREVIOUS LEVELS
    all_days = daily_df.index.date
    
    # Previous Week Range (UTC Mon-Sun)
    pw_df = daily_df[(all_days >= prev_week_start) & (all_days <= prev_week_end)]
    if not pw_df.empty:
        levels["PWH"] = float(pw_df["High"].max())
        levels["PWL"] = float(pw_df["Low"].min())
    else:
        levels["PWH"] = levels.get("PDH", levels["DO"])
        levels["PWL"] = levels.get("PDL", levels["DO"])

    if "WO" not in levels:
        if weekly_df is not None and not weekly_df.empty:
            levels["WO"] = float(weekly_df["Open"].iloc[-1])
        else:
            levels["WO"] = levels["DO"]

    # Previous Month Range (UTC)
    pm_df = daily_df[(all_days >= first_day_prev_month) & (all_days <= last_day_prev_month)]
    if not pm_df.empty:
        levels["PMH"] = float(pm_df["High"].max())
        levels["PML"] = float(pm_df["Low"].min())
    else:
        levels["PMH"] = levels.get("PWH", levels["PDH"])
        levels["PML"] = levels.get("PWL", levels["PDL"])

    if "MO" not in levels:
        if monthly_df is not None and not monthly_df.empty:
            levels["MO"] = float(monthly_df["Open"].iloc[-1])
        else:
            levels["MO"] = levels["DO"]

    # Volatility Zones (ADR-based)
    history_adr = daily_df[daily_df.index.date < today_utc]
    lookback = min(ADR_LEN, len(history_adr))
    if lookback < 1:
        avg_pump = avg_dump = max_pump = max_dump = 0
    else:
        recent = history_adr.iloc[-lookback:]
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
