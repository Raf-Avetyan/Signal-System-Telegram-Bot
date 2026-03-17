# ─── Ponch Levels & Volatility Zones ──────────────────────────

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from config import ADR_LEN, SWEEP_POINTS, SWEEP_STRENGTH, VOL_ZONE_POINTS, VOL_ZONE_STRENGTH


def calculate_levels(daily_df, weekly_df=None, monthly_df=None, hourly_df=None):
    """
    Calculate key price levels. 
    Ideally, hourly_df is provided for UTC-accurate candle reconstruction.
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

    # --- UTC CALENDAR RECONSTRUCTION (HIGH ACCURACY) ---
    h_df = pd.DataFrame()
    h_daily = pd.DataFrame()
    if hourly_df is not None and not hourly_df.empty:
        h_df = hourly_df.copy()
        if 'date' not in h_df.columns:
            h_df['date'] = h_df.index.date
        h_daily = h_df.groupby('date').agg({
            'High': 'max', 'Low': 'min', 'Open': 'first', 'Close': 'last'
        })

    # 1. DAILY LEVELS (DO / PDH / PDL)
    if today_utc in h_daily.index:
        levels["DO"] = float(h_daily.loc[today_utc]["Open"])
    elif not daily_df.empty:
        levels["DO"] = float(daily_df["Open"].iloc[-1])

    if yesterday_utc in h_daily.index:
        y_candle = h_daily.loc[yesterday_utc]
        levels["PDH"] = float(y_candle["High"])
        levels["PDL"] = float(y_candle["Low"])
        levels["PD_Date"] = yesterday_utc.strftime("%d.%m.%Y")
    else:
        # Fallback to Daily DF
        df_dates = daily_df.index.date
        yesterday_df = daily_df[df_dates == yesterday_utc]
        if not yesterday_df.empty:
            levels["PDH"] = float(yesterday_df["High"].max())
            levels["PDL"] = float(yesterday_df["Low"].min())
            levels["PD_Date"] = yesterday_utc.strftime("%d.%m.%Y")

    # 2. WEEKLY LEVELS (WO / PWH / PWL)
    cw_start_dt = datetime.combine(curr_week_start, datetime.min.time(), tzinfo=timezone.utc)
    pw_start_dt = datetime.combine(prev_week_start, datetime.min.time(), tzinfo=timezone.utc)
    
    # Weekly Open (WO)
    if weekly_df is not None and not weekly_df.empty:
        levels["WO"] = float(weekly_df["Open"].iloc[-1])
    elif not h_df.empty:
        cw_slice = h_df[h_df.index >= cw_start_dt]
        if not cw_slice.empty:
            levels["WO"] = float(cw_slice["Open"].iloc[0])
    
    if "WO" not in levels:
        levels["WO"] = levels.get("DO", 0)

    # Previous Week Range (PWH / PWL)
    if not h_df.empty:
        pw_slice = h_df[(h_df.index >= pw_start_dt) & (h_df.index < cw_start_dt)]
        if not pw_slice.empty:
            levels["PWH"] = float(pw_slice["High"].max())
            levels["PWL"] = float(pw_slice["Low"].min())

    if "PWH" not in levels:
        all_days = daily_df.index.date
        pw_df = daily_df[(all_days >= prev_week_start) & (all_days <= prev_week_end)]
        if not pw_df.empty:
            levels["PWH"] = float(pw_df["High"].max())
            levels["PWL"] = float(pw_df["Low"].min())
        elif weekly_df is not None and len(weekly_df) >= 2:
            pw_candle = weekly_df.iloc[-2]
            levels["PWH"] = float(pw_candle["High"])
            levels["PWL"] = float(pw_candle["Low"])

    # 3. MONTHLY LEVELS (MO / PMH / PML)
    # Target MO ~67k (March 1st open on many charts)
    if "MO" not in levels:
        # Search daily_df for the candle starting on the 1st
        target_mo_date = first_day_curr_month
        all_daily_dates = daily_df.index.date
        match_mo = daily_df[all_daily_dates == target_mo_date]
        if not match_mo.empty:
            levels["MO"] = float(match_mo["Open"].iloc[0])
        elif monthly_df is not None and not monthly_df.empty:
            levels["MO"] = float(monthly_df["Open"].iloc[-1])
        else:
            levels["MO"] = levels.get("DO", 0)

    # Previous Month Range (PMH / PML)
    pm_start_dt = datetime.combine(first_day_prev_month, datetime.min.time(), tzinfo=timezone.utc)
    cm_start_dt = datetime.combine(first_day_curr_month, datetime.min.time(), tzinfo=timezone.utc)
    if not h_df.empty and h_df.index[0] <= pm_start_dt:
        pm_slice = h_df[(h_df.index >= pm_start_dt) & (h_df.index < cm_start_dt)]
        if not pm_slice.empty:
            levels["PMH"] = float(pm_slice["High"].max())
            levels["PML"] = float(pm_slice["Low"].min())

    if "PMH" not in levels:
        all_days = daily_df.index.date
        pm_df = daily_df[(all_days >= first_day_prev_month) & (all_days <= last_day_prev_month)]
        if not pm_df.empty:
            levels["PMH"] = float(pm_df["High"].max())
            levels["PML"] = float(pm_df["Low"].min())
        elif monthly_df is not None and len(monthly_df) >= 2:
            pm_candle = monthly_df.iloc[-2]
            levels["PMH"] = float(pm_candle["High"])
            levels["PML"] = float(pm_candle["Low"])

    # 4. Volatility Zones (ADR-based using SMA for standard ranges as requested)
    history_adr = daily_df[daily_df.index.date < today_utc]
    lookback = min(ADR_LEN, len(history_adr))
    if lookback < 1:
        avg_pump = avg_dump = max_pump = max_dump = 0
    else:
        recent = history_adr.iloc[-lookback:]
        pumps_raw = recent["High"] - recent["Open"]
        dumps_raw = recent["Open"] - recent["Low"]
        avg_pump = float(pumps_raw.mean())
        avg_dump = float(dumps_raw.mean())
        max_pump = float(pumps_raw.max())
        max_dump = float(dumps_raw.max())

    do = levels["DO"]
    levels["Pump"]     = do + avg_pump
    levels["Dump"]     = do - avg_dump
    levels["PumpMax"]  = do + max_pump
    levels["DumpMax"]  = do - max_dump

    # Daily levels report values
    levels["Resistance"]   = levels["Pump"]
    levels["Support"]      = levels["Dump"]
    levels["Volatility"]   = avg_pump + avg_dump
    levels["CriticalHigh"] = levels["PumpMax"]
    levels["CriticalLow"]  = levels["DumpMax"]

    # Percentages
    levels["ResistancePct"] = (avg_pump / do) * 100 if do else 0
    levels["SupportPct"]    = (avg_dump / do) * 100 if do else 0
    levels["VolatilityPct"] = ((avg_pump + avg_dump) / do) * 100 if do else 0

    return levels


def check_liquidity_sweep(price_high, price_low, levels, prev_high=None, prev_low=None):
    """Check if current candle sweeps any liquidity levels."""
    sweeps = []
    long_levels = {"PDL": levels.get("PDL"), "PWL": levels.get("PWL"), "PML": levels.get("PML")}
    short_levels = {"PDH": levels.get("PDH"), "PWH": levels.get("PWH"), "PMH": levels.get("PMH")}

    for name, value in long_levels.items():
        if value is None: continue
        if price_low <= value:
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
        if value is None: continue
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
    """Check if current candle touches volatility zones."""
    touches = []
    dump_levels = {"DUMP": levels.get("Dump"), "DUMPMAX": levels.get("DumpMax")}
    pump_levels = {"PUMP": levels.get("Pump"), "PUMPMAX": levels.get("PumpMax")}

    for name, value in dump_levels.items():
        if value is None: continue
        if price_low <= value:
            if prev_low is None or prev_low > value:
                pts = VOL_ZONE_POINTS.get(name, 1)
                touches.append({
                    "side":     "LONG",
                    "level":    name,
                    "price":    value,
                    "points":   pts,
                    "strength": VOL_ZONE_STRENGTH.get(pts, "Low"),
                    "note":     f"Vol line cross {name.replace('MAX', ' Max')} {'(weak)' if pts == 1 else '(strong)'}",
                })

    for name, value in pump_levels.items():
        if value is None: continue
        if price_high >= value:
            if prev_high is None or prev_high < value:
                pts = VOL_ZONE_POINTS.get(name, 1)
                touches.append({
                    "side":     "SHORT",
                    "level":    name,
                    "price":    value,
                    "points":   pts,
                    "strength": VOL_ZONE_STRENGTH.get(pts, "Low"),
                    "note":     f"Vol line cross {name.replace('MAX', ' Max')} {'(weak)' if pts == 1 else '(strong)'}",
                })
    return touches
