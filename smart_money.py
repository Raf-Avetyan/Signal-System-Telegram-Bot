"""
STRATEGY: SMART MONEY LIQUIDITY

1. DAILY BIAS
- Identify HTF liquidity (1H/4H highs & lows, session highs/lows)
- Determine trend (HH/HL = long, LH/LL = short)
- Trade only in direction of HTF bias

2. KEY LEVEL
- Wait for price to reach:
  - HTF liquidity
  - Order Block (OB)
  - HTF Fair Value Gap (FVG)
- Ensure price is in:
  - Discount (for longs)
  - Premium (for shorts)

3. LIQUIDITY SWEEP (LTF 1m-5m)
- Must take previous high/low (stop hunt)
- No sweep = no trade

4. REVERSAL CONFIRMATION
- Break of Structure (BOS)
- Strong displacement (momentum candle)

5. ENTRY MODEL
- Identify FVG after BOS
- Wait for retrace into FVG
- Enter on FVG retest

6. TIME FILTER
- Trade only during:
  - London session
  - New York session
- Avoid:
  - Asian session
  - High-impact news

7. RISK MANAGEMENT
- Risk per trade: 1-2%
- Minimum RR: 1:2
- Max trades per day: 2-3

8. STOP LOSS
- Place SL beyond liquidity sweep

9. TAKE PROFIT
- TP1: nearest liquidity (equal highs/lows)
- TP2: HTF high/low
- TP3: session high/low
- Move SL to BE after TP1

10. NO TRADE CONDITIONS
- No HTF bias
- Price in middle of range
- No sweep
- No BOS
- No displacement
- Outside trading sessions

FORMULA:
HTF bias -> level -> sweep -> BOS -> displacement -> FVG -> entry -> liquidity TP
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from channels import atr
from config import (
    SMART_MONEY_ALLOWED_SESSIONS,
    SMART_MONEY_BOS_SWING_LOOKBACK,
    SMART_MONEY_DEALING_RANGE_BUFFER_PCT,
    SMART_MONEY_DISPLACEMENT_BODY_ATR,
    SMART_MONEY_ENABLED,
    SMART_MONEY_EXECUTION_TFS,
    SMART_MONEY_FVG_LOOKBACK,
    SMART_MONEY_HTF_SWING_LOOKBACK,
    SMART_MONEY_KEYLEVEL_TOLERANCE_PCT,
    SMART_MONEY_LTF_SWEEP_LOOKBACK,
    SMART_MONEY_MAX_TRADES_PER_DAY,
    SMART_MONEY_MAX_EXECUTION_CANDLE_RISK_RATIO_BY_TF,
    SMART_MONEY_MIN_RR,
    SMART_MONEY_OB_LOOKBACK,
    SMART_MONEY_POST_SWEEP_CONFIRM_BARS,
    SMART_MONEY_RISK_PCT,
    SMART_MONEY_SL_BUFFER_ATR,
    SMART_MONEY_SWING_PIVOT_BARS,
    get_adjusted_sessions,
)


def _in_allowed_session(now: datetime) -> Optional[str]:
    current_float_hour = now.hour + now.minute / 60.0
    sessions = get_adjusted_sessions(now)
    for session_name in SMART_MONEY_ALLOWED_SESSIONS:
        session = sessions.get(session_name)
        if not session:
            continue
        start = session["open"]
        end = session["close"]
        if start < end:
            active = start <= current_float_hour < end
        else:
            active = current_float_hour >= start or current_float_hour < end
        if active:
            return session_name
    return None


def _find_swings(df: pd.DataFrame, pivot_bars: int, lookback: int) -> Tuple[List[Tuple[pd.Timestamp, float]], List[Tuple[pd.Timestamp, float]]]:
    highs: List[Tuple[pd.Timestamp, float]] = []
    lows: List[Tuple[pd.Timestamp, float]] = []
    if df is None or df.empty:
        return highs, lows

    sub = df.iloc[-max(lookback, pivot_bars * 4 + 6):]
    for i in range(pivot_bars, len(sub) - pivot_bars):
        high = float(sub["High"].iloc[i])
        low = float(sub["Low"].iloc[i])
        left_high = float(sub["High"].iloc[i - pivot_bars:i].max())
        right_high = float(sub["High"].iloc[i + 1:i + pivot_bars + 1].max())
        left_low = float(sub["Low"].iloc[i - pivot_bars:i].min())
        right_low = float(sub["Low"].iloc[i + 1:i + pivot_bars + 1].min())
        if high >= left_high and high >= right_high:
            highs.append((sub.index[i], high))
        if low <= left_low and low <= right_low:
            lows.append((sub.index[i], low))
    return highs[-4:], lows[-4:]


def _ema_bias(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty or len(df) < 30:
        return None
    close = df["Close"]
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema55 = close.ewm(span=55, adjust=False).mean()
    curr_close = float(close.iloc[-1])
    e21 = float(ema21.iloc[-1])
    e55 = float(ema55.iloc[-1])
    if curr_close > e21 > e55:
        return "LONG"
    if curr_close < e21 < e55:
        return "SHORT"
    return None


def _structure_bias(df: pd.DataFrame) -> Optional[str]:
    highs, lows = _find_swings(df, SMART_MONEY_SWING_PIVOT_BARS, SMART_MONEY_HTF_SWING_LOOKBACK)
    long_score = 0
    short_score = 0

    if len(highs) >= 2:
        prev_high, last_high = highs[-2][1], highs[-1][1]
        if last_high > prev_high:
            long_score += 1
        elif last_high < prev_high:
            short_score += 1

    if len(lows) >= 2:
        prev_low, last_low = lows[-2][1], lows[-1][1]
        if last_low > prev_low:
            long_score += 1
        elif last_low < prev_low:
            short_score += 1

    if long_score == 2:
        return "LONG"
    if short_score == 2:
        return "SHORT"
    if long_score > 0 and short_score == 0:
        return "LONG"
    if short_score > 0 and long_score == 0:
        return "SHORT"
    return _ema_bias(df)


def _find_recent_fvg(df: pd.DataFrame, side: str, lookback: int) -> Optional[Dict[str, float]]:
    if df is None or len(df) < 3:
        return None
    sub = df.iloc[-max(lookback, 8):]
    atr_series = atr(sub.copy(), 14)
    for i in range(len(sub) - 1, 1, -1):
        first = sub.iloc[i - 2]
        middle = sub.iloc[i - 1]
        third = sub.iloc[i]
        atr_val = float(atr_series.iloc[i] or 0)
        body = abs(float(middle["Close"]) - float(middle["Open"]))
        displaced = atr_val > 0 and body >= atr_val * SMART_MONEY_DISPLACEMENT_BODY_ATR
        if side == "LONG":
            if float(first["High"]) < float(third["Low"]) and displaced:
                return {
                    "low": float(first["High"]),
                    "high": float(third["Low"]),
                    "created_at": third.name,
                    "displacement_close": float(middle["Close"]),
                }
        else:
            if float(first["Low"]) > float(third["High"]) and displaced:
                return {
                    "low": float(third["High"]),
                    "high": float(first["Low"]),
                    "created_at": third.name,
                    "displacement_close": float(middle["Close"]),
                }
    return None


def _find_recent_order_block(df: pd.DataFrame, side: str, lookback: int) -> Optional[Dict[str, float]]:
    if df is None or len(df) < 3:
        return None
    sub = df.iloc[-max(lookback, 10):]
    atr_series = atr(sub.copy(), 14)
    for i in range(len(sub) - 1, 1, -1):
        impulse = sub.iloc[i]
        previous = sub.iloc[i - 1]
        atr_val = float(atr_series.iloc[i] or 0)
        body = abs(float(impulse["Close"]) - float(impulse["Open"]))
        if atr_val <= 0 or body < atr_val * SMART_MONEY_DISPLACEMENT_BODY_ATR:
            continue
        if side == "LONG" and float(impulse["Close"]) > float(impulse["Open"]) and float(previous["Close"]) < float(previous["Open"]):
            return {"low": float(previous["Low"]), "high": float(previous["High"])}
        if side == "SHORT" and float(impulse["Close"]) < float(impulse["Open"]) and float(previous["Close"]) > float(previous["Open"]):
            return {"low": float(previous["Low"]), "high": float(previous["High"])}
    return None


def _zone_contains(price: float, zone: Optional[Dict[str, float]], tolerance_pct: float) -> bool:
    if not zone:
        return False
    low = min(float(zone["low"]), float(zone["high"]))
    high = max(float(zone["low"]), float(zone["high"]))
    tol = price * (tolerance_pct / 100.0)
    return (low - tol) <= price <= (high + tol)


def _near_level(price: float, level: Optional[float], tolerance_pct: float) -> bool:
    if level is None or level <= 0:
        return False
    return abs(price - level) / price * 100.0 <= tolerance_pct


def _session_range(df: pd.DataFrame, now: datetime, session_name: str) -> Optional[Dict[str, float]]:
    if df is None or df.empty:
        return None
    sessions = get_adjusted_sessions(now)
    session = sessions.get(session_name)
    if not session:
        return None
    current_float_hour = now.hour + now.minute / 60.0
    start = session["open"]
    end = session["close"]
    if start < end:
        active = start <= current_float_hour < end
    else:
        active = current_float_hour >= start or current_float_hour < end
    if not active:
        return None

    now_utc = now.astimezone(df.index.tz if df.index.tz is not None else None) if now.tzinfo else now
    day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    def _float_hour_to_dt(base: datetime, hour_value: float) -> datetime:
        hours = int(hour_value)
        minutes = int(round((hour_value - hours) * 60))
        if minutes >= 60:
            hours += 1
            minutes -= 60
        return base + timedelta(hours=hours, minutes=minutes)

    if start < end:
        session_start = _float_hour_to_dt(day_start, start)
        session_end = _float_hour_to_dt(day_start, end)
    else:
        if current_float_hour >= start:
            session_start = _float_hour_to_dt(day_start, start)
            session_end = _float_hour_to_dt(day_start + timedelta(days=1), end)
        else:
            session_start = _float_hour_to_dt(day_start - timedelta(days=1), start)
            session_end = _float_hour_to_dt(day_start, end)

    session_df = df.loc[(df.index >= session_start) & (df.index < session_end)]
    if session_df.empty:
        return None
    return {
        "high": float(session_df["High"].max()),
        "low": float(session_df["Low"].min()),
    }


def _equal_liquidity_candidates(df: pd.DataFrame, tf_name: str, side: str, lookback: int = 80, tolerance_pct: float = 0.12) -> List[Tuple[str, float]]:
    if df is None or df.empty:
        return []
    pivot_bars = 1 if tf_name in {"1m", "5m"} else SMART_MONEY_SWING_PIVOT_BARS
    highs, lows = _find_swings(df, pivot_bars, max(lookback, pivot_bars * 4 + 8))
    swings = highs if side == "LONG" else lows
    if len(swings) < 2:
        return []

    results: List[Tuple[str, float]] = []
    recent = swings[-8:]
    for i in range(len(recent) - 1, 0, -1):
        base = float(recent[i][1])
        cluster = [base]
        for j in range(i - 1, -1, -1):
            other = float(recent[j][1])
            if abs(other - base) / max(abs(base), 1.0) * 100.0 <= tolerance_pct:
                cluster.append(other)
        if len(cluster) >= 2:
            level = float(sum(cluster) / len(cluster))
            label = f"EQ_HIGH_{tf_name}" if side == "LONG" else f"EQ_LOW_{tf_name}"
            if not results or abs(level - results[-1][1]) / max(abs(level), 1.0) > 0.0008:
                results.append((label, level))
    return results


def _liquidity_candidates(side: str, levels: Dict[str, float], data: Dict[str, pd.DataFrame], session_range: Optional[Dict[str, float]]) -> List[Tuple[str, float]]:
    candidates: List[Tuple[str, float]] = []
    for name in ("PDH", "PWH", "PMH", "PDL", "PWL", "PML"):
        value = levels.get(name)
        if value is not None:
            candidates.append((name, float(value)))

    for tf in ("5m", "15m", "1h"):
        candidates.extend(_equal_liquidity_candidates(data.get(tf), tf, side))

    for tf in ("1h", "4h"):
        df = data.get(tf)
        if df is None or df.empty:
            continue
        highs, lows = _find_swings(df, SMART_MONEY_SWING_PIVOT_BARS, SMART_MONEY_HTF_SWING_LOOKBACK)
        if highs:
            candidates.append((f"{tf}_HIGH", float(highs[-1][1])))
        if lows:
            candidates.append((f"{tf}_LOW", float(lows[-1][1])))

    if session_range:
        candidates.append(("SESSION_HIGH", float(session_range["high"])))
        candidates.append(("SESSION_LOW", float(session_range["low"])))

    filtered: List[Tuple[str, float]] = []
    for name, value in candidates:
        if side == "LONG" and ("HIGH" in name or name in {"PDH", "PWH", "PMH"}):
            filtered.append((name, value))
        elif side == "SHORT" and ("LOW" in name or name in {"PDL", "PWL", "PML"}):
            filtered.append((name, value))
    return filtered


def _dealing_range(data: Dict[str, pd.DataFrame]) -> Optional[Tuple[float, float]]:
    for tf in ("4h", "1h"):
        df = data.get(tf)
        if df is None or df.empty:
            continue
        sub = df.iloc[-SMART_MONEY_HTF_SWING_LOOKBACK:]
        if sub.empty:
            continue
        high = float(sub["High"].max())
        low = float(sub["Low"].min())
        if high > low:
            return low, high
    return None


def _find_sweep(ltf_df: pd.DataFrame, side: str) -> Optional[Dict[str, float]]:
    if ltf_df is None or len(ltf_df) < SMART_MONEY_LTF_SWEEP_LOOKBACK + 3:
        return None
    max_age = max(2, int(SMART_MONEY_POST_SWEEP_CONFIRM_BARS))
    recent = ltf_df.iloc[-(SMART_MONEY_LTF_SWEEP_LOOKBACK + max_age + 6):]
    earliest_pos = max(int(SMART_MONEY_LTF_SWEEP_LOOKBACK), len(recent) - max_age - 1)
    min_history = max(4, int(SMART_MONEY_LTF_SWEEP_LOOKBACK // 2))

    for pos in range(len(recent) - 1, earliest_pos - 1, -1):
        curr = recent.iloc[pos]
        history = recent.iloc[max(0, pos - int(SMART_MONEY_LTF_SWEEP_LOOKBACK)):pos]
        if len(history) < min_history:
            continue
        if side == "LONG":
            previous_low = float(history["Low"].min())
            if float(curr["Low"]) < previous_low and float(curr["Close"]) > previous_low:
                return {"price": float(curr["Low"]), "level": previous_low, "index": recent.index[pos]}
        else:
            previous_high = float(history["High"].max())
            if float(curr["High"]) > previous_high and float(curr["Close"]) < previous_high:
                return {"price": float(curr["High"]), "level": previous_high, "index": recent.index[pos]}
    return None


def _find_bos_and_displacement(ltf_df: pd.DataFrame, side: str, sweep_index) -> Optional[Dict[str, float]]:
    if ltf_df is None or len(ltf_df) < 8:
        return None
    try:
        sweep_pos = ltf_df.index.get_loc(sweep_index)
    except KeyError:
        sweep_pos = len(ltf_df) - 2
    if isinstance(sweep_pos, slice):
        sweep_pos = sweep_pos.stop - 1
    sweep_pos = int(sweep_pos)
    start = max(2, sweep_pos + 1)
    end = min(len(ltf_df), sweep_pos + max(2, int(SMART_MONEY_POST_SWEEP_CONFIRM_BARS)) + 1)

    if side == "LONG":
        structure_key = "High"
    else:
        structure_key = "Low"

    atr_series = atr(ltf_df.copy(), 14)
    for i in range(start, end):
        curr = ltf_df.iloc[i]
        structure_window = ltf_df.iloc[max(sweep_pos, i - max(2, int(SMART_MONEY_BOS_SWING_LOOKBACK))):i]
        if structure_window.empty:
            continue
        if side == "LONG":
            structure_level = float(structure_window[structure_key].max())
        else:
            structure_level = float(structure_window[structure_key].min())
        atr_val = float(atr_series.iloc[i] or 0)
        body = abs(float(curr["Close"]) - float(curr["Open"]))
        if atr_val <= 0 or body < atr_val * SMART_MONEY_DISPLACEMENT_BODY_ATR:
            continue
        if side == "LONG":
            if float(curr["Close"]) > structure_level or (
                float(curr["High"]) > structure_level and float(curr["Close"]) > float(curr["Open"])
            ):
                return {"index": ltf_df.index[i], "structure": structure_level, "close": float(curr["Close"])}
        else:
            if float(curr["Close"]) < structure_level or (
                float(curr["Low"]) < structure_level and float(curr["Close"]) < float(curr["Open"])
            ):
                return {"index": ltf_df.index[i], "structure": structure_level, "close": float(curr["Close"])}
    return None


def _find_entry_fvg_retest(ltf_df: pd.DataFrame, side: str, bos_index) -> Optional[Dict[str, float]]:
    if ltf_df is None or len(ltf_df) < 4:
        return None
    try:
        bos_pos = ltf_df.index.get_loc(bos_index)
    except KeyError:
        bos_pos = len(ltf_df) - 2
    if isinstance(bos_pos, slice):
        bos_pos = bos_pos.stop - 1
    start = max(2, bos_pos - 1)
    end = min(len(ltf_df), bos_pos + 3)
    for i in range(start, end):
        first = ltf_df.iloc[i - 2]
        middle = ltf_df.iloc[i - 1]
        third = ltf_df.iloc[i]
        if side == "LONG":
            gap_low = float(first["High"])
            gap_high = float(third["Low"])
            if gap_high <= gap_low:
                continue
        else:
            gap_low = float(third["High"])
            gap_high = float(first["Low"])
            if gap_high <= gap_low:
                continue
        latest = ltf_df.iloc[-1]
        latest_low = float(latest["Low"])
        latest_high = float(latest["High"])
        touched = latest_low <= gap_high and latest_high >= gap_low
        if touched:
            gap_mid = (gap_low + gap_high) / 2.0
            if latest_low <= gap_mid <= latest_high:
                entry_price = float(gap_mid)
            elif side == "LONG":
                entry_price = float(min(float(latest["Close"]), gap_high))
            else:
                entry_price = float(max(float(latest["Close"]), gap_low))
            return {
                "low": gap_low,
                "high": gap_high,
                "entry": entry_price,
                "created_at": third.name,
                "retest_at": latest.name,
            }
    return None


def _pick_ltf_data(data: Dict[str, pd.DataFrame], execution_tf: str) -> Tuple[Optional[pd.DataFrame], str]:
    preferred = {
        "5m": ("1m", "5m"),
        "15m": ("5m", "1m", "15m"),
    }.get(execution_tf, ("5m", execution_tf))
    for tf in preferred:
        df = data.get(tf)
        if df is not None and not df.empty:
            return df, tf
    return None, execution_tf


def _pick_take_profits(side: str, entry: float, sl: float, levels: Dict[str, float], data: Dict[str, pd.DataFrame], session_range: Optional[Dict[str, float]]) -> Optional[Dict[str, float]]:
    candidates = _liquidity_candidates(side, levels, data, session_range)
    if not candidates:
        return None

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    if side == "LONG":
        directional = sorted((name, price) for name, price in candidates if price > entry)
    else:
        directional = sorted(((name, price) for name, price in candidates if price < entry), key=lambda x: x[1], reverse=True)
    if not directional:
        return None

    unique: List[Tuple[str, float]] = []
    for name, price in directional:
        if not unique or abs(price - unique[-1][1]) / entry > 0.0008:
            unique.append((name, price))

    tp1_name, tp1 = unique[0]
    tp2 = None
    tp3 = None

    for name, price in unique[1:]:
        if tp2 is None and (("1h_" in name) or ("4h_" in name) or name in {"PDH", "PWH", "PMH", "PDL", "PWL", "PML"}):
            tp2 = price
        if tp3 is None and name.startswith("SESSION_"):
            tp3 = price

    if tp2 is None:
        tp2 = unique[min(1, len(unique) - 1)][1]
    if tp3 is None:
        tp3 = unique[min(2, len(unique) - 1)][1]

    rr_tp2 = abs(tp2 - entry) / risk
    if rr_tp2 < SMART_MONEY_MIN_RR:
        return None

    return {
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tp3": float(tp3),
        "tp1_label": tp1_name,
    }


def detect_smart_money_entry(
    data: Dict[str, pd.DataFrame],
    levels: Dict[str, float],
    now: datetime,
    trades_today: int = 0,
    execution_tf: str = "5m",
) -> Optional[Dict[str, object]]:
    if not SMART_MONEY_ENABLED or execution_tf not in SMART_MONEY_EXECUTION_TFS:
        return None
    if trades_today >= SMART_MONEY_MAX_TRADES_PER_DAY:
        return None

    session_name = _in_allowed_session(now)
    if not session_name:
        return None

    bias_4h = _structure_bias(data.get("4h"))
    bias_1h = _structure_bias(data.get("1h"))
    if bias_4h and bias_1h and bias_4h != bias_1h:
        return None
    side = bias_4h or bias_1h
    if not side:
        return None

    entry_df = data.get(execution_tf)
    if entry_df is None or entry_df.empty:
        return None
    current_price = float(entry_df["Close"].iloc[-1])

    dealing_range = _dealing_range(data)
    if not dealing_range:
        return None
    range_low, range_high = dealing_range
    mid = (range_low + range_high) / 2.0
    middle_buffer = abs(range_high - range_low) * (SMART_MONEY_DEALING_RANGE_BUFFER_PCT / 100.0)
    if abs(current_price - mid) <= middle_buffer:
        return None
    if side == "LONG" and current_price > mid:
        return None
    if side == "SHORT" and current_price < mid:
        return None

    htf_fvg = _find_recent_fvg(data.get("1h"), side, SMART_MONEY_FVG_LOOKBACK) or _find_recent_fvg(data.get("4h"), side, SMART_MONEY_FVG_LOOKBACK)
    htf_ob = _find_recent_order_block(data.get("1h"), side, SMART_MONEY_OB_LOOKBACK) or _find_recent_order_block(data.get("4h"), side, SMART_MONEY_OB_LOOKBACK)

    level_side = ("PDL", "PWL", "PML") if side == "LONG" else ("PDH", "PWH", "PMH")
    near_liquidity = any(_near_level(current_price, levels.get(name), SMART_MONEY_KEYLEVEL_TOLERANCE_PCT) for name in level_side)
    if not (near_liquidity or _zone_contains(current_price, htf_fvg, SMART_MONEY_KEYLEVEL_TOLERANCE_PCT) or _zone_contains(current_price, htf_ob, SMART_MONEY_KEYLEVEL_TOLERANCE_PCT)):
        return None

    ltf_df, ltf_tf = _pick_ltf_data(data, execution_tf)
    if ltf_df is None or ltf_df.empty:
        ltf_df = entry_df
        ltf_tf = execution_tf
    sweep = _find_sweep(ltf_df, side)
    if not sweep:
        return None

    bos = _find_bos_and_displacement(ltf_df, side, sweep["index"])
    if not bos:
        return None

    entry_fvg = _find_entry_fvg_retest(ltf_df, side, bos["index"])
    if not entry_fvg:
        return None

    atr_source = atr(entry_df.copy(), 14)
    atr_val = float(atr_source.iloc[-1] or 0)
    if atr_val <= 0:
        return None

    entry = float(entry_fvg["entry"])
    sl_buffer = atr_val * SMART_MONEY_SL_BUFFER_ATR
    sl = float(sweep["price"] - sl_buffer) if side == "LONG" else float(sweep["price"] + sl_buffer)
    if (side == "LONG" and sl >= entry) or (side == "SHORT" and sl <= entry):
        return None

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    max_exec_ratio = SMART_MONEY_MAX_EXECUTION_CANDLE_RISK_RATIO_BY_TF.get(execution_tf)
    if max_exec_ratio is not None:
        latest_exec_bar = entry_df.iloc[-1]
        exec_range = abs(float(latest_exec_bar["High"]) - float(latest_exec_bar["Low"]))
        if exec_range / risk > float(max_exec_ratio):
            return None

    # Invalidate the setup if the retest candle already traded through the stop
    # side. This avoids late signals where the setup is already broken by the
    # time we would enter.
    latest_retest_bar = ltf_df.iloc[-1]
    if side == "LONG" and float(latest_retest_bar["Low"]) <= sl:
        return None
    if side == "SHORT" and float(latest_retest_bar["High"]) >= sl:
        return None

    session_range = _session_range(data.get("15m"), now, session_name) or _session_range(entry_df, now, session_name)
    targets = _pick_take_profits(side, entry, sl, levels, data, session_range)
    if not targets:
        return None

    reasons = [
        f"HTF Bias {side}",
        "Key Level",
        "Liquidity Sweep",
        "BOS + Displacement",
        "FVG Retest",
    ]
    if side == "LONG":
        reasons.insert(1, "Discount")
    else:
        reasons.insert(1, "Premium")

    sweep_ts = pd.Timestamp(sweep["index"]).strftime("%Y%m%d%H%M")
    fvg_ts = pd.Timestamp(entry_fvg["created_at"]).strftime("%Y%m%d%H%M")
    event_id = f"SM_{execution_tf}_{ltf_tf}_{side}_{sweep_ts}_{fvg_ts}"

    return {
        "type": "CONFIRMED",
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": targets["tp1"],
        "tp2": targets["tp2"],
        "tp3": targets["tp3"],
        "trigger": "SMART_MONEY_LIQUIDITY",
        "trigger_label": "Smart Money Liquidity",
        "strategy": "SMART_MONEY_LIQUIDITY",
        "session": session_name,
        "size": float(SMART_MONEY_RISK_PCT),
        "execution_tf": execution_tf,
        "ltf_tf": ltf_tf,
        "event_id": event_id,
        "reasons": reasons,
        "note": (
            "HTF bias -> level -> sweep -> BOS -> displacement -> FVG -> entry -> liquidity TP"
        ),
    }
