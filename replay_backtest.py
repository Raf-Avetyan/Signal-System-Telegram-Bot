import argparse
import math
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

from channels import calculate_channels
from config import (
    BASE_MOMENTUM_ENABLED_TFS,
    BITUNIX_DEFAULT_LEVERAGE,
    BITUNIX_LIQUIDATION_MAX_LEVERAGE_BY_TF,
    BITUNIX_MAX_RISK_USD,
    BITUNIX_MIN_BASE_QTY,
    BITUNIX_MIN_NOTIONAL_USD,
    BITUNIX_POSITION_MODE,
    BITUNIX_QTY_STEP,
    BITUNIX_RISK_CAP_PCT,
    BITUNIX_TP_SPLITS,
    BREAKEVEN_WIN_MIN_TP,
    BREAKEVEN_MOVE_AFTER_TP,
    BREAKEVEN_FEE_BUFFER_PCT,
    FALLING_KNIFE_FILTER_ENABLED,
    FALLING_KNIFE_LOOKBACK_5M,
    FALLING_KNIFE_LOOKBACK_15M,
    FALLING_KNIFE_MOVE_PCT_5M,
    FALLING_KNIFE_MOVE_PCT_15M,
    ORDERFLOW_ANOMALY_SCORE_MIN,
    ORDERFLOW_OI_PCT_ANOMALY,
    ORDERFLOW_LIQ_ANOMALY_USD,
    ORDERFLOW_SAFETY_ENABLED,
    SCALP_ALLOWED_SESSIONS_BY_TF,
    SCALP_COUNTERTREND_MAX_PER_WINDOW,
    SCALP_COUNTERTREND_MIN_SCORE,
    SCALP_COUNTERTREND_WINDOW_SEC,
    SCALP_LOSS_COOLDOWN_SEC,
    SCALP_LOSS_STREAK_LIMIT,
    SCALP_MIN_SCORE_BY_TF,
    SCALP_TREND_FILTER_MODE,
    SCALP_TREND_FILTER_MODE_BY_TF,
    SCALP_COUNTERTREND_MIN_SCORE_BY_TF,
    REVERSAL_OVERRIDE_ENABLED,
    REVERSAL_OVERRIDE_MIN_SCORE,
    REVERSAL_OVERRIDE_MIN_PROOFS,
    WEEKEND_TRADING_ENABLED,
    BOS_GUARD_ENABLED,
    BOS_GUARD_SWING_LOOKBACK,
    BOS_GUARD_RECENT_BARS,
    BOS_GUARD_RECLAIM_BARS,
    STRUCTURE_GUARD_MODE_BY_TF,
    RSI_PULLBACK_SCALP_ENABLED,
    RSI_PULLBACK_SCALP_TFS,
    RSI_PULLBACK_SCALP_OB,
    RSI_PULLBACK_SCALP_OS,
    RSI_PULLBACK_SCALP_MIN_FILTERS,
    RSI_PULLBACK_SCALP_MIN_EMA_ATR_DISTANCE,
    RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR,
    RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR,
    RSI_PULLBACK_SCALP_MIN_WICK_BODY_RATIO,
    RSI_PULLBACK_SCALP_TP1_R,
    RSI_PULLBACK_SCALP_TP2_R,
    RSI_PULLBACK_SCALP_TP3_R,
    SCALP_RELAX_MIN_SCORE_DELTA,
    SCALP_RELAX_VOL_MIN_MULT,
    SCALP_RELAX_VOL_MAX_MULT,
    SCALP_RELAX_COUNTERTREND_EXTRA,
    SCALP_RELAX_ALLOW_OFFSESSION,
    SCALP_RELAXED_FILTERS,
    SCALP_REGIME_SWITCHING,
    SCALP_REGIME_PROFILES,
    SCALP_SELF_TUNING_ENABLED,
    SCALP_SELF_TUNE_LOOKBACK,
    SCALP_SELF_TUNE_MIN_CLOSED,
    SCALP_SELF_TUNE_LOW_WR,
    SCALP_SELF_TUNE_HIGH_WR,
    SCALP_SELF_TUNE_LOW_AVGR,
    SCALP_SELF_TUNE_HIGH_AVGR,
    SCALP_EXPOSURE_ENABLED,
    MIN_SIGNAL_SIZE_PCT,
    MAX_SIGNAL_SIZE_PCT,
    SCALP_MAX_OPEN_TOTAL,
    SCALP_MAX_OPEN_PER_SIDE,
    SCALP_MAX_OPEN_PER_TF,
    SMART_MONEY_EXECUTION_TFS,
    SMART_MONEY_TP_SPLITS,
    TIMEFRAME_PROFILES,
    SESSION_SCALP_MODE,
    SIGNAL_TIMEFRAMES,
    SYMBOL,
    FIVE_MIN_REQUIRE_15M_PERMISSION,
    VOLATILITY_FILTER_ENABLED,
    VOLATILITY_MAX_ATR_PCT,
    VOLATILITY_MIN_ATR_PCT,
    get_adjusted_sessions,
)
from data import INTERVAL_MAP, OKX_BASE, fetch_klines
from levels import check_liquidity_sweep
from momentum import ScalpTracker, calculate_momentum, classify_momentum_zone, check_htf_pullback_entry, check_one_h_reclaim_entry
from scoring import calculate_signal_score
from signals import check_rsi_divergence
from smart_money import detect_smart_money_entry

KLINES_URL = f"{OKX_BASE}/api/v5/market/candles"
HISTORY_KLINES_URL = f"{OKX_BASE}/api/v5/market/history-candles"

def _resolve_trade_event(tr: dict, high: float, low: float, candle_ts: str):
    """
    Mirror tracker.check_outcomes() semantics for one open trade on one candle.
    Returns terminal event type or None.
    """
    is_long = tr["side"] == "LONG"
    entry_candle = tr.get("entry_candle_ts") == candle_ts
    sl_touched = (low <= tr["sl"]) if is_long else (high >= tr["sl"])
    tp1_touched = (high >= tr["tp1"]) if is_long else (low <= tr["tp1"])
    active_tp_indices = list(tr.get("active_tp_indices") or [1, 2, 3])
    breakeven_trigger = tr.get("breakeven_trigger")
    breakeven_price = float(tr.get("breakeven_price", tr["entry"]) or tr["entry"])

    # Same-candle ambiguity for fresh trades: TP1+SL => ENTRY_CLOSE (never direct SL)
    if not entry_candle and breakeven_trigger == 1 and not tr["tp1_hit"] and tp1_touched and sl_touched:
        tr["tp1_hit"] = True
        tr["sl"] = breakeven_price
        return "ENTRY_CLOSE"

    # Progressive TP checks (disabled on entry candle)
    if not entry_candle:
        tp_price = high if is_long else low
        if is_long:
            if 1 in active_tp_indices and not tr["tp1_hit"] and tp_price >= tr["tp1"]:
                tr["tp1_hit"] = True
                if len(active_tp_indices) == 1 and active_tp_indices[0] == 1:
                    return "TP1"
            if 2 in active_tp_indices and not tr["tp2_hit"] and tp_price >= tr["tp2"]:
                tr["tp2_hit"] = True
                if breakeven_trigger == 2:
                    tr["sl"] = breakeven_price
                if len(active_tp_indices) == 1 and active_tp_indices[0] == 2:
                    return "TP2"
            if 3 in active_tp_indices and not tr["tp3_hit"] and tp_price >= tr["tp3"]:
                tr["tp3_hit"] = True
                if breakeven_trigger == 3:
                    tr["sl"] = breakeven_price
                return "TP3"
        else:
            if 1 in active_tp_indices and not tr["tp1_hit"] and tp_price <= tr["tp1"]:
                tr["tp1_hit"] = True
                if len(active_tp_indices) == 1 and active_tp_indices[0] == 1:
                    return "TP1"
            if 2 in active_tp_indices and not tr["tp2_hit"] and tp_price <= tr["tp2"]:
                tr["tp2_hit"] = True
                if breakeven_trigger == 2:
                    tr["sl"] = breakeven_price
                if len(active_tp_indices) == 1 and active_tp_indices[0] == 2:
                    return "TP2"
            if 3 in active_tp_indices and not tr["tp3_hit"] and tp_price <= tr["tp3"]:
                tr["tp3_hit"] = True
                if breakeven_trigger == 3:
                    tr["sl"] = breakeven_price
                return "TP3"

    # Breakeven close while stop is parked at entry.
    stop_at_entry = abs(float(tr.get("sl", tr["entry"])) - breakeven_price) < 1e-9
    trigger_hit = (
        (breakeven_trigger == 1 and tr.get("tp1_hit"))
        or (breakeven_trigger == 2 and tr.get("tp2_hit"))
        or (breakeven_trigger == 3 and tr.get("tp3_hit"))
    )
    if trigger_hit and stop_at_entry:
        entry_hit = (low <= breakeven_price) if is_long else (high >= breakeven_price)
        if entry_hit:
            return "ENTRY_CLOSE"

    # SL / protected SL-in-profit
    sl_price = low if is_long else high
    if (is_long and sl_price <= tr["sl"]) or ((not is_long) and sl_price >= tr["sl"]):
        return "PROFIT_SL" if (trigger_hit and not stop_at_entry) else "SL"

    return None


def _breakeven_counts_as_win(tr: dict) -> bool:
    threshold = int(BREAKEVEN_WIN_MIN_TP)
    if threshold <= 1:
        return bool(tr.get("tp1_hit"))
    if threshold == 2:
        return bool(tr.get("tp2_hit"))
    return bool(tr.get("tp3_hit"))


def _tp_fracs_from_trade(tr: dict):
    qtys = list(tr.get("tp_qtys") or [])
    if qtys:
        qtys = [max(0.0, float(q or 0)) for q in qtys[:3]]
        total = sum(qtys)
        if total > 0:
            while len(qtys) < 3:
                qtys.append(0.0)
            return [q / total for q in qtys[:3]]
    base = [max(0.0, float(x or 0)) for x in BITUNIX_TP_SPLITS[:3]]
    while len(base) < 3:
        base.append(0.0)
    total = sum(base) or 1.0
    return [x / total for x in base[:3]]


def _trade_stop_r(tr: dict) -> float:
    risk = float(tr.get("risk") or 0.0)
    if risk <= 0:
        return 0.0
    entry = float(tr.get("entry") or 0.0)
    sl = float(tr.get("sl") or entry)
    if tr.get("side") == "LONG":
        return (sl - entry) / risk
    return (entry - sl) / risk


def _trade_realized_partial_r(tr: dict) -> float:
    risk = float(tr.get("risk") or 0.0)
    if risk <= 0:
        return 0.0
    entry = float(tr.get("entry") or 0.0)
    fracs = _tp_fracs_from_trade(tr)
    prices = [
        float(tr.get("tp1") or entry),
        float(tr.get("tp2") or entry),
        float(tr.get("tp3") or entry),
    ]
    hits = [
        bool(tr.get("tp1_hit")),
        bool(tr.get("tp2_hit")),
        bool(tr.get("tp3_hit")),
    ]
    realized = 0.0
    for frac, price, hit in zip(fracs, prices, hits):
        if hit:
            realized += frac * (abs(price - entry) / risk)
    return realized


def _trade_remaining_frac(tr: dict) -> float:
    fracs = _tp_fracs_from_trade(tr)
    hits = [
        bool(tr.get("tp1_hit")),
        bool(tr.get("tp2_hit")),
        bool(tr.get("tp3_hit")),
    ]
    consumed = sum(frac for frac, hit in zip(fracs, hits) if hit)
    return max(0.0, 1.0 - consumed)


def _breakeven_trigger_index_for_active(active_tp_indices):
    active_tp_indices = list(active_tp_indices or [])
    if len(active_tp_indices) <= 1:
        return None
    threshold = int(BREAKEVEN_MOVE_AFTER_TP)
    if threshold in active_tp_indices:
        return threshold
    return active_tp_indices[min(1, len(active_tp_indices) - 1)]


def _breakeven_lock_price_for_trade(side: str, entry: float) -> float:
    entry = float(entry or 0)
    buffer_pct = max(0.0, float(BREAKEVEN_FEE_BUFFER_PCT or 0.0))
    side = str(side or "").upper()
    if side == "LONG":
        return entry * (1.0 + buffer_pct / 100.0)
    if side == "SHORT":
        return entry * (1.0 - buffer_pct / 100.0)
    return entry


def _round_qty_down_replay(qty: float, step: float) -> float:
    if step <= 0:
        return max(0.0, float(qty))
    return max(0.0, int(float(qty) / step) * step)


def _split_qty_replay(qty: float, splits) -> tuple[list[float], list[int]]:
    step = max(float(BITUNIX_QTY_STEP or 0), 0.00000001)
    min_leg = max(float(BITUNIX_MIN_BASE_QTY or 0), step)
    total = _round_qty_down_replay(float(qty), step)
    if total <= 0 or total < min_leg:
        return [0.0, 0.0, 0.0], []

    a, b, c = splits
    q1 = _round_qty_down_replay(total * a, step)
    q2 = _round_qty_down_replay(total * b, step)
    q3 = _round_qty_down_replay(max(0.0, total - q1 - q2), step)
    nonzero = [q for q in (q1, q2, q3) if q > 0]
    if len(nonzero) == 3 and min(nonzero) >= min_leg and abs((q1 + q2 + q3) - total) < (step + 1e-12):
        qtys = [q1, q2, q3]
        active = [1, 2, 3]
        return qtys, active

    qtys = [total, 0.0, 0.0]
    return qtys, [1]


def _simulate_replay_tp_plan(entry: float, sl: float, size_pct: float, tf: str, strategy_name: str = ""):
    risk_per_unit = abs(float(entry) - float(sl))
    if risk_per_unit <= 0:
        return [0.0, 0.0, 0.0], []
    balance_available = max(0.0, float(BITUNIX_MIN_NOTIONAL_USD or 0.0) * 2.0)
    leverage = int(BITUNIX_LIQUIDATION_MAX_LEVERAGE_BY_TF.get(tf, BITUNIX_DEFAULT_LEVERAGE) or BITUNIX_DEFAULT_LEVERAGE or 1)
    leverage = max(1, leverage)
    effective_risk_cap_pct = (float(size_pct) / 100.0) if float(size_pct or 0) > 0 else float(BITUNIX_RISK_CAP_PCT)
    risk_from_balance = balance_available * effective_risk_cap_pct
    risk_budget = min(float(BITUNIX_MAX_RISK_USD), risk_from_balance) if balance_available > 0 else 0.0
    risk_qty = (risk_budget / risk_per_unit) if risk_budget > 0 else 0.0
    affordable_notional = max(0.0, balance_available * leverage * 0.98)
    affordable_qty = (affordable_notional / float(entry)) if float(entry) > 0 else 0.0
    qty = min(risk_qty, affordable_qty) if affordable_qty > 0 else 0.0
    if (
        qty > 0
        and balance_available >= float(BITUNIX_MIN_NOTIONAL_USD)
        and affordable_notional >= float(BITUNIX_MIN_NOTIONAL_USD)
        and qty * float(entry) < float(BITUNIX_MIN_NOTIONAL_USD)
    ):
        qty = min(float(BITUNIX_MIN_NOTIONAL_USD) / float(entry), affordable_qty)
    qty = _round_qty_down_replay(qty, float(BITUNIX_QTY_STEP))
    if qty < float(BITUNIX_MIN_BASE_QTY):
        qty = float(BITUNIX_MIN_BASE_QTY) if affordable_qty >= float(BITUNIX_MIN_BASE_QTY) else 0.0
    qty = _round_qty_down_replay(qty, float(BITUNIX_QTY_STEP))
    splits = SMART_MONEY_TP_SPLITS if str(strategy_name or "").upper() == "SMART_MONEY_LIQUIDITY" else BITUNIX_TP_SPLITS
    return _split_qty_replay(qty, splits)


def _trade_outcome_r(tr: dict, evt_type: str) -> float:
    if evt_type in {"TP1", "TP2", "TP3"}:
        return _trade_realized_partial_r(tr)
    if evt_type == "TP3":
        return _trade_realized_partial_r(tr)
    if evt_type == "ENTRY_CLOSE":
        return _trade_realized_partial_r(tr)
    if evt_type == "PROFIT_SL":
        return _trade_realized_partial_r(tr) + (_trade_remaining_frac(tr) * max(0.0, _trade_stop_r(tr)))
    if evt_type == "SL":
        return _trade_realized_partial_r(tr) + (_trade_remaining_frac(tr) * _trade_stop_r(tr))
    return 0.0


def _to_df(rows, bars_needed: int):
    if not rows:
        return pd.DataFrame()
    by_ts = {}
    for row in rows:
        by_ts[row[0]] = row
    ordered = sorted(by_ts.values(), key=lambda x: float(x[0]))
    if len(ordered) > bars_needed:
        ordered = ordered[-bars_needed:]
    df = pd.DataFrame(
        ordered,
        columns=["OpenTime", "Open", "High", "Low", "Close", "Volume", "VolCcy", "VolCcyQuote", "Confirm"],
    )
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = df[col].astype(float)
    df.index = pd.to_datetime(df["OpenTime"].astype(float), unit="ms", utc=True)
    df.index.name = "Datetime"
    return df[["Open", "High", "Low", "Close", "Volume"]]


def _fetch_attempt(interval: str, bars_needed: int, url: str, cursor_param: str):
    okx_symbol = SYMBOL.replace("USDT", "-USDT-SWAP")
    okx_interval = INTERVAL_MAP.get(interval, "1H")
    per_call = 300
    max_calls = max(2, math.ceil(bars_needed / per_call) + 4)
    cursor = None
    all_rows = []
    oldest_seen = None

    for _ in range(max_calls):
        params = {"instId": okx_symbol, "bar": okx_interval, "limit": per_call}
        if cursor is not None:
            params[cursor_param] = cursor
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != "0":
            break
        rows = payload.get("data", [])
        if not rows:
            break
        all_rows.extend(rows)
        page_oldest = min(float(r[0]) for r in rows)
        if oldest_seen is not None and page_oldest >= oldest_seen:
            break
        oldest_seen = page_oldest
        cursor = str(int(page_oldest))
        if len(all_rows) >= bars_needed + per_call:
            break

    return _to_df(all_rows, bars_needed)


def fetch_klines_history(interval: str, bars_needed: int):
    """Fetch enough candles for replay using multiple OKX pagination strategies."""
    candidates = []
    for url in (HISTORY_KLINES_URL, KLINES_URL):
        for cursor_param in ("after", "before"):
            try:
                df = _fetch_attempt(interval, bars_needed, url, cursor_param)
                if not df.empty:
                    candidates.append(df)
            except Exception:
                continue

    if candidates:
        candidates.sort(key=lambda d: (len(d), d.index.max() - d.index.min()), reverse=True)
        return candidates[0]

    return fetch_klines(interval=interval, limit=min(2000, bars_needed))


def _session_name(ts):
    sessions = get_adjusted_sessions(ts.to_pydatetime())
    h = ts.hour + ts.minute / 60.0
    for s_name, s_times in sessions.items():
        s_open = s_times["open"]
        s_close = s_times["close"]
        if s_open < s_close:
            if s_open <= h < s_close:
                return s_name
        else:
            if h >= s_open or h < s_close:
                return s_name
    return "ASIA"


def _build_macro_trend_series(days: int):
    bars_needed = max(500, 24 * days + 400)
    df_1h = fetch_klines_history("1h", bars_needed)
    if df_1h.empty:
        return pd.Series(dtype="object")
    close = df_1h["Close"]
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema100 = close.ewm(span=100, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()

    trend = pd.Series("Ranging", index=df_1h.index, dtype="object")
    bull = (close > ema50) & (ema50 > ema100) & (ema100 > ema200)
    bear = (close < ema50) & (ema50 < ema100) & (ema100 < ema200)
    trend[bull] = "Trending Bullish"
    trend[bear] = "Trending Bearish"
    return trend


def _build_trend_series(interval: str, days: int, extra_bars: int = 400):
    candles_per_day = {"5m": 288, "15m": 96, "1h": 24, "4h": 6, "1d": 1, "1w": 1 / 7}.get(interval, 24)
    bars_needed = max(300, int(candles_per_day * days + extra_bars))
    df = fetch_klines_history(interval, bars_needed)
    if df.empty:
        return pd.Series(dtype="object")
    close = df["Close"]
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema100 = close.ewm(span=100, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()

    trend = pd.Series("Ranging", index=df.index, dtype="object")
    bull = (close > ema50) & (ema50 > ema100) & (ema100 > ema200)
    bear = (close < ema50) & (ema50 < ema100) & (ema100 < ema200)
    trend[bull] = "Trending Bullish"
    trend[bear] = "Trending Bearish"
    return trend


def _trend_side(trend_name: str):
    t = str(trend_name or "").lower()
    if "bullish" in t:
        return "LONG"
    if "bearish" in t:
        return "SHORT"
    return None


def _anchor_trend_for_tf(tf: str, idx, trend_map: dict):
    order_map = {
        "5m": ["15m", "1h", "4h", "1d", "1w"],
        "15m": ["15m", "1h", "4h", "1d", "1w"],
        "1h": ["1h", "4h", "1d", "1w"],
        "4h": ["4h", "1d", "1w"],
    }
    for k in order_map.get(tf, [tf, "1h", "4h", "1d", "1w"]):
        series = trend_map.get(k)
        if series is None or series.empty:
            continue
        val = series.asof(idx)
        if isinstance(val, str) and _trend_side(val):
            return val, k
    return "Ranging", None


def _orderflow_anomaly_placeholder():
    # Historical OI/liquidation block data is not reconstructed in replay yet.
    # Keep the same gate shape but with neutral anomaly signal.
    return False, 0.0, 0.0


def _has_opposite_divergence(df_slice: pd.DataFrame, side: str, timeframe: str) -> bool:
    if df_slice is None or df_slice.empty:
        return False
    opposite = "SHORT" if side == "LONG" else "LONG"
    try:
        div_sigs = check_rsi_divergence(df_slice, timeframe)
    except Exception:
        return False
    return any(sig.get("active", True) and sig.get("side") == opposite for sig in div_sigs)


def _same_side_divergence_hits_replay(slice_data: dict, side: str, timeframe: str):
    hits = []
    for tf in [timeframe, "5m", "15m", "1h", "4h"]:
        df_slice = (slice_data or {}).get(tf)
        if df_slice is None or df_slice.empty:
            continue
        try:
            div_sigs = check_rsi_divergence(df_slice, tf)
        except Exception:
            continue
        if any(sig.get("active", True) and sig.get("side") == side for sig in div_sigs):
            if tf not in hits:
                hits.append(tf)
    return hits


def _recent_liquidity_sweep_hits_replay(slice_data: dict, side: str, timeframe: str, levels_proxy: dict):
    if not levels_proxy:
        return []
    hits = []
    for tf in [timeframe, "5m", "15m", "1h", "4h"]:
        df_slice = (slice_data or {}).get(tf)
        if df_slice is None or df_slice.empty or len(df_slice) < 2:
            continue
        curr = df_slice.iloc[-1]
        prev = df_slice.iloc[-2]
        sweeps = check_liquidity_sweep(
            float(curr.get("High", 0) or 0),
            float(curr.get("Low", 0) or 0),
            levels_proxy,
            prev_high=float(prev.get("High", 0) or 0),
            prev_low=float(prev.get("Low", 0) or 0),
        )
        side_sweeps = [sw for sw in sweeps if str(sw.get("side", "")).upper() == str(side or "").upper()]
        if side_sweeps and tf not in hits:
            hits.append(tf)
    return hits


def _key_level_reaction_hits_replay(slice_data: dict, side: str, timeframe: str, levels_proxy: dict):
    hits = []
    target_levels = (
        ["DO", "PDL", "PWL", "PML", "Dump", "DumpMax"]
        if str(side or "").upper() == "LONG"
        else ["DO", "PDH", "PWH", "PMH", "Pump", "PumpMax"]
    )
    for tf in [timeframe, "5m", "15m", "1h", "4h"]:
        df_slice = (slice_data or {}).get(tf)
        if df_slice is None or df_slice.empty:
            continue
        curr = df_slice.iloc[-1]
        close = float(curr.get("Close", 0) or 0)
        open_ = float(curr.get("Open", close) or close)
        high = float(curr.get("High", close) or close)
        low = float(curr.get("Low", close) or close)
        atr_val = float(curr.get("ATR", 0) or 0)
        if close <= 0:
            continue
        band = max(close * 0.0010, atr_val * 0.35 if atr_val > 0 else 0.0)
        body = max(1e-9, abs(close - open_))
        if str(side or "").upper() == "LONG":
            wick_ok = max(0.0, min(open_, close) - low) / body >= 1.0 or close > open_
        else:
            wick_ok = max(0.0, high - max(open_, close)) / body >= 1.0 or close < open_
        if not wick_ok:
            continue
        for level_name in target_levels:
            level = float((levels_proxy or {}).get(level_name, 0) or 0)
            if level <= 0:
                continue
            if str(side or "").upper() == "LONG":
                near_level = abs(low - level) <= band and close >= level
            else:
                near_level = abs(high - level) <= band and close <= level
            if near_level:
                hits.append(f"{tf} key-level reaction ({level_name})")
                break
    return hits


def _get_reversal_override_replay(tf: str, side: str, evt: dict, score: float, slice_data: dict, levels_proxy: dict):
    if not REVERSAL_OVERRIDE_ENABLED:
        return False
    proofs = []
    divergence_hits = _same_side_divergence_hits_replay(slice_data, side, tf)
    if divergence_hits:
        proofs.append(f"RSI divergence on {', '.join(divergence_hits)}")
    sweep_hits = _recent_liquidity_sweep_hits_replay(slice_data, side, tf, levels_proxy)
    if sweep_hits:
        proofs.append(f"liquidity sweep on {', '.join(sweep_hits)}")
    level_hits = _key_level_reaction_hits_replay(slice_data, side, tf, levels_proxy)
    if level_hits:
        proofs.append(f"key-level reaction on {', '.join(level_hits)}")

    trigger = str((evt or {}).get("trigger") or "").strip().upper()
    strategy = str((evt or {}).get("strategy") or "").strip().upper()
    if trigger == "ONE_H_RECLAIM":
        proofs.append("reclaim of key level")
    elif trigger == "HTF_PULLBACK":
        proofs.append("pullback reclaim")
    if strategy == "SMART_MONEY_LIQUIDITY":
        proofs.append("liquidity sweep structure break")

    if score >= float(REVERSAL_OVERRIDE_MIN_SCORE):
        proofs.append(f"score {int(score)}")

    unique_proofs = []
    for proof in proofs:
        if proof not in unique_proofs:
            unique_proofs.append(proof)
    has_strong_score = any(p.startswith("score ") for p in unique_proofs)
    return has_strong_score and len(unique_proofs) >= int(REVERSAL_OVERRIDE_MIN_PROOFS)


def _structure_anchor_tf_replay(tf: str):
    return "15m" if str(tf) == "5m" else str(tf)


def _structure_guard_mode_replay(tf: str):
    return str((STRUCTURE_GUARD_MODE_BY_TF or {}).get(str(tf), "hard")).strip().lower()


def _get_recent_bos_context_replay(slice_data: dict, tf: str):
    if not BOS_GUARD_ENABLED:
        return {}
    anchor_tf = _structure_anchor_tf_replay(tf)
    df = (slice_data or {}).get(anchor_tf)
    if df is None or df.empty:
        return {}
    swing_lb = int(max(3, BOS_GUARD_SWING_LOOKBACK))
    recent_bars = int(max(2, BOS_GUARD_RECENT_BARS))
    reclaim_bars = int(max(1, BOS_GUARD_RECLAIM_BARS))
    if len(df) < swing_lb + 3:
        return {}

    found = None
    start_idx = max(swing_lb, len(df) - recent_bars)
    for i in range(start_idx, len(df)):
        prior = df.iloc[i - swing_lb:i]
        if prior is None or prior.empty:
            continue
        prev_high = float(prior["High"].max())
        prev_low = float(prior["Low"].min())
        row = df.iloc[i]
        row_close = float(row.get("Close", 0) or 0)
        row_high = float(row.get("High", 0) or 0)
        row_low = float(row.get("Low", 0) or 0)
        if row_close > prev_high or row_high > prev_high:
            found = {"side": "LONG", "level": prev_high, "idx": i, "tf": anchor_tf}
        if row_close < prev_low or row_low < prev_low:
            found = {"side": "SHORT", "level": prev_low, "idx": i, "tf": anchor_tf}

    if not found:
        return {}

    bos_idx = int(found["idx"])
    pre_bos = df.iloc[max(0, bos_idx - swing_lb):bos_idx]
    bos_bar = df.iloc[bos_idx]
    latest = df.iloc[-1]
    latest_close = float(latest.get("Close", 0) or 0)
    latest_high = float(latest.get("High", latest_close) or latest_close)
    latest_low = float(latest.get("Low", latest_close) or latest_close)
    atr_ref = float(df.iloc[bos_idx].get("ATR", 0) or 0)
    tol = atr_ref * 0.15 if atr_ref > 0 else abs(float(found["level"])) * 0.0004
    follow = df.iloc[bos_idx:min(len(df), bos_idx + reclaim_bars + 1)]
    post_bos = df.iloc[min(len(df), bos_idx + 1):]
    reclaimed = False
    weak_follow = True
    lower_high = False
    lower_low = False
    higher_high = False
    higher_low = False
    continuation_after_reclaim = False
    bos_reference_high = float(pre_bos["High"].max()) if not pre_bos.empty else float(bos_bar.get("High", latest_high) or latest_high)
    bos_reference_low = float(pre_bos["Low"].min()) if not pre_bos.empty else float(bos_bar.get("Low", latest_low) or latest_low)
    if not follow.empty:
        if found["side"] == "SHORT":
            min_follow = float(follow["Low"].min())
            extension = max(0.0, float(found["level"]) - min_follow)
            weak_follow = atr_ref <= 0 or extension <= atr_ref * 0.8
            reclaimed = latest_close > float(found["level"]) and weak_follow
        else:
            max_follow = float(follow["High"].max())
            extension = max(0.0, max_follow - float(found["level"]))
            weak_follow = atr_ref <= 0 or extension <= atr_ref * 0.8
            reclaimed = latest_close < float(found["level"]) and weak_follow
    if not post_bos.empty:
        post_high = float(post_bos["High"].max())
        post_low = float(post_bos["Low"].min())
        lower_high = post_high < (bos_reference_high - tol)
        lower_low = post_low < (float(bos_bar.get("Low", post_low) or post_low) - tol)
        higher_high = post_high > (bos_reference_high + tol)
        higher_low = post_low > (float(found["level"]) + tol)
        if found["side"] == "SHORT":
            continuation_after_reclaim = reclaimed and higher_high and latest_close > bos_reference_high - tol
        else:
            continuation_after_reclaim = reclaimed and lower_low and latest_close < bos_reference_low + tol
    found["reclaimed"] = reclaimed
    found["weak_follow"] = weak_follow
    found["lower_high"] = lower_high
    found["lower_low"] = lower_low
    found["higher_high"] = higher_high
    found["higher_low"] = higher_low
    found["continuation_after_reclaim"] = continuation_after_reclaim
    if found["side"] == "SHORT":
        found["pullback_only"] = bool(reclaimed or (weak_follow and not lower_high and not lower_low) or continuation_after_reclaim)
        found["reversal_confirmed"] = bool(lower_high and lower_low and not reclaimed)
    else:
        found["pullback_only"] = bool(reclaimed or (weak_follow and not higher_high and not higher_low) or continuation_after_reclaim)
        found["reversal_confirmed"] = bool(higher_high and higher_low and not reclaimed)
    return found


def _get_bos_guard_reason_replay(slice_data: dict, tf: str, side: str):
    mode = _structure_guard_mode_replay(tf)
    if mode == "off":
        return ""
    ctx = _get_recent_bos_context_replay(slice_data, tf)
    if not ctx:
        return ""
    bos_side = str(ctx.get("side", "")).upper()
    if not bos_side or bos_side == str(side or "").upper():
        return ""
    if ctx.get("reclaimed") or ctx.get("pullback_only") or ctx.get("continuation_after_reclaim"):
        return ""
    if mode == "soft" and not ctx.get("reversal_confirmed"):
        return ""
    state = "reversal follow-through" if ctx.get("reversal_confirmed") else "active BOS"
    return f"recent {bos_side.lower()} BOS on {ctx.get('tf', tf)} is still active ({state})"


def _get_rsi_pullback_scalp_override_replay(slice_data: dict, tf: str, side: str, levels_proxy: dict):
    if not RSI_PULLBACK_SCALP_ENABLED:
        return False
    if str(tf) not in set(RSI_PULLBACK_SCALP_TFS):
        return False
    df = (slice_data or {}).get(tf)
    if df is None or df.empty or len(df) < 3:
        return False
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    side = str(side or "").upper()
    close = float(curr.get("Close", 0) or 0)
    open_ = float(curr.get("Open", close) or close)
    high = float(curr.get("High", close) or close)
    low = float(curr.get("Low", close) or close)
    prev_close = float(prev.get("Close", close) or close)
    prev_high = float(prev.get("High", high) or high)
    prev_low = float(prev.get("Low", low) or low)
    rsi = float(curr.get("RSI", 50) or 50)
    atr_val = float(curr.get("ATR", 0) or 0)
    ema2 = float(curr.get("EMA2", close) or close)
    if atr_val <= 0:
        return False
    bos_ctx = _get_recent_bos_context_replay(slice_data, tf)

    if side == "SHORT":
        if rsi < float(RSI_PULLBACK_SCALP_OB):
            return False
    elif side == "LONG":
        if rsi > float(RSI_PULLBACK_SCALP_OS):
            return False
    else:
        return False

    body_atr = abs(close - open_) / atr_val
    displacement_atr = abs(close - prev_close) / atr_val
    if side == "SHORT":
        impulse_filter = (
            (close > open_ and body_atr >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
            or displacement_atr >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
        )
        ema_filter = close > ema2 and abs(close - ema2) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_EMA_ATR_DISTANCE)
        upper_wick = max(0.0, high - max(open_, close))
        body_abs = max(1e-9, abs(close - open_))
        wick_filter = upper_wick / body_abs >= float(RSI_PULLBACK_SCALP_MIN_WICK_BODY_RATIO)
        structure_shift = close < prev_low
    else:
        impulse_filter = (
            (close < open_ and body_atr >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
            or displacement_atr >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
        )
        ema_filter = close < ema2 and abs(close - ema2) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_EMA_ATR_DISTANCE)
        lower_wick = max(0.0, min(open_, close) - low)
        body_abs = max(1e-9, abs(close - open_))
        wick_filter = lower_wick / body_abs >= float(RSI_PULLBACK_SCALP_MIN_WICK_BODY_RATIO)
        structure_shift = close > prev_high

    sweep_hits = _recent_liquidity_sweep_hits_replay(slice_data, side, tf, levels_proxy)
    level_hits = _key_level_reaction_hits_replay(slice_data, side, tf, levels_proxy)
    filters = 0
    filters += 1 if sweep_hits else 0
    filters += 1 if level_hits else 0
    filters += 1 if impulse_filter else 0
    filters += 1 if ema_filter else 0
    filters += 1 if wick_filter else 0
    after_bos_or_impulse = impulse_filter or (bos_ctx and str(bos_ctx.get("side", "")).upper() == side)
    return structure_shift and after_bos_or_impulse and filters >= int(RSI_PULLBACK_SCALP_MIN_FILTERS)


def _get_weekend_scalp_exception_replay(slice_data: dict, tf: str, side: str, levels_proxy: dict):
    if str(tf) not in set(RSI_PULLBACK_SCALP_TFS):
        return False
    df = (slice_data or {}).get(tf)
    if df is None or df.empty or len(df) < 3:
        return False
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    side = str(side or "").upper()
    close = float(curr.get("Close", 0) or 0)
    open_ = float(curr.get("Open", close) or close)
    prev_close = float(prev.get("Close", close) or close)
    rsi = float(curr.get("RSI", 50) or 50)
    atr_val = float(curr.get("ATR", 0) or 0)
    if atr_val <= 0:
        return False
    if side == "SHORT":
        extreme_ok = rsi >= float(RSI_PULLBACK_SCALP_OB)
        impulse_ok = (
            (close > open_ and abs(close - open_) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
            or abs(close - prev_close) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
        )
    elif side == "LONG":
        extreme_ok = rsi <= float(RSI_PULLBACK_SCALP_OS)
        impulse_ok = (
            (close < open_ and abs(close - open_) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
            or abs(close - prev_close) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
        )
    else:
        return False

    sweep_ok = bool(_recent_liquidity_sweep_hits_replay(slice_data, side, tf, levels_proxy))
    pullback_ok = _get_rsi_pullback_scalp_override_replay(slice_data, tf, side, levels_proxy)
    return extreme_ok and impulse_ok and sweep_ok and pullback_ok


def _apply_rsi_pullback_fast_targets_replay(evt: dict):
    entry = float(evt.get("entry", 0) or 0)
    sl = float(evt.get("sl", 0) or 0)
    side = str(evt.get("side", "")).upper()
    risk = abs(entry - sl)
    if risk <= 0 or side not in {"LONG", "SHORT"}:
        return evt
    mult = 1.0 if side == "LONG" else -1.0
    evt = dict(evt)
    evt["tp1"] = float(entry + mult * risk * float(RSI_PULLBACK_SCALP_TP1_R))
    evt["tp2"] = float(entry + mult * risk * float(RSI_PULLBACK_SCALP_TP2_R))
    evt["tp3"] = float(entry + mult * risk * float(RSI_PULLBACK_SCALP_TP3_R))
    evt["trigger"] = "RSI_PULLBACK_SCALP"
    return evt


def _get_5m_higher_tf_guard_reason_replay(slice_data: dict, side: str, trend_map: dict, idx):
    if not FIVE_MIN_REQUIRE_15M_PERMISSION:
        return None
    df_15m = (slice_data or {}).get("15m")
    try:
        if df_15m is None or df_15m.empty or len(df_15m) < 3:
            return None
    except Exception:
        return None

    curr = df_15m.iloc[-1]
    prev = df_15m.iloc[-2]
    rsi_now = float(curr.get("RSI", 50) or 50)
    rsi_prev = float(prev.get("RSI", rsi_now) or rsi_now)
    smooth_now = float(curr.get("MomentumSmooth", rsi_now) or rsi_now)
    smooth_prev = float(prev.get("MomentumSmooth", smooth_now) or smooth_now)
    zone_15m = classify_momentum_zone(smooth_now, "15m")

    trend_15m = "Ranging"
    try:
        series_15m = (trend_map or {}).get("15m")
        if series_15m is not None and not series_15m.empty:
            val = series_15m.asof(idx)
            if isinstance(val, str):
                trend_15m = val
    except Exception:
        trend_15m = "Ranging"
    trend_side_15m = _trend_side(trend_15m)
    side = str(side or "").upper()

    if side == "LONG":
        if trend_side_15m == "SHORT" and rsi_now < 52 and smooth_now <= smooth_prev:
            return f"15m trend is still bearish and RSI is not recovering yet ({rsi_now:.1f})"
        if zone_15m != "OS" and rsi_now < 50 and rsi_now <= rsi_prev and smooth_now <= smooth_prev:
            return f"15m RSI still has room to fall ({rsi_now:.1f})"
    elif side == "SHORT":
        if trend_side_15m == "LONG" and rsi_now > 48 and smooth_now >= smooth_prev:
            return f"15m trend is still bullish and RSI is not rolling over yet ({rsi_now:.1f})"
        if zone_15m != "OB" and rsi_now > 50 and rsi_now >= rsi_prev and smooth_now >= smooth_prev:
            return f"15m RSI still has room to rise ({rsi_now:.1f})"
    return None


def _is_unstable_impulse_replay(data: dict, side: str):
    if not FALLING_KNIFE_FILTER_ENABLED:
        return False, ""

    checks = [
        ("5m", int(FALLING_KNIFE_LOOKBACK_5M), float(FALLING_KNIFE_MOVE_PCT_5M)),
        ("15m", int(FALLING_KNIFE_LOOKBACK_15M), float(FALLING_KNIFE_MOVE_PCT_15M)),
    ]
    for tf, lookback, move_thr in checks:
        df = data.get(tf)
        if df is None or df.empty or len(df) < lookback + 1:
            continue

        closes = df["Close"]
        opens = df["Open"]
        prev_close = float(closes.iloc[-(lookback + 1)])
        curr_close = float(closes.iloc[-1])
        if prev_close <= 0:
            continue

        move_pct = (curr_close / prev_close - 1.0) * 100.0
        c1, c2, c3 = float(closes.iloc[-1]), float(closes.iloc[-2]), float(closes.iloc[-3])
        o1, o2, o3 = float(opens.iloc[-1]), float(opens.iloc[-2]), float(opens.iloc[-3])
        red_count = int(c1 < o1) + int(c2 < o2) + int(c3 < o3)
        green_count = int(c1 > o1) + int(c2 > o2) + int(c3 > o3)
        down_streak = c1 < c2 < c3
        up_streak = c1 > c2 > c3
        bounce = c1 > c2 > c3
        pullback = c1 < c2 < c3

        if side == "LONG":
            knife = (move_pct <= -abs(move_thr)) and (down_streak or red_count >= 2)
            if knife and not bounce:
                return True, f"{tf} impulse {move_pct:+.2f}% (no base)"
        else:
            blowoff = (move_pct >= abs(move_thr)) and (up_streak or green_count >= 2)
            if blowoff and not pullback:
                return True, f"{tf} impulse {move_pct:+.2f}% (no top)"

    return False, ""


def _detect_regime_from_df(df_slice: pd.DataFrame):
    if df_slice is None or df_slice.empty or len(df_slice) < 80:
        return "RANGE"
    close = df_slice["Close"]
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema55 = close.ewm(span=55, adjust=False).mean()
    curr_close = float(close.iloc[-1])
    atr_pct = float(df_slice["ATR"].iloc[-1] / curr_close * 100) if curr_close else 0.0
    spread_pct = abs(float(ema21.iloc[-1] - ema55.iloc[-1])) / curr_close * 100 if curr_close else 0.0
    slope_pct = abs(float(ema21.iloc[-1] - ema21.iloc[-6])) / curr_close * 100 if len(ema21) >= 6 and curr_close else 0.0
    if atr_pct >= 1.2:
        return "HIGH_VOL"
    if spread_pct >= 0.35 and slope_pct >= 0.20:
        return "TREND"
    return "RANGE"


def _recent_health_from_results(results, lookback: int):
    sample = results[-lookback:]
    if not sample:
        return "NEUTRAL"
    trades = len(sample)
    if trades < SCALP_SELF_TUNE_MIN_CLOSED:
        return "NEUTRAL"
    def _metric_outcome(item):
        if isinstance(item, dict):
            return item.get("metric", "breakeven"), float(item.get("r", 0.0))
        val = float(item)
        if val > 0:
            return "wins", val
        if val < 0:
            return "losses", val
        return "breakeven", val

    parsed = [_metric_outcome(item) for item in sample]
    wins = sum(1 for metric, _ in parsed if metric == "wins")
    losses = sum(1 for metric, _ in parsed if metric == "losses")
    win_rate = (wins / (wins + losses) * 100.0) if (wins + losses) else 0.0
    avg_r = sum(r for _, r in parsed) / len(parsed)
    if win_rate <= SCALP_SELF_TUNE_LOW_WR or avg_r <= SCALP_SELF_TUNE_LOW_AVGR:
        return "TIGHTEN"
    if win_rate >= SCALP_SELF_TUNE_HIGH_WR and avg_r >= SCALP_SELF_TUNE_HIGH_AVGR:
        return "LOOSEN"
    return "NEUTRAL"


def _build_proxy_levels(df: pd.DataFrame, i: int, idx):
    """Build lightweight historical level proxies for score calculation."""
    hist = df.iloc[: i + 1]
    if hist.empty:
        return {}

    day_start = idx.floor("D")
    curr_day = hist[hist.index >= day_start]
    prev_day = hist[(hist.index >= day_start - pd.Timedelta(days=1)) & (hist.index < day_start)]
    prev_week = hist[(hist.index >= day_start - pd.Timedelta(days=7)) & (hist.index < day_start)]
    prev_month = hist[(hist.index >= day_start - pd.Timedelta(days=30)) & (hist.index < day_start)]

    do_val = float(curr_day.iloc[0]["Open"]) if not curr_day.empty else float(hist.iloc[-1]["Close"])
    pdh = float(prev_day["High"].max()) if not prev_day.empty else float(hist["High"].tail(24).max())
    pdl = float(prev_day["Low"].min()) if not prev_day.empty else float(hist["Low"].tail(24).min())
    pwh = float(prev_week["High"].max()) if not prev_week.empty else pdh
    pwl = float(prev_week["Low"].min()) if not prev_week.empty else pdl
    pmh = float(prev_month["High"].max()) if not prev_month.empty else pwh
    pml = float(prev_month["Low"].min()) if not prev_month.empty else pwl

    return {
        "DO": do_val,
        "PDH": pdh,
        "PDL": pdl,
        "PWH": pwh,
        "PWL": pwl,
        "PMH": pmh,
        "PML": pml,
        "Pump": pdh,
        "Dump": pdl,
        "PumpMax": pwh,
        "DumpMax": pwl,
    }


def _build_proxy_levels_from_hist(hist: pd.DataFrame, idx):
    if hist is None or hist.empty:
        return {}

    day_start = idx.floor("D")
    curr_day = hist[hist.index >= day_start]
    prev_day = hist[(hist.index >= day_start - pd.Timedelta(days=1)) & (hist.index < day_start)]
    prev_week = hist[(hist.index >= day_start - pd.Timedelta(days=7)) & (hist.index < day_start)]
    prev_month = hist[(hist.index >= day_start - pd.Timedelta(days=30)) & (hist.index < day_start)]

    do_val = float(curr_day.iloc[0]["Open"]) if not curr_day.empty else float(hist.iloc[-1]["Close"])
    pdh = float(prev_day["High"].max()) if not prev_day.empty else float(hist["High"].tail(24).max())
    pdl = float(prev_day["Low"].min()) if not prev_day.empty else float(hist["Low"].tail(24).min())
    pwh = float(prev_week["High"].max()) if not prev_week.empty else pdh
    pwl = float(prev_week["Low"].min()) if not prev_week.empty else pdl
    pmh = float(prev_month["High"].max()) if not prev_month.empty else pwh
    pml = float(prev_month["Low"].min()) if not prev_month.empty else pwl

    return {
        "DO": do_val,
        "PDH": pdh,
        "PDL": pdl,
        "PWH": pwh,
        "PWL": pwl,
        "PMH": pmh,
        "PML": pml,
        "Pump": pdh,
        "Dump": pdl,
        "PumpMax": pwh,
        "DumpMax": pwl,
    }


def _bars_for_days(tf: str, days: int, extra: int = 120):
    candles_per_day = {
        "1m": 1440,
        "5m": 288,
        "15m": 96,
        "1h": 24,
        "4h": 6,
        "1d": 1,
        "1w": 1 / 7,
    }.get(tf, 24)
    return max(300, int(candles_per_day * days + extra))


def simulate_timeframe(tf: str, days: int, macro_trend_series: pd.Series, trend_map: dict, relaxed: bool = False):
    candles_per_day = {"5m": 288, "15m": 96, "1h": 24, "4h": 6}.get(tf, 24)
    bars_needed = max(300, candles_per_day * days + 50)
    df = fetch_klines_history(tf, bars_needed)
    if df.empty or len(df) < 120:
        return {
            "tf": tf,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "win_rate": 0.0,
            "hit_rate": 0.0,
            "avg_r": 0.0,
            "covered_days": 0.0,
        }

    covered_days = (df.index.max() - df.index.min()).total_seconds() / 86400.0
    df = calculate_channels(df)
    df = calculate_momentum(df)
    tracker = ScalpTracker(tf)
    aux_history = {tf: df}
    if tf == "5m":
        bars_aux = _bars_for_days("15m", days, extra=180)
        aux_15m = fetch_klines_history("15m", bars_aux)
        if aux_15m is not None and not aux_15m.empty:
            aux_history["15m"] = calculate_momentum(aux_15m)
    elif tf == "15m":
        bars_aux = _bars_for_days("5m", days, extra=180)
        aux_5m = fetch_klines_history("5m", bars_aux)
        if aux_5m is not None and not aux_5m.empty:
            aux_history["5m"] = calculate_momentum(aux_5m)

    active = []
    results = []
    hedge_mode = str(BITUNIX_POSITION_MODE or "").strip().upper() == "HEDGE"
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    side_hits = {"LONG": [], "SHORT": []}
    loss_streak = {"LONG": 0, "SHORT": 0}
    side_cooldown_until = {"LONG": 0.0, "SHORT": 0.0}

    for i, (idx, row) in enumerate(df.iterrows()):
        zone = classify_momentum_zone(float(row.get("MomentumSmooth", 50)), tf)
        close = float(row["Close"])
        high = float(row["High"])
        low = float(row["Low"])
        atr = float(row.get("ATR", 0))
        rsi_raw = float(row.get("RSI", 50))
        rsi_sm = float(row.get("MomentumSmooth", 50))
        candle_ts = idx.strftime("%Y-%m-%d %H:%M")
        now_ts = idx.timestamp()

        # Build new entries first (bot does signals then outcome checks in the same poll)
        events = []
        if tf in BASE_MOMENTUM_ENABLED_TFS:
            events.extend(tracker.update(zone, close, atr, candle_ts=candle_ts, rsi_raw=rsi_raw, rsi_smooth=rsi_sm))
        htf_evt = check_htf_pullback_entry(df.loc[:idx], tf)
        if htf_evt:
            events.append(htf_evt)
        one_h_evt = check_one_h_reclaim_entry(df.loc[:idx], tf)
        if one_h_evt:
            events.append(one_h_evt)
        for evt in events:
            if evt["type"] != "CONFIRMED":
                continue
            if idx.to_pydatetime() < cutoff_dt:
                continue

            side = evt["side"]
            slice_data = {}
            for aux_tf, aux_df in aux_history.items():
                sliced = aux_df.loc[:idx]
                if not sliced.empty:
                    slice_data[aux_tf] = sliced

            if _has_opposite_divergence(df.loc[:idx], side, tf):
                continue

            impulse_blocked, _ = _is_unstable_impulse_replay(slice_data, side)
            if impulse_blocked:
                continue

            if (not hedge_mode) and any(tr.get("side") != side for tr in active):
                continue

            if now_ts < side_cooldown_until.get(side, 0.0):
                continue

            # Build score same stack style as bot
            macro_trend = "Ranging"
            if not macro_trend_series.empty:
                mt = macro_trend_series.asof(idx)
                if isinstance(mt, str):
                    macro_trend = mt
            levels_proxy = _build_proxy_levels(df, i, idx)
            local_trend, _ = _anchor_trend_for_tf(tf, idx, trend_map)
            score, _ = calculate_signal_score(evt, df.loc[:idx], levels_proxy, local_trend or macro_trend, None, 0)
            reversal_override_allowed = _get_reversal_override_replay(
                tf, side, evt, score, slice_data, levels_proxy
            )
            rsi_pullback_override = _get_rsi_pullback_scalp_override_replay(slice_data, tf, side, levels_proxy)
            weekend_exception = _get_weekend_scalp_exception_replay(slice_data, tf, side, levels_proxy)
            if rsi_pullback_override or weekend_exception:
                reversal_override_allowed = True
                evt = _apply_rsi_pullback_fast_targets_replay(evt)
            regime_name = _detect_regime_from_df(df.loc[:idx]) if SCALP_REGIME_SWITCHING else "RANGE"
            regime_cfg = SCALP_REGIME_PROFILES.get(regime_name, {})
            score_delta = int(regime_cfg.get("score_delta", 0))
            regime_vol_min_mult = float(regime_cfg.get("vol_min_mult", 1.0))
            regime_vol_max_mult = float(regime_cfg.get("vol_max_mult", 1.0))
            tuning_state = _recent_health_from_results(results, SCALP_SELF_TUNE_LOOKBACK) if SCALP_SELF_TUNING_ENABLED else "NEUTRAL"
            tuning_delta = 1 if tuning_state == "TIGHTEN" else (-1 if tuning_state == "LOOSEN" else 0)

            if not WEEKEND_TRADING_ENABLED and idx.weekday() >= 5 and not weekend_exception:
                continue

            bos_guard_reason = _get_bos_guard_reason_replay(slice_data, tf, side)
            if bos_guard_reason and not rsi_pullback_override:
                continue

            # Volatility gate
            if VOLATILITY_FILTER_ENABLED and close > 0 and atr > 0:
                atr_pct = atr / close * 100
                min_pct = VOLATILITY_MIN_ATR_PCT.get(tf, 0.0)
                max_pct = VOLATILITY_MAX_ATR_PCT.get(tf, 99.0)
                if relaxed:
                    min_pct *= float(SCALP_RELAX_VOL_MIN_MULT)
                    max_pct *= float(SCALP_RELAX_VOL_MAX_MULT)
                min_pct *= regime_vol_min_mult
                max_pct *= regime_vol_max_mult
                if atr_pct < min_pct or atr_pct > max_pct:
                    continue

            # Orderflow safety gate
            if ORDERFLOW_SAFETY_ENABLED:
                anomaly, oi_pct, liq_usd = _orderflow_anomaly_placeholder()
                if anomaly and score < ORDERFLOW_ANOMALY_SCORE_MIN:
                    _ = oi_pct, liq_usd
                    continue

            session_name = _session_name(idx)
            allowed = SCALP_ALLOWED_SESSIONS_BY_TF.get(tf)
            if allowed and session_name not in allowed and not (relaxed and SCALP_RELAX_ALLOW_OFFSESSION):
                continue

            if tf == "5m":
                htf_guard_reason = _get_5m_higher_tf_guard_reason_replay(slice_data, side, trend_map, idx)
                if htf_guard_reason and not reversal_override_allowed:
                    continue

            min_score_tf = SCALP_MIN_SCORE_BY_TF.get(tf, 0)
            if relaxed:
                min_score_tf = max(0, int(min_score_tf) - int(SCALP_RELAX_MIN_SCORE_DELTA))
            min_score_tf = max(0, int(min_score_tf) + score_delta + tuning_delta)
            if score < min_score_tf:
                continue

            if SCALP_EXPOSURE_ENABLED:
                open_total = len(active)
                open_side = sum(1 for tr in active if tr["side"] == side)
                open_tf = sum(1 for tr in active if tr["tf"] == tf)
                tf_limit = int(SCALP_MAX_OPEN_PER_TF.get(tf, 1))
                if open_total >= SCALP_MAX_OPEN_TOTAL:
                    continue
                if open_side >= SCALP_MAX_OPEN_PER_SIDE:
                    continue
                if open_tf >= tf_limit:
                    continue

            # Trend gate and countertrend quota (use hierarchical anchor trend)
            bullish = {"Bullish", "Trending Bullish", "Strong Bullish"}
            bearish = {"Bearish", "Trending Bearish", "Strong Bearish"}
            trend_name = local_trend or macro_trend

            # Hard reversal guard: do not SHORT in bullish anchor trend and vice-versa.
            anchor_side = _trend_side(trend_name)
            if anchor_side and side != anchor_side and not reversal_override_allowed:
                continue

            trend_aligned = (
                trend_name == "Ranging"
                or (side == "LONG" and trend_name in bullish)
                or (side == "SHORT" and trend_name in bearish)
            )
            mode = str(SCALP_TREND_FILTER_MODE_BY_TF.get(tf, SCALP_TREND_FILTER_MODE)).strip().lower()
            countertrend_min_score = int(SCALP_COUNTERTREND_MIN_SCORE_BY_TF.get(tf, SCALP_COUNTERTREND_MIN_SCORE))
            session_cfg = SESSION_SCALP_MODE.get(session_name, {})
            session_countertrend_max = session_cfg.get("countertrend_max", SCALP_COUNTERTREND_MAX_PER_WINDOW)
            session_score_boost = session_cfg.get("score_boost", 0)

            if not trend_aligned:
                if mode == "hard":
                    if not reversal_override_allowed:
                        continue
                if mode == "soft":
                    if score < countertrend_min_score + session_score_boost:
                        continue
                    cutoff_ts = now_ts - SCALP_COUNTERTREND_WINDOW_SEC
                    side_hits[side] = [t for t in side_hits[side] if t >= cutoff_ts]
                    ct_extra = int(SCALP_RELAX_COUNTERTREND_EXTRA) if relaxed else 0
                    ct_max = session_countertrend_max + ct_extra
                    if len(side_hits[side]) >= ct_max:
                        continue
                    side_hits[side].append(now_ts)

            risk = abs(evt["entry"] - evt["sl"])
            if risk <= 0:
                continue

            profile = TIMEFRAME_PROFILES.get(tf, TIMEFRAME_PROFILES.get("5m", {"size": float(MIN_SIGNAL_SIZE_PCT)}))
            size_mult = float(regime_cfg.get("size_mult", 1.0))
            dyn_base = max(float(MIN_SIGNAL_SIZE_PCT), (score / 10) * float(profile.get("size", MIN_SIGNAL_SIZE_PCT))) if score else max(float(MIN_SIGNAL_SIZE_PCT), float(profile.get("size", MIN_SIGNAL_SIZE_PCT)))
            dyn_size = round(min(float(MAX_SIGNAL_SIZE_PCT), max(float(MIN_SIGNAL_SIZE_PCT), dyn_base * size_mult)), 1)
            tp_qtys, active_tp_indices = _simulate_replay_tp_plan(evt["entry"], evt["sl"], dyn_size, tf, evt.get("strategy"))
            if not active_tp_indices:
                active_tp_indices = [1, 2, 3]

            active.append(
                {
                    "side": side,
                    "entry": evt["entry"],
                    "sl": evt["sl"],
                    "tp1": evt["tp1"],
                    "tp2": evt["tp2"],
                    "tp3": evt["tp3"],
                    "risk": risk,
                    "tp1_hit": False,
                    "tp2_hit": False,
                    "tp3_hit": False,
                    "tf": tf,
                    "entry_candle_ts": candle_ts,
                    "tp_qtys": tp_qtys,
                    "signal_size_pct": dyn_size,
                    "active_tp_indices": active_tp_indices,
                    "breakeven_trigger": _breakeven_trigger_index_for_active(active_tp_indices),
                    "breakeven_price": _breakeven_lock_price_for_trade(side, evt["entry"]),
                }
            )

        # Resolve outcomes for all open trades on this candle
        survivors = []
        for tr in active:
            evt_type = _resolve_trade_event(tr, high, low, candle_ts)
            if evt_type is None:
                survivors.append(tr)
                continue

            side = tr["side"]
            if evt_type in {"TP1", "TP2", "TP3"}:
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "wins"})
                loss_streak[side] = 0
            elif evt_type == "PROFIT_SL":
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "wins"})
                loss_streak[side] = 0
            elif evt_type == "SL":
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "losses"})
                loss_streak[side] += 1
                if loss_streak[side] >= SCALP_LOSS_STREAK_LIMIT:
                    side_cooldown_until[side] = now_ts + SCALP_LOSS_COOLDOWN_SEC
            elif evt_type == "ENTRY_CLOSE":
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "wins" if _breakeven_counts_as_win(tr) else "breakeven"})
                if loss_streak[side] > 0:
                    loss_streak[side] -= 1
            else:
                survivors.append(tr)

        active = survivors

    for _ in active:
        results.append({"r": 0.0, "metric": "breakeven"})

    wins = sum(1 for r in results if r["metric"] == "wins")
    losses = sum(1 for r in results if r["metric"] == "losses")
    breakeven = sum(1 for r in results if r["metric"] == "breakeven")
    avg_r = (sum(r["r"] for r in results) / len(results)) if results else 0.0
    closed = wins + losses
    win_rate = (wins / closed * 100.0) if closed else 0.0
    hit_rate = (wins / len(results) * 100.0) if results else 0.0

    return {
        "tf": tf,
        "trades": len(results),
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": win_rate,
        "hit_rate": hit_rate,
        "avg_r": avg_r,
        "covered_days": covered_days,
    }


def simulate_smart_money_timeframe(tf: str, days: int):
    required_tfs = ["5m", "15m", "1h", "4h"]
    history = {}
    for req_tf in required_tfs:
        bars_needed = _bars_for_days(req_tf, days, extra=180)
        history[req_tf] = fetch_klines_history(req_tf, bars_needed)

    entry_df = history.get(tf)
    if entry_df is None or entry_df.empty or len(entry_df) < 120:
        return {
            "tf": tf,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "win_rate": 0.0,
            "hit_rate": 0.0,
            "avg_r": 0.0,
            "covered_days": 0.0,
        }

    covered_days = (entry_df.index.max() - entry_df.index.min()).total_seconds() / 86400.0
    active = []
    results = []
    seen_event_ids = set()
    daily_counts = {}
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)

    for idx, row in entry_df.iterrows():
        high = float(row["High"])
        low = float(row["Low"])
        candle_ts = idx.strftime("%Y-%m-%d %H:%M")

        if idx.to_pydatetime() >= cutoff_dt:
            slice_data = {}
            for req_tf, hist_df in history.items():
                if hist_df is None or hist_df.empty:
                    continue
                sliced = hist_df.loc[:idx]
                if not sliced.empty:
                    slice_data[req_tf] = sliced

            level_hist = slice_data.get("5m")
            if level_hist is None or level_hist.empty:
                level_hist = slice_data.get(tf)
            levels_proxy = _build_proxy_levels_from_hist(level_hist, idx) if level_hist is not None else {}
            day_key = idx.strftime("%Y-%m-%d")
            trades_today = int(daily_counts.get(day_key, 0))
            evt = detect_smart_money_entry(
                slice_data,
                levels_proxy,
                idx.to_pydatetime(),
                trades_today=trades_today,
                execution_tf=tf,
            )
            if evt:
                event_id = evt.get("event_id", f"{tf}_{evt['side']}_{candle_ts}")
                if event_id not in seen_event_ids:
                    risk = abs(float(evt["entry"]) - float(evt["sl"]))
                    if risk > 0:
                        size_pct = float(evt.get("size", 0) or 0) if evt.get("size") is not None else 0.0
                        if size_pct <= 0:
                            size_pct = float(MIN_SIGNAL_SIZE_PCT)
                        tp_qtys, active_tp_indices = _simulate_replay_tp_plan(
                            evt["entry"], evt["sl"], size_pct, tf, "SMART_MONEY_LIQUIDITY"
                        )
                        if not active_tp_indices:
                            active_tp_indices = [1, 2, 3]
                        active.append(
                            {
                                "side": evt["side"],
                                "entry": float(evt["entry"]),
                                "sl": float(evt["sl"]),
                                "tp1": float(evt["tp1"]),
                                "tp2": float(evt["tp2"]),
                                "tp3": float(evt["tp3"]),
                                "risk": risk,
                                "tp1_hit": False,
                                "tp2_hit": False,
                                "tp3_hit": False,
                                "tf": tf,
                                "entry_candle_ts": candle_ts,
                                "event_id": event_id,
                                "tp_qtys": tp_qtys,
                                "signal_size_pct": size_pct,
                                "active_tp_indices": active_tp_indices,
                                "breakeven_trigger": _breakeven_trigger_index_for_active(active_tp_indices),
                                "breakeven_price": _breakeven_lock_price_for_trade(evt["side"], evt["entry"]),
                            }
                        )
                        seen_event_ids.add(event_id)
                        daily_counts[day_key] = trades_today + 1

        survivors = []
        for tr in active:
            evt_type = _resolve_trade_event(tr, high, low, candle_ts)
            if evt_type is None:
                survivors.append(tr)
                continue

            if evt_type in {"TP1", "TP2", "TP3"}:
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "wins"})
            elif evt_type == "PROFIT_SL":
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "wins"})
            elif evt_type == "SL":
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "losses"})
            elif evt_type == "ENTRY_CLOSE":
                results.append({"r": _trade_outcome_r(tr, evt_type), "metric": "wins" if _breakeven_counts_as_win(tr) else "breakeven"})
            else:
                survivors.append(tr)

        active = survivors

    for _ in active:
        results.append({"r": 0.0, "metric": "breakeven"})

    wins = sum(1 for r in results if r["metric"] == "wins")
    losses = sum(1 for r in results if r["metric"] == "losses")
    breakeven = sum(1 for r in results if r["metric"] == "breakeven")
    avg_r = (sum(r["r"] for r in results) / len(results)) if results else 0.0
    closed = wins + losses
    win_rate = (wins / closed * 100.0) if closed else 0.0
    hit_rate = (wins / len(results) * 100.0) if results else 0.0

    return {
        "tf": tf,
        "trades": len(results),
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": win_rate,
        "hit_rate": hit_rate,
        "avg_r": avg_r,
        "covered_days": covered_days,
    }


def _merge_backtest_rows(base_row: dict, extra_row: dict):
    if not base_row:
        return dict(extra_row)
    if not extra_row:
        return dict(base_row)

    trades = int(base_row.get("trades", 0)) + int(extra_row.get("trades", 0))
    wins = int(base_row.get("wins", 0)) + int(extra_row.get("wins", 0))
    losses = int(base_row.get("losses", 0)) + int(extra_row.get("losses", 0))
    breakeven = int(base_row.get("breakeven", 0)) + int(extra_row.get("breakeven", 0))
    covered_days = max(float(base_row.get("covered_days", 0.0)), float(extra_row.get("covered_days", 0.0)))
    avg_r = 0.0
    if trades:
        avg_r = (
            float(base_row.get("avg_r", 0.0)) * int(base_row.get("trades", 0))
            + float(extra_row.get("avg_r", 0.0)) * int(extra_row.get("trades", 0))
        ) / trades
    closed = wins + losses
    win_rate = (wins / closed * 100.0) if closed else 0.0
    hit_rate = (wins / trades * 100.0) if trades else 0.0

    return {
        "tf": base_row.get("tf", extra_row.get("tf")),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": win_rate,
        "hit_rate": hit_rate,
        "avg_r": avg_r,
        "covered_days": covered_days,
    }


def _print_replay_row(row: dict):
    print(
        f"{row['tf']:>4} | trades={row['trades']:>4} wins={row['wins']:>4} "
        f"losses={row['losses']:>4} be={row['breakeven']:>4} "
        f"wr={row['win_rate']:>5.1f}% hit={row['hit_rate']:>5.1f}% "
        f"avgR={row['avg_r']:+.2f} cov={row['covered_days']:.1f}d"
    )


def _print_replay_total(rows: list[dict]):
    total_trades = sum(r["trades"] for r in rows)
    total_wins = sum(r["wins"] for r in rows)
    total_losses = sum(r["losses"] for r in rows)
    total_avg_r = (sum(r["avg_r"] * r["trades"] for r in rows) / total_trades) if total_trades else 0.0
    total_closed = total_wins + total_losses
    total_win_rate = (total_wins / total_closed * 100.0) if total_closed else 0.0
    total_hit_rate = (total_wins / total_trades * 100.0) if total_trades else 0.0
    print(f"TOTAL | trades={total_trades} wr={total_win_rate:.1f}% hit={total_hit_rate:.1f}% weighted_avgR={total_avg_r:+.2f}")


def main():
    parser = argparse.ArgumentParser(description="Replay backtest for scalp logic and Smart Money Liquidity")
    parser.add_argument("--days", type=int, default=30, help="Lookback period in days (30-180)")
    parser.add_argument("--relaxed", action="store_true", help="Force relaxed scalp filters")
    parser.add_argument("--strict", action="store_true", help="Force strict scalp filters")
    parser.add_argument("--smart-money-only", action="store_true", help="Replay only Smart Money Liquidity strategy")
    parser.add_argument(
        "--with-smart-money",
        "--smart-money-combined",
        action="store_true",
        help="Replay main strategy combined with Smart Money Liquidity",
    )
    args = parser.parse_args()
    if args.days < 30 or args.days > 180:
        parser.error("--days must be between 30 and 180")
    if args.relaxed and args.strict:
        parser.error("Use only one of --relaxed or --strict")
    if args.smart_money_only and args.with_smart_money:
        parser.error("Use either --smart-money-only or --with-smart-money, not both")

    relaxed_mode = bool(args.relaxed or (SCALP_RELAXED_FILTERS and not args.strict))

    position_mode_label = str(BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper()

    if args.smart_money_only:
        print(f"Replay backtest for {SYMBOL} | days={args.days} | mode=SMART_MONEY_ONLY | position_mode={position_mode_label}")
        print("-" * 64)
        all_rows = []
        for tf in SMART_MONEY_EXECUTION_TFS:
            row = simulate_smart_money_timeframe(tf, args.days)
            all_rows.append(row)
            _print_replay_row(row)
        print("-" * 64)
        _print_replay_total(all_rows)
        return

    macro_series = _build_macro_trend_series(args.days)
    trend_map = {
        "15m": _build_trend_series("15m", args.days),
        "1h": _build_trend_series("1h", args.days),
        "4h": _build_trend_series("4h", args.days),
        "1d": _build_trend_series("1d", args.days),
        "1w": _build_trend_series("1w", args.days),
    }
    mode_label = "RELAXED" if relaxed_mode else "STRICT"
    if args.with_smart_money:
        mode_label = f"{mode_label}+SMART_MONEY"
    print(f"Replay backtest for {SYMBOL} | days={args.days} | mode={mode_label} | position_mode={position_mode_label}")
    print("-" * 64)
    sm_rows_by_tf = {}
    if args.with_smart_money:
        for tf in SMART_MONEY_EXECUTION_TFS:
            sm_rows_by_tf[tf] = simulate_smart_money_timeframe(tf, args.days)
    all_rows = []
    for tf in SIGNAL_TIMEFRAMES:
        row = simulate_timeframe(tf, args.days, macro_series, trend_map, relaxed=relaxed_mode)
        if args.with_smart_money and tf in sm_rows_by_tf:
            row = _merge_backtest_rows(row, sm_rows_by_tf[tf])
        all_rows.append(row)
        _print_replay_row(row)
    print("-" * 64)
    _print_replay_total(all_rows)


if __name__ == "__main__":
    main()
