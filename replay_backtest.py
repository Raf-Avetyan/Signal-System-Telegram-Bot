import argparse
import math
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

from channels import calculate_channels
from config import (
    BASE_MOMENTUM_ENABLED_TFS,
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
    SCALP_MAX_OPEN_TOTAL,
    SCALP_MAX_OPEN_PER_SIDE,
    SCALP_MAX_OPEN_PER_TF,
    SMART_MONEY_EXECUTION_TFS,
    SESSION_SCALP_MODE,
    SIGNAL_TIMEFRAMES,
    SYMBOL,
    VOLATILITY_FILTER_ENABLED,
    VOLATILITY_MAX_ATR_PCT,
    VOLATILITY_MIN_ATR_PCT,
    get_adjusted_sessions,
)
from data import INTERVAL_MAP, OKX_BASE, fetch_klines
from momentum import ScalpTracker, calculate_momentum, classify_momentum_zone, check_htf_pullback_entry, check_one_h_reclaim_entry
from scoring import calculate_signal_score
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

    # Same-candle ambiguity for fresh trades: TP1+SL => ENTRY_CLOSE (never direct SL)
    if not entry_candle and not tr["tp1_hit"] and tp1_touched and sl_touched:
        tr["tp1_hit"] = True
        return "ENTRY_CLOSE"

    # Progressive TP checks (disabled on entry candle)
    if not entry_candle:
        tp_price = high if is_long else low
        if is_long:
            if not tr["tp1_hit"] and tp_price >= tr["tp1"]:
                tr["tp1_hit"] = True
            if not tr["tp2_hit"] and tp_price >= tr["tp2"]:
                tr["tp2_hit"] = True
                tr["sl"] = max(float(tr.get("sl", tr["entry"])), float(tr.get("tp1", tr["entry"])))
            if not tr["tp3_hit"] and tp_price >= tr["tp3"]:
                tr["tp3_hit"] = True
                return "TP3"
        else:
            if not tr["tp1_hit"] and tp_price <= tr["tp1"]:
                tr["tp1_hit"] = True
            if not tr["tp2_hit"] and tp_price <= tr["tp2"]:
                tr["tp2_hit"] = True
                tr["sl"] = min(float(tr.get("sl", tr["entry"])), float(tr.get("tp1", tr["entry"])))
            if not tr["tp3_hit"] and tp_price <= tr["tp3"]:
                tr["tp3_hit"] = True
                return "TP3"

    # Breakeven close after TP1 only; after TP2 the stop is locked at TP1.
    if tr["tp1_hit"] and not tr["tp2_hit"]:
        entry_hit = (low <= tr["entry"]) if is_long else (high >= tr["entry"])
        if entry_hit:
            return "ENTRY_CLOSE"

    # SL / protected SL-in-profit
    sl_price = low if is_long else high
    if (is_long and sl_price <= tr["sl"]) or ((not is_long) and sl_price >= tr["sl"]):
        return "PROFIT_SL" if tr["tp1_hit"] else "SL"

    return None


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
    wins = sum(1 for val in sample if val > 0)
    losses = sum(1 for val in sample if val < 0)
    win_rate = (wins / (wins + losses) * 100.0) if (wins + losses) else 0.0
    avg_r = sum(sample) / len(sample)
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

    active = []
    results = []
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
            regime_name = _detect_regime_from_df(df.loc[:idx]) if SCALP_REGIME_SWITCHING else "RANGE"
            regime_cfg = SCALP_REGIME_PROFILES.get(regime_name, {})
            score_delta = int(regime_cfg.get("score_delta", 0))
            regime_vol_min_mult = float(regime_cfg.get("vol_min_mult", 1.0))
            regime_vol_max_mult = float(regime_cfg.get("vol_max_mult", 1.0))
            tuning_state = _recent_health_from_results(results, SCALP_SELF_TUNE_LOOKBACK) if SCALP_SELF_TUNING_ENABLED else "NEUTRAL"
            tuning_delta = 1 if tuning_state == "TIGHTEN" else (-1 if tuning_state == "LOOSEN" else 0)

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
            if anchor_side and side != anchor_side:
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
            if evt_type == "TP3":
                results.append(abs(tr["tp3"] - tr["entry"]) / tr["risk"])
                loss_streak[side] = 0
            elif evt_type == "PROFIT_SL":
                results.append(max(0.2, abs(tr["tp1"] - tr["entry"]) / tr["risk"] * 0.6))
                loss_streak[side] = 0
            elif evt_type == "SL":
                results.append(-1.0)
                loss_streak[side] += 1
                if loss_streak[side] >= SCALP_LOSS_STREAK_LIMIT:
                    side_cooldown_until[side] = now_ts + SCALP_LOSS_COOLDOWN_SEC
            elif evt_type == "ENTRY_CLOSE":
                results.append(0.0)
                if loss_streak[side] > 0:
                    loss_streak[side] -= 1
            else:
                survivors.append(tr)

        active = survivors

    for _ in active:
        results.append(0.0)

    wins = sum(1 for r in results if r > 0)
    losses = sum(1 for r in results if r < 0)
    breakeven = sum(1 for r in results if r == 0)
    avg_r = sum(results) / len(results) if results else 0.0
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

            if evt_type == "TP3":
                results.append(abs(tr["tp3"] - tr["entry"]) / tr["risk"])
            elif evt_type == "PROFIT_SL":
                results.append(max(0.2, abs(tr["tp1"] - tr["entry"]) / tr["risk"] * 0.6))
            elif evt_type == "SL":
                results.append(-1.0)
            elif evt_type == "ENTRY_CLOSE":
                results.append(0.0)
            else:
                survivors.append(tr)

        active = survivors

    for _ in active:
        results.append(0.0)

    wins = sum(1 for r in results if r > 0)
    losses = sum(1 for r in results if r < 0)
    breakeven = sum(1 for r in results if r == 0)
    avg_r = sum(results) / len(results) if results else 0.0
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

    if args.smart_money_only:
        print(f"Replay backtest for {SYMBOL} | days={args.days} | mode=SMART_MONEY_ONLY")
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
    print(f"Replay backtest for {SYMBOL} | days={args.days} | mode={mode_label}")
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
