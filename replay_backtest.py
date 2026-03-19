import argparse
import math
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

from channels import calculate_channels
from config import (
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
    SCALP_RELAX_MIN_SCORE_DELTA,
    SCALP_RELAX_VOL_MIN_MULT,
    SCALP_RELAX_VOL_MAX_MULT,
    SCALP_RELAX_COUNTERTREND_EXTRA,
    SCALP_RELAX_ALLOW_OFFSESSION,
    SCALP_RELAXED_FILTERS,
    SESSION_SCALP_MODE,
    SIGNAL_TIMEFRAMES,
    SYMBOL,
    VOLATILITY_FILTER_ENABLED,
    VOLATILITY_MAX_ATR_PCT,
    VOLATILITY_MIN_ATR_PCT,
    get_adjusted_sessions,
)
from data import INTERVAL_MAP, OKX_BASE, fetch_klines
from momentum import ScalpTracker, calculate_momentum
from scoring import calculate_signal_score

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
            if not tr["tp3_hit"] and tp_price >= tr["tp3"]:
                tr["tp3_hit"] = True
                return "TP3"
        else:
            if not tr["tp1_hit"] and tp_price <= tr["tp1"]:
                tr["tp1_hit"] = True
            if not tr["tp2_hit"] and tp_price <= tr["tp2"]:
                tr["tp2_hit"] = True
            if not tr["tp3_hit"] and tp_price <= tr["tp3"]:
                tr["tp3_hit"] = True
                return "TP3"

    # Breakeven close after TP1
    if tr["tp1_hit"]:
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


def _orderflow_anomaly_placeholder():
    # Historical OI/liquidation block data is not reconstructed in replay yet.
    # Keep the same gate shape but with neutral anomaly signal.
    return False, 0.0, 0.0


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


def simulate_timeframe(tf: str, days: int, macro_trend_series: pd.Series, relaxed: bool = False):
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
        zone = row.get("MomentumZone", "NEUTRAL")
        close = float(row["Close"])
        high = float(row["High"])
        low = float(row["Low"])
        atr = float(row.get("ATR", 0))
        rsi_raw = float(row.get("RSI", 50))
        rsi_sm = float(row.get("MomentumSmooth", 50))
        candle_ts = idx.strftime("%Y-%m-%d %H:%M")
        now_ts = idx.timestamp()

        # Build new entries first (bot does signals then outcome checks in the same poll)
        events = tracker.update(zone, close, atr, candle_ts=candle_ts, rsi_raw=rsi_raw, rsi_smooth=rsi_sm)
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
            score, _ = calculate_signal_score(evt, df.loc[:idx], levels_proxy, macro_trend, None, 0)

            # Volatility gate
            if VOLATILITY_FILTER_ENABLED and close > 0 and atr > 0:
                atr_pct = atr / close * 100
                min_pct = VOLATILITY_MIN_ATR_PCT.get(tf, 0.0)
                max_pct = VOLATILITY_MAX_ATR_PCT.get(tf, 99.0)
                if relaxed:
                    min_pct *= float(SCALP_RELAX_VOL_MIN_MULT)
                    max_pct *= float(SCALP_RELAX_VOL_MAX_MULT)
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
            if score < min_score_tf:
                continue

            # Trend gate and countertrend quota
            bullish = {"Bullish", "Trending Bullish", "Strong Bullish"}
            bearish = {"Bearish", "Trending Bearish", "Strong Bearish"}
            trend_aligned = (
                macro_trend == "Ranging"
                or (side == "LONG" and macro_trend in bullish)
                or (side == "SHORT" and macro_trend in bearish)
            )
            mode = str(SCALP_TREND_FILTER_MODE).strip().lower()
            session_cfg = SESSION_SCALP_MODE.get(session_name, {})
            session_countertrend_max = session_cfg.get("countertrend_max", SCALP_COUNTERTREND_MAX_PER_WINDOW)
            session_score_boost = session_cfg.get("score_boost", 0)

            if not trend_aligned:
                if mode == "hard":
                    continue
                if mode == "soft":
                    if score < SCALP_COUNTERTREND_MIN_SCORE + session_score_boost:
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


def main():
    parser = argparse.ArgumentParser(description="Replay backtest for scalp logic")
    parser.add_argument("--days", type=int, default=30, help="Lookback period in days (30-90)")
    parser.add_argument("--relaxed", action="store_true", help="Force relaxed scalp filters")
    parser.add_argument("--strict", action="store_true", help="Force strict scalp filters")
    args = parser.parse_args()
    if args.days < 30 or args.days > 90:
        parser.error("--days must be between 30 and 90")
    if args.relaxed and args.strict:
        parser.error("Use only one of --relaxed or --strict")

    relaxed_mode = bool(args.relaxed or (SCALP_RELAXED_FILTERS and not args.strict))

    macro_series = _build_macro_trend_series(args.days)
    mode_label = "RELAXED" if relaxed_mode else "STRICT"
    print(f"Replay backtest for {SYMBOL} | days={args.days} | mode={mode_label}")
    print("-" * 64)
    all_rows = []
    for tf in SIGNAL_TIMEFRAMES:
        row = simulate_timeframe(tf, args.days, macro_series, relaxed=relaxed_mode)
        all_rows.append(row)
        print(
            f"{tf:>4} | trades={row['trades']:>4} wins={row['wins']:>4} "
            f"losses={row['losses']:>4} be={row['breakeven']:>4} "
            f"wr={row['win_rate']:>5.1f}% hit={row['hit_rate']:>5.1f}% "
            f"avgR={row['avg_r']:+.2f} cov={row['covered_days']:.1f}d"
        )
    print("-" * 64)
    total_trades = sum(r["trades"] for r in all_rows)
    total_wins = sum(r["wins"] for r in all_rows)
    total_losses = sum(r["losses"] for r in all_rows)
    total_avg_r = (sum(r["avg_r"] * r["trades"] for r in all_rows) / total_trades) if total_trades else 0.0
    total_closed = total_wins + total_losses
    total_win_rate = (total_wins / total_closed * 100.0) if total_closed else 0.0
    total_hit_rate = (total_wins / total_trades * 100.0) if total_trades else 0.0
    print(f"TOTAL | trades={total_trades} wr={total_win_rate:.1f}% hit={total_hit_rate:.1f}% weighted_avgR={total_avg_r:+.2f}")


if __name__ == "__main__":
    main()
