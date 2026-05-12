from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os
import time

import pandas as pd

from bitunix_trade import BitunixFuturesClient
from channels import calculate_channels, check_channel_signals
from config import SYMBOL
from data import (
    fetch_funding_rate as fetch_okx_funding_rate,
    fetch_liquidation_orders,
    fetch_liquidations,
    fetch_open_interest,
    fetch_order_book as fetch_okx_order_book,
)
from levels import calculate_levels
from liquidation_engine import build_liquidation_map
from liquidity_map import detect_liquidity_candidates
from momentum import calculate_momentum, check_htf_pullback_entry, check_one_h_reclaim_entry


BITUNIX_TIMEFRAMES = ["15m", "1h", "4h", "1d", "1w", "1M"]
BITUNIX_LIMITS = {
    "15m": 240,
    "1h": 240,
    "4h": 180,
    "1d": 150,
    "1w": 80,
    "1M": 36,
}
LIQUIDATION_HEATMAP_HISTORY_FILE = "liquidation_heatmap_history.json"
LIQUIDATION_HEATMAP_HISTORY_MAX_POINTS = 192
LIQUIDATION_HEATMAP_HISTORY_MIN_SECONDS = 300


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _pct(current, reference):
    current_val = _safe_float(current)
    reference_val = _safe_float(reference)
    if reference_val == 0:
        return 0.0
    return ((current_val / reference_val) - 1.0) * 100.0


def _fmt_price(value):
    return f"{_safe_float(value):,.2f}"


def _bias_value(bias_text):
    bias = str(bias_text or "").strip().lower()
    if "trending bullish" in bias:
        return 2.0
    if bias == "bullish":
        return 1.0
    if "trending bearish" in bias:
        return -2.0
    if bias == "bearish":
        return -1.0
    return 0.0


def _cluster_levels(items, tolerance_pct=0.35):
    cleaned = []
    for label, price in items:
        price_val = _safe_float(price)
        if price_val > 0:
            cleaned.append((str(label), price_val))
    cleaned.sort(key=lambda row: row[1])
    clusters = []
    for label, price in cleaned:
        if not clusters:
            clusters.append({"price": price, "labels": [label], "prices": [price]})
            continue
        cluster = clusters[-1]
        base_price = _safe_float(cluster["price"], price)
        dist_pct = abs(price - base_price) / max(base_price, 1e-9) * 100.0
        if dist_pct <= tolerance_pct:
            cluster["labels"].append(label)
            cluster["prices"].append(price)
            cluster["price"] = sum(cluster["prices"]) / len(cluster["prices"])
        else:
            clusters.append({"price": price, "labels": [label], "prices": [price]})
    return clusters


def _format_cluster(cluster):
    if not cluster:
        return "n/a"
    labels = ", ".join(cluster.get("labels", [])[:3])
    return f"{_fmt_price(cluster.get('price'))} ({labels})"


def _distance_pct(price_a, price_b):
    price_a = _safe_float(price_a)
    price_b = _safe_float(price_b)
    if price_b <= 0:
        return 0.0
    return abs(price_a - price_b) / price_b * 100.0


def _pick_planning_cluster(clusters, current_price, *, min_dist_pct, max_dist_pct, preferred_dist_pct):
    if not clusters:
        return None
    eligible = []
    for cluster in clusters:
        dist_pct = _distance_pct(cluster.get("price"), current_price)
        if min_dist_pct <= dist_pct <= max_dist_pct:
            eligible.append((abs(dist_pct - preferred_dist_pct), cluster))
    if eligible:
        eligible.sort(key=lambda item: item[0])
        return eligible[0][1]
    fallback = []
    for cluster in clusters:
        dist_pct = _distance_pct(cluster.get("price"), current_price)
        if dist_pct >= min_dist_pct:
            fallback.append((dist_pct, cluster))
    if fallback:
        fallback.sort(key=lambda item: item[0])
        return fallback[0][1]
    return None


def _scenario_risk(probability, trend_aligned):
    if trend_aligned and probability >= 72:
        return "normal", 0.75
    if trend_aligned and probability >= 64:
        return "medium", 0.60
    if probability >= 58:
        return "reduced", 0.45
    return "small", 0.30


def _position_label(risk_style):
    style = str(risk_style or "").strip().lower()
    if style == "normal":
        return "full-confirmation"
    if style == "medium":
        return "standard"
    if style == "reduced":
        return "reduced"
    return "starter"


def _fetch_bitunix_klines(symbol=SYMBOL, interval="1h", limit=None):
    client = BitunixFuturesClient()
    raw = client.get_kline(symbol, interval, limit=int(limit or BITUNIX_LIMITS.get(interval, 200)))
    rows = raw.get("data") or []
    if not rows:
        return pd.DataFrame()

    normalized = []
    for row in reversed(rows):
        normalized.append(
            {
                "Open": _safe_float(row.get("open")),
                "High": _safe_float(row.get("high")),
                "Low": _safe_float(row.get("low")),
                "Close": _safe_float(row.get("close")),
                "Volume": _safe_float(row.get("quoteVol") or row.get("baseVol")),
                "OpenTime": int(_safe_float(row.get("time"))),
            }
        )
    df = pd.DataFrame(normalized)
    df.index = pd.to_datetime(df["OpenTime"], unit="ms", utc=True)
    df.index.name = "Datetime"
    return df[["Open", "High", "Low", "Close", "Volume"]]


def _fetch_all_bitunix_timeframes(symbol=SYMBOL, timeframes=None):
    timeframes = list(timeframes or BITUNIX_TIMEFRAMES)
    output = {}
    with ThreadPoolExecutor(max_workers=len(timeframes)) as executor:
        future_map = {
            executor.submit(_fetch_bitunix_klines, symbol, tf, BITUNIX_LIMITS.get(tf)): tf
            for tf in timeframes
        }
        for future in as_completed(future_map):
            tf = future_map[future]
            df = future.result()
            if not df.empty:
                output[tf] = df
    return output


def _fetch_bitunix_ticker(symbol=SYMBOL):
    client = BitunixFuturesClient()
    raw = client.get_tickers(symbol)
    rows = raw.get("data") or []
    return rows[0] if rows else {}


def _fetch_bitunix_funding(symbol=SYMBOL):
    client = BitunixFuturesClient()
    raw = client.get_funding_rate(symbol)
    return raw.get("data") or {}


def _fetch_bitunix_funding_history(symbol=SYMBOL, limit=30):
    client = BitunixFuturesClient()
    raw = client.get_funding_rate_history(symbol, limit=int(limit))
    return raw.get("data") or []


def _fetch_bitunix_depth(symbol=SYMBOL, limit="50"):
    client = BitunixFuturesClient()
    raw = client.get_depth(symbol, str(limit))
    payload = raw.get("data") or {}
    bids = [[_safe_float(px), _safe_float(sz)] for px, sz in (payload.get("bids") or [])]
    asks = [[_safe_float(px), _safe_float(sz)] for px, sz in (payload.get("asks") or [])]
    return {"bids": bids, "asks": asks}


def _funding_context(current_rate, history_rows):
    current_rate = _safe_float(current_rate)
    rates = [_safe_float(row.get("fundingRate")) for row in (history_rows or []) if row]
    if not rates:
        avg_rate = current_rate
        trend = 0.0
    else:
        recent = rates[: min(9, len(rates))]
        older = rates[min(9, len(rates)) : min(18, len(rates))]
        avg_rate = sum(recent) / max(len(recent), 1)
        older_avg = (sum(older) / len(older)) if older else avg_rate
        trend = avg_rate - older_avg

    if current_rate < 0:
        bias = "shorts paying"
    elif current_rate > 0:
        bias = "longs paying"
    else:
        bias = "flat"

    if trend > 0.00002:
        trend_label = "rising"
    elif trend < -0.00002:
        trend_label = "cooling"
    else:
        trend_label = "stable"

    return {
        "current_rate": current_rate,
        "avg_rate": avg_rate,
        "trend": trend,
        "bias": bias,
        "trend_label": trend_label,
    }


def _ticker_context(ticker):
    high = _safe_float(ticker.get("high"))
    low = _safe_float(ticker.get("low"))
    last = _safe_float(ticker.get("lastPrice") or ticker.get("last"))
    open_price = _safe_float(ticker.get("open"))
    quote_vol = _safe_float(ticker.get("quoteVol"))
    range_pos = 0.5
    if high > low and last > 0:
        range_pos = (last - low) / max(high - low, 1e-9)
    return {
        "high": high,
        "low": low,
        "last": last,
        "open": open_price,
        "day_change_pct": _pct(last, open_price) if open_price > 0 else 0.0,
        "range_position": max(0.0, min(1.0, range_pos)),
        "quote_vol": quote_vol,
    }


def _liquidity_context(book, current_price, atr_1h):
    candidates = detect_liquidity_candidates(
        order_book=book,
        price=current_price,
        atr=max(atr_1h, current_price * 0.0025),
        timeframe="1h",
        max_distance_atr_mult=8.0,
        bucket_pct=0.10,
    )
    above = [
        row for row in candidates
        if row.get("side") == "LONG"
        and float(row.get("distance_pct", 0) or 0) >= 0.45
        and float(row.get("size_usd", 0) or 0) >= 10_000_000
    ]
    below = [
        row for row in candidates
        if row.get("side") == "SHORT"
        and float(row.get("distance_pct", 0) or 0) >= 0.45
        and float(row.get("size_usd", 0) or 0) >= 10_000_000
    ]
    top_above = above[0] if above else None
    top_below = below[0] if below else None
    return {
        "raw_book": book,
        "top_above": top_above,
        "top_below": top_below,
        "above_text": (
            f"{_fmt_price(top_above.get('level_price'))} (${top_above.get('size_usd', 0)/1e6:.1f}M)"
            if top_above else "n/a"
        ),
        "below_text": (
            f"{_fmt_price(top_below.get('level_price'))} (${top_below.get('size_usd', 0)/1e6:.1f}M)"
            if top_below else "n/a"
        ),
    }


def _okx_liquidation_context(current_price, atr_1h, levels):
    try:
        order_book = fetch_okx_order_book(depth=400)
    except Exception:
        order_book = None
    try:
        liquidation_orders = fetch_liquidation_orders(limit=100)
    except Exception:
        liquidation_orders = []
    try:
        oi_value = _safe_float(fetch_open_interest())
    except Exception:
        oi_value = 0.0
    try:
        liquidation_value = _safe_float(fetch_liquidations())
    except Exception:
        liquidation_value = 0.0
    try:
        funding_rate = _safe_float(fetch_okx_funding_rate())
    except Exception:
        funding_rate = 0.0

    if not order_book or current_price <= 0:
        return {
            "oi": oi_value,
            "liquidations_usd": liquidation_value,
            "funding_rate": funding_rate,
            "order_book": order_book,
            "liquidation_orders": liquidation_orders,
            "mid_above": None,
            "mid_below": None,
            "far_above": None,
            "far_below": None,
            "summary_above": "n/a",
            "summary_below": "n/a",
        }

    def pick_candidate(max_mult, min_dist_pct):
        rows = detect_liquidity_candidates(
            order_book=order_book,
            price=current_price,
            atr=max(atr_1h, current_price * 0.0025),
            timeframe="okx",
            max_distance_atr_mult=max_mult,
            bucket_pct=0.12 if max_mult >= 4 else 0.08,
        )
        above = [
            row for row in rows
            if row.get("side") == "LONG"
            and float(row.get("distance_pct", 0) or 0) >= min_dist_pct
            and float(row.get("size_usd", 0) or 0) >= 15_000_000
        ]
        below = [
            row for row in rows
            if row.get("side") == "SHORT"
            and float(row.get("distance_pct", 0) or 0) >= min_dist_pct
            and float(row.get("size_usd", 0) or 0) >= 15_000_000
        ]
        return (above[0] if above else None), (below[0] if below else None)

    mid_above, mid_below = pick_candidate(5.0, 0.60)
    far_above, far_below = pick_candidate(14.0, 1.20)

    def structural(level_keys, is_above):
        best = None
        best_dist = None
        for key in level_keys:
            px = _safe_float(levels.get(key))
            if px <= 0:
                continue
            if is_above and px <= current_price:
                continue
            if (not is_above) and px >= current_price:
                continue
            dist_pct = abs(px - current_price) / max(current_price, 1e-9) * 100.0
            if best is None or dist_pct < best_dist:
                best = {"level_price": px, "size_usd": 0.0, "distance_pct": dist_pct, "source": key}
                best_dist = dist_pct
        return best

    if not far_above:
        far_above = structural(["PDH", "PWH", "PMH", "PumpMax"], True)
    if not far_below:
        far_below = structural(["PDL", "PWL", "PML", "DumpMax"], False)

    def fmt_zone(item):
        if not item:
            return "n/a"
        source = str(item.get("source") or "wall")
        size_usd = _safe_float(item.get("size_usd"))
        if size_usd > 0:
            return f"{_fmt_price(item.get('level_price'))} (${size_usd/1e6:.1f}M, {source})"
        return f"{_fmt_price(item.get('level_price'))} ({source})"

    return {
        "oi": oi_value,
        "liquidations_usd": liquidation_value,
        "funding_rate": funding_rate,
        "order_book": order_book,
        "liquidation_orders": liquidation_orders,
        "mid_above": mid_above,
        "mid_below": mid_below,
        "far_above": far_above,
        "far_below": far_below,
        "summary_above": f"mid {fmt_zone(mid_above)} | far {fmt_zone(far_above)}",
        "summary_below": f"mid {fmt_zone(mid_below)} | far {fmt_zone(far_below)}",
    }


def _order_book_context(book, current_price):
    if not book or current_price <= 0:
        return {"pressure": "balanced", "imbalance": 0.0}

    band_pct = 0.50 / 100.0
    low_band = current_price * (1.0 - band_pct)
    high_band = current_price * (1.0 + band_pct)
    bid_usd = 0.0
    ask_usd = 0.0
    for price, size in book.get("bids", []):
        if price >= low_band:
            bid_usd += price * size
    for price, size in book.get("asks", []):
        if price <= high_band:
            ask_usd += price * size
    total = bid_usd + ask_usd
    imbalance = ((bid_usd - ask_usd) / total) if total > 0 else 0.0
    if imbalance >= 0.10:
        pressure = "bullish"
    elif imbalance <= -0.10:
        pressure = "bearish"
    else:
        pressure = "balanced"
    return {"pressure": pressure, "imbalance": imbalance}


def _tf_summary(tf, df):
    if df is None or df.empty or len(df) < 3:
        return {}

    enriched = calculate_channels(df)
    enriched = calculate_momentum(enriched)
    curr = enriched.iloc[-1]
    prev = enriched.iloc[-2]

    close = _safe_float(curr.get("Close"))
    ema2 = _safe_float(curr.get("EMA2"), close)
    ema3 = _safe_float(curr.get("EMA3"), close)
    atr = _safe_float(curr.get("ATR"))
    rsi = _safe_float(curr.get("RSI"), 50.0)
    mom = _safe_float(curr.get("MomentumSmooth"), rsi)
    if not math.isfinite(rsi):
        rsi = 50.0
    if not math.isfinite(mom):
        mom = rsi

    if close > ema2 > ema3 and mom >= 50:
        bias = "Trending Bullish"
    elif close < ema2 < ema3 and mom <= 50:
        bias = "Trending Bearish"
    elif close > ema2 and ema2 > ema3:
        bias = "Bullish"
    elif close < ema2 and ema2 < ema3:
        bias = "Bearish"
    else:
        bias = "Ranging"

    lookback = {"15m": 16, "1h": 12, "4h": 10, "1d": 7, "1w": 4, "1M": 3}.get(tf, 6)
    lookback = max(1, min(lookback, len(enriched) - 1))
    anchor_close = _safe_float(enriched["Close"].iloc[-1 - lookback], close)
    period_change = _pct(close, anchor_close)
    recent_slice = enriched.tail(max(lookback + 1, 6))
    recent_high = _safe_float(recent_slice["High"].max(), close)
    recent_low = _safe_float(recent_slice["Low"].min(), close)
    if period_change >= 10.0 and "bearish" in bias.lower():
        bias = "Ranging"
    elif period_change <= -10.0 and "bullish" in bias.lower():
        bias = "Ranging"

    channel_events = check_channel_signals(enriched)
    return {
        "tf": tf,
        "df": enriched,
        "close": close,
        "change_pct": _pct(close, _safe_float(prev.get("Close"), close)),
        "period_change_pct": period_change,
        "ema2": ema2,
        "ema3": ema3,
        "atr": atr,
        "rsi": rsi,
        "bias": bias,
        "range_high": recent_high,
        "range_low": recent_low,
        "channel_signal": channel_events[0] if channel_events else None,
        "active_pullback": check_htf_pullback_entry(enriched, tf),
        "active_reclaim": check_one_h_reclaim_entry(enriched, tf),
    }


def _nearest_levels(levels, current_price, tf_map):
    level_items = [
        ("DO", levels.get("DO")),
        ("WO", levels.get("WO")),
        ("MO", levels.get("MO")),
        ("PDH", levels.get("PDH")),
        ("PDL", levels.get("PDL")),
        ("PWH", levels.get("PWH")),
        ("PWL", levels.get("PWL")),
        ("PMH", levels.get("PMH")),
        ("PML", levels.get("PML")),
        ("Pump", levels.get("Pump")),
        ("Dump", levels.get("Dump")),
        ("PumpMax", levels.get("PumpMax")),
        ("DumpMax", levels.get("DumpMax")),
        ("1H EMA21", (tf_map.get("1h") or {}).get("ema2")),
        ("1H EMA55", (tf_map.get("1h") or {}).get("ema3")),
        ("4H EMA21", (tf_map.get("4h") or {}).get("ema2")),
        ("4H EMA55", (tf_map.get("4h") or {}).get("ema3")),
        ("1H swing high", (tf_map.get("1h") or {}).get("range_high")),
        ("1H swing low", (tf_map.get("1h") or {}).get("range_low")),
        ("4H swing high", (tf_map.get("4h") or {}).get("range_high")),
        ("4H swing low", (tf_map.get("4h") or {}).get("range_low")),
    ]
    support_clusters = list(reversed(_cluster_levels([
        (label, price) for label, price in level_items if _safe_float(price) < current_price
    ])))
    resistance_clusters = _cluster_levels([
        (label, price) for label, price in level_items if _safe_float(price) > current_price
    ])
    return support_clusters, resistance_clusters


def _zone_to_cluster(zone):
    if not zone:
        return None
    return {
        "price": _safe_float(zone.get("price")),
        "labels": list(zone.get("labels") or [])[:3],
    }


def _is_usable_entry_zone(zone, current_price, max_dist_pct=3.2):
    if not zone:
        return False
    return _distance_pct(zone.get("price"), current_price) <= max_dist_pct


def _tight_plan_stop(entry_low, entry_high, current_price, atr_1h, side, nearby_invalidation=None):
    base_pad = max(atr_1h * 0.45, current_price * 0.0012)
    if str(side).upper() == "LONG":
        stop = entry_low - base_pad
        if nearby_invalidation and 0 < _distance_pct(nearby_invalidation, entry_low) <= 0.90:
            stop = min(stop, _safe_float(nearby_invalidation) - max(atr_1h * 0.08, current_price * 0.00045))
        return stop
    stop = entry_high + base_pad
    if nearby_invalidation and 0 < _distance_pct(nearby_invalidation, entry_high) <= 0.90:
        stop = max(stop, _safe_float(nearby_invalidation) + max(atr_1h * 0.08, current_price * 0.00045))
    return stop


def _anchor_price(anchor):
    if isinstance(anchor, dict):
        return _safe_float(anchor.get("price"))
    return _safe_float(anchor)


def _nearest_above(reference_price, *anchors):
    ref = _safe_float(reference_price)
    candidates = []
    for anchor in anchors:
        price = _anchor_price(anchor)
        if price > ref:
            candidates.append(price)
    return min(candidates) if candidates else 0.0


def _nearest_below(reference_price, *anchors):
    ref = _safe_float(reference_price)
    candidates = []
    for anchor in anchors:
        price = _anchor_price(anchor)
        if 0 < price < ref:
            candidates.append(price)
    return max(candidates) if candidates else 0.0


def _scenario_tp_multipliers(kind, mode_cfg):
    kind_name = str(kind or "").strip().lower()
    mode_name = str(mode_cfg.get("mode_name") or "swing").strip().lower()
    if mode_name == "short_term":
        if kind_name in {"breakout", "breakdown"}:
            return (0.95, 1.35, 1.85), (0.72, 1.05, 1.40)
        if kind_name in {"major_flush", "major_squeeze"}:
            return (1.10, 1.65, 2.25), (0.85, 1.25, 1.70)
        return (0.95, 1.45, 2.00), (0.75, 1.15, 1.55)
    if kind_name in {"breakout", "breakdown"}:
        return (1.20, 2.20, 3.20), (1.00, 1.50, 2.20)
    return (1.20, 2.00, 3.00), (0.95, 1.35, 2.00)


def _normalize_targets(side, entry, tp1, tp2, tp3, current_price, atr_1h, mode_cfg):
    side_name = str(side or "").upper()
    min_gap = max(
        current_price * float(mode_cfg.get("tp_min_gap_pct") or 0.00125),
        atr_1h * float(mode_cfg.get("tp_min_gap_atr") or 0.18),
        float(mode_cfg.get("tp_min_gap_usd") or 90.0),
    )
    entry_val = _safe_float(entry)
    vals = [_safe_float(tp1), _safe_float(tp2), _safe_float(tp3)]
    if side_name == "LONG":
        vals[0] = max(vals[0], entry_val + min_gap)
        vals[1] = max(vals[1], vals[0] + min_gap)
        vals[2] = max(vals[2], vals[1] + min_gap)
        return vals
    vals[0] = min(vals[0], entry_val - min_gap)
    vals[1] = min(vals[1], vals[0] - min_gap)
    vals[2] = min(vals[2], vals[1] - min_gap)
    return vals


def _build_targets(side, entry, risk, current_price, atr_1h, mode_cfg, kind, primary_anchor=None, secondary_anchor=None, tertiary_anchor=None):
    side_name = str(side or "").upper()
    entry_val = _safe_float(entry)
    risk_val = max(_safe_float(risk), max(atr_1h * 0.40, current_price * 0.0020))
    caps, floors = _scenario_tp_multipliers(kind, mode_cfg)

    if side_name == "LONG":
        cap_targets = [entry_val + risk_val * mult for mult in caps]
        floor_targets = [entry_val + risk_val * mult for mult in floors]
        if str(mode_cfg.get("mode_name") or "swing").lower() == "short_term":
            raw = [
                min(_nearest_above(entry_val, primary_anchor, secondary_anchor, tertiary_anchor) or cap_targets[0], cap_targets[0]),
                min(_nearest_above(cap_targets[0], secondary_anchor, tertiary_anchor, primary_anchor) or cap_targets[1], cap_targets[1]),
                min(_nearest_above(cap_targets[1], tertiary_anchor, secondary_anchor, primary_anchor) or cap_targets[2], cap_targets[2]),
            ]
            raw = [max(raw[i], floor_targets[i]) for i in range(3)]
        else:
            raw = [
                max(_anchor_price(primary_anchor), cap_targets[0]),
                max(_anchor_price(secondary_anchor), cap_targets[1]),
                max(_anchor_price(tertiary_anchor), cap_targets[2]),
            ]
        return _normalize_targets(side_name, entry_val, raw[0], raw[1], raw[2], current_price, atr_1h, mode_cfg)

    cap_targets = [entry_val - risk_val * mult for mult in caps]
    floor_targets = [entry_val - risk_val * mult for mult in floors]
    if str(mode_cfg.get("mode_name") or "swing").lower() == "short_term":
        raw = [
            max(_nearest_below(entry_val, primary_anchor, secondary_anchor, tertiary_anchor) or cap_targets[0], cap_targets[0]),
            max(_nearest_below(cap_targets[0], secondary_anchor, tertiary_anchor, primary_anchor) or cap_targets[1], cap_targets[1]),
            max(_nearest_below(cap_targets[1], tertiary_anchor, secondary_anchor, primary_anchor) or cap_targets[2], cap_targets[2]),
        ]
        raw = [min(raw[i], floor_targets[i]) for i in range(3)]
    else:
        raw = [
            min(_anchor_price(primary_anchor) or cap_targets[0], cap_targets[0]),
            min(_anchor_price(secondary_anchor) or cap_targets[1], cap_targets[1]),
            min(_anchor_price(tertiary_anchor) or cap_targets[2], cap_targets[2]),
        ]
    return _normalize_targets(side_name, entry_val, raw[0], raw[1], raw[2], current_price, atr_1h, mode_cfg)


def _scenario_mode_config(mode):
    mode_name = str(mode or "swing").strip().lower()
    if mode_name == "short_term":
        return {
            "mode_name": "short_term",
            "min_dist_pct": 0.55,
            "max_dist_pct": 2.10,
            "preferred_dist_pct": 0.95,
            "near_pick_max_dist_pct": 1.90,
            "near_pick_target_dist_pct": 1.10,
            "near_pick_min_usd": 500.0,
            "near_pick_max_usd": 1500.0,
            "near_pick_target_usd": 950.0,
            "usable_entry_dist_pct": 1.75,
            "major_liq_min_dist_pct": 1.40,
            "tp_min_gap_pct": 0.00120,
            "tp_min_gap_atr": 0.16,
            "tp_min_gap_usd": 85.0,
            "title": "BTC Short-Term Plans",
            "final_note": "Use only the plan whose short-term trigger prints first.",
            "execution_tf": "15m",
        }
    return {
        "mode_name": "swing",
        "min_dist_pct": 0.80,
        "max_dist_pct": 5.50,
        "preferred_dist_pct": 1.50,
        "near_pick_max_dist_pct": 2.40,
        "near_pick_target_dist_pct": 1.20,
        "near_pick_min_usd": 1200.0,
        "near_pick_max_usd": 2800.0,
        "near_pick_target_usd": 1800.0,
        "usable_entry_dist_pct": 3.20,
        "major_liq_min_dist_pct": 2.20,
        "tp_min_gap_pct": 0.00160,
        "tp_min_gap_atr": 0.22,
        "tp_min_gap_usd": 140.0,
        "title": "BTC Scenarios",
        "final_note": "Plan the long only if the long trigger prints. Plan the short only if the short trigger prints.",
        "execution_tf": "1h",
    }


def _build_scenarios(current_price, levels, tf_map, book_ctx, funding_ctx, ticker_ctx, liq_ctx, okx_ctx, liq_map, mode="swing", max_scenarios=4):
    current_price = _safe_float(current_price)
    mode_cfg = _scenario_mode_config(mode)
    tf_weights = {"1M": 4.0, "1w": 3.0, "1d": 2.0, "4h": 2.0, "1h": 1.0}
    overall_bias_score = 0.0
    for tf, weight in tf_weights.items():
        overall_bias_score += _bias_value((tf_map.get(tf) or {}).get("bias")) * weight

    tf_15m = tf_map.get("15m") or {}
    tf_1h = tf_map.get("1h") or {}
    tf_4h = tf_map.get("4h") or {}
    trend_bias_score = _bias_value(tf_4h.get("bias")) * 1.5 + _bias_value(tf_1h.get("bias"))
    support_clusters, resistance_clusters = _nearest_levels(levels, current_price, tf_map)
    fallback_support = _pick_planning_cluster(
        support_clusters,
        current_price,
        min_dist_pct=mode_cfg["min_dist_pct"],
        max_dist_pct=mode_cfg["max_dist_pct"],
        preferred_dist_pct=mode_cfg["preferred_dist_pct"],
    )
    fallback_resistance = _pick_planning_cluster(
        resistance_clusters,
        current_price,
        min_dist_pct=mode_cfg["min_dist_pct"],
        max_dist_pct=mode_cfg["max_dist_pct"],
        preferred_dist_pct=mode_cfg["preferred_dist_pct"],
    )
    mapped_long_entry = _zone_to_cluster((liq_map or {}).get("long_entry_zone"))
    mapped_short_entry = _zone_to_cluster((liq_map or {}).get("short_entry_zone"))
    planning_support = mapped_long_entry if _is_usable_entry_zone(mapped_long_entry, current_price, mode_cfg["usable_entry_dist_pct"]) else fallback_support
    planning_resistance = mapped_short_entry if _is_usable_entry_zone(mapped_short_entry, current_price, mode_cfg["usable_entry_dist_pct"]) else fallback_resistance
    next_support = _zone_to_cluster((liq_map or {}).get("short_target_zone"))
    next_resistance = _zone_to_cluster((liq_map or {}).get("long_target_zone"))
    if not next_support and planning_support and support_clusters:
        lower_supports = [c for c in support_clusters if _safe_float(c.get("price")) < _safe_float(planning_support.get("price"))]
        next_support = lower_supports[0] if lower_supports else None
    if not next_resistance and planning_resistance and resistance_clusters:
        upper_resistances = [c for c in resistance_clusters if _safe_float(c.get("price")) > _safe_float(planning_resistance.get("price"))]
        next_resistance = upper_resistances[0] if upper_resistances else None
    if next_support and planning_support and _distance_pct(next_support.get("price"), planning_support.get("price")) < 0.30:
        lower_supports = [c for c in support_clusters if _safe_float(c.get("price")) < _safe_float(planning_support.get("price"))]
        next_support = lower_supports[0] if lower_supports else next_support
    if next_resistance and planning_resistance and _distance_pct(next_resistance.get("price"), planning_resistance.get("price")) < 0.30:
        upper_resistances = [c for c in resistance_clusters if _safe_float(c.get("price")) > _safe_float(planning_resistance.get("price"))]
        next_resistance = upper_resistances[0] if upper_resistances else next_resistance
    atr_1h = max(_safe_float(tf_1h.get("atr")), current_price * 0.0035)
    book_pressure = str(book_ctx.get("pressure") or "balanced")
    funding_rate = _safe_float((funding_ctx or {}).get("current_rate"))
    funding_bias = str((funding_ctx or {}).get("bias") or "flat")
    range_position = _safe_float((ticker_ctx or {}).get("range_position"), 0.5)
    day_change_pct = _safe_float((ticker_ctx or {}).get("day_change_pct"))
    top_above = (liq_ctx or {}).get("top_above") or {}
    top_below = (liq_ctx or {}).get("top_below") or {}
    liq_positioning = (liq_map or {}).get("positioning") or {}
    dominant_side = str((liq_map or {}).get("dominant_side") or "balanced")
    okx_mid_above = (okx_ctx or {}).get("mid_above") or {}
    okx_mid_below = (okx_ctx or {}).get("mid_below") or {}
    okx_far_above = (okx_ctx or {}).get("far_above") or {}
    okx_far_below = (okx_ctx or {}).get("far_below") or {}
    okx_liq_usd = _safe_float((okx_ctx or {}).get("liquidations_usd"))
    okx_oi = _safe_float((okx_ctx or {}).get("oi"))

    scenarios = []

    if planning_support:
        entry_mid = _safe_float(planning_support.get("price"))
        zone_half = max(entry_mid * 0.00035, atr_1h * 0.06)
        entry_low = entry_mid - zone_half
        entry_high = entry_mid + zone_half
        lower_invalidation = None
        if support_clusters:
            lower_supports = [c for c in support_clusters if _safe_float(c.get("price")) < entry_low]
            lower_invalidation = _safe_float((lower_supports[0] or {}).get("price")) if lower_supports else None
        stop = _tight_plan_stop(entry_low, entry_high, current_price, atr_1h, "LONG", lower_invalidation)
        risk = max(entry_mid - stop, atr_1h * 0.55)
        probability = 56.0 + max(0.0, overall_bias_score) * 1.7
        if _bias_value(tf_1h.get("bias")) > 0:
            probability += 4.0
        if tf_1h.get("active_reclaim") and str((tf_1h.get("active_reclaim") or {}).get("side")) == "LONG":
            probability += 7.0
        if tf_4h.get("active_pullback") and str((tf_4h.get("active_pullback") or {}).get("side")) == "LONG":
            probability += 5.0
        if funding_rate < 0:
            probability += 2.0
        if funding_bias == "shorts paying":
            probability += 2.0
        if book_pressure == "bullish":
            probability += 3.0
        if range_position < 0.45:
            probability += 2.0
        if top_below:
            probability += 2.0
        if okx_mid_below:
            probability += 2.0
        if okx_liq_usd >= 10_000_000:
            probability += 1.0
        if dominant_side == "shorts_vulnerable":
            probability += 2.0
        elif dominant_side == "longs_vulnerable":
            probability -= 2.0
        if trend_bias_score < 0:
            probability -= 5.0
        probability = max(40.0, min(84.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score >= 0)
        tp1, tp2, tp3 = _build_targets(
            "LONG",
            entry_mid,
            risk,
            current_price,
            atr_1h,
            mode_cfg,
            "pullback",
            planning_resistance,
            next_resistance,
            okx_far_above,
        )
        scenarios.append(
            {
                "title": "Long pullback plan",
                "side": "LONG",
                "kind": "pullback",
                "probability": probability,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trend_aligned": trend_bias_score >= 0,
                "trigger": (
                    f"If BTC sweeps {_format_cluster(planning_support)}"
                    + (f" with a flush into major long-liq zone {_fmt_price(next_support.get('price'))}" if next_support else "")
                    + (f" and tags OKX mid-zone {_fmt_price(okx_mid_below.get('level_price'))}" if okx_mid_below else "")
                    + " and reclaims on 15m, long the bounce."
                ),
                "note": (
                    "Use only after reclaim confirmation."
                    + (" Funding supports this long." if funding_bias == "shorts paying" else "")
                    + (" Liquidation pressure is elevated." if okx_liq_usd >= 10_000_000 else "")
                    + (" Shorts look more vulnerable." if dominant_side == "shorts_vulnerable" else "")
                ),
            }
        )

    if planning_resistance:
        entry_mid = _safe_float(planning_resistance.get("price"))
        zone_half = max(entry_mid * 0.00035, atr_1h * 0.06)
        entry_low = entry_mid - zone_half
        entry_high = entry_mid + zone_half
        upper_invalidation = None
        if resistance_clusters:
            upper_resistances = [c for c in resistance_clusters if _safe_float(c.get("price")) > entry_high]
            upper_invalidation = _safe_float((upper_resistances[0] or {}).get("price")) if upper_resistances else None
        stop = _tight_plan_stop(entry_low, entry_high, current_price, atr_1h, "SHORT", upper_invalidation)
        risk = max(stop - entry_mid, atr_1h * 0.55)
        probability = 54.0 + max(0.0, -overall_bias_score) * 1.7
        if _bias_value(tf_1h.get("bias")) < 0:
            probability += 4.0
        if tf_1h.get("channel_signal") and str((tf_1h.get("channel_signal") or {}).get("side")) == "SHORT":
            probability += 4.0
        if funding_rate > 0:
            probability += 2.0
        if funding_bias == "longs paying":
            probability += 2.0
        if book_pressure == "bearish":
            probability += 3.0
        if range_position > 0.55:
            probability += 2.0
        if top_above:
            probability += 2.0
        if okx_mid_above:
            probability += 2.0
        if okx_liq_usd >= 10_000_000:
            probability += 1.0
        if dominant_side == "longs_vulnerable":
            probability += 2.0
        elif dominant_side == "shorts_vulnerable":
            probability -= 2.0
        if trend_bias_score > 0:
            probability -= 5.0
        probability = max(40.0, min(82.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score <= 0)
        tp1, tp2, tp3 = _build_targets(
            "SHORT",
            entry_mid,
            risk,
            current_price,
            atr_1h,
            mode_cfg,
            "rejection",
            planning_support,
            next_support,
            okx_far_below,
        )
        scenarios.append(
            {
                "title": "Short rejection plan",
                "side": "SHORT",
                "kind": "rejection",
                "probability": probability,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trend_aligned": trend_bias_score <= 0,
                "trigger": (
                    f"If BTC runs into {_format_cluster(planning_resistance)}"
                    + (f" with an extension into major short-liq zone {_fmt_price(next_resistance.get('price'))}" if next_resistance else "")
                    + (f" and tags OKX mid-zone {_fmt_price(okx_mid_above.get('level_price'))}" if okx_mid_above else "")
                    + " and rejects on 15m, short the fade."
                ),
                "note": (
                    "Use only after rejection confirmation."
                    + (" Funding supports this short." if funding_bias == "longs paying" else "")
                    + (" Longs look more vulnerable." if dominant_side == "longs_vulnerable" else "")
                ),
            }
        )

    if planning_resistance:
        breakout = _safe_float(planning_resistance.get("price"))
        entry = breakout + max(current_price * 0.0012, atr_1h * 0.18)
        stop = breakout - max(current_price * 0.0012, atr_1h * 0.35)
        risk = max(entry - stop, atr_1h * 0.55)
        probability = 47.0 + max(0.0, overall_bias_score) * 1.4
        if tf_15m.get("bias") in {"Bullish", "Trending Bullish"}:
            probability += 3.0
        if tf_1h.get("bias") in {"Bullish", "Trending Bullish"}:
            probability += 4.0
        if book_pressure == "bullish":
            probability += 2.0
        if range_position > 0.62:
            probability += 2.0
        if day_change_pct > 0:
            probability += 1.0
        if okx_mid_above:
            probability += 1.0
        if overall_bias_score < 0:
            probability -= 6.0
        probability = max(34.0, min(76.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score >= 0)
        tp1, tp2, tp3 = _build_targets(
            "LONG",
            entry,
            risk,
            current_price,
            atr_1h,
            mode_cfg,
            "breakout",
            next_resistance,
            okx_far_above,
            top_above.get("level_price"),
        )
        scenarios.append(
            {
                "title": "Long breakout plan",
                "side": "LONG",
                "kind": "breakout",
                "probability": probability,
                "entry_low": entry,
                "entry_high": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trend_aligned": trend_bias_score >= 0,
                "trigger": (
                    f"If BTC closes above {_format_cluster(planning_resistance)}"
                    + (f" and then targets OKX far-zone {_fmt_price(okx_far_above.get('level_price'))}" if okx_far_above else "")
                    + " and holds the retest, long continuation."
                ),
                "note": "Use only if momentum holds and no pullback entry appears first.",
            }
        )

    if next_support and _distance_pct(next_support.get("price"), current_price) >= mode_cfg["major_liq_min_dist_pct"]:
        liq_price = _safe_float(next_support.get("price"))
        zone_half = max(liq_price * 0.00030, atr_1h * 0.05)
        entry_low = liq_price - zone_half
        entry_high = liq_price + zone_half
        stop = _tight_plan_stop(entry_low, entry_high, current_price, atr_1h, "LONG")
        risk = max(liq_price - stop, atr_1h * 0.50)
        probability = 46.0
        if dominant_side == "shorts_vulnerable":
            probability += 6.0
        if funding_bias == "shorts paying":
            probability += 3.0
        if okx_liq_usd >= 10_000_000:
            probability += 2.0
        if trend_bias_score >= 0:
            probability += 2.0
        probability = max(38.0, min(74.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=trend_bias_score >= 0)
        tp1, tp2, tp3 = _build_targets(
            "LONG",
            liq_price,
            risk,
            current_price,
            atr_1h,
            mode_cfg,
            "major_flush",
            planning_support,
            planning_resistance,
            next_resistance,
        )
        scenarios.append(
            {
                "title": "Long major liq flush plan",
                "side": "LONG",
                "kind": "major_flush",
                "probability": probability,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trend_aligned": trend_bias_score >= 0,
                "trigger": f"If BTC flushes into major long-liq zone {_fmt_price(liq_price)} and instantly reclaims, long the reversal.",
                "note": "This is a deeper sweep plan, not the first pullback entry.",
            }
        )

    if planning_support:
        breakdown = _safe_float(planning_support.get("price"))
        entry = breakdown - max(current_price * 0.0012, atr_1h * 0.18)
        stop = breakdown + max(current_price * 0.0012, atr_1h * 0.35)
        risk = max(stop - entry, atr_1h * 0.55)
        probability = 47.0 + max(0.0, -overall_bias_score) * 1.4
        if tf_15m.get("bias") in {"Bearish", "Trending Bearish"}:
            probability += 3.0
        if tf_1h.get("bias") in {"Bearish", "Trending Bearish"}:
            probability += 4.0
        if book_pressure == "bearish":
            probability += 2.0
        if range_position < 0.38:
            probability += 2.0
        if day_change_pct < 0:
            probability += 1.0
        if okx_mid_below:
            probability += 1.0
        if overall_bias_score > 0:
            probability -= 6.0
        probability = max(34.0, min(76.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score <= 0)
        tp1, tp2, tp3 = _build_targets(
            "SHORT",
            entry,
            risk,
            current_price,
            atr_1h,
            mode_cfg,
            "breakdown",
            next_support,
            okx_far_below,
            top_below.get("level_price"),
        )
        scenarios.append(
            {
                "title": "Short breakdown plan",
                "side": "SHORT",
                "kind": "breakdown",
                "probability": probability,
                "entry_low": entry,
                "entry_high": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trend_aligned": trend_bias_score <= 0,
                "trigger": (
                    f"If BTC closes below {_format_cluster(planning_support)}"
                    + (f" and then targets OKX far-zone {_fmt_price(okx_far_below.get('level_price'))}" if okx_far_below else "")
                    + " and fails the retest, short continuation."
                ),
                "note": "Use only if support is lost cleanly and downside momentum expands.",
            }
        )

    if next_resistance and _distance_pct(next_resistance.get("price"), current_price) >= mode_cfg["major_liq_min_dist_pct"]:
        liq_price = _safe_float(next_resistance.get("price"))
        zone_half = max(liq_price * 0.00030, atr_1h * 0.05)
        entry_low = liq_price - zone_half
        entry_high = liq_price + zone_half
        stop = _tight_plan_stop(entry_low, entry_high, current_price, atr_1h, "SHORT")
        risk = max(stop - liq_price, atr_1h * 0.50)
        probability = 46.0
        if dominant_side == "longs_vulnerable":
            probability += 6.0
        if funding_bias == "longs paying":
            probability += 3.0
        if okx_liq_usd >= 10_000_000:
            probability += 2.0
        if trend_bias_score <= 0:
            probability += 2.0
        probability = max(38.0, min(74.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=trend_bias_score <= 0)
        tp1, tp2, tp3 = _build_targets(
            "SHORT",
            liq_price,
            risk,
            current_price,
            atr_1h,
            mode_cfg,
            "major_squeeze",
            planning_resistance,
            planning_support,
            next_support,
        )
        scenarios.append(
            {
                "title": "Short major liq squeeze plan",
                "side": "SHORT",
                "kind": "major_squeeze",
                "probability": probability,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trend_aligned": trend_bias_score <= 0,
                "trigger": f"If BTC squeezes into major short-liq zone {_fmt_price(liq_price)} and fails back below it, short the reversal.",
                "note": "This is a deeper squeeze plan, not the first rejection entry.",
            }
        )
    long_candidates = [row for row in scenarios if row.get("side") == "LONG"]
    short_candidates = [row for row in scenarios if row.get("side") == "SHORT"]

    def _entry_distance_pct(row):
        entry_mid = (_safe_float(row.get("entry_low")) + _safe_float(row.get("entry_high"))) / 2.0
        return _distance_pct(entry_mid, current_price)

    def _entry_distance_usd(row):
        entry_mid = (_safe_float(row.get("entry_low")) + _safe_float(row.get("entry_high"))) / 2.0
        return abs(entry_mid - current_price)

    def _select_best(rows, preferred_kinds):
        if not rows:
            return None
        preferred = [row for row in rows if row.get("kind") in preferred_kinds]
        source = preferred or rows
        return max(
            source,
            key=lambda row: (
                row.get("trend_aligned", False),
                float(row.get("probability", 0.0)),
            ),
        )

    def _select_near(rows, preferred_kinds, *, expected_side_of_price=None, fallback_any_side=False):
        if not rows:
            return None
        directional_rows = []
        for row in rows:
            entry_mid = (_safe_float(row.get("entry_low")) + _safe_float(row.get("entry_high"))) / 2.0
            if expected_side_of_price == "below" and entry_mid >= current_price:
                continue
            if expected_side_of_price == "above" and entry_mid <= current_price:
                continue
            directional_rows.append(row)
        if not directional_rows and fallback_any_side:
            directional_rows = list(rows)
        if not directional_rows:
            return None
        band_rows = [
            row for row in directional_rows
            if (
                mode_cfg["min_dist_pct"] <= _entry_distance_pct(row) <= mode_cfg["near_pick_max_dist_pct"]
                and mode_cfg["near_pick_min_usd"] <= _entry_distance_usd(row) <= mode_cfg["near_pick_max_usd"]
            )
        ]
        bounded_rows = [
            row for row in directional_rows
            if mode_cfg["min_dist_pct"] <= _entry_distance_pct(row) <= mode_cfg["max_dist_pct"]
        ]
        source_pool = band_rows or bounded_rows or directional_rows
        preferred = [row for row in source_pool if row.get("kind") in preferred_kinds]
        source = preferred or source_pool
        return min(
            source,
            key=lambda row: (
                abs(_entry_distance_usd(row) - mode_cfg["near_pick_target_usd"]),
                abs(_entry_distance_pct(row) - mode_cfg["near_pick_target_dist_pct"]),
                -float(row.get("probability", 0.0)),
                not row.get("trend_aligned", False),
            ),
        )

    if trend_bias_score >= 1.5:
        best_long = _select_best(long_candidates, {"pullback", "breakout", "major_flush"})
        best_short = _select_best(short_candidates, {"rejection", "major_squeeze", "breakdown"})
        near_long = _select_near(long_candidates, {"pullback", "major_flush", "breakout"}, expected_side_of_price="below", fallback_any_side=True)
        near_short = _select_near(short_candidates, {"rejection", "major_squeeze", "breakdown"}, expected_side_of_price="above", fallback_any_side=True)
    elif trend_bias_score <= -1.5:
        best_long = _select_best(long_candidates, {"pullback", "major_flush", "breakout"})
        best_short = _select_best(short_candidates, {"breakdown", "rejection", "major_squeeze"})
        near_long = _select_near(long_candidates, {"pullback", "major_flush", "breakout"}, expected_side_of_price="below", fallback_any_side=True)
        near_short = _select_near(short_candidates, {"rejection", "major_squeeze", "breakdown"}, expected_side_of_price="above", fallback_any_side=True)
    else:
        best_long = _select_best(long_candidates, {"pullback", "breakout", "major_flush"})
        best_short = _select_best(short_candidates, {"rejection", "breakdown", "major_squeeze"})
        near_long = _select_near(long_candidates, {"pullback", "major_flush", "breakout"}, expected_side_of_price="below", fallback_any_side=True)
        near_short = _select_near(short_candidates, {"rejection", "major_squeeze", "breakdown"}, expected_side_of_price="above", fallback_any_side=True)

    selected = []
    if near_long:
        selected.append(near_long)
    if near_short and near_short not in selected:
        selected.append(near_short)
    if best_long and best_long not in selected:
        selected.append(best_long)
    if best_short and best_short not in selected:
        selected.append(best_short)

    extra_long_candidates = [row for row in sorted(long_candidates, key=lambda row: row.get("probability", 0), reverse=True) if row not in selected]
    extra_short_candidates = [row for row in sorted(short_candidates, key=lambda row: row.get("probability", 0), reverse=True) if row not in selected]
    if extra_long_candidates and len(selected) < max_scenarios:
        selected.append(extra_long_candidates[0])
    if extra_short_candidates and len(selected) < max_scenarios:
        selected.append(extra_short_candidates[0])
    if len(selected) < max_scenarios:
        remaining = [row for row in sorted(scenarios, key=lambda row: row.get("probability", 0), reverse=True) if row not in selected]
        for row in remaining:
            selected.append(row)
            if len(selected) >= max_scenarios:
                break
    for row in selected:
        row["execution_tf"] = str(mode_cfg.get("execution_tf") or "15m")
        row["scenario_mode"] = str(mode or "swing")
        row["entry_mid"] = (_safe_float(row.get("entry_low")) + _safe_float(row.get("entry_high"))) / 2.0
    selected = sorted(selected, key=lambda row: row.get("probability", 0), reverse=True)
    return selected[:max_scenarios]


def _scenario_html(idx, scenario):
    side = str(scenario.get("side") or "").upper()
    icon = "\U0001F7E2" if side == "LONG" else "\U0001F534"
    title = str(scenario.get("title") or "Setup").upper()
    trend_tag = " (with trend)" if scenario.get("trend_aligned") else " (counter-trend)"
    position_text = _position_label(scenario.get("risk_style"))
    if abs(_safe_float(scenario.get("entry_high")) - _safe_float(scenario.get("entry_low"))) > 1e-9:
        entry_text = f"{_fmt_price(scenario.get('entry_low'))} - {_fmt_price(scenario.get('entry_high'))}"
    else:
        entry_text = _fmt_price(scenario.get("entry_low"))
    return (
        f"{icon} <b>{idx}. {title}{trend_tag}</b>\n"
        f"<blockquote>"
        f"Plan: {entry_text}\n"
        f"SL: {_fmt_price(scenario.get('stop'))}\n"
        f"TP: {_fmt_price(scenario.get('tp1'))} / {_fmt_price(scenario.get('tp2'))} / {_fmt_price(scenario.get('tp3'))}"
        f"</blockquote>\n"
        f"Position: <b>{position_text}</b> | Chance: <b>{float(scenario.get('probability') or 0):.0f}%</b>\n"
        f"<blockquote>{scenario.get('trigger')}</blockquote>\n"
        f"{scenario.get('note')}"
    )


def _pick_zone_for_distance(rows, target_dist_pct, tolerance_pct):
    if not rows:
        return None
    eligible = [row for row in rows if abs(_safe_float(row.get("distance_pct")) - target_dist_pct) <= tolerance_pct]
    source = eligible or rows
    return min(
        source,
        key=lambda row: (
            abs(_safe_float(row.get("distance_pct")) - target_dist_pct),
            -_safe_float(row.get("score")),
            -_safe_float(row.get("size_usd")),
        ),
    )


def _build_liquidation_heatmap_rows(current_price, atr_1h, bitunix_book, okx_order_book, liq_map):
    rows = []

    def add_book_rows(order_book, source_name, max_distance_mult, bucket_pct, min_usd):
        if not order_book:
            return
        raw_rows = detect_liquidity_candidates(
            order_book=order_book,
            price=current_price,
            atr=max(atr_1h, current_price * 0.0025),
            timeframe=source_name,
            max_distance_atr_mult=max_distance_mult,
            bucket_pct=bucket_pct,
        )
        for row in raw_rows:
            price = _safe_float(row.get("level_price"))
            size_usd = _safe_float(row.get("size_usd"))
            dist_pct = _safe_float(row.get("distance_pct"))
            side = str(row.get("side") or "").upper()
            if price <= 0 or size_usd < min_usd:
                continue
            if dist_pct < 0.10 or dist_pct > 8.5:
                continue
            rows.append(
                {
                    "price": price,
                    "size_usd": size_usd,
                    "distance_pct": dist_pct,
                    "zone_side": "short_liq" if side == "LONG" else "long_liq",
                    "bucket": "major" if dist_pct >= 2.5 or size_usd >= 18_000_000 else ("mid" if dist_pct >= 0.7 else "near"),
                    "source": source_name,
                }
            )

    add_book_rows(bitunix_book, "bitunix", 24.0, 0.03, 350_000)
    add_book_rows(okx_order_book, "okx", 24.0, 0.03, 2_000_000)

    for row in list((liq_map or {}).get("all_short_liq_zones") or []) + list((liq_map or {}).get("all_long_liq_zones") or []):
        price = _safe_float(row.get("price"))
        size_usd = _safe_float(row.get("size_usd"))
        dist_pct = _safe_float(row.get("distance_pct"))
        if price <= 0 or size_usd <= 0 or dist_pct > 9.5:
            continue
        rows.append(
            {
                "price": price,
                "size_usd": size_usd,
                "distance_pct": dist_pct,
                "zone_side": row.get("zone_side"),
                "bucket": row.get("bucket") or ("major" if dist_pct >= 2.5 else "mid"),
                "source": "cluster",
            }
        )

    if not rows:
        return []

    grouped = {}
    bucket_size = max(current_price * 0.0007, 1.0)
    for row in rows:
        zone_side = str(row.get("zone_side") or "")
        key = (zone_side, int(_safe_float(row.get("price")) / bucket_size))
        current = grouped.get(key)
        if current is None:
            grouped[key] = dict(row)
            continue
        if _safe_float(row.get("size_usd")) > _safe_float(current.get("size_usd")):
            current["price"] = _safe_float(row.get("price"))
            current["bucket"] = row.get("bucket")
            current["source"] = row.get("source")
        current["size_usd"] = _safe_float(current.get("size_usd")) + _safe_float(row.get("size_usd")) * 0.35
        current["distance_pct"] = min(_safe_float(current.get("distance_pct")), _safe_float(row.get("distance_pct")))
        grouped[key] = current

    merged_rows = list(grouped.values())
    merged_rows.sort(
        key=lambda row: (
            str(row.get("bucket") or "") == "major",
            _safe_float(row.get("size_usd")),
            -_safe_float(row.get("distance_pct")),
        ),
        reverse=True,
    )
    return merged_rows[:90]


def _load_liquidation_heatmap_history():
    if not os.path.exists(LIQUIDATION_HEATMAP_HISTORY_FILE):
        return []
    try:
        with open(LIQUIDATION_HEATMAP_HISTORY_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _save_liquidation_heatmap_history(history_rows):
    try:
        with open(LIQUIDATION_HEATMAP_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history_rows, f, separators=(",", ":"))
    except Exception:
        pass


def _record_liquidation_heatmap_history(current_price, heatmap_rows):
    now_ts = int(time.time())
    compact_rows = []
    for row in (heatmap_rows or [])[:60]:
        price = _safe_float(row.get("price"))
        size_usd = _safe_float(row.get("size_usd"))
        if price <= 0 or size_usd <= 0:
            continue
        compact_rows.append(
            {
                "price": price,
                "size_usd": size_usd,
                "distance_pct": _safe_float(row.get("distance_pct")),
                "zone_side": str(row.get("zone_side") or ""),
                "bucket": str(row.get("bucket") or ""),
            }
        )
    history = _load_liquidation_heatmap_history()
    snapshot = {
        "ts": now_ts,
        "price": _safe_float(current_price),
        "rows": compact_rows,
    }
    if history and (now_ts - int(history[-1].get("ts") or 0)) < LIQUIDATION_HEATMAP_HISTORY_MIN_SECONDS:
        history[-1] = snapshot
    else:
        history.append(snapshot)
    history = history[-LIQUIDATION_HEATMAP_HISTORY_MAX_POINTS:]
    _save_liquidation_heatmap_history(history)
    return history


def build_liquidation_map_snapshot(symbol=SYMBOL):
    data = _fetch_all_bitunix_timeframes(symbol=symbol, timeframes=BITUNIX_TIMEFRAMES)
    missing = [tf for tf in BITUNIX_TIMEFRAMES if tf not in data or data[tf].empty]
    if missing:
        raise RuntimeError(f"Missing Bitunix data for: {', '.join(missing)}")

    tf_map = {tf: _tf_summary(tf, data[tf]) for tf in BITUNIX_TIMEFRAMES}
    ticker = _fetch_bitunix_ticker(symbol=symbol)
    funding = _fetch_bitunix_funding(symbol=symbol)
    funding_history = _fetch_bitunix_funding_history(symbol=symbol, limit=30)
    book = _fetch_bitunix_depth(symbol=symbol, limit="max")

    current_price = _safe_float(
        ticker.get("lastPrice") or ticker.get("last") or funding.get("lastPrice") or funding.get("markPrice")
    )
    if current_price <= 0:
        current_price = _safe_float((tf_map.get("1h") or {}).get("close"))
    if current_price <= 0:
        raise RuntimeError("Could not load the current BTC price from Bitunix.")

    levels = calculate_levels(
        data["1d"],
        weekly_df=data["1w"],
        monthly_df=data["1M"],
        hourly_df=data["1h"],
    )
    book_ctx = _order_book_context(book, current_price)
    atr_1h = max(_safe_float((tf_map.get("1h") or {}).get("atr")), current_price * 0.0035)
    funding_rate_raw = _safe_float(funding.get("fundingRate"))
    funding_ctx = _funding_context(funding_rate_raw, funding_history)
    ticker_ctx = _ticker_context(ticker)
    liq_ctx = _liquidity_context(book, current_price, atr_1h)
    okx_ctx = _okx_liquidation_context(current_price, atr_1h, levels)
    liq_map = build_liquidation_map(
        current_price=current_price,
        levels=levels,
        tf_map=tf_map,
        funding_ctx=funding_ctx,
        ticker_ctx=ticker_ctx,
        book_ctx=book_ctx,
        liq_ctx=liq_ctx,
        okx_ctx=okx_ctx,
    )

    upside_rows = list((liq_map or {}).get("all_short_liq_zones") or (liq_map or {}).get("short_liq_zones") or [])
    downside_rows = list((liq_map or {}).get("all_long_liq_zones") or (liq_map or {}).get("long_liq_zones") or [])
    horizon_targets = [
        ("12H", 0.7, 0.35),
        ("24H", 1.1, 0.45),
        ("48H", 1.8, 0.60),
        ("3D", 2.6, 0.75),
        ("1W", 3.7, 1.00),
        ("2W", 5.2, 1.35),
        ("1M", 7.0, 1.80),
    ]
    horizons = []
    for name, target_dist, tolerance in horizon_targets:
        upside = _pick_zone_for_distance(upside_rows, target_dist, tolerance)
        downside = _pick_zone_for_distance(downside_rows, target_dist, tolerance)
        horizons.append(
            {
                "horizon": name,
                "upside": _safe_float((upside or {}).get("price")) if upside else None,
                "downside": _safe_float((downside or {}).get("price")) if downside else None,
                "upside_zone": upside,
                "downside_zone": downside,
            }
        )

    heatmap_rows = _build_liquidation_heatmap_rows(
        current_price=current_price,
        atr_1h=atr_1h,
        bitunix_book=book,
        okx_order_book=(okx_ctx or {}).get("order_book"),
        liq_map=liq_map,
    )
    heatmap_history = _record_liquidation_heatmap_history(current_price, heatmap_rows)

    return {
        "current_price": current_price,
        "funding_rate": funding_rate_raw,
        "chart_df": data["15m"],
        "chart_timeframe": "15m",
        "liq_map": liq_map,
        "horizons": horizons,
        "heatmap_rows": heatmap_rows,
        "heatmap_history": heatmap_history,
    }


def build_btc_scenarios_payload(symbol=SYMBOL, mode="swing"):
    mode_cfg = _scenario_mode_config(mode)
    data = _fetch_all_bitunix_timeframes(symbol=symbol, timeframes=BITUNIX_TIMEFRAMES)
    missing = [tf for tf in BITUNIX_TIMEFRAMES if tf not in data or data[tf].empty]
    if missing:
        raise RuntimeError(f"Missing Bitunix data for: {', '.join(missing)}")

    tf_map = {tf: _tf_summary(tf, data[tf]) for tf in BITUNIX_TIMEFRAMES}
    ticker = _fetch_bitunix_ticker(symbol=symbol)
    funding = _fetch_bitunix_funding(symbol=symbol)
    funding_history = _fetch_bitunix_funding_history(symbol=symbol, limit=30)
    book = _fetch_bitunix_depth(symbol=symbol, limit="50")

    current_price = _safe_float(
        ticker.get("lastPrice") or ticker.get("last") or funding.get("lastPrice") or funding.get("markPrice")
    )
    if current_price <= 0:
        current_price = _safe_float((tf_map.get("1h") or {}).get("close"))
    if current_price <= 0:
        raise RuntimeError("Could not load the current BTC price from Bitunix.")

    levels = calculate_levels(
        data["1d"],
        weekly_df=data["1w"],
        monthly_df=data["1M"],
        hourly_df=data["1h"],
    )
    book_ctx = _order_book_context(book, current_price)
    atr_1h = max(_safe_float((tf_map.get("1h") or {}).get("atr")), current_price * 0.0035)
    funding_rate_raw = _safe_float(funding.get("fundingRate"))
    funding_ctx = _funding_context(funding_rate_raw, funding_history)
    ticker_ctx = _ticker_context(ticker)
    liq_ctx = _liquidity_context(book, current_price, atr_1h)
    okx_ctx = _okx_liquidation_context(current_price, atr_1h, levels)
    liq_map = build_liquidation_map(
        current_price=current_price,
        levels=levels,
        tf_map=tf_map,
        funding_ctx=funding_ctx,
        ticker_ctx=ticker_ctx,
        book_ctx=book_ctx,
        liq_ctx=liq_ctx,
        okx_ctx=okx_ctx,
    )
    scenarios = _build_scenarios(
        current_price=current_price,
        levels=levels,
        tf_map=tf_map,
        book_ctx=book_ctx,
        funding_ctx=funding_ctx,
        ticker_ctx=ticker_ctx,
        liq_ctx=liq_ctx,
        okx_ctx=okx_ctx,
        liq_map=liq_map,
        mode=mode,
        max_scenarios=4,
    )
    if not scenarios:
        raise RuntimeError("No valid BTC scenarios could be built from Bitunix data.")

    return {
        "symbol": symbol,
        "mode": str(mode or "swing"),
        "mode_cfg": mode_cfg,
        "current_price": current_price,
        "funding_rate": funding_rate_raw,
        "tf_map": tf_map,
        "levels": levels,
        "book_ctx": book_ctx,
        "funding_ctx": funding_ctx,
        "ticker_ctx": ticker_ctx,
        "liq_ctx": liq_ctx,
        "okx_ctx": okx_ctx,
        "liq_map": liq_map,
        "scenarios": scenarios,
    }


def build_btc_market_report(symbol=SYMBOL, mode="swing"):
    payload = build_btc_scenarios_payload(symbol=symbol, mode=mode)
    mode_cfg = payload["mode_cfg"]
    current_price = _safe_float(payload.get("current_price"))
    funding_rate_raw = _safe_float(payload.get("funding_rate"))
    scenarios = list(payload.get("scenarios") or [])

    blocks = [
        (
            f"\U0001F4CD <b>{mode_cfg['title']}</b>\n\n"
            "<blockquote>"
            f"Price: {_fmt_price(current_price)}\n"
            f"Funding: {funding_rate_raw:+.6f}%"
            "</blockquote>"
        ),
    ]
    for idx, scenario in enumerate(scenarios, start=1):
        blocks.append(_scenario_html(idx, scenario))
    blocks.append(f"<blockquote>{mode_cfg['final_note']}</blockquote>")
    return "\n\n".join(blocks)
