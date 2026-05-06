from concurrent.futures import ThreadPoolExecutor, as_completed
import math

import pandas as pd

from bitunix_trade import BitunixFuturesClient
from channels import calculate_channels, check_channel_signals
from config import SYMBOL
from levels import calculate_levels
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


def _fmt_price(value):
    return f"{_safe_float(value):,.2f}"


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


def _scenario_risk(probability, trend_aligned):
    if trend_aligned and probability >= 72:
        return "normal", 0.75
    if trend_aligned and probability >= 64:
        return "medium", 0.60
    if probability >= 58:
        return "reduced", 0.45
    return "small", 0.30


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
    if not rows:
        return {}
    return rows[0] or {}


def _fetch_bitunix_funding(symbol=SYMBOL):
    client = BitunixFuturesClient()
    raw = client.get_funding_rate(symbol)
    return raw.get("data") or {}


def _fetch_bitunix_depth(symbol=SYMBOL, limit="50"):
    client = BitunixFuturesClient()
    raw = client.get_depth(symbol, str(limit))
    payload = raw.get("data") or {}
    bids = []
    asks = []
    for price, size in payload.get("bids", []) or []:
        bids.append([_safe_float(price), _safe_float(size)])
    for price, size in payload.get("asks", []) or []:
        asks.append([_safe_float(price), _safe_float(size)])
    return {"bids": bids, "asks": asks}


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
    channel_signal = channel_events[0] if channel_events else None
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
        "channel_signal": channel_signal,
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


def _build_scenarios(current_price, levels, tf_map, book_ctx, funding_rate):
    current_price = _safe_float(current_price)
    tf_weights = {"1M": 4.0, "1w": 3.0, "1d": 2.0, "4h": 2.0, "1h": 1.0}
    overall_bias_score = 0.0
    for tf, weight in tf_weights.items():
        overall_bias_score += _bias_value((tf_map.get(tf) or {}).get("bias")) * weight

    tf_15m = tf_map.get("15m") or {}
    tf_1h = tf_map.get("1h") or {}
    tf_4h = tf_map.get("4h") or {}
    support_clusters, resistance_clusters = _nearest_levels(levels, current_price, tf_map)
    nearest_support = support_clusters[0] if support_clusters else None
    next_support = support_clusters[1] if len(support_clusters) > 1 else None
    nearest_resistance = resistance_clusters[0] if resistance_clusters else None
    next_resistance = resistance_clusters[1] if len(resistance_clusters) > 1 else None
    atr_1h = max(_safe_float(tf_1h.get("atr")), current_price * 0.0035)
    book_pressure = str(book_ctx.get("pressure") or "balanced")
    funding_rate = _safe_float(funding_rate)

    scenarios = []

    if nearest_support:
        entry_mid = _safe_float(nearest_support.get("price"))
        zone_half = max(entry_mid * 0.0012, atr_1h * 0.18)
        entry_low = entry_mid - zone_half
        entry_high = entry_mid + zone_half
        stop = entry_low - max(atr_1h * 0.70, current_price * 0.0022)
        if next_support:
            stop = min(stop, _safe_float(next_support.get("price")) - max(atr_1h * 0.20, current_price * 0.0008))
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
        if book_pressure == "bullish":
            probability += 3.0
        probability = max(40.0, min(84.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score >= 0)
        scenarios.append(
            {
                "title": "LONG setup",
                "side": "LONG",
                "probability": probability,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "stop": stop,
                "tp1": _safe_float((nearest_resistance or {}).get("price"), entry_mid + risk * 1.2),
                "tp2": max(_safe_float((nearest_resistance or {}).get("price"), entry_mid + risk * 1.2), entry_mid + risk * 2.0),
                "tp3": max(_safe_float((next_resistance or {}).get("price"), entry_mid + risk * 2.2), entry_mid + risk * 3.0),
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trigger": f"Sweep into {_format_cluster(nearest_support)} then a 15m close back above EMA9.",
                "note": "Best long if BTC dips, reclaims, and higher timeframes stay supportive.",
            }
        )

    if nearest_resistance:
        entry_mid = _safe_float(nearest_resistance.get("price"))
        zone_half = max(entry_mid * 0.0012, atr_1h * 0.18)
        entry_low = entry_mid - zone_half
        entry_high = entry_mid + zone_half
        stop = entry_high + max(atr_1h * 0.70, current_price * 0.0022)
        if next_resistance:
            stop = max(stop, _safe_float(next_resistance.get("price")) + max(atr_1h * 0.20, current_price * 0.0008))
        risk = max(stop - entry_mid, atr_1h * 0.55)
        probability = 54.0 + max(0.0, -overall_bias_score) * 1.7
        if _bias_value(tf_1h.get("bias")) < 0:
            probability += 4.0
        if tf_1h.get("channel_signal") and str((tf_1h.get("channel_signal") or {}).get("side")) == "SHORT":
            probability += 4.0
        if funding_rate > 0:
            probability += 2.0
        if book_pressure == "bearish":
            probability += 3.0
        probability = max(40.0, min(82.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score <= 0)
        scenarios.append(
            {
                "title": "SHORT setup",
                "side": "SHORT",
                "probability": probability,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "stop": stop,
                "tp1": _safe_float((nearest_support or {}).get("price"), entry_mid - risk * 1.2),
                "tp2": min(_safe_float((nearest_support or {}).get("price"), entry_mid - risk * 1.2), entry_mid - risk * 2.0),
                "tp3": min(_safe_float((next_support or {}).get("price"), entry_mid - risk * 2.2), entry_mid - risk * 3.0),
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trigger": f"Reject {_format_cluster(nearest_resistance)} then a 15m close back under EMA9.",
                "note": "Best short only on a clean rejection. No blind short in the middle.",
            }
        )

    if nearest_resistance:
        breakout = _safe_float(nearest_resistance.get("price"))
        entry = breakout + max(current_price * 0.0012, atr_1h * 0.18)
        stop = breakout - max(current_price * 0.0020, atr_1h * 0.55)
        risk = max(entry - stop, atr_1h * 0.55)
        probability = 47.0 + max(0.0, overall_bias_score) * 1.4
        if tf_15m.get("bias") in {"Bullish", "Trending Bullish"}:
            probability += 3.0
        if tf_1h.get("bias") in {"Bullish", "Trending Bullish"}:
            probability += 4.0
        if book_pressure == "bullish":
            probability += 2.0
        probability = max(34.0, min(76.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score >= 0)
        scenarios.append(
            {
                "title": "LONG breakout",
                "side": "LONG",
                "probability": probability,
                "entry_low": entry,
                "entry_high": entry,
                "stop": stop,
                "tp1": max(_safe_float((next_resistance or {}).get("price"), entry + risk * 1.2), entry + risk * 1.2),
                "tp2": entry + risk * 2.2,
                "tp3": entry + risk * 3.2,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trigger": f"Take only after a close above {_format_cluster(nearest_resistance)} and a hold on retest.",
                "note": "Use this if BTC does not pull back and continuation is cleaner.",
            }
        )

    if nearest_support:
        breakdown = _safe_float(nearest_support.get("price"))
        entry = breakdown - max(current_price * 0.0012, atr_1h * 0.18)
        stop = breakdown + max(current_price * 0.0020, atr_1h * 0.55)
        risk = max(stop - entry, atr_1h * 0.55)
        probability = 47.0 + max(0.0, -overall_bias_score) * 1.4
        if tf_15m.get("bias") in {"Bearish", "Trending Bearish"}:
            probability += 3.0
        if tf_1h.get("bias") in {"Bearish", "Trending Bearish"}:
            probability += 4.0
        if book_pressure == "bearish":
            probability += 2.0
        probability = max(34.0, min(76.0, probability))
        risk_style, risk_pct = _scenario_risk(probability, trend_aligned=overall_bias_score <= 0)
        scenarios.append(
            {
                "title": "SHORT breakdown",
                "side": "SHORT",
                "probability": probability,
                "entry_low": entry,
                "entry_high": entry,
                "stop": stop,
                "tp1": min(_safe_float((next_support or {}).get("price"), entry - risk * 1.2), entry - risk * 1.2),
                "tp2": entry - risk * 2.2,
                "tp3": entry - risk * 3.2,
                "risk_style": risk_style,
                "risk_pct": risk_pct,
                "trigger": f"Take only after a close below {_format_cluster(nearest_support)} and failed reclaim.",
                "note": "Use this only if support is clearly lost. Fast reclaim means skip.",
            }
        )

    return sorted(scenarios, key=lambda row: row.get("probability", 0), reverse=True)[:2]


def _scenario_html(idx, scenario):
    side = str(scenario.get("side") or "").upper()
    icon = "🟢" if side == "LONG" else "🔴"
    if abs(_safe_float(scenario.get("entry_high")) - _safe_float(scenario.get("entry_low"))) > 1e-9:
        entry_text = f"{_fmt_price(scenario.get('entry_low'))} - {_fmt_price(scenario.get('entry_high'))}"
    else:
        entry_text = _fmt_price(scenario.get("entry_low"))
    return (
        f"{icon} <b>{idx}. {scenario.get('title').upper()}</b>\n"
        f"<blockquote>🎯 Entry: <b>{entry_text}</b>\n"
        f"🛑 SL: {_fmt_price(scenario.get('stop'))}\n"
        f"💰 TP: {_fmt_price(scenario.get('tp1'))} / {_fmt_price(scenario.get('tp2'))} / {_fmt_price(scenario.get('tp3'))}\n"
        f"📊 Chance: {float(scenario.get('probability') or 0):.0f}% | Risk: {float(scenario.get('risk_pct') or 0):.2f}%\n\n</blockquote>"
        f"<blockquote>{scenario.get('trigger')}</blockquote>\n"
        f"<blockquote>{scenario.get('note')}</blockquote>"
    )


def build_btc_market_report(symbol=SYMBOL):
    data = _fetch_all_bitunix_timeframes(symbol=symbol, timeframes=BITUNIX_TIMEFRAMES)
    missing = [tf for tf in BITUNIX_TIMEFRAMES if tf not in data or data[tf].empty]
    if missing:
        raise RuntimeError(f"Missing Bitunix data for: {', '.join(missing)}")

    tf_map = {tf: _tf_summary(tf, data[tf]) for tf in BITUNIX_TIMEFRAMES}
    ticker = _fetch_bitunix_ticker(symbol=symbol)
    funding = _fetch_bitunix_funding(symbol=symbol)
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
    funding_rate_raw = _safe_float(funding.get("fundingRate"))
    scenarios = _build_scenarios(
        current_price=current_price,
        levels=levels,
        tf_map=tf_map,
        book_ctx=book_ctx,
        funding_rate=funding_rate_raw,
    )
    if not scenarios:
        raise RuntimeError("No valid BTC scenarios could be built from Bitunix data.")

    top = scenarios[0]
    if abs(_safe_float(top.get("entry_high")) - _safe_float(top.get("entry_low"))) > 1e-9:
        patience_zone = f"{_fmt_price(top.get('entry_low'))} - {_fmt_price(top.get('entry_high'))}"
    else:
        patience_zone = _fmt_price(top.get("entry_low"))

    blocks = [f"🧭 <b>BTC SCENARIOS</b>\n💵 Price: {_fmt_price(current_price)}", _scenario_html(1, scenarios[0])]
    if len(scenarios) > 1:
        blocks.append(_scenario_html(2, scenarios[1]))
    blocks.append(
        "<blockquote>"
        f"Best wait zone: {patience_zone}. "
        "If BTC stays in the middle with no sweep or reclaim, skip."
        "</blockquote>"
    )
    return "\n\n".join(blocks)
