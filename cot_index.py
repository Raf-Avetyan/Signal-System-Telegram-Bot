import math


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _clamp(value, low, high):
    return max(float(low), min(float(high), float(value)))


def _bias_value(text):
    bias = str(text or "").strip().lower()
    if "trending bullish" in bias:
        return 2.0
    if bias == "bullish":
        return 1.0
    if "trending bearish" in bias:
        return -2.0
    if bias == "bearish":
        return -1.0
    return 0.0


def _weighted_avg(rows):
    num = 0.0
    den = 0.0
    for value, weight in rows:
        num += float(value) * float(weight)
        den += float(weight)
    if den <= 0:
        return 0.0
    return num / den


def build_synthetic_cot_index(
    *,
    current_price,
    tf_map=None,
    funding_ctx=None,
    ticker_ctx=None,
    book_ctx=None,
    okx_ctx=None,
    multi_ctx=None,
    liq_map=None,
    oi_change_pct=None,
):
    tf_map = tf_map or {}
    funding_ctx = funding_ctx or {}
    ticker_ctx = ticker_ctx or {}
    book_ctx = book_ctx or {}
    okx_ctx = okx_ctx or {}
    multi_ctx = multi_ctx or {}
    liq_map = liq_map or {}

    exchange_funding = dict(multi_ctx.get("funding") or {})
    weighted_rates = []
    for name, rate in exchange_funding.items():
        rate_val = _safe_float(rate)
        if not math.isfinite(rate_val):
            continue
        weight = 2.5 if str(name).lower() == "bitget" else 1.0
        weighted_rates.append((_clamp(rate_val / 0.0006, -1.35, 1.35), weight))
    if weighted_rates:
        funding_component = _weighted_avg(weighted_rates)
    else:
        funding_component = _clamp(_safe_float(funding_ctx.get("current_rate")) / 0.0006, -1.0, 1.0)

    funding_trend_component = _clamp(_safe_float(funding_ctx.get("trend")) / 0.00003, -1.0, 1.0)

    trend_component = _clamp(
        (
            _bias_value((tf_map.get("4h") or {}).get("bias")) * 0.45
            + _bias_value((tf_map.get("1d") or {}).get("bias")) * 0.40
            + _bias_value((tf_map.get("1h") or {}).get("bias")) * 0.15
        )
        / 2.0,
        -1.0,
        1.0,
    )

    range_position = _clamp(_safe_float(ticker_ctx.get("range_position"), 0.5), 0.0, 1.0)
    day_change_pct = _safe_float(ticker_ctx.get("day_change_pct"))
    extension_component = _clamp(((range_position - 0.5) * 1.15) + _clamp(day_change_pct / 3.0, -1.0, 1.0) * 0.85, -1.25, 1.25)

    book_component = _clamp(_safe_float(book_ctx.get("imbalance")) * 4.0, -1.0, 1.0)

    dominant_side = str(liq_map.get("dominant_side") or "").strip().lower()
    liquidity_component = 0.0
    if dominant_side == "longs_vulnerable":
        liquidity_component = 0.95
    elif dominant_side == "shorts_vulnerable":
        liquidity_component = -0.95
    else:
        total_above = sum(_safe_float(row.get("size_usd")) for row in (multi_ctx.get("liquidity_above") or []))
        total_below = sum(_safe_float(row.get("size_usd")) for row in (multi_ctx.get("liquidity_below") or []))
        if total_below > total_above * 1.25 and total_below > 0:
            liquidity_component = 0.45
        elif total_above > total_below * 1.25 and total_above > 0:
            liquidity_component = -0.45

    oi_values = [abs(_safe_float(v)) for v in (multi_ctx.get("oi") or {}).values() if abs(_safe_float(v)) > 0]
    oi_level_component = 0.0
    if oi_values and _safe_float(current_price) > 0:
        oi_level_component = _clamp((sum(oi_values) / len(oi_values)) / max(_safe_float(current_price) * 300.0, 1.0), 0.0, 1.0)
    oi_change_component = _clamp(_safe_float(oi_change_pct) / 0.08, -1.0, 1.0)
    oi_direction_component = funding_component * max(0.0, oi_change_component)

    raw = (
        funding_component * 0.35
        + funding_trend_component * 0.15
        + oi_direction_component * 0.15
        + extension_component * 0.15
        + liquidity_component * 0.10
        + book_component * 0.05
        + trend_component * 0.05
    )
    amplitude = 1.0 + min(0.22, oi_level_component * 0.22)
    raw *= amplitude
    raw = _clamp(raw, -1.5, 1.5)

    cot_index = _clamp(50.0 + (raw * 30.0), 0.0, 100.0)
    if cot_index >= 62.0:
        bias = "longs_crowded"
    elif cot_index <= 38.0:
        bias = "shorts_crowded"
    else:
        bias = "neutral"
    extreme = cot_index >= 75.0 or cot_index <= 25.0
    confidence = _clamp(
        46.0
        + abs(raw) * 28.0
        + oi_level_component * 8.0
        + abs(funding_trend_component) * 6.0
        + (6.0 if extreme else 0.0),
        35.0,
        92.0,
    )

    if bias == "longs_crowded":
        summary = f"Longs crowded ({cot_index:.0f}/100)"
    elif bias == "shorts_crowded":
        summary = f"Shorts crowded ({cot_index:.0f}/100)"
    else:
        summary = f"Neutral positioning ({cot_index:.0f}/100)"

    return {
        "cot_index": round(cot_index, 2),
        "bias": bias,
        "extreme": bool(extreme),
        "confidence": round(confidence, 2),
        "long_crowded_score": round(max(0.0, cot_index - 50.0) * 2.0, 2),
        "short_crowded_score": round(max(0.0, 50.0 - cot_index) * 2.0, 2),
        "summary": summary,
        "components": {
            "funding": round(funding_component, 3),
            "funding_trend": round(funding_trend_component, 3),
            "oi_direction": round(oi_direction_component, 3),
            "extension": round(extension_component, 3),
            "liquidity": round(liquidity_component, 3),
            "book": round(book_component, 3),
            "trend": round(trend_component, 3),
            "oi_level": round(oi_level_component, 3),
        },
    }
