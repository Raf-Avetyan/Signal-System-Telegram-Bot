from liquidity_map import detect_liquidity_candidates


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _clamp(value, low, high):
    return max(float(low), min(float(high), float(value)))


def _pct(current, reference):
    current_val = _safe_float(current)
    reference_val = _safe_float(reference)
    if reference_val == 0:
        return 0.0
    return ((current_val / reference_val) - 1.0) * 100.0


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


def _distance_pct(level_price, current_price):
    level_price = _safe_float(level_price)
    current_price = _safe_float(current_price)
    if current_price <= 0:
        return 0.0
    return abs(level_price - current_price) / current_price * 100.0


def _summarize_zone(zone):
    if not zone:
        return "n/a"
    labels = ", ".join((zone.get("labels") or [])[:3])
    if labels:
        return f"{_safe_float(zone.get('price')):,.2f} ({labels})"
    return f"{_safe_float(zone.get('price')):,.2f}"


def _positioning_model(current_price, tf_map, funding_ctx, ticker_ctx, book_ctx, okx_ctx):
    funding_rate = _safe_float((funding_ctx or {}).get("current_rate"))
    funding_trend = _safe_float((funding_ctx or {}).get("trend"))
    range_position = _safe_float((ticker_ctx or {}).get("range_position"), 0.5)
    day_change_pct = _safe_float((ticker_ctx or {}).get("day_change_pct"))
    imbalance = _safe_float((book_ctx or {}).get("imbalance"))
    oi_value = _safe_float((okx_ctx or {}).get("oi"))
    okx_funding = _safe_float((okx_ctx or {}).get("funding_rate"))

    trend_score = (
        _bias_value((tf_map.get("4h") or {}).get("bias")) * 1.8
        + _bias_value((tf_map.get("1h") or {}).get("bias")) * 1.0
        + _bias_value((tf_map.get("1d") or {}).get("bias")) * 1.4
    )
    range_score = (range_position - 0.5) * 2.0
    day_score = _clamp(day_change_pct / 2.5, -1.5, 1.5)
    funding_score = _clamp((funding_rate + okx_funding) * 10000.0, -2.0, 2.0)
    funding_trend_score = _clamp(funding_trend * 50000.0, -1.0, 1.0)
    book_score = _clamp(imbalance * 5.0, -1.5, 1.5)
    oi_score = 0.0
    if current_price > 0:
        oi_score = _clamp(oi_value / max(current_price * 250.0, 1.0), 0.0, 1.0)

    crowded_longs = max(0.0, funding_score) + max(0.0, funding_trend_score) + max(0.0, range_score) + max(0.0, day_score) + max(0.0, book_score)
    crowded_shorts = max(0.0, -funding_score) + max(0.0, -funding_trend_score) + max(0.0, -range_score) + max(0.0, -day_score) + max(0.0, -book_score)

    if trend_score > 1.5:
        crowded_longs += 0.5
    elif trend_score < -1.5:
        crowded_shorts += 0.5

    crowded_longs += oi_score * 0.25
    crowded_shorts += oi_score * 0.25

    diff = crowded_shorts - crowded_longs
    if diff >= 0.45:
        dominant_side = "shorts_vulnerable"
    elif diff <= -0.45:
        dominant_side = "longs_vulnerable"
    else:
        dominant_side = "balanced"

    confidence = _clamp(52.0 + abs(diff) * 12.0 + abs(trend_score) * 3.0, 50.0, 86.0)
    return {
        "trend_score": trend_score,
        "crowded_longs": crowded_longs,
        "crowded_shorts": crowded_shorts,
        "dominant_side": dominant_side,
        "confidence": confidence,
    }


def _structural_candidates(current_price, levels, tf_map):
    raw_levels = [
        ("DO", levels.get("DO"), 0.8),
        ("WO", levels.get("WO"), 1.5),
        ("MO", levels.get("MO"), 1.8),
        ("PDH", levels.get("PDH"), 1.1),
        ("PDL", levels.get("PDL"), 1.1),
        ("PWH", levels.get("PWH"), 1.8),
        ("PWL", levels.get("PWL"), 1.8),
        ("PMH", levels.get("PMH"), 2.1),
        ("PML", levels.get("PML"), 2.1),
        ("Pump", levels.get("Pump"), 1.0),
        ("Dump", levels.get("Dump"), 1.0),
        ("PumpMax", levels.get("PumpMax"), 1.6),
        ("DumpMax", levels.get("DumpMax"), 1.6),
        ("1H EMA21", (tf_map.get("1h") or {}).get("ema2"), 0.8),
        ("1H EMA55", (tf_map.get("1h") or {}).get("ema3"), 0.9),
        ("4H EMA21", (tf_map.get("4h") or {}).get("ema2"), 1.2),
        ("4H EMA55", (tf_map.get("4h") or {}).get("ema3"), 1.4),
        ("1H swing high", (tf_map.get("1h") or {}).get("range_high"), 1.0),
        ("1H swing low", (tf_map.get("1h") or {}).get("range_low"), 1.0),
        ("4H swing high", (tf_map.get("4h") or {}).get("range_high"), 1.5),
        ("4H swing low", (tf_map.get("4h") or {}).get("range_low"), 1.5),
        ("1D swing high", (tf_map.get("1d") or {}).get("range_high"), 2.0),
        ("1D swing low", (tf_map.get("1d") or {}).get("range_low"), 2.0),
    ]
    out = []
    for label, price, weight in raw_levels:
        price_val = _safe_float(price)
        if price_val <= 0 or current_price <= 0 or price_val == current_price:
            continue
        zone_side = "short_liq" if price_val > current_price else "long_liq"
        out.append(
            {
                "zone_side": zone_side,
                "price": price_val,
                "size_usd": weight * 6_000_000.0,
                "score": weight * 7.0,
                "weight": weight,
                "distance_pct": _distance_pct(price_val, current_price),
                "labels": [label],
                "sources": ["structure"],
                "timeframes": ["htf"],
            }
        )
    return out


def _book_candidates(order_book, current_price, atr_1h, source_name, source_weight):
    if not order_book or current_price <= 0:
        return []
    rows = detect_liquidity_candidates(
        order_book=order_book,
        price=current_price,
        atr=max(_safe_float(atr_1h), current_price * 0.0030),
        timeframe=source_name,
        max_distance_atr_mult=15.0,
        bucket_pct=0.16,
    )
    out = []
    for row in rows:
        distance_pct = _safe_float(row.get("distance_pct"))
        size_usd = _safe_float(row.get("size_usd"))
        if distance_pct < 0.80 or size_usd < 12_000_000:
            continue
        zone_side = "short_liq" if str(row.get("side")) == "LONG" else "long_liq"
        out.append(
            {
                "zone_side": zone_side,
                "price": _safe_float(row.get("level_price")),
                "size_usd": size_usd,
                "score": (size_usd / 1_000_000.0) * source_weight,
                "weight": source_weight,
                "distance_pct": distance_pct,
                "labels": [source_name.upper()],
                "sources": [source_name],
                "timeframes": [source_name],
            }
        )
    return out


def _realized_liquidation_candidates(liquidation_orders, current_price):
    buckets = {}
    for row in liquidation_orders or []:
        price = _safe_float(row.get("price"))
        size_usd = _safe_float(row.get("usd_value"))
        side = str(row.get("side") or "").strip().lower()
        if price <= 0 or size_usd <= 0 or current_price <= 0:
            continue
        zone_side = "short_liq" if side == "short" else "long_liq"
        distance_pct = _distance_pct(price, current_price)
        if distance_pct < 0.80:
            continue
        bucket = (zone_side, int(price / max(current_price * 0.0035, 1.0)))
        if bucket not in buckets:
            buckets[bucket] = {
                "zone_side": zone_side,
                "px_sz": 0.0,
                "size_usd": 0.0,
                "labels": ["OKX filled liq"],
                "sources": ["okx_liquidations"],
                "timeframes": ["recent"],
            }
        buckets[bucket]["px_sz"] += price * size_usd
        buckets[bucket]["size_usd"] += size_usd

    out = []
    for row in buckets.values():
        size_usd = _safe_float(row.get("size_usd"))
        if size_usd < 8_000_000:
            continue
        price = row["px_sz"] / max(size_usd, 1e-9)
        out.append(
            {
                "zone_side": row["zone_side"],
                "price": price,
                "size_usd": size_usd,
                "score": size_usd / 850_000.0,
                "weight": 2.0,
                "distance_pct": _distance_pct(price, current_price),
                "labels": row["labels"],
                "sources": row["sources"],
                "timeframes": row["timeframes"],
            }
        )
    return out


def _synthetic_candidates(structural_candidates, positioning, oi_value):
    leverage_weights = [(5, 0.35), (10, 0.75), (15, 1.0), (20, 0.85)]
    crowded_longs = _safe_float(positioning.get("crowded_longs"))
    crowded_shorts = _safe_float(positioning.get("crowded_shorts"))
    out = []
    for row in structural_candidates:
        zone_side = row["zone_side"]
        crowd_boost = crowded_shorts if zone_side == "short_liq" else crowded_longs
        leverage_score = sum(weight for _, weight in leverage_weights)
        synthetic_size = max(7_500_000.0, oi_value * (0.012 + crowd_boost * 0.0025) * max(row["weight"], 0.8))
        out.append(
            {
                "zone_side": zone_side,
                "price": row["price"],
                "size_usd": synthetic_size,
                "score": row["weight"] * (4.5 + crowd_boost) * leverage_score,
                "weight": row["weight"] * 0.9,
                "distance_pct": row["distance_pct"],
                "labels": list(row["labels"]),
                "sources": ["synthetic"],
                "timeframes": ["multi"],
            }
        )
    return out


def _merge_zones(rows, current_price):
    grouped = {"short_liq": [], "long_liq": []}
    for row in rows:
        side = row.get("zone_side")
        if side not in grouped:
            continue
        placed = False
        for cluster in grouped[side]:
            if _pct(row.get("price"), cluster.get("price")) <= 0.35 and _pct(cluster.get("price"), row.get("price")) <= 0.35:
                cluster["px_sz"] += _safe_float(row.get("price")) * max(_safe_float(row.get("size_usd")), 1.0)
                cluster["size_usd"] += _safe_float(row.get("size_usd"))
                cluster["score"] += _safe_float(row.get("score"))
                cluster["weight"] += _safe_float(row.get("weight"))
                cluster["labels"].extend(row.get("labels") or [])
                cluster["sources"].extend(row.get("sources") or [])
                cluster["timeframes"].extend(row.get("timeframes") or [])
                placed = True
                break
        if not placed:
            grouped[side].append(
                {
                    "zone_side": side,
                    "px_sz": _safe_float(row.get("price")) * max(_safe_float(row.get("size_usd")), 1.0),
                    "size_usd": _safe_float(row.get("size_usd")),
                    "score": _safe_float(row.get("score")),
                    "weight": _safe_float(row.get("weight")),
                    "labels": list(row.get("labels") or []),
                    "sources": list(row.get("sources") or []),
                    "timeframes": list(row.get("timeframes") or []),
                }
            )

    merged = {"short_liq": [], "long_liq": []}
    for side, clusters in grouped.items():
        for cluster in clusters:
            size_usd = max(_safe_float(cluster.get("size_usd")), 1.0)
            price = cluster["px_sz"] / size_usd
            labels = []
            for label in cluster["labels"]:
                label_text = str(label or "").strip()
                if label_text and label_text not in labels:
                    labels.append(label_text)
            sources = []
            for source in cluster["sources"]:
                source_text = str(source or "").strip()
                if source_text and source_text not in sources:
                    sources.append(source_text)
            item = {
                "zone_side": side,
                "price": price,
                "size_usd": size_usd,
                "score": _safe_float(cluster.get("score")),
                "weight": _safe_float(cluster.get("weight")),
                "distance_pct": _distance_pct(price, current_price),
                "labels": labels[:4],
                "sources": sources[:4],
            }
            dist = item["distance_pct"]
            if dist >= 2.75:
                item["bucket"] = "major"
            elif dist >= 0.80:
                item["bucket"] = "mid"
            else:
                item["bucket"] = "near"
            merged[side].append(item)
        merged[side].sort(key=lambda row: (row.get("bucket") == "major", row.get("score", 0.0), row.get("size_usd", 0.0)), reverse=True)
    return merged


def _best_zone(rows, preferred_buckets, preferred_dist_pct):
    eligible = [row for row in rows if row.get("bucket") in preferred_buckets]
    if not eligible:
        eligible = list(rows)
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda row: (
            -abs(_safe_float(row.get("distance_pct")) - preferred_dist_pct),
            _safe_float(row.get("score")),
            _safe_float(row.get("size_usd")),
        ),
    )


def build_liquidation_map(current_price, levels, tf_map, funding_ctx, ticker_ctx, book_ctx, liq_ctx, okx_ctx):
    tf_1h = tf_map.get("1h") or {}
    atr_1h = max(_safe_float(tf_1h.get("atr")), _safe_float(current_price) * 0.0035)
    positioning = _positioning_model(current_price, tf_map, funding_ctx, ticker_ctx, book_ctx, okx_ctx)
    structural = _structural_candidates(current_price, levels, tf_map)
    okx_oi = _safe_float((okx_ctx or {}).get("oi"))

    all_rows = []
    all_rows.extend(structural)
    all_rows.extend(_synthetic_candidates(structural, positioning, okx_oi))
    all_rows.extend(_book_candidates(liq_ctx.get("raw_book") if liq_ctx else None, current_price, atr_1h, "bitunix", 1.0))
    all_rows.extend(_book_candidates((okx_ctx or {}).get("order_book"), current_price, atr_1h, "okx", 1.25))
    all_rows.extend(_realized_liquidation_candidates((okx_ctx or {}).get("liquidation_orders"), current_price))

    merged = _merge_zones(all_rows, current_price)
    short_liq_zones = merged["short_liq"]
    long_liq_zones = merged["long_liq"]

    short_mid = [row for row in short_liq_zones if row.get("bucket") == "mid"]
    short_major = [row for row in short_liq_zones if row.get("bucket") == "major"]
    long_mid = [row for row in long_liq_zones if row.get("bucket") == "mid"]
    long_major = [row for row in long_liq_zones if row.get("bucket") == "major"]

    long_entry_zone = _best_zone(long_mid + long_major, {"mid", "major"}, 1.35)
    short_entry_zone = _best_zone(short_mid + short_major, {"mid", "major"}, 1.35)
    long_target_zone = _best_zone(short_major + short_mid, {"major", "mid"}, 3.0)
    short_target_zone = _best_zone(long_major + long_mid, {"major", "mid"}, 3.0)

    return {
        "positioning": positioning,
        "dominant_side": positioning.get("dominant_side"),
        "confidence": positioning.get("confidence"),
        "all_long_liq_zones": long_liq_zones,
        "all_short_liq_zones": short_liq_zones,
        "long_liq_zones": long_liq_zones[:6],
        "short_liq_zones": short_liq_zones[:6],
        "long_entry_zone": long_entry_zone,
        "short_entry_zone": short_entry_zone,
        "long_target_zone": long_target_zone,
        "short_target_zone": short_target_zone,
        "summary": {
            "long_entry": _summarize_zone(long_entry_zone),
            "short_entry": _summarize_zone(short_entry_zone),
            "long_target": _summarize_zone(long_target_zone),
            "short_target": _summarize_zone(short_target_zone),
        },
    }
