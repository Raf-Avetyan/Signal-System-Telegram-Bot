from typing import Dict, List, Optional


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def detect_liquidity_event(
    order_book: Dict[str, List[List[float]]],
    price: float,
    atr: float,
    timeframe: str,
    min_usd: float,
    max_distance_atr_mult: float,
    min_distance_pct: float = 0.0,
    huge_usd_override: float = 0.0,
) -> Optional[Dict]:
    """
    Detect largest nearby liquidity pool on each side and return strongest event.
    side=LONG means sweep target is above current price (ask wall).
    side=SHORT means sweep target is below current price (bid wall).
    """
    if not order_book or price <= 0:
        return None
    horizon = max(atr * max_distance_atr_mult, price * 0.002)

    best = None
    for side_name, side_dir in (("asks", "LONG"), ("bids", "SHORT")):
        rows = order_book.get(side_name, [])
        for px, sz in rows:
            dist = abs(px - price)
            if dist > horizon:
                continue
            usd = px * sz
            if usd < min_usd:
                continue
            distance_pct = (dist / price) * 100
            min_dist = max(0.0, float(min_distance_pct))
            huge_override = max(0.0, float(huge_usd_override))
            if distance_pct < min_dist:
                if not (huge_override > 0 and usd >= huge_override):
                    continue

            dist_score = 1.0 - (dist / horizon)
            denom = max(float(min_usd) * 4.0, 1.0)
            size_score = _clamp(usd / denom, 0.0, 1.0)
            probability = _clamp(15 + dist_score * 65 + size_score * 20, 5, 95)
            score = probability * usd

            event = {
                "timeframe": timeframe,
                "side": side_dir,
                "level_price": px,
                "size_usd": usd,
                "distance_pct": distance_pct,
                "probability_pct": probability,
                "score": score,
            }
            if best is None or event["score"] > best["score"]:
                best = event

    return best


def detect_liquidity_candidates(
    order_book: Dict[str, List[List[float]]],
    price: float,
    atr: float,
    timeframe: str,
    max_distance_atr_mult: float,
    bucket_pct: float = 0.0,
) -> List[Dict]:
    """
    Return all visible liquidity candidates in horizon, sorted by size desc.
    side=LONG -> ask wall above price; side=SHORT -> bid wall below price.
    """
    if not order_book or price <= 0:
        return []
    horizon = max(atr * max_distance_atr_mult, price * 0.002)

    out = []
    for side_name, side_dir in (("asks", "LONG"), ("bids", "SHORT")):
        rows = order_book.get(side_name, [])
        if bucket_pct > 0:
            bucket_size = max(price * (bucket_pct / 100.0), 1.0)
            bins = {}
            for px, sz in rows:
                dist = abs(px - price)
                if dist > horizon:
                    continue
                usd = px * sz
                bucket = int(px / bucket_size)
                if bucket not in bins:
                    bins[bucket] = {"usd": 0.0, "sz": 0.0, "px_sz": 0.0}
                bins[bucket]["usd"] += usd
                bins[bucket]["sz"] += sz
                bins[bucket]["px_sz"] += px * sz
            for b in bins.values():
                if b["sz"] <= 0:
                    continue
                level_px = b["px_sz"] / b["sz"]
                dist = abs(level_px - price)
                distance_pct = (dist / price) * 100
                out.append({
                    "timeframe": timeframe,
                    "side": side_dir,
                    "level_price": level_px,
                    "size_usd": b["usd"],
                    "distance_pct": distance_pct,
                    "probability_pct": 50.0,
                    "score": b["usd"],
                })
        else:
            for px, sz in rows:
                dist = abs(px - price)
                if dist > horizon:
                    continue
                usd = px * sz
                distance_pct = (dist / price) * 100
                out.append({
                    "timeframe": timeframe,
                    "side": side_dir,
                    "level_price": px,
                    "size_usd": usd,
                    "distance_pct": distance_pct,
                    "probability_pct": 50.0,
                    "score": usd,
                })

    out.sort(key=lambda x: x["size_usd"], reverse=True)
    return out
