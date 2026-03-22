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
            if distance_pct < max(0.0, float(min_distance_pct)) and usd < max(0.0, float(huge_usd_override)):
                continue

            dist_score = 1.0 - (dist / horizon)
            size_score = _clamp(usd / (min_usd * 4.0), 0.0, 1.0)
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
