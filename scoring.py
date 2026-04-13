# ─── Ponch Signal Scoring Engine ──────────────────────────────

import pandas as pd
import numpy as np
from config import SYMBOL

def _normalize_score(raw_score: float, raw_min: float = -2.0, raw_max: float = 12.0) -> int:
    """Map a sparse raw confluence score into a more human-readable 1-10 scale."""
    clipped = max(raw_min, min(raw_max, float(raw_score)))
    scaled = 1.0 + ((clipped - raw_min) / max(1e-9, (raw_max - raw_min))) * 9.0
    return int(max(1, min(10, round(scaled))))


def _reason_confluence_bonus(signal_data) -> tuple[int, list[str]]:
    """Count structural trigger/reason quality that the old score ignored."""
    bonus = 0
    bonus_labels = []
    trigger = str(signal_data.get("trigger_label") or signal_data.get("trigger") or "").strip().lower()
    strategy = str(signal_data.get("strategy") or "").strip().upper()
    reasons = [str(x).strip().lower() for x in (signal_data.get("reasons") or []) if str(x).strip()]

    if strategy == "SMART_MONEY_LIQUIDITY":
        bonus += 4
        bonus_labels.append("Smart Money Model")
    elif trigger == "smart money liquidity":
        bonus += 4
        bonus_labels.append("Smart Money Model")
    elif trigger in {"htf_pullback", "one_h_reclaim", "rsi pullback scalp"}:
        bonus += 3
        bonus_labels.append(trigger.replace("_", " ").title())
    elif trigger == "momentum exit":
        bonus += 1
        bonus_labels.append("Momentum Trigger")

    pattern_map = [
        (("liquidity", "sweep", "stop hunt"), "Liquidity Sweep"),
        (("fvg", "fair value gap"), "FVG"),
        (("order block", " ob", "ob "), "Order Block"),
        (("discount", "premium"), "Dealing Range"),
        (("displacement", "impulse"), "Displacement"),
        (("bos", "choch", "structure"), "Structure Shift"),
        (("divergence",), "RSI Divergence"),
        (("reclaim", "pullback"), "Reclaim/Pullback"),
    ]
    matched = []
    joined = " | ".join(reasons)
    for patterns, label in pattern_map:
        if any(p in joined for p in patterns):
            matched.append(label)
    matched = matched[:3]
    if matched:
        bonus += len(matched)
        bonus_labels.extend(matched)

    return bonus, bonus_labels


def calculate_signal_score(signal_data, df_tf, levels, trend, oi_data=None, liq_usd=0):
    """
    Calculate a 1-10 Signal Strength Score based on multiple confluences.
    
    Points:
    - Level Proximity: +3
    - Extreme Channel: +3
    - Momentum OB/OS: +2
    - Volume Spike:    +2
    - Liquidations:    +2 (Bonus for high liquidations)
    - Trend Alignment: +2
    
    Returns: (int score, list of reasons)
    """
    score = 0
    reasons = []
    
    price = signal_data.get("price") or signal_data.get("entry")
    side = signal_data.get("side")

    if price is None:
        return 0, []
    
    # 1. Level Proximity (+3)
    # Check if price is within 0.15% of any major daily level
    proximity_threshold = price * 0.0015
    major_levels = ["DO", "PDH", "PDL", "PWH", "PWL", "PMH", "PML", "Pump", "Dump"]
    
    hit_level = None
    for lvl_name in major_levels:
        lvl_val = levels.get(lvl_name, 0)
        if lvl_val > 0 and abs(price - lvl_val) <= proximity_threshold:
            hit_level = lvl_name
            break
            
    if hit_level:
        score += 3
        reasons.append(f"Near {hit_level}")

    # 2. Extreme Channel (+3)
    # Check if the signal came from L3/S3 or L2/S2 (Extreme/Outer)
    # The 'signal' type usually carries this info e.g. 'L3', 'S3'
    sig_type = signal_data.get("signal", "")
    if "3" in sig_type:
        score += 3
        reasons.append("Extreme Channel")
    elif "2" in sig_type:
        score += 2
        reasons.append("Outer Channel")

    # 3. Momentum OB/OS (+2)
    # Check current RSI from the timeframe DF
    if "RSI" in df_tf.columns:
        last_rsi = df_tf["RSI"].iloc[-1]
        if (side == "LONG" and last_rsi <= 35) or (side == "SHORT" and last_rsi >= 65):
            score += 2
            reasons.append("Momentum Overextended")

    # 4. Volume Spike (+2)
    # Use the logic from check_flow_confirm: 2x avg volume
    if "Volume" in df_tf.columns and len(df_tf) >= 10:
        curr_vol = df_tf["Volume"].iloc[-1]
        avg_vol = df_tf["Volume"].iloc[-11:-1].mean()
        if curr_vol >= avg_vol * 1.8:
            score += 2
            reasons.append("Volume Spike")

    # 5. Liquidations (+2)
    # If liquidations > $100k (configurable) in the last block
    if liq_usd > 100000:
        score += 2
        reasons.append(f"High Liquids (${liq_usd/1000:,.0f}k)")
    elif liq_usd > 50000:
        score += 1
        reasons.append("Moderate Liquids")

    # 6. Trend Alignment (+2)
    # Match the signal side with the macro trend
    is_aligned = False
    if side == "LONG" and trend == "Trending Bullish":
        is_aligned = True
    elif side == "SHORT" and trend == "Trending Bearish":
        is_aligned = True
        
    if is_aligned:
        score += 2
        reasons.append("Trend Aligned")
    elif trend == "Ranging":
        # Neutral trend is fine
        pass
    else:
        # Counter-trend signal
        score -= 2
        reasons.append("Counter-trend")

    # 7. Structural / trigger confluence bonuses
    extra_bonus, extra_labels = _reason_confluence_bonus(signal_data)
    if extra_bonus:
        score += extra_bonus
        for label in extra_labels:
            if label not in reasons:
                reasons.append(label)

    # Final score processing
    final_score = _normalize_score(score)
    
    return final_score, reasons
