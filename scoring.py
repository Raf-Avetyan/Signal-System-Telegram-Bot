# ─── Ponch Signal Scoring Engine ──────────────────────────────

import pandas as pd
import numpy as np
from config import SYMBOL

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
    
    price = signal_data.get("price")
    side = signal_data.get("side")
    
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

    # Final score processing
    # Cap at 10, min at 1
    final_score = max(1, min(10, score))
    
    return final_score, reasons
