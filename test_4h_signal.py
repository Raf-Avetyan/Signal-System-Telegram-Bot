# test_4h_signal.py
import telegram as tg
from config import TIMEFRAME_PROFILES

def test_4h():
    print("Testing 4h Signal Profile...")
    profile = TIMEFRAME_PROFILES.get("4h", {})
    strength = profile.get("strength", "Unknown")
    emoji = profile.get("emoji", "❓")
    size = profile.get("size", 1.0)
    
    print(f"Profile: Strength={strength}, Emoji={emoji}, Size={size}")
    
    # Send a mock confirmation for 4h
    tg.send_scalp_confirmed(
        timeframe="4h",
        side="LONG",
        entry=70000.00,
        sl=68000.00,
        tp1=72000.00, tp2=75000.00, tp3=80000.00,
        strength=strength,
        size=size,
        score=10,
        trend="Bullish",
        reasons=["Higher Timeframe Swing", "💎 Double Bottom", "Major Support"],
        emoji=emoji
    )
    print("Test signal sent to Telegram!")

if __name__ == "__main__":
    test_4h()
