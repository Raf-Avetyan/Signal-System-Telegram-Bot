# test_messages.py
import telegram as tg

import telegram as tg
from data import fetch_klines
from charting import generate_daily_levels_chart

# print("Fetching dummy chart data for test...")
# df = fetch_klines(interval="1h", limit=48)
# levels_dict = {
#     "Pump": 72000, "ResistancePct": 1.4,
#     "Dump": 70500, "SupportPct": -0.7,
#     "Volatility": 73000, "VolatilityPct": 2.8,
#     "PumpMax": 74000, "DumpMax": 68000,
#     "DO": 71000
# }
# chart_path = generate_daily_levels_chart(df, levels_dict)

# print("Sending Daily Levels with Chart...")
# tg.send_daily_levels(
#     "15.03.2024", 71000.00, 
#     72000.00, 1.4, 70500.00, -0.7, 
#     73000.00, 2.8, 74000.00, 68000.00,
#     chart_path=chart_path
# )

# print("Sending Liquidity Sweep...")
# tg.send_liquidity_sweep("LONG", "PDL", 70500.00, 2, "Medium", "Swept yesterday's low")

# print("Sending Volatility Touch...")
# tg.send_volatility_touch("SHORT", "Pump", 72500.00, 1, "Low", "Touched main pump zone")

# print("Sending Performance Summary...")
# tg.send_performance_summary({
#     "total": 10,
#     "tp1_hits": 6,
#     "tp2_hits": 3,
#     "tp3_hits": 1,
#     "sl_hits": 2,
#     "still_open": 2,
#     "win_rate": 60.0
# })

# print("Sending Price Approaching Level...")
# tg.send_approaching_level("Pump", 72500.00, 72400.00, 0.13)

# print("Sending Funding Alert...")
# tg.send_funding_alert(0.00065, "POSITIVE")

# print("Sending Volume Spike...")
# tg.send_volume_spike("1h", 50000, 15000, 3.3, 71200.00)

# print("Sending Session Summary...")
# tg.send_session_summary("LONDON", 70500.00, 71200.00, 4, "PDH, Pump")

# print("Sending Batched Alerts...")
# tg.send_batched_alerts([
#     {"type": "VOLUME SPIKE", "tf": "15m", "price": 71200.00, "note": "3.3x average volume"},
#     {"type": "FUNDING ALERT", "note": "POSITIVE Funding Rate: 0.0650%"}
# ])

# # print("Test messages sent to Telegram!")

# print("1. Sending Daily Levels with Global Context...")
# indicators = fetch_global_indicators() # Get real live proxy values
# # Mock some data for the chart
# try:
#     df = fetch_klines(interval="1h", limit=48)
#     levels_dict = {
#         "Pump": 72000, "ResistancePct": 1.4,
#         "Dump": 70500, "SupportPct": -0.7,
#         "Volatility": 71200, "VolatilityPct": 0.5,
#         "PumpMax": 74000, "DumpMax": 68000,
#         "DO": 71000
#     }
#     chart_path = generate_daily_levels_chart(df, levels_dict)
# except:
#     chart_path = None

# tg.send_daily_levels(
#     "15.03.2024", 71000.00, 
#     72000.00, 1.4, 70500.00, -0.7, 
#     71200.00, 0.5, 74000.00, 68000.00,
#     indicators=indicators,
#     chart_path=chart_path
# )

print("Sending Scalp Open...")
tg.send_scalp_open("15m", "LONG", 70800.00, emoji="🚀")

print("Sending Scalp Prepare...")
tg.send_scalp_prepare("15m", "LONG", points=3, strength="Strong", emoji="🚀")

print("2. Sending Extreme Scalp Signal (Calculated Score)...")
# Mocking an 'Extreme' 9/10 signal
tg.send_scalp_confirmed(
    timeframe="15m", 
    side="LONG", 
    entry=70850.00, 
    sl=70600.00, 
    tp1=70950.00, tp2=71200.00, tp3=71500.00,
    strength="Extreme", 
    size=2.0, 
    score=9, 
    trend="Trending Bullish", 
    reasons=["Near DO", "Extreme Channel", "Volume Spike", "Trend Aligned", "High Liquidations ($150k)"],
    emoji="🚀"
)

print("3. Sending Counter-Trend Scalp Signal (Low Score)...")
# Mocking a weak signal
tg.send_scalp_confirmed(
    timeframe="5m", 
    side="SHORT", 
    entry=71500.00, 
    sl=71800.00, 
    tp1=71300.00, tp2=71100.00, tp3=70800.00,
    strength="Weak", 
    size=1.0, 
    score=3, 
    trend="Trending Bullish", 
    reasons=["Outer Channel", "Counter-trend"],
    emoji="⚡️"
)

print("Sending Scalp Closed...")
tg.send_scalp_closed("15m", "LONG", 71000.00, emoji="🚀")

# print("\n[OK] Test messages sent to Telegram! Check your bot.")
