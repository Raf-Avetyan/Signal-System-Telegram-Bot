# test_messages.py
import time
import telegram as tg
from config import PRIVATE_CHAT_ID

def test_live_updates():
    print("🚀 Starting Live Message Update Test...")
    
    # 1. TEST SCALP SIGNAL (Success Path: TP1 -> TP2 -> TP3)
    print("\n1. Testing Scalp Signal (TP1 -> TP2 -> TP3)...")
    entry, sl = 73500.0, 73000.0
    tp1, tp2, tp3 = 73800.0, 74200.0, 74800.0
    
    resp = tg.send_scalp_confirmed(
        timeframe="15m", side="LONG",
        entry=entry, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
        strength="Strong", size=1.0, score=8, trend="Bullish",
        reasons=["RSI Entry", "Near DO"],
        chat_id=PRIVATE_CHAT_ID
    )
    
    if not resp:
        print("❌ Failed to send initial scalp message.")
        return

    msg_id = resp["result"]["message_id"]
    sig_data = {
        "type": "SCALP", "side": "LONG", "tf": "15m",
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "status": "OPEN", "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "sl_hit": False,
        "meta": {"score": 8, "trend": "Bullish", "reasons": ["RSI Entry", "Near DO"]}
    }

    time.sleep(3)
    print("   -> Simulating TP1 Hit...")
    sig_data["tp1_hit"] = True
    sig_data["status"] = "TP1"
    tg.update_signal_message(PRIVATE_CHAT_ID, msg_id, sig_data)

    time.sleep(3)
    print("   -> Simulating TP2 Hit...")
    sig_data["tp2_hit"] = True
    sig_data["status"] = "TP2"
    tg.update_signal_message(PRIVATE_CHAT_ID, msg_id, sig_data)

    time.sleep(3)
    print("   -> Simulating TP3 Hit (ALL TARGETS)...")
    sig_data["tp3_hit"] = True
    sig_data["status"] = "TP3"
    tg.update_signal_message(PRIVATE_CHAT_ID, msg_id, sig_data)


    # 2. TEST STRONG CONFLUENCE (Failure Path: SL Hit)
    print("\n2. Testing Strong Confluence (SL Hit)...")
    entry_s, sl_s = 74500.0, 75000.0
    tp1_s, tp2_s, tp3_s = 74000.0, 73500.0, 72500.0
    indicators = [
        {"name": "Ponch_Trader", "signal": "S+", "points": 2, "tf": "1h"},
        {"name": "Ponch_Momentum_Confirm", "signal": "S", "points": 1, "tf": "15m"}
    ]

    resp_s = tg.send_strong(
        side="SHORT", total_points=3, confirmations=2,
        indicators_list=indicators,
        price=entry_s, sl=sl_s, tp1=tp1_s, tp2=tp2_s, tp3=tp3_s,
        chat_id=PRIVATE_CHAT_ID
    )
    
    if resp_s:
        msg_id_s = resp_s["result"]["message_id"]
        sig_data_s = {
            "type": "STRONG", "side": "SHORT", "tf": "1h, 15m",
            "entry": entry_s, "sl": sl_s, "tp1": tp1_s, "tp2": tp2_s, "tp3": tp3_s,
            "status": "OPEN", "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "sl_hit": False,
            "meta": {"indicators": indicators}
        }
        
        time.sleep(3)
        print("   -> Simulating SL Hit...")
        sig_data_s["sl_hit"] = True
        sig_data_s["status"] = "SL"
        tg.update_signal_message(PRIVATE_CHAT_ID, msg_id_s, sig_data_s)


    # 3. TEST EXTREME CONFLUENCE (Protected Path: TP1 -> SL)
    print("\n3. Testing Extreme Confluence (TP1 then SL)...")
    indicators_e = [
        {"name": "Ponch_Trader", "signal": "L++", "points": 3, "tf": "4h"},
        {"name": "Ponch_Flow_Confirm", "signal": "L", "points": 1, "tf": "5m"},
        {"name": "Ponch_Range_Confirm", "signal": "L+", "points": 2, "tf": "1h"}
    ]
    
    resp_e = tg.send_extreme(
        side="LONG", total_points=6, confirmations=3,
        indicators_list=indicators_e,
        price=72400.0, sl=72000.0, tp1=72800.0, tp2=73500.0, tp3=74500.0,
        chat_id=PRIVATE_CHAT_ID
    )
    
    if resp_e:
        msg_id_e = resp_e["result"]["message_id"]
        sig_data_e = {
            "type": "EXTREME", "side": "LONG", "tf": "5m, 1h, 4h",
            "entry": 72400.0, "sl": 72000.0, "tp1": 72800.0, "tp2": 73500.0, "tp3": 74500.0,
            "status": "OPEN", "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "sl_hit": False,
            "meta": {"indicators": indicators_e}
        }
        
        time.sleep(3)
        print("   -> Simulating TP1 Hit...")
        sig_data_e["tp1_hit"] = True
        sig_data_e["status"] = "TP1"
        tg.update_signal_message(PRIVATE_CHAT_ID, msg_id_e, sig_data_e)
        
        time.sleep(3)
        print("   -> Simulating Price returning to SL after TP1 (CLOSED)...")
        sig_data_e["status"] = "CLOSED" # Logic from tracker: TP1 hit then SL = CLOSED
        tg.update_signal_message(PRIVATE_CHAT_ID, msg_id_e, sig_data_e)

    print("\n✅ Live Update Tests Finished! Check Telegram.")

if __name__ == "__main__":
    test_live_updates()
