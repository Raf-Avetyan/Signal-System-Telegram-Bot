# ─── Ponch Signal System — Main Bot ───────────────────────────

"""
Main entry point. Monitors BTCUSDT across multiple timeframes,
detects signals, and sends formatted Telegram alerts.
"""

import time
import traceback
from datetime import datetime, timezone, timedelta

from config import (
    SYMBOL, SIGNAL_TIMEFRAMES, POLL_INTERVAL,
    TIMEFRAME_PROFILES, FUNDING_THRESHOLD, FUNDING_CHECK_INTERVAL,
    FUNDING_COOLDOWN, VOLUME_SPIKE_MULT, VOLUME_SPIKE_TIMEFRAMES,
    VOLUME_AVG_PERIOD, APPROACH_THRESHOLD, APPROACH_COOLDOWN,
    APPROACH_LEVELS, SESSIONS, get_adjusted_sessions, ALERT_BATCH_WINDOW,
    OI_CHANGE_THRESHOLD, LIQ_SQUEEZE_THRESHOLD, LIQ_ALERT_COOLDOWN, PRIVATE_CHAT_ID,
    FAST_MOVE_THRESHOLD, FAST_MOVE_WINDOW, FAST_MOVE_COOLDOWN
)
from levels import calculate_levels, check_liquidity_sweep, check_volatility_touch
from channels import calculate_channels, check_channel_signals
from momentum import calculate_momentum, ScalpTracker, detect_trend
from scoring import calculate_signal_score
from signals import check_momentum_confirm, check_range_confirm, check_flow_confirm
from confirmation import ConfirmationTracker
from charting import generate_daily_levels_chart
from data import (
    fetch_klines, fetch_all_timeframes, fetch_daily, fetch_weekly, fetch_monthly, 
    fetch_funding_rate, fetch_open_interest, fetch_liquidations, fetch_global_indicators
)
from tracker import SignalTracker
import telegram as tg


class PonchBot:
    """Main Ponch Signal System bot."""

    def __init__(self):
        # Scalp trackers — one per timeframe
        self.scalp_trackers = {
            tf: ScalpTracker(tf) for tf in SIGNAL_TIMEFRAMES
        }

        # Confirmation aggregation
        self.confirmations = ConfirmationTracker()

        # Previous candle data for cross detection
        self.prev_candles = {}   # {timeframe: {"High": ..., "Low": ...}}

        # Track sent signals to avoid duplicates (restored from state below)

        # Daily levels
        self.levels = {}
        self.last_levels_date = None

        # ─── New Features ─────────────────────────────────
        self.tracker = SignalTracker()
        self.approach_alerts = {}      # { "Pump": timestamp }
        self.last_funding_check = 0
        self.last_funding_alert = 0
        self.sent_sessions = set()     # "session_LONDON_2023-10-14"
        self.session_data = {}         # { "LONDON_2024-03-15": {"open": 70000, "levels": set()} }
        self.session_history = {}      # { "ASIA": "Asia recap text" }
        self.state_file = "bot_state.json"
        
        state = self._load_state()
        self.daily_report_msg_id = state.get("daily_report_msg_id")
        self.session_msg_ids     = state.get("session_msg_ids", {})
        self.confirmations.from_dict(state.get("confirmations", {})) # Restore confirmation state
        self.session_data        = state.get("session_data", {})
        self.last_levels_date    = state.get("last_levels_date")
        self.sent_signals        = set(state.get("sent_signals", []))
        self.sent_sessions       = set(state.get("sent_sessions", [])) # New: Persist session summaries
        self.approach_alerts     = state.get("approach_alerts", {})    # New: Persist approaching level cooldowns
        self.last_funding_alert  = state.get("last_funding_alert", 0)   
        self.last_market_alert   = state.get("last_market_alert", 0)
        self.last_summary_date   = state.get("last_summary_date")      # New: Track summary schedule
        self.last_session_update = time.time()
        self.last_daily_update   = time.time()

        # Macro Trend & Context
        self.macro_trend = "Ranging"
        self.last_oi = 0
        self.last_liqs = 0
        self.last_oi_price = 0
        self.last_liq_alert_time = 0
        self.is_booting = True         # Start in quiet mode for first check

        # Alert Batching
        self.pending_alerts = []
        self.batch_timer_start = None

        # Mute state
        self.muted_until = None

    def queue_alert(self, alert_dict, callback=None, args=None, chat_id=None):
        """Queue alert for batching."""
        if self.muted_until and datetime.now(timezone.utc) < self.muted_until:
            return  # Suppress alerts if muted
        
        # Add to queue with callback info for individual send if needed
        self.pending_alerts.append({
            "data": alert_dict,
            "callback": callback,
            "args": args or (),
            "chat_id": chat_id
        })
        
        if self.batch_timer_start is None:
            self.batch_timer_start = time.time()


    def run(self):
        """Main loop — fetches data and processes signals."""

        print(f"{'='*50}")
        print(f"  Ponch Signal System (v2)")
        print(f"  Symbol: {SYMBOL}")
        print(f"  Timeframes: {', '.join(SIGNAL_TIMEFRAMES)}")
        print(f"  Poll interval: {POLL_INTERVAL}s")
        print(f"  Private Chat: {PRIVATE_CHAT_ID}")
        print(f"{'='*50}")

        tg.send_startup()
        print("[OK] Startup message sent to Telegram\n")

        # Smart initial data load: only update if needed/missing
        now = datetime.now(timezone.utc)
        today = now.strftime("%d.%m.%Y")
        
        if not self.levels or today != self.last_levels_date:
            print("[STARTUP] Levels missing or outdated. Updating...")
            self._update_levels()
            self.last_levels_date = today
            self._save_state()
        else:
            print(f"[STARTUP] Levels for {today} already loaded. Skipping recalculation.")

        while True:
            try:
                self._tick()
            except Exception as e:
                print(f"[ERROR] {e}")
                traceback.print_exc()
            
            # Pulse the lock file to stay alive
            if hasattr(self, 'heartbeat_callback'):
                self.heartbeat_callback()

            # End of first tick
            if self.is_booting:
                self.is_booting = False
                print("[SYSTEM] Silent startup finished. Alerts active.")

            time.sleep(POLL_INTERVAL)


    def get_price_at_hour(self, target_hour):
        """Fetch the exact opening price for a specific UTC hour today from OKX."""
        try:
            # Use 15m for precision with floats (e.g. 13.5)
            df = fetch_klines(interval="15m", limit=96)
            if df.empty:
                return None
            
            today = datetime.now(timezone.utc).date()
            for i in range(len(df)):
                idx = df.index[i]
                float_h = idx.hour + idx.minute / 60.0
                if idx.date() == today and abs(float_h - target_hour) < 0.01:
                    return float(df.iloc[i]["Open"])
        except Exception as e:
            print(f"[ERROR] Failed to fetch price for hour {target_hour}: {e}")
        return None

    def get_session_ohlc(self, start_hour, end_hour):
        """Fetch O, H, L, C for a session period today from OKX."""
        try:
            # Use 15m for precision (e.g. session starting at 13:30)
            df = fetch_klines(interval="15m", limit=192)
            if df.empty:
                return None, None, None, None
            
            today = datetime.now(timezone.utc).date()
            df["float_hour"] = df.index.hour + df.index.minute / 60.0

            # Filter session candles
            if start_hour < end_hour:
                mask = (df.index.date == today) & (df["float_hour"] >= start_hour - 0.01) & (df["float_hour"] < end_hour - 0.01)
            else: # Crosses midnight
                mask = ((df.index.date == today) & (df["float_hour"] >= start_hour - 0.01)) | \
                       ((df.index.date == today) & (df["float_hour"] < end_hour - 0.01))
            
            session_df = df[mask]
            
            if session_df.empty:
                # Try finding just the open price
                open_p = self.get_price_at_hour(start_hour)
                return open_p, open_p, open_p, open_p if open_p else (None, None, None, None)
            
            return (
                float(session_df.iloc[0]["Open"]),
                float(session_df["High"].max()),
                float(session_df["Low"].min()),
                float(session_df.iloc[-1]["Close"])
            )
        except Exception as e:
            print(f"[ERROR] Failed to fetch session OHLC: {e}")
            return None, None, None, None


    def _reconstruct_session_history(self, current_float_hour):
        """Reconstruct previous session summaries of the day if bot started late."""
        sessions = get_adjusted_sessions(datetime.now(timezone.utc))
        for s_name, s_times in sessions.items():
            if s_name in self.session_history:
                continue
            
            # Check if session is finished
            is_completed = False
            s_open = s_times["open"]
            s_close = s_times["close"]

            if s_open < s_close:
                # Normal: open at 8, close at 16. Finished if current hour >= 16.
                is_completed = current_float_hour >= s_close - 0.01
            else:
                # Cross Midnight: open at 22, close at 6. 
                # Finished if current hour is between 6 and 22.
                is_completed = s_close <= current_float_hour < s_open
            
            if is_completed:
                print(f"[SESSION] Reconstructing history for {s_name}...")
                open_p, high_p, low_p, close_p = self.get_session_ohlc(s_open, s_close)
                
                if open_p and close_p:
                    stats = self.tracker.get_session_stats(s_open, s_close)
                    
                    change = close_p - open_p
                    pct = (change / open_p) * 100 if open_p else 0
                    sign = "+" if change >= 0 else ""
                    
                    summary_str = (
                        f"<b>{s_name} SESSION</b>\n"
                        f"<pre>"
                        f"Open:    {open_p:,.2f}\n"
                        f"High:    {high_p:,.2f}\n"
                        f"Low:     {low_p:,.2f}\n"
                        f"Close:   {close_p:,.2f}\n"
                        f"Change:  {sign}{pct:.2f}%"
                        f"</pre>"
                    )
                    self.session_history[s_name] = summary_str
                    print(f"  [OK] {s_name} history reconstructed.")

    def _get_history_text(self, current_session_name, latest_price):
        """Build a combined history string of all sessions that started before the current one."""
        now = datetime.now(timezone.utc)
        today = now.strftime("%d.%m.%Y")
        sessions = get_adjusted_sessions(now)
        
        hist_items = []
        # Define the chronological order for display
        order = ["ASIA", "LONDON", "NY"]
        
        for s_name in order:
            if s_name == current_session_name:
                break
                
            # 1. If it's already closed and in history, use that
            if s_name in self.session_history:
                hist_items.append(self.session_history[s_name])
            
            # 2. If it's currently active (started before us), show Snapshot "YET"
            else:
                session_id = f"{s_name}_{today}"
                if session_id in self.session_data:
                    s_data = self.session_data[session_id]
                    o = s_data["open_price"]
                    h = s_data.get("high", latest_price)
                    l = s_data.get("low", latest_price)
                    c = latest_price
                    
                    change = c - o
                    pct = (change / o) * 100 if o else 0
                    sign = "+" if change >= 0 else ""
                    
                    stats = self.tracker.get_session_stats(sessions[s_name]["open"], sessions[s_name]["close"])
                    
                    snap_str = (
                        f"<b>{s_name} SESSION</b>\n"
                        f"<pre>"
                        f"Open:    {o:,.2f}\n"
                        f"High:    {h:,.2f}\n"
                        f"Low:     {l:,.2f}\n"
                        f"Now:     {c:,.2f}\n"
                        f"Change:  {sign}{pct:.2f}%"
                        f"</pre>"
                    )
                    hist_items.append(snap_str)
                    
        return "\n\n".join(hist_items) if hist_items else None

    def _load_state(self):
        import json
        import os
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    
                    # Deep-restore sets in session_data
                    session_data = state.get("session_data", {})
                    for sid, sdata in session_data.items():
                        if isinstance(sdata, dict) and "levels_tested" in sdata:
                            sdata["levels_tested"] = set(sdata.get("levels_tested", []))
                    
                    # Restore Scalp Tracker states
                    scalp_states = state.get("scalp_trackers", {})
                    for tf, t_state in scalp_states.items():
                        if tf in self.scalp_trackers:
                            self.scalp_trackers[tf].from_dict(t_state)
                    
                    return state
            except Exception as e:
                print(f"[STATE] Error loading state file: {e}")
        return {}

    def _save_state(self):
        import json
        import os
        import tempfile
        try:
            # Prepare session_data by deep-converting sets to lists
            serializable_sessions = {}
            for sid, sdata in self.session_data.items():
                if isinstance(sdata, dict):
                    serializable_sessions[sid] = sdata.copy()
                    if "levels_tested" in serializable_sessions[sid]:
                        serializable_sessions[sid]["levels_tested"] = list(sdata["levels_tested"])
                else:
                    serializable_sessions[sid] = sdata

            # Collect all state items
            payload = {
                "daily_report_msg_id": self.daily_report_msg_id,
                "session_msg_ids": self.session_msg_ids,
                "confirmations": self.confirmations.to_dict(),
                "session_data": serializable_sessions,
                "last_levels_date": self.last_levels_date,
                "sent_signals": list(self.sent_signals),
                "sent_sessions": list(self.sent_sessions),
                "approach_alerts": self.approach_alerts,
                "last_funding_alert": self.last_funding_alert,
                "last_market_alert": self.last_market_alert,
                "last_summary_date": self.last_summary_date,
                "scalp_trackers": {tf: tracker.to_dict() for tf, tracker in self.scalp_trackers.items()}
            }

            # Atomic write: Write to temp file then rename
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(self.state_file)))
            try:
                with os.fdopen(fd, 'w') as tmp:
                    json.dump(payload, tmp)
                # On Windows, os.replace might fail if target exists, but we use os.replace/remove
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
                os.rename(temp_path, self.state_file)
            except Exception as e:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise e

        except Exception as e:
            print(f"[STATE ERROR] Failed to save {self.state_file}: {e}")
            import traceback
            traceback.print_exc()


    def _generate_current_chart(self, output_path="session_chart.png", show_sessions=True):
        """Generate a fresh chart image with current levels and sessions."""
        try:
            # Prepare session stats for visual sync (High/Low)
            stats_map = {}
            if self.session_data:
                for sid, data in self.session_data.items():
                    # sid is "ASIA_15.03.2024", we need "ASIA"
                    s_name = sid.split("_")[0]
                    stats_map[s_name] = {
                        "high": data.get("high"),
                        "low": data.get("low")
                    }

            chart_df = fetch_klines(interval="1h", limit=48)
            if not chart_df.empty and self.levels:
                return generate_daily_levels_chart(
                    chart_df, self.levels, 
                    output_path=output_path, 
                    show_sessions=show_sessions,
                    session_stats=stats_map
                )
        except Exception as e:
            print(f"[CHARTING] Failed to generate session chart: {e}")
        return None

    def _tick(self):
        """One iteration of the main loop."""
        now = datetime.now(timezone.utc)
        current_time = time.time()
        print(f"\n[{now.strftime('%H:%M:%S')} UTC] Fetching data...")

        # 1. Update Levels if new day
        self._update_levels_if_needed(now)

        # 1.5 Scheduled Daily Summary (12:00 PM Local / 08:00 UTC)
        if now.hour == 8 and now.minute == 0:
            today_str = now.strftime("%d.%m.%Y")
            if self.last_summary_date != today_str:
                # Send summary for the previous day recap
                yesterday = now - timedelta(days=1)
                self._send_performance_summary(yesterday.strftime("%Y-%m-%d"))
                self.last_summary_date = today_str
                self._save_state()

        # 2. Fetch Global context
        self.last_oi = fetch_open_interest()
        self.last_liqs = fetch_liquidations()

        # 3. Fetch all timeframes (only Signal TFs to avoid lag)
        data = fetch_all_timeframes(timeframes=SIGNAL_TIMEFRAMES)
        if not data: return

        # 3.1 Market Alert (Fast Move)
        if "1h" in data:
            df_1h = data["1h"]
            if len(df_1h) > FAST_MOVE_WINDOW:
                curr_p = float(df_1h.iloc[-1]["Close"])
                past_p = float(df_1h.iloc[-(FAST_MOVE_WINDOW + 1)]["Open"])
                move_pct = (curr_p - past_p) / past_p
                
                if abs(move_pct) >= FAST_MOVE_THRESHOLD:
                    if current_time - self.last_market_alert > FAST_MOVE_COOLDOWN:
                        if not self.is_booting:
                            tg.send_market_alert(move_pct * 100, FAST_MOVE_WINDOW, past_p, curr_p, chat_id=PRIVATE_CHAT_ID)
                        self.last_market_alert = current_time
                        self._save_state()
                        print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Market Alert: {move_pct*100:.1f}%")

        # 4. Update Macro Trend (1h baseline)
        if "1h" in data:
            self.macro_trend = detect_trend(data["1h"])
            print(f"  Trend: {self.macro_trend}")

        # 5. Check Funding Rate
        if current_time - self.last_funding_check > FUNDING_CHECK_INTERVAL:
            self.last_funding_check = current_time
            rate = fetch_funding_rate()
            if rate is not None:
                if abs(rate) >= FUNDING_THRESHOLD:
                    if current_time - self.last_funding_alert > FUNDING_COOLDOWN:
                        direction = "POSITIVE" if rate > 0 else "NEGATIVE"
                        if not self.is_booting:
                            tg.send_funding_alert(rate, direction, chat_id=PRIVATE_CHAT_ID)
                        self.last_funding_alert = current_time
                        print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Funding Alert: {direction} {rate:.4f}")


        # 6. Process each scalp timeframe
        latest_price = None
        current_candle_high = 0
        current_candle_low = 9999999
        
        # Track reference values for confluence alerts
        ref_atr = 0
        conf_ts = now.strftime("%Y-%m-%d %H:%M")  # Minute-level key for alerts
        ref_ts  = conf_ts 
        
        for tf in SIGNAL_TIMEFRAMES:
            if tf not in data: continue
            df = data[tf]
            
            # Update session-level H/L tracking using the current candle's wicks
            last_c = df.iloc[-1]
            current_candle_high = max(current_candle_high, float(last_c["High"]))
            current_candle_low = min(current_candle_low, float(last_c["Low"]))
            
            if latest_price is None:
                latest_price = float(df.iloc[-1]["Close"])

            # Process timeframe and capture ATR/TS for confluence reference
            tf_atr, tf_ts = self._process_timeframe(tf, df, now)
            
            # Use 1h ATR for confluence targets if available, otherwise fallback
            if tf == "1h" or ref_atr == 0:
                if tf_atr > 0:
                    ref_atr = tf_atr
                    ref_ts  = tf_ts

        # ─── Check Confirmation Aggregation (Once per Tick) ──────
        if latest_price is not None:
            for side in ["LONG", "SHORT"]:
                conf_events = self.confirmations.check_confirmations(side)
                for ce in conf_events:
                    # BLOCK DUPLICATES: use a minute-level key for confluence
                    conf_key = f"conf_{side}_{ce['type']}_{conf_ts}"
                    if conf_key in self.sent_signals:
                        continue
                    
                    self.sent_signals.add(conf_key)
                    
                    # --- CONFLUENCE FILTERS & QUALITY CONTROL ---
                    # 1. Trend Alignment: Confluence must match the macro trend
                    trend_ok = False
                    if side == "LONG" and self.macro_trend in ["Bullish", "Trending Bullish", "Strong Bullish"]: trend_ok = True
                    if side == "SHORT" and self.macro_trend in ["Bearish", "Trending Bearish", "Strong Bearish"]: trend_ok = True
                    
                    if not trend_ok:
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: Against Macro Trend ({self.macro_trend})")
                        continue

                    # 2. Proximity Protection: Don't enter Long near levels, etc.
                    proximity_blocked = False
                    block_threshold = 0.002 # 0.2%
                    if side == "LONG":
                        # Block if near major resistance
                        for l_key in ["PDH", "PWH", "PMH", "PumpMax"]:
                            lv = self.levels.get(l_key)
                            if lv and (lv > latest_price) and ((lv - latest_price)/latest_price < block_threshold):
                                proximity_blocked = True
                                print(f"  [CONFLUENCE] Blocked LONG: Too close to {l_key}")
                                break
                    else:
                        # Block if near major support
                        for l_key in ["PDL", "PWL", "PML", "DumpMax"]:
                            lv = self.levels.get(l_key)
                            if lv and (lv < latest_price) and ((latest_price - lv)/latest_price < block_threshold):
                                proximity_blocked = True
                                print(f"  [CONFLUENCE] Blocked SHORT: Too close to {l_key}")
                                break
                    
                    if proximity_blocked: continue

                    # 3. Calculate targets (Wider 1.0 ATR for confluence breathing room)
                    sl_m, tp1_m, tp2_m, tp3_m = 1.0, 0.8, 1.6, 2.4 
                    if side == "LONG":
                        sl_c  = latest_price - ref_atr * sl_m
                        tp1_c = latest_price + ref_atr * tp1_m
                        tp2_c = latest_price + ref_atr * tp2_m
                        tp3_c = latest_price + ref_atr * tp3_m
                    else:
                        sl_c  = latest_price + ref_atr * sl_m
                        tp1_c = latest_price - ref_atr * tp1_m
                        tp2_c = latest_price - ref_atr * tp2_m
                        tp3_c = latest_price - ref_atr * tp3_m

                    if ce["type"] == "STRONG":
                        resp = tg.send_strong(
                            side=ce["side"],
                            total_points=ce["points"],
                            confirmations=ce["confirmations"],
                            indicators_list=ce["indicators"],
                            price=latest_price,
                            sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            chat_id=PRIVATE_CHAT_ID
                        )
                        msg_id = resp.get("result", {}).get("message_id") if resp else None
                        self.tracker.log_signal(
                            side=ce["side"], entry=latest_price, sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            tf="Confluence", timestamp=ref_ts,
                            msg_id=msg_id, chat_id=PRIVATE_CHAT_ID, signal_type="STRONG",
                            meta={"indicators": ce["indicators"]}
                        )
                        self._save_state()
                        print(f"  [CONFLUENCE] ✅ STRONG {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                    elif ce["type"] == "EXTREME":
                        resp = tg.send_extreme(
                            side=ce["side"],
                            total_points=ce["points"],
                            confirmations=ce["confirmations"],
                            indicators_list=ce["indicators"],
                            price=latest_price,
                            sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            chat_id=PRIVATE_CHAT_ID
                        )
                        msg_id = resp.get("result", {}).get("message_id") if resp else None
                        self.tracker.log_signal(
                            side=ce["side"], entry=latest_price, sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            tf="Confluence", timestamp=ref_ts,
                            msg_id=msg_id, chat_id=PRIVATE_CHAT_ID, signal_type="EXTREME",
                            meta={"indicators": ce["indicators"]}
                        )
                        self._save_state()
                        print(f"  [CONFLUENCE] 🔥 EXTREME {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

        # ─── Update Performance Tracker & Success Teasers ────
        if latest_price is not None:
            # 1. Success Teasers (Public Marketing FOMO)
            trade_events = self.tracker.check_outcomes(latest_price)
            today_str = now.strftime("%Y-%m-%d")

            for event in trade_events:
                sig = event["sig"]
                evt_type = event["type"] # "TP1", "TP2", "TP3", "SL"
                
                # --- LIVE MESSAGE UPDATE ---
                # Update the original signal message with hit markers
                if sig.get("msg_id") and sig.get("chat_id"):
                    tg.update_signal_message(sig["chat_id"], sig["msg_id"], sig)

            # 2. Liquidation Squeezes
            if self.last_liqs >= LIQ_SQUEEZE_THRESHOLD:
                if current_time - self.last_liq_alert_time > LIQ_ALERT_COOLDOWN:
                    if not self.is_booting:
                        tg.send_squeeze_alert(self.last_liqs, latest_price, chat_id=PRIVATE_CHAT_ID)
                    self.last_liq_alert_time = current_time
                    print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} 🚨 Liquidation Squeeze: ${self.last_liqs/1e6:.1f}M")

            # 3. OI Divergence
            if self.last_oi and self.last_oi_price:
                price_chg = (latest_price / self.last_oi_price) - 1
                oi_chg = (self.last_oi / self.last_oi_base) - 1 if hasattr(self, 'last_oi_base') else 0
                
                # We only check if OI change is significant
                if abs(oi_chg) >= OI_CHANGE_THRESHOLD:
                    note = None
                    if price_chg > 0.005 and oi_chg < -0.01: # Price up, OI down
                        note = "Short Covering (Weak Pump). Price rising as shorts close, not as new longs open."
                    elif price_chg < -0.005 and oi_chg < -0.01: # Price down, OI down
                        note = "Long Liquidation (Weak Dump). Price falling as longs are forced out."
                    elif abs(price_chg) < 0.005 and oi_chg > 0.02: # Price flat, OI up
                        note = "Accumulation/Distribution. Huge new positions opening while price stays flat. Breakout imminent."
                    
                    if note:
                        sig_key = f"oi_div_{now.strftime('%Y-%m-%d_%H')}" # Max 1 per hour
                        if sig_key not in self.sent_signals:
                            self.sent_signals.add(sig_key)
                            if not self.is_booting:
                                tg.send_oi_divergence(price_chg*100, oi_chg*100, note, chat_id=PRIVATE_CHAT_ID)
                            self._save_state()
                            print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} ⚠️ OI Divergence: {note}")

            # Update baselines for next tick comparison
            self.last_oi_price = latest_price
            self.last_oi_base = self.last_oi

            # --- Session Tracking ---
            today = now.strftime("%d.%m.%Y")
            current_float_hour = now.hour + now.minute / 60.0
            is_weekend = now.weekday() >= 5 # 5=Sat, 6=Sun
            
            # Dynamic session times (handles DST and fractional hours)
            sessions = get_adjusted_sessions(now)

            if not is_weekend:
                for s_name, s_times in sessions.items():
                    session_id = f"{s_name}_{today}"
                    
                    # Check if session is active now
                    is_active = False
                    s_open = s_times["open"]
                    s_close = s_times["close"]

                    if s_open < s_close:
                        is_active = s_open <= current_float_hour < s_close
                    else: # Crosses midnight
                        is_active = current_float_hour >= s_open or current_float_hour < s_close

                    # Capture Open & Recover H/L from OKX
                    if is_active and session_id not in self.session_data:
                        open_p, high_p, low_p, _ = self.get_session_ohlc(s_open, current_float_hour + 0.5)
                        if open_p is None:
                            open_p = latest_price
                        if high_p is None: high_p = latest_price
                        if low_p is None: low_p = latest_price
                            
                        self.session_data[session_id] = {
                            "open_price": open_p,
                            "high": high_p,
                            "low": low_p,
                            "levels_tested": set()
                        }
                        
                        # Use a small tolerance for "exactly at open"
                        is_mid = abs(current_float_hour - s_open) > 0.08 # > 5 mins
                        status = "opened" if not is_mid else "active (recovered from OKX)"
                        print(f"[SESSION] {s_name} {status} at {open_p:,.2f}")
                        
                        # Fetch stats so far if mid-session
                        stats = self.tracker.get_session_stats(s_open, s_close)
                        
                        # Construct history string
                        history_text = self._get_history_text(s_name, latest_price)

                        # Generate session chart
                        chart_path = self._generate_current_chart(f"session_open_{s_name}.png")

                        # Always send session status on discovery (even during boot) 
                        # so the user knows the bot is actively tracking the current session.
                        resp = tg.send_session_open(
                            session_name=s_name, 
                            open_price=open_p, 
                            current_price=latest_price if is_mid else None,
                            history=history_text,
                            high=self.session_data[session_id]["high"],
                            low=self.session_data[session_id]["low"],
                            chart_path=chart_path,
                            chat_id=PRIVATE_CHAT_ID
                        )
                        
                        if resp and "response" in resp:
                            msg_data = resp["response"]
                            if msg_data and "result" in msg_data:
                                msg_id = msg_data["result"]["message_id"]
                                # Now we store metadata so we can REGENERATE the text later
                                self.session_msg_ids[session_id] = {
                                    "msg_id": msg_id,
                                    "name": s_name,
                                    "open": open_p,
                                    "history": history_text
                                }
                                self.last_session_update = current_time
                                self._save_state()


                    # Update High/Low with current candle wicks
                    if is_active and session_id in self.session_data:
                        self.session_data[session_id]["high"] = max(self.session_data[session_id]["high"], current_candle_high)
                        self.session_data[session_id]["low"] = min(self.session_data[session_id]["low"], current_candle_low)

                    # Capture Close & Send Summary
                    is_closing = False
                    if s_open < s_close:
                        is_closing = current_float_hour >= s_close
                    else: # Crosses midnight
                        is_closing = s_close <= current_float_hour < s_open
                    
                    if is_closing:
                        sent_key = f"sent_{session_id}"
                        if sent_key not in self.sent_sessions:
                            self.sent_sessions.add(sent_key)
                            
                            s_data = self.session_data.get(session_id, {"open_price": latest_price, "levels_tested": set(), "high": latest_price, "low": latest_price})
                            open_p = s_data["open_price"]
                            s_high = s_data.get("high", latest_price)
                            s_low = s_data.get("low", latest_price)
                            levels = ", ".join(sorted(list(s_data["levels_tested"]))) if s_data["levels_tested"] else "None"
                            
                            stats = self.tracker.get_session_stats(s_open, s_close)
                            
                            # Construct history string
                            history_text = self._get_history_text(s_name, latest_price)
                            
                            # Generate session chart
                            chart_path = self._generate_current_chart(f"session_close_{s_name}.png")

                            if not self.is_booting:
                                tg.send_session_summary(s_name, open_p, latest_price, stats["total"], levels, history=history_text, high=s_high, low=s_low, chart_path=chart_path, chat_id=PRIVATE_CHAT_ID)
                            
                            # Save to history for NEXT sessions
                            change = latest_price - open_p
                            pct = (change / open_p) * 100 if open_p else 0
                            sign = "+" if change >= 0 else ""
                            summary_str = (
                                f"<b>{s_name} RECAP</b>\n"
                                f"<pre>"
                                f"Open:    {open_p:,.2f}\n"
                                f"High:    {s_high:,.2f}\n"
                                f"Low:     {s_low:,.2f}\n"
                                f"Close:   {latest_price:,.2f}\n"
                                f"Change:  {sign}{pct:.2f}%\n"
                                f"Levels:  {levels}"
                                f"</pre>"
                            )
                            self.session_history[s_name] = summary_str
                            
                            # Stop updating the opening message once closed
                            if session_id in self.session_msg_ids:
                                del self.session_msg_ids[session_id]
                                
                            self._save_state() # Save summary sent state
                            
                            print(f"[SESSION] {s_name} closed. {'Skipped sending recap' if self.is_booting else 'Recap sent'}.")

        # ─── Flush Batched Alerts ────────────────────────────
        if self.pending_alerts and self.batch_timer_start:
            # Check if batch window has passed
            if current_time - self.batch_timer_start >= ALERT_BATCH_WINDOW:
                # Group alerts by chat_id
                by_chat = {}
                for a in self.pending_alerts:
                    cid = a.get("chat_id")
                    if cid not in by_chat: by_chat[cid] = []
                    by_chat[cid].append(a)
                
                for cid, alerts in by_chat.items():
                    if len(alerts) > 1:
                        # Send as batch
                        batch_data = [a["data"] for a in alerts]
                        if not self.is_booting:
                            tg.send_batched_alerts(batch_data, chat_id=cid)
                        print(f"[TG] {'Skipped' if self.is_booting else 'Sent'} batch of {len(batch_data)} alerts to {cid}")
                    elif len(alerts) == 1:
                        # Send as individual alert
                        alert = alerts[0]
                        if alert["callback"]:
                            # All tg functions now accept chat_id
                            kwargs = {"chat_id": cid}
                            if not self.is_booting:
                                alert["callback"](*alert["args"], **kwargs)
                            print(f"[TG] {'Skipped' if self.is_booting else 'Sent'} individual alert: {alert['data']['type']} to {cid}")
                
                self.pending_alerts = []
                self.batch_timer_start = None

        # ─── Periodic Chart Updates ──────────────────────────
        # 1. Session Updates (30s)
        if current_time - self.last_session_update > 30:
            self.last_session_update = current_time
            if self.session_msg_ids:
                print(f"[BOT] Refreshing session charts (30s interval)...")
                chart_path = self._generate_current_chart(f"session_update_{now.strftime('%H%M%S')}.png")
                if chart_path:
                    for s_id, info in list(self.session_msg_ids.items()):
                        s_data = self.session_data.get(s_id, {})
                        new_html = tg.get_session_open_html(
                            session_name=info["name"],
                            open_price=info["open"],
                            current_price=latest_price,
                            history=info["history"],
                            high=s_data.get("high"),
                            low=s_data.get("low")
                        )
                        if not self.is_booting:
                            res = tg.edit_message_media(info["msg_id"], chart_path, caption=new_html, chat_id=PRIVATE_CHAT_ID)
                            if res == "DELETED":
                                del self.session_msg_ids[s_id]
                                self._save_state()
                        else:
                            print(f"  [TG] Skipped editing session message for {info['name']} (booting)")
                    
                    try:
                        import os
                        if os.path.exists(chart_path): os.remove(chart_path)
                    except: pass

        # 2. Daily Levels Update (600s)
        # ─── End of Tick ─────────────────────────────────────
        tick_duration = time.time() - current_time
        if tick_duration > 2.0:
            print(f"[PERF] Tick took {tick_duration:.1f}s (Threshold: 2.0s)")
        
        if current_time - self.last_daily_update > 600:
            self.last_daily_update = current_time
            if self.daily_report_msg_id:
                print(f"[BOT] Refreshing daily levels report (600s interval)...")
                chart_path = self._generate_current_chart(f"daily_update_{now.strftime('%H%M%S')}.png", show_sessions=False)
                if chart_path:
                    d_msg_id = self.daily_report_msg_id if isinstance(self.daily_report_msg_id, (int, str)) else self.daily_report_msg_id.get("msg_id")
                    d_data = self.daily_report_msg_id.get("data") if isinstance(self.daily_report_msg_id, dict) else None
                    if not d_data and self.levels:
                        d_data = {
                            "date": now.strftime("%d.%m.%Y"),
                            "do": self.levels.get("DO", 0),
                            "res": self.levels.get("Pump", 0), "res_p": self.levels.get("ResistancePct", 0),
                            "sup": self.levels.get("Dump", 0), "sup_p": self.levels.get("SupportPct", 0),
                            "vol": self.levels.get("Volatility", 0), "vol_p": self.levels.get("VolatilityPct", 0),
                            "high": self.levels.get("PumpMax", 0), "low": self.levels.get("DumpMax", 0)
                        }
                    if d_data:
                        new_inds = fetch_global_indicators()
                        new_html = tg.get_daily_levels_html(
                            date_str=d_data["date"], daily_open=d_data["do"],
                            resistance=d_data["res"], resistance_pct=d_data["res_p"],
                            support=d_data["sup"], support_pct=d_data["sup_p"],
                            volatility=d_data["vol"], volatility_pct=d_data["vol_p"],
                            critical_high=d_data["high"], critical_low=d_data["low"],
                            indicators=new_inds
                        )
                        if not self.is_booting:
                            tg.edit_message_media(d_msg_id, chart_path, caption=new_html, chat_id=PRIVATE_CHAT_ID)
                        else:
                            print(f"  [TG] Skipped editing daily report (booting)")

                    try:
                        import os
                        if os.path.exists(chart_path): os.remove(chart_path)
                    except: pass

    def _update_levels_if_needed(self, now):
        """Update levels at the start of a new day."""
        today = now.strftime("%d.%m.%Y")
        if today != self.last_levels_date:
            print(f"\n[SYSTEM] New day detected ({today}). Resetting data...")
            
            # 1. Reset everything for the new day
            self.daily_report_msg_id = None 
            self._update_levels()
            self._send_daily_report(now)
            
            self.last_levels_date = today
            self.sent_signals.clear()  # Reset duplicate tracking
            self.session_history.clear() # Reset session history for new day
            self.session_msg_ids.clear() # Reset message IDs for new day
            self.session_data.clear()   # Actually clear old sessions data
            self.sent_sessions.clear()   # Reset session recap tracking
            self.approach_alerts.clear() # Reset level approach tracking
            self._save_state()
        
        self._reconstruct_session_history(now.hour)

    def _update_levels(self):
        """Fetch daily/weekly/monthly/hourly data and calculate levels."""
        print("[LEVELS] Updating daily/weekly/monthly/hourly levels...")

        daily_df   = fetch_daily()
        weekly_df  = fetch_weekly()
        monthly_df = fetch_monthly()
        # Fetch 200 hours to cover current day, yesterday, and day before for stability
        hourly_df  = fetch_klines(interval="1h", limit=200)

        self.levels = calculate_levels(daily_df, weekly_df, monthly_df, hourly_df=hourly_df)

        if self.levels:     
            now = datetime.now(timezone.utc)
            pd_date = self.levels.get("PD_Date", "N/A")
            print(f"  [LEVELS] Updated for {now.strftime('%d.%m.%Y')} (PD: {pd_date}):")
            for k in ["DO", "PDH", "PDL", "PWH", "PWL", "PMH", "PML", "Pump", "Dump"]:
                val = self.levels.get(k)
                if val: print(f"    - {k}: {val:,.2f}")

    def _send_daily_report(self, now):
        """Send daily levels report to Telegram."""
        if not self.levels:
            return

        # --- Performance Summary first ---
        try:
            stats = self.tracker.get_daily_summary()
            if stats:
                if not self.is_booting:
                    tg.send_performance_summary(stats, chat_id=PRIVATE_CHAT_ID)
                else:
                    print(f"  [TG] Skipped sending performance summary (booting)")
            self.tracker.cleanup_old(7)
        except Exception as e:
            print(f"[TRACKER ERROR] {e}")

        date_str = now.strftime("%d.%m.%Y")
        do = self.levels["DO"]

        # Generate visual chart for the report
        chart_path = None
        try:
            # Fetch 1h data for charting (last 48h)
            chart_df = fetch_klines(interval="1h", limit=48)
            if not chart_df.empty:
                chart_path = generate_daily_levels_chart(chart_df, self.levels, show_sessions=False)
        except Exception as e:
            print(f"[CHARTING] Failed to generate: {e}")

        # Fetch indicators
        indicators = fetch_global_indicators()

        resp_data = None
        if not self.is_booting:
            resp_data = tg.send_daily_levels(
                date_str=date_str,
                daily_open=do,
                resistance=self.levels.get("Pump", 0),
                resistance_pct=self.levels.get("ResistancePct", 0),
                support=self.levels.get("Dump", 0),
                support_pct=self.levels.get("SupportPct", 0),
                volatility=self.levels.get("Volatility", 0),
                volatility_pct=self.levels.get("VolatilityPct", 0),
                critical_high=self.levels.get("PumpMax", 0),
                critical_low=self.levels.get("DumpMax", 0),
                indicators=indicators,
                chart_path=chart_path,
                chat_id=PRIVATE_CHAT_ID
            )
        else:
            print(f"  [TG] Skipped sending daily levels report (booting)")
        
        if resp_data and "response" in resp_data:
            msg_data = resp_data["response"]
            if msg_data and "result" in msg_data:
                self.daily_report_msg_id = {
                    "msg_id": msg_data["result"]["message_id"],
                    "data": {
                        "date": date_str, "do": do,
                        "res": self.levels.get("Pump", 0), "res_p": self.levels.get("ResistancePct", 0),
                        "sup": self.levels.get("Dump", 0), "sup_p": self.levels.get("SupportPct", 0),
                        "vol": self.levels.get("Volatility", 0), "vol_p": self.levels.get("VolatilityPct", 0),
                        "high": self.levels.get("PumpMax", 0), "low": self.levels.get("DumpMax", 0)
                    }
                }
                self._save_state()
                self.last_daily_update = time.time()
        
        print(f"[TG] Daily levels report {'skipped' if self.is_booting else 'sent'}")
    def _send_performance_summary(self, target_date_str=None):
        """Fetch and send the performance summary for a specific date."""
        try:
            stats = self.tracker.get_daily_summary(target_date_str)
            if stats:
                if not self.is_booting:
                    tg.send_performance_summary(stats, chat_id=PRIVATE_CHAT_ID)
                else:
                    print(f"  [TG] Skipped sending performance summary (booting)")
            else:
                print(f"  [TRACKER] No signals found for {target_date_str or 'today'}. Skipping summary.")
            
            # Clean up old signals (keep 7 days)
            self.tracker.cleanup_old(7)
        except Exception as e:
            print(f"[TRACKER ERROR] Failed to send performance summary: {e}")


    def _process_timeframe(self, tf, df, now):
        """Process one timeframe: channels, momentum, signals."""

        # ─── Calculate indicators ────────────────────────
        df = calculate_channels(df)
        df = calculate_momentum(df)

        if df.empty or len(df) < 2:
            return 0, ""

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        price_high = float(curr["High"])
        price_low  = float(curr["Low"])
        close      = float(curr["Close"])
        atr_val    = float(curr["ATR"]) if "ATR" in curr else 0
        zone       = curr["MomentumZone"] if "MomentumZone" in curr else "NEUTRAL"
        rsi_val    = float(curr["MomentumSmooth"]) if "MomentumSmooth" in curr else 50

        # ─── REAL-TIME MONITOR (Debug) ───────────────────

        prev_high = float(prev["High"])
        prev_low  = float(prev["Low"])

        candle_ts = curr.name.strftime("%Y-%m-%d %H:%M") if hasattr(curr.name, 'strftime') else str(curr.name)
        current_time = time.time()
        
        def record_level(lvl):
            # Use now from outer scope
            today = now.strftime("%d.%m.%Y")
            current_float_hour = now.hour + now.minute / 60.0
            
            # Use adjusted sessions
            sessions = get_adjusted_sessions(now)
            
            for s_name, s_times in sessions.items():
                is_active = False
                s_open = s_times["open"]
                s_close = s_times["close"]

                if s_open < s_close:
                    is_active = s_open <= current_float_hour < s_close
                else: # Crosses midnight
                    is_active = current_float_hour >= s_open or current_float_hour < s_close
                
                if is_active:
                    session_id = f"{s_name}_{today}"
                    if session_id in self.session_data:
                        self.session_data[session_id]["levels_tested"].add(lvl)

        # ─── Volume Spike Detection ──────────────────────
        if tf in VOLUME_SPIKE_TIMEFRAMES and len(df) > VOLUME_AVG_PERIOD:
            vol_col = df["Volume"]
            avg_vol = vol_col.iloc[-VOLUME_AVG_PERIOD-1:-1].mean()
            current_vol = float(curr["Volume"])
            if avg_vol > 0 and current_vol > (avg_vol * VOLUME_SPIKE_MULT):
                sig_key = f"volspike_{tf}_{candle_ts}"
                if sig_key not in self.sent_signals:
                    self.sent_signals.add(sig_key)
                    # queue_alert already handles the is_booting check implicitly via the batching logic
                    self.queue_alert(
                        alert_dict={
                            "type": "VOLUME SPIKE",
                            "tf": tf,
                            "price": close,
                            "note": f"{current_vol/avg_vol:.1f}x average volume"
                        },
                        callback=tg.send_volume_spike,
                        args=(tf, current_vol, avg_vol, current_vol/avg_vol, close),
                        chat_id=PRIVATE_CHAT_ID
                    )
                    print(f"  [SIG] Volume Spike [{tf}] {current_vol/avg_vol:.1f}x avg vol")


        if tf == "5m" and self.levels and not self.is_booting:
            prev_close = float(prev["Close"])
            
            # 1. Identify all levels with directional momentum
            triggered_levels = []
            for lvl_name in APPROACH_LEVELS:
                lvl_price = self.levels.get(lvl_name)
                if not lvl_price: continue
                
                dist_pct = abs(close - lvl_price) / lvl_price
                prev_dist = abs(prev_close - lvl_price) / lvl_price
                # Velocity > 0 means we are getting closer
                velocity = prev_dist - dist_pct

                # ALERT LOGIC:
                # A. Extremely Close: Within 0.1%, alert regardless of motion
                is_urgent = (dist_pct <= 0.001)
                # B. Approach Momentum: Within 0.4%, but must be moving DECISIVELY towards it
                # (velocity > 0.0002 means price moved 0.02% towards level in 5 minutes)
                is_approaching = (dist_pct <= 0.004) and (velocity >= 0.0002)

                if is_urgent or is_approaching:
                    importance = APPROACH_LEVELS.index(lvl_name)
                    # We store (distance, importance, name, price, is_urgent)
                    triggered_levels.append((dist_pct, importance, lvl_name, lvl_price, is_urgent))

            if triggered_levels:
                # 2. Sort primarily by CLOSENESS (dist_pct), then by importance.
                # This fixes "PDH showing instead of PWH" when both are near.
                triggered_levels.sort(key=lambda x: (x[0], x[1]))
                
                closest_dist, importance, lvl_name, lvl_price, is_urgent = triggered_levels[0]
                
                # 2. Check cooldown and threshold crossings
                prev_dist = abs(prev_close - lvl_price) / lvl_price
                is_new_proximity = (prev_dist > APPROACH_THRESHOLD) and (closest_dist <= APPROACH_THRESHOLD)
                
                last_alert = self.approach_alerts.get(lvl_name, 0)
                if (is_new_proximity or (current_time - last_alert > APPROACH_COOLDOWN)):
                    # Safety throttle: never more than once per HOUR for the SAME level
                    if current_time - last_alert > 3600:
                        self.queue_alert(
                            alert_dict={
                                "type": "APPROACHING LEVEL",
                                "note": f"Approaching {lvl_name} ({closest_dist*100:.2f}%)"
                            },
                            callback=tg.send_approaching_level,
                            args=(lvl_name, lvl_price, close, closest_dist * 100),
                            chat_id=PRIVATE_CHAT_ID
                        )
                        self.approach_alerts[lvl_name] = current_time
                        self._save_state()
                        print(f"  [SIG] Approaching Level Triggered: {lvl_name} ({closest_dist*100:.2f}%)")


        # ─── Liquidity Sweeps ────────────────────────────
        if self.levels:
            sweeps = check_liquidity_sweep(
                price_high, price_low, self.levels,
                prev_high=prev_high, prev_low=prev_low
            )
            for sw in sweeps:
                # Always record for session tracking
                record_level(sw['level'])

                # Use a very specific key for the individual sweep event
                sweep_key = f"sweep_{sw['level']}_{sw['side']}_{now.strftime('%Y-%m-%d')}"
                if sweep_key not in self.sent_signals:
                    self.sent_signals.add(sweep_key)
                    # Force save state immediately before sending to ensure persistence
                    self._save_state()
                    
                    if not self.is_booting:
                        tg.send_liquidity_sweep(**sw, chat_id=PRIVATE_CHAT_ID)
                    
                    print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Liquidity Sweep: {sw['level']} ({sw['side']})")

                    # Add to confirmation tracker
                    self.confirmations.add_signal({
                        "side":      sw["side"],
                        "indicator": f"Ponch_RangeTrader_Sweep_{sw['level']}",
                        "signal":    f"LIQUIDITY SWEEP: {sw['level']}",
                        "points":    sw["points"],
                        "tf":        tf
                    })

        # ─── Volatility Zone Touches ─────────────────────
        if self.levels:
            touches = check_volatility_touch(
                price_high, price_low, self.levels,
                prev_high=prev_high, prev_low=prev_low
            )
            for vt in touches:
                # Always record for session tracking
                record_level(vt['level'])

                # TF-independent key for daily/weekly zones
                sig_key = f"vol_{vt['level']}_{vt['side']}_{now.strftime('%Y-%m-%d')}"
                if sig_key not in self.sent_signals:
                    self.sent_signals.add(sig_key)
                    
                    if not self.is_booting:
                        tg.send_volatility_touch(**vt, chat_id=PRIVATE_CHAT_ID)
                        self._save_state()
                    
                    print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Vol Zone Touch: {vt['level']} ({vt['side']})")

                    # Add to confirmation tracker
                    self.confirmations.add_signal({
                        "side":      vt["side"],
                        "indicator": f"Ponch_RangeTrader_VolZone_{vt['level']}",
                        "signal":    f"VOL ZONE TOUCH: {vt['level']}",
                        "points":    vt["points"],
                        "tf":        tf
                    })

        # ─── Trade Signals (Channels) ────────────────────
        ch_sigs = check_channel_signals(df)
        for sig in ch_sigs:
            sig_key = f"tr_sig_{tf}_{sig['signal']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # 2. Momentum confirmation
        mom_sigs = check_momentum_confirm(df)
        for sig in mom_sigs:
            sig_key = f"mom_sig_{tf}_{sig['side']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # 3. Range trader confirmation
        rng_sigs = check_range_confirm(df, self.levels)
        for sig in rng_sigs:
            sig_key = f"rng_sig_{tf}_{sig['signal']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # 4. Flow confirmation
        flow_sigs = check_flow_confirm(df)
        for sig in flow_sigs:
            sig_key = f"flow_sig_{tf}_{sig['side']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # ─── Scalp Momentum System ───────────────────────
        tracker = self.scalp_trackers[tf]
        events = tracker.update(zone, close, atr_val, candle_ts=candle_ts, rsi_value=rsi_val)

        profile = TIMEFRAME_PROFILES.get(tf, TIMEFRAME_PROFILES["5m"])
        emoji = profile["emoji"]

        for evt in events:
            # Scalp signals usually only one of each type per candle
            evt_key = f"scalp_{tf}_{evt['type']}_{evt['side']}_{candle_ts}"

            if evt_key in self.sent_signals:
                continue

            self.sent_signals.add(evt_key)

            if evt["type"] == "OPEN":
                tg.send_scalp_open(tf, evt["side"], evt["price"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self.sent_signals.add(evt_key)
                self._save_state()
                print(f"  [TG] Scalp Open [{tf}] {evt['side']}")

            elif evt["type"] == "PREPARE":
                tg.send_scalp_prepare(tf, evt["side"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self.sent_signals.add(evt_key)
                self._save_state()
                print(f"  [TG] Prepare [{tf}] {evt['side']}")

            elif evt["type"] == "CONFIRMED":
                # --- Calculate Signal Strength Score ---
                score, reasons = calculate_signal_score(
                    evt, df, self.levels, self.macro_trend, self.last_oi, self.last_liqs
                )

                resp = tg.send_scalp_confirmed(
                    timeframe=tf,
                    side=evt["side"],
                    entry=evt["entry"],
                    sl=evt["sl"],
                    tp1=evt["tp1"],
                    tp2=evt["tp2"],
                    tp3=evt["tp3"],
                    strength=profile["strength"],
                    size=profile["size"],
                    score=score,
                    trend=self.macro_trend,
                    reasons=reasons,
                    chat_id=PRIVATE_CHAT_ID
                )
                msg_id = resp.get("result", {}).get("message_id") if resp else None
                self._save_state()
                print(f"  [TG] Scalp Confirmed [{tf}] {evt['side']} @ {evt['entry']:,.2f}")
                
                # Log signal for performance tracking
                self.tracker.log_signal(
                    side=evt["side"],
                    entry=evt["entry"],
                    sl=evt["sl"],
                    tp1=evt["tp1"],
                    tp2=evt["tp2"],
                    tp3=evt["tp3"],
                    tf=tf,
                    timestamp=candle_ts,
                    msg_id=msg_id,
                    chat_id=PRIVATE_CHAT_ID,
                    signal_type="SCALP",
                    meta={"score": score, "trend": self.macro_trend, "reasons": reasons}
                )

            elif evt["type"] == "CLOSED":
                tg.send_scalp_closed(tf, evt["side"], evt["price"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self._save_state()
                print(f"  [TG] Scalp Closed [{tf}] {evt['side']}")


        # ─── Store prev candle data ──────────────────────
        self.prev_candles[tf] = {
            "High": price_high,
            "Low":  price_low,
        }

        return atr_val, candle_ts


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import os

    lock_file = "bot.lock"
    
    # Strict singleton check with PID awareness
    if os.path.exists(lock_file):
        import time
        try:
            with open(lock_file, "r") as f:
                old_pid = int(f.read().strip())
            
            # Check if that PID is actually running (Unix/Linux check)
            try:
                os.kill(old_pid, 0)
                is_running = True
            except (OSError, ProcessLookupError, ValueError):
                is_running = False

            # If it's running AND it's a recent update (heartbeat)
            if is_running and (time.time() - os.path.getmtime(lock_file) < 60):
                print(f"\n[FATAL] Another instance (PID {old_pid}) is already running.")
                sys.exit(1)
            else:
                # Process is dead OR lock is stale
                os.remove(lock_file)
        except Exception:
            # Fallback if file is corrupted or OS doesn't support os.kill
            if time.time() - os.getmtime(lock_file) < 60:
                print(f"\n[FATAL] Stale lock detected, but it's too fresh. Exiting.")
                sys.exit(1)
            else:
                try: os.remove(lock_file)
                except: pass
            
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))

    try:
        bot = PonchBot()
        
        # Periodic lock file pulse to show we are alive
        def heartbeat():
            with open(lock_file, "w") as f:
                f.write(str(os.getpid()))
        
        bot.heartbeat_callback = heartbeat
        bot.run()
    except KeyboardInterrupt:
        print("\n[!] Bot stopping...")
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
