# ─── Ponch Signal System — Main Bot ───────────────────────────

"""
Main entry point. Monitors BTCUSDT across multiple timeframes,
detects signals, and sends formatted Telegram alerts.
"""

import time
import traceback
from datetime import datetime, timezone

from config import (
    SYMBOL, SIGNAL_TIMEFRAMES, POLL_INTERVAL,
    TIMEFRAME_PROFILES, FUNDING_THRESHOLD, FUNDING_CHECK_INTERVAL,
    FUNDING_COOLDOWN, VOLUME_SPIKE_MULT, VOLUME_SPIKE_TIMEFRAMES,
    VOLUME_AVG_PERIOD, APPROACH_THRESHOLD, APPROACH_COOLDOWN,
    APPROACH_LEVELS, SESSIONS, ALERT_BATCH_WINDOW,
    OI_CHANGE_THRESHOLD, LIQ_SQUEEZE_THRESHOLD, LIQ_ALERT_COOLDOWN, PUBLIC_TEASER_TP_LEVEL,
    PUBLIC_CHAT_ID, PRIVATE_CHAT_ID
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
        self.last_funding_alert  = state.get("last_funding_alert", 0)   # New: Persist funding alert cooldown
        self.last_session_update = time.time()
        self.last_daily_update   = time.time()

        # Macro Trend & Context
        self.macro_trend = "Ranging"
        self.last_oi = 0
        self.last_liqs = 0
        self.last_oi_price = 0
        self.last_liq_alert_time = 0

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
        print(f"  Public Chat:  {PUBLIC_CHAT_ID}")
        print(f"  Private Chat: {PRIVATE_CHAT_ID}")
        print(f"{'='*50}")

        tg.send_startup()
        print("[OK] Startup message sent to Telegram\n")

        # Initial data load
        self._update_levels()

        while True:
            try:
                self._tick()
            except Exception as e:
                print(f"[ERROR] {e}")
                traceback.print_exc()
            
            # Pulse the lock file to stay alive
            if hasattr(self, 'heartbeat_callback'):
                self.heartbeat_callback()

            time.sleep(POLL_INTERVAL)

    def get_price_at_hour(self, target_hour):
        """Fetch the exact opening price for a specific UTC hour today from OKX."""
        try:
            # Fetch 1h klines (limit 24 is plenty for today)
            df = fetch_klines(interval="1h", limit=48)
            if df.empty:
                return None
            
            # Search backwards for the MOST RECENT candle matching this hour
            for i in range(len(df) - 1, -1, -1):
                idx = df.index[i]
                if idx.hour == target_hour:
                    return float(df.iloc[i]["Open"])
        except Exception as e:
            print(f"[ERROR] Failed to fetch price for hour {target_hour}: {e}")
        return None

    def get_session_ohlc(self, start_hour, end_hour):
        """Fetch O, H, L, C for a session period today from OKX."""
        try:
            df = fetch_klines(interval="1h", limit=48)
            if df.empty:
                return None, None, None, None
            
            today = datetime.now(timezone.utc).date()
            # Filter session candles
            if start_hour < end_hour:
                mask = (df.index.date == today) & (df.index.hour >= start_hour) & (df.index.hour < end_hour)
            else: # Crosses midnight
                mask = ((df.index.date == today) & (df.index.hour >= start_hour)) | \
                       ((df.index.date == today) & (df.index.hour < end_hour))
            
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

    def _reconstruct_session_history(self, current_hour):
        """Reconstruct previous session summaries of the day if bot started late."""
        for s_name, s_times in SESSIONS.items():
            if s_name in self.session_history:
                continue
            
            # Check if session is finished
            is_completed = False
            if s_times["open"] < s_times["close"]:
                # Normal: open at 8, close at 16. Finished if current hour >= 16.
                is_completed = current_hour >= s_times["close"]
            else:
                # Cross Midnight: open at 22, close at 6. 
                # Finished if current hour is between 6 and 22.
                is_completed = s_times["close"] <= current_hour < s_times["open"]
            
            if is_completed:
                print(f"[SESSION] Reconstructing history for {s_name}...")
                open_p, high_p, low_p, close_p = self.get_session_ohlc(s_times["open"], s_times["close"])
                
                if open_p and close_p:
                    stats = self.tracker.get_session_stats(s_times["open"], s_times["close"])
                    
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
                today = datetime.now(timezone.utc).strftime("%d.%m.%Y")
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
                    
                    stats = self.tracker.get_session_stats(SESSIONS[s_name]["open"], SESSIONS[s_name]["close"])
                    
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
            chart_df = fetch_klines(interval="1h", limit=48)
            if not chart_df.empty and self.levels:
                return generate_daily_levels_chart(chart_df, self.levels, output_path=output_path, show_sessions=show_sessions)
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

        # 2. Fetch Global context
        self.last_oi = fetch_open_interest()
        self.last_liqs = fetch_liquidations()

        # 3. Fetch all timeframes
        data = fetch_all_timeframes()
        if not data: return

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
                        tg.send_funding_alert(rate, direction, chat_id=PUBLIC_CHAT_ID)
                        self.last_funding_alert = current_time

        # 6. Process each scalp timeframe
        latest_price = None
        current_candle_high = 0
        current_candle_low = 9999999
        
        for tf in SIGNAL_TIMEFRAMES:
            if tf not in data: continue
            df = data[tf]
            
            # Update session-level H/L tracking using the current candle's wicks
            last_c = df.iloc[-1]
            current_candle_high = max(current_candle_high, float(last_c["High"]))
            current_candle_low = min(current_candle_low, float(last_c["Low"]))
            
            if latest_price is None:
                latest_price = float(df.iloc[-1]["Close"])

            self._process_timeframe(tf, df, now)

        # ─── Update Performance Tracker & Success Teasers ────
        if latest_price is not None:
            # 1. Success Teasers
            trade_events = self.tracker.check_outcomes(latest_price)
            for event in trade_events:
                sig = event["sig"]
                # Only send teaser if it's the specific TP level we want and it's haven't already sent one for this trade
                if event["type"] == PUBLIC_TEASER_TP_LEVEL and not sig.get("teaser_sent"):
                    sig["teaser_sent"] = True
                    # Calculate profit % (absolute distance from entry to current price)
                    profit = abs(latest_price - sig["entry"]) / sig["entry"] * 100
                    tg.send_success_teaser(sig["side"], sig["tf"], profit, chat_id=PUBLIC_CHAT_ID)
                    self.tracker._save() # Persist the teaser_sent flag
            
            # 2. Liquidation Squeezes
            if self.last_liqs >= LIQ_SQUEEZE_THRESHOLD:
                if current_time - self.last_liq_alert_time > LIQ_ALERT_COOLDOWN:
                    tg.send_squeeze_alert(self.last_liqs, latest_price, chat_id=PRIVATE_CHAT_ID)
                    self.last_liq_alert_time = current_time
                    print(f"  [TG] 🚨 Liquidation Squeeze: ${self.last_liqs/1e6:.1f}M")

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
                            tg.send_oi_divergence(price_chg*100, oi_chg*100, note, chat_id=PRIVATE_CHAT_ID)
                            self._save_state()
                            print(f"  [TG] ⚠️ OI Divergence: {note}")

            # Update baselines for next tick comparison
            self.last_oi_price = latest_price
            self.last_oi_base = self.last_oi

            # --- Session Tracking ---
            today = now.strftime("%d.%m.%Y")
            current_hour = now.hour
            is_weekend = now.weekday() >= 5 # 5=Sat, 6=Sun
            
            if not is_weekend:
                for s_name, s_times in SESSIONS.items():
                    session_id = f"{s_name}_{today}"
                    
                    # Check if session is active now
                    is_active = False
                    if s_times["open"] < s_times["close"]:
                        is_active = s_times["open"] <= current_hour < s_times["close"]
                    else:
                        is_active = current_hour >= s_times["open"] or current_hour < s_times["close"]

                    # Capture Open & Recover H/L from OKX
                    if is_active and session_id not in self.session_data:
                        open_p, high_p, low_p, _ = self.get_session_ohlc(s_times["open"], current_hour + 1)
                        if open_p is None:
                            open_p = latest_price
                        if high_p is None: high_p = current_candle_high
                        if low_p is None: low_p = current_candle_low
                            
                        self.session_data[session_id] = {
                            "open_price": open_p,
                            "high": high_p,
                            "low": low_p,
                            "levels_tested": set()
                        }
                        
                        is_mid = current_hour != s_times["open"]
                        status = "opened" if not is_mid else "active (recovered from OKX)"
                        print(f"[SESSION] {s_name} {status} at {open_p:,.2f}")
                        
                        # Fetch stats so far if mid-session
                        stats = self.tracker.get_session_stats(s_times["open"], s_times["close"])
                        
                        # Construct history string
                        history_text = self._get_history_text(s_name, latest_price)

                        # Generate session chart
                        chart_path = self._generate_current_chart(f"session_open_{s_name}.png")

                        # Notify Telegram
                        resp = tg.send_session_open(
                            session_name=s_name, 
                            open_price=open_p, 
                            current_price=latest_price if is_mid else None,
                            history=history_text,
                            high=self.session_data[session_id]["high"],
                            low=self.session_data[session_id]["low"],
                            chart_path=chart_path,
                            chat_id=PUBLIC_CHAT_ID
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
                    if current_hour == s_times["close"]:
                        sent_key = f"sent_{session_id}"
                        if sent_key not in self.sent_sessions:
                            self.sent_sessions.add(sent_key)
                            
                            s_data = self.session_data.get(session_id, {"open_price": latest_price, "levels_tested": set(), "high": latest_price, "low": latest_price})
                            open_p = s_data["open_price"]
                            s_high = s_data.get("high", latest_price)
                            s_low = s_data.get("low", latest_price)
                            levels = ", ".join(sorted(list(s_data["levels_tested"]))) if s_data["levels_tested"] else "None"
                            
                            stats = self.tracker.get_session_stats(s_times["open"], s_times["close"])
                            
                            # Construct history string
                            history_text = self._get_history_text(s_name, latest_price)
                            
                            # Generate session chart
                            chart_path = self._generate_current_chart(f"session_close_{s_name}.png")

                            tg.send_session_summary(s_name, open_p, latest_price, stats["total"], levels, history=history_text, high=s_high, low=s_low, chart_path=chart_path, chat_id=PUBLIC_CHAT_ID)
                            
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
                            
                            print(f"[SESSION] {s_name} closed. Recap sent.")

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
                        tg.send_batched_alerts(batch_data, chat_id=cid)
                        print(f"[TG] Sent batch of {len(batch_data)} alerts to {cid}")
                    elif len(alerts) == 1:
                        # Send as individual alert
                        alert = alerts[0]
                        if alert["callback"]:
                            # All tg functions now accept chat_id
                            kwargs = {"chat_id": cid}
                            alert["callback"](*alert["args"], **kwargs)
                            print(f"[TG] Sent individual alert: {alert['data']['type']} to {cid}")
                
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
                        res = tg.edit_message_media(info["msg_id"], chart_path, caption=new_html, chat_id=PUBLIC_CHAT_ID)
                        if res == "DELETED":
                            del self.session_msg_ids[s_id]
                            self._save_state()
                    
                    try:
                        import os
                        if os.path.exists(chart_path): os.remove(chart_path)
                    except: pass

        # 2. Daily Levels Update (600s)
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
                        tg.edit_message_media(d_msg_id, chart_path, caption=new_html)

                    try:
                        import os
                        if os.path.exists(chart_path): os.remove(chart_path)
                    except: pass

    def _update_levels_if_needed(self, now):
        """Update levels at the start of a new day."""
        today = now.strftime("%d.%m.%Y")
        if today != self.last_levels_date:
            self.daily_report_msg_id = None # Clear OLD one before sending NEW one
            self._update_levels()
            self._send_daily_report(now)
            
            self.last_levels_date = today
            self.sent_signals.clear()  # Reset duplicate tracking
            self.session_history.clear() # Reset session history for new day
            self.session_msg_ids.clear() # Reset message IDs for new day
            self.session_data.clear()    # Clear old session data for new day
            self.sent_sessions.clear()   # New: Reset session recap tracking
            self.approach_alerts.clear() # New: Reset level approach tracking
            self._save_state()
        
        self._reconstruct_session_history(now.hour)

    def _update_levels(self):
        """Fetch daily/weekly/monthly data and calculate levels."""
        print("[LEVELS] Updating daily/weekly/monthly levels...")

        daily_df   = fetch_daily()
        weekly_df  = fetch_weekly()
        monthly_df = fetch_monthly()

        self.levels = calculate_levels(daily_df, weekly_df, monthly_df)

        if self.levels:
            do = self.levels.get("DO", 0)
            print(f"  DO: {do:,.2f}")
            print(f"  PDH: {self.levels.get('PDH', 0):,.2f}  PDL: {self.levels.get('PDL', 0):,.2f}")
            print(f"  PWH: {self.levels.get('PWH', 0):,.2f}  PWL: {self.levels.get('PWL', 0):,.2f}")
            print(f"  PMH: {self.levels.get('PMH', 0):,.2f}  PML: {self.levels.get('PML', 0):,.2f}")
            print(f"  Pump: {self.levels.get('Pump', 0):,.2f}  Dump: {self.levels.get('Dump', 0):,.2f}")
            print(f"  PumpMax: {self.levels.get('PumpMax', 0):,.2f}  DumpMax: {self.levels.get('DumpMax', 0):,.2f}")

    def _send_daily_report(self, now):
        """Send daily levels report to Telegram."""
        if not self.levels:
            return

        # --- Performance Summary first ---
        try:
            stats = self.tracker.get_daily_summary()
            if stats:
                tg.send_performance_summary(stats, chat_id=PUBLIC_CHAT_ID)
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
            chat_id=PUBLIC_CHAT_ID
        )
        
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
        
        print("[TG] Daily levels report sent")

    def _process_timeframe(self, tf, df, now):
        """Process one timeframe: channels, momentum, signals."""

        # ─── Calculate indicators ────────────────────────
        df = calculate_channels(df)
        df = calculate_momentum(df)

        if df.empty or len(df) < 2:
            return

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        price_high = float(curr["High"])
        price_low  = float(curr["Low"])
        close      = float(curr["Close"])
        atr_val    = float(curr["ATR"]) if "ATR" in curr else 0
        zone       = curr["MomentumZone"] if "MomentumZone" in curr else "NEUTRAL"

        # ─── REAL-TIME MONITOR (Debug) ───────────────────

        prev_high = float(prev["High"])
        prev_low  = float(prev["Low"])

        candle_ts = curr.name.strftime("%Y-%m-%d %H:%M") if hasattr(curr.name, 'strftime') else str(curr.name)
        current_time = time.time()
        
        def record_level(lvl):
            # Use now from outer scope
            today = now.strftime("%d.%m.%Y")
            current_hour = now.hour
            for s_name, s_times in SESSIONS.items():
                # Check if session is currently active
                is_active = False
                if s_times["open"] < s_times["close"]:
                    is_active = s_times["open"] <= current_hour < s_times["close"]
                else: # Crosses midnight
                    is_active = current_hour >= s_times["open"] or current_hour < s_times["close"]
                
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

        # ─── Price Approaching Key Levels ────────────────
        if tf == "1h" and self.levels:
            for lvl_name in APPROACH_LEVELS:
                lvl_price = self.levels.get(lvl_name)
                if lvl_price:
                    dist_pct = abs(close - lvl_price) / lvl_price
                    if dist_pct <= APPROACH_THRESHOLD:
                        last_alert = self.approach_alerts.get(lvl_name, 0)
                        if current_time - last_alert > APPROACH_COOLDOWN:
                            self.queue_alert(
                                alert_dict={
                                    "type": "APPROACHING LEVEL",
                                    "note": f"Approaching {lvl_name} ({dist_pct*100:.2f}%)"
                                },
                                callback=tg.send_approaching_level,
                                args=(lvl_name, lvl_price, close, dist_pct * 100),
                                chat_id=PUBLIC_CHAT_ID
                            )
                            self.approach_alerts[lvl_name] = current_time
                            self._save_state() # Save approach timestamp

        # ─── Liquidity Sweeps ────────────────────────────
        if self.levels:
            sweeps = check_liquidity_sweep(
                price_high, price_low, self.levels,
                prev_high=prev_high, prev_low=prev_low
            )
            for sw in sweeps:
                # Always record for session tracking
                record_level(sw['level'])

                # IMPORTANT: Use a TF-independent key for Global Levels 
                # to prevent duplicates across 5m/15m/1h/4h
                sig_key = f"sweep_{sw['level']}_{sw['side']}_{now.strftime('%Y-%m-%d')}"
                if sig_key not in self.sent_signals:
                    self.sent_signals.add(sig_key)
                    tg.send_liquidity_sweep(**sw, chat_id=PUBLIC_CHAT_ID)
                    self._save_state() # Save immediately to avoid double sends
                    print(f"  [TG] Liquidity Sweep: {sw['level']} ({sw['side']})")

                    # Add to confirmation tracker
                    self.confirmations.add_signal({
                        "side":      sw["side"],
                        "indicator": f"Ponch_RangeTrader_Sweep_{sw['level']}",
                        "signal":    f"LIQUIDITY SWEEP: {sw['level']}",
                        "points":    sw["points"],
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
                    tg.send_volatility_touch(**vt, chat_id=PRIVATE_CHAT_ID)
                    self._save_state()
                    print(f"  [TG] Vol Zone Touch: {vt['level']} ({vt['side']})")

                    # Add to confirmation tracker
                    self.confirmations.add_signal({
                        "side":      vt["side"],
                        "indicator": f"Ponch_RangeTrader_VolZone_{vt['level']}",
                        "signal":    f"VOL ZONE TOUCH: {vt['level']}",
                        "points":    vt["points"],
                    })

        # ─── Trade Signals (Channels) ────────────────────
        ch_sigs = check_channel_signals(df)
        for sig in ch_sigs:
            sig_key = f"tr_sig_{tf}_{sig['signal']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                print(f"  [SIG] Trade Signal [{tf}] {sig['signal']} @ {sig['price']:,.2f}")
                self.confirmations.add_signal(sig)

        # 2. Momentum confirmation
        mom_sigs = check_momentum_confirm(df)
        for sig in mom_sigs:
            sig_key = f"mom_sig_{tf}_{sig['side']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                self.confirmations.add_signal(sig)

        # 3. Range trader confirmation
        rng_sigs = check_range_confirm(df, self.levels)
        for sig in rng_sigs:
            sig_key = f"rng_sig_{tf}_{sig['signal']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                self.confirmations.add_signal(sig)

        # 4. Flow confirmation
        flow_sigs = check_flow_confirm(df)
        for sig in flow_sigs:
            sig_key = f"flow_sig_{tf}_{sig['side']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                self.confirmations.add_signal(sig)

        # ─── Scalp Momentum System ───────────────────────
        tracker = self.scalp_trackers[tf]
        events = tracker.update(zone, close, atr_val, candle_ts=candle_ts)

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
                self._save_state()
                print(f"  [TG] Scalp Open [{tf}] {evt['side']}")

            elif evt["type"] == "PREPARE":
                tg.send_scalp_prepare(tf, evt["side"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self._save_state()
                print(f"  [TG] Prepare [{tf}] {evt['side']}")

            elif evt["type"] == "CONFIRMED":
                # --- Calculate Signal Strength Score ---
                score, reasons = calculate_signal_score(
                    evt, df, self.levels, self.macro_trend, self.last_oi, self.last_liqs
                )

                tg.send_scalp_confirmed(
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
                    emoji=emoji,
                    chat_id=PRIVATE_CHAT_ID
                )
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
                    timestamp=candle_ts
                )

            elif evt["type"] == "CLOSED":
                tg.send_scalp_closed(tf, evt["side"], evt["price"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self._save_state()
                print(f"  [TG] Scalp Closed [{tf}] {evt['side']}")

        # ─── Check Confirmation Aggregation ──────────────
        for side in ["LONG", "SHORT"]:
            conf_events = self.confirmations.check_confirmations(side)
            for ce in conf_events:
                if ce["type"] == "STRONG":
                    # Calculate targets for confluence
                    sl_m, tp1_m, tp2_m, tp3_m = 0.7, 0.7, 1.4, 2.1 # Standard multipliers
                    if side == "LONG":
                        sl_c  = close - atr_val * sl_m
                        tp1_c = close + atr_val * tp1_m
                        tp2_c = close + atr_val * tp2_m
                        tp3_c = close + atr_val * tp3_m
                    else:
                        sl_c  = close + atr_val * sl_m
                        tp1_c = close - atr_val * tp1_m
                        tp2_c = close - atr_val * tp2_m
                        tp3_c = close - atr_val * tp3_m

                    tg.send_strong(
                        side=ce["side"],
                        total_points=ce["points"],
                        confirmations=ce["confirmations"],
                        indicators_list=ce["indicators"],
                        price=close,
                        sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                        chat_id=PRIVATE_CHAT_ID
                    )
                    self._save_state() # Save confirmation send state
                    print(f"  [TG] ✅ STRONG {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                elif ce["type"] == "EXTREME":
                    # Calculate targets for confluence
                    sl_m, tp1_m, tp2_m, tp3_m = 0.7, 0.7, 1.4, 2.1 # Standard multipliers
                    if side == "LONG":
                        sl_c  = close - atr_val * sl_m
                        tp1_c = close + atr_val * tp1_m
                        tp2_c = close + atr_val * tp2_m
                        tp3_c = close + atr_val * tp3_m
                    else:
                        sl_c  = close + atr_val * sl_m
                        tp1_c = close - atr_val * tp1_m
                        tp2_c = close - atr_val * tp2_m
                        tp3_c = close - atr_val * tp3_m

                    tg.send_extreme(
                        side=ce["side"],
                        total_points=ce["points"],
                        confirmations=ce["confirmations"],
                        indicators_list=ce["indicators"],
                        price=close,
                        sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                        chat_id=PRIVATE_CHAT_ID
                    )
                    self._save_state() # Save confirmation send state
                    print(f"  [TG] 🔥 EXTREME {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

        # ─── Store prev candle data ──────────────────────
        self.prev_candles[tf] = {
            "High": price_high,
            "Low":  price_low,
        }


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import os

    lock_file = "bot.lock"
    
    # Check if another instance is running
    if os.path.exists(lock_file):
        # Try to remove it. If it's locked by another process (on some OS) or exists, 
        # we check the timestamp. If it was updated in the last 30s, we assume bot is alive.
        import time
        if time.time() - os.path.getmtime(lock_file) < 60:
            print("\n[!] FATAL: Another instance of PonchBot is already running.")
            print("[!] If you are sure it's not, delete 'bot.lock' and try again.\n")
            sys.exit(1)
    
    # Create/Touch lock file
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
