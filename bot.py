# ─── Ponch Signal System — Main Bot ───────────────────────────

"""
Main entry point. Monitors BTCUSDT across multiple timeframes,
detects signals, and sends formatted Telegram alerts.
"""

import time
import traceback
from datetime import datetime, timezone

from config import (
    SYMBOL, SCALP_TIMEFRAMES, POLL_INTERVAL,
    TIMEFRAME_PROFILES, FUNDING_THRESHOLD, FUNDING_CHECK_INTERVAL,
    FUNDING_COOLDOWN, VOLUME_SPIKE_MULT, VOLUME_SPIKE_TIMEFRAMES,
    VOLUME_AVG_PERIOD, APPROACH_THRESHOLD, APPROACH_COOLDOWN,
    APPROACH_LEVELS, SESSIONS
)
from data import fetch_klines, fetch_daily, fetch_weekly, fetch_monthly, fetch_funding_rate
from levels import calculate_levels, check_liquidity_sweep, check_volatility_touch
from channels import calculate_channels, check_channel_signals
from momentum import calculate_momentum, ScalpTracker
from signals import check_momentum_confirm, check_range_confirm, check_flow_confirm
from confirmation import ConfirmationTracker
from charting import generate_daily_levels_chart
from tracker import SignalTracker
import telegram as tg


class PonchBot:
    """Main Ponch Signal System bot."""

    def __init__(self):
        # Scalp trackers — one per timeframe
        self.scalp_trackers = {
            tf: ScalpTracker(tf) for tf in SCALP_TIMEFRAMES
        }

        # Confirmation aggregation
        self.confirmations = ConfirmationTracker()

        # Previous candle data for cross detection
        self.prev_candles = {}   # {timeframe: {"High": ..., "Low": ...}}

        # Track sent signals to avoid duplicates
        self.sent_signals = set()

        # Daily levels
        self.levels = {}
        self.last_levels_date = None

        # ─── New Features ─────────────────────────────────
        self.tracker = SignalTracker()
        self.approach_alerts = {}      # { "Pump": timestamp }
        self.last_funding_check = 0
        self.last_funding_alert = 0
        self.sent_sessions = set()     # "session_LONDON_2023-10-14"

        # Alert Batching
        self.pending_alerts = []
        self.batch_timer_start = None

        # Mute state
        self.muted_until = None

    def queue_alert(self, alert_dict):
        """Queue alert for batching."""
        if self.muted_until and datetime.now(timezone.utc) < self.muted_until:
            return  # Suppress alerts if muted
        self.pending_alerts.append(alert_dict)
        if self.batch_timer_start is None:
            self.batch_timer_start = time.time()


    def run(self):
        """Main loop — fetches data and processes signals."""

        print(f"{'='*50}")
        print(f"  Ponch Signal System (v2)")
        print(f"  Symbol: {SYMBOL}")
        print(f"  Timeframes: {', '.join(SCALP_TIMEFRAMES)}")
        print(f"  Poll interval: {POLL_INTERVAL}s")
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

            time.sleep(POLL_INTERVAL)

    def _tick(self):
        """One iteration of the main loop."""
        now = datetime.now(timezone.utc)
        current_time = time.time()
        print(f"\n[{now.strftime('%H:%M:%S')} UTC] Fetching data...")

        # ─── Check if new day → update levels & send report ──
        today = now.strftime("%d.%m.%Y")
        if today != self.last_levels_date:
            self._update_levels()
            self._send_daily_report(now)
            self.last_levels_date = today
            self.sent_signals.clear()  # Reset duplicate tracking

        # ─── Check Funding Rate ──────────────────────────────
        if current_time - self.last_funding_check > FUNDING_CHECK_INTERVAL:
            self.last_funding_check = current_time
            rate = fetch_funding_rate()
            if rate is not None:
                if abs(rate) >= FUNDING_THRESHOLD:
                    if current_time - self.last_funding_alert > FUNDING_COOLDOWN:
                        direction = "POSITIVE" if rate > 0 else "NEGATIVE"
                        self.queue_alert({
                            "type": "FUNDING ALERT",
                            "note": f"{direction} Funding Rate: {rate*100:.4f}%"
                        })
                        tg.send_funding_alert(rate, direction)
                        self.last_funding_alert = current_time

        # ─── Check Session Closes ────────────────────────────
        current_hour = now.hour
        for s_name, s_times in SESSIONS.items():
            if current_hour == s_times["close"]:
                session_key = f"session_{s_name}_{today}"
                if session_key not in self.sent_sessions:
                    self.sent_sessions.add(session_key)
                    # Get session recap data
                    stats = self.tracker.get_session_stats(s_times["open"], s_times["close"])
                    # Send telegram msg (price data mock for now, we'd need exact open/close, simplifying to just stats or 0)
                    tg.send_session_summary(s_name, 0, 0, stats["total"], "Active")

        # ─── Process each scalp timeframe ────────────────────
        latest_price = None
        for tf in SCALP_TIMEFRAMES:
            df = fetch_klines(interval=tf, limit=200)
            if df.empty:
                continue

            if latest_price is None:
                latest_price = float(df.iloc[-1]["Close"])

            self._process_timeframe(tf, df)

        # ─── Update Performance Tracker ──────────────────────
        if latest_price is not None:
            self.tracker.check_outcomes(latest_price)

        # ─── Flush Batched Alerts ────────────────────────────
        if self.pending_alerts and self.batch_timer_start:
            # If 30 seconds have passed since the first queued alert, flush
            if current_time - self.batch_timer_start >= 30:
                tg.send_batched_alerts(self.pending_alerts)
                self.pending_alerts = []
                self.batch_timer_start = None

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
                tg.send_performance_summary(stats)
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
                chart_path = generate_daily_levels_chart(chart_df, self.levels)
        except Exception as e:
            print(f"[CHARTING] Failed to generate: {e}")

        tg.send_daily_levels(
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
            chart_path=chart_path
        )
        print("[TG] Daily levels report sent")

    def _process_timeframe(self, tf, df):
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

        candle_ts = curr.name.strftime("%d.%m.%Y %H:%M UTC") if hasattr(curr.name, 'strftime') else ""

        current_time = time.time()

        # ─── Volume Spike Detection ──────────────────────
        if tf in VOLUME_SPIKE_TIMEFRAMES and len(df) > VOLUME_AVG_PERIOD:
            vol_col = df["Volume"]
            avg_vol = vol_col.iloc[-VOLUME_AVG_PERIOD-1:-1].mean()
            current_vol = float(curr["Volume"])
            if avg_vol > 0 and current_vol > (avg_vol * VOLUME_SPIKE_MULT):
                sig_key = f"volspike_{tf}_{candle_ts}"
                if sig_key not in self.sent_signals:
                    self.sent_signals.add(sig_key)
                    self.queue_alert({
                        "type": "VOLUME SPIKE",
                        "tf": tf,
                        "price": close,
                        "note": f"{current_vol/avg_vol:.1f}x average volume"
                    })
                    tg.send_volume_spike(tf, current_vol, avg_vol, current_vol/avg_vol, close)

        # ─── Price Approaching Key Levels ────────────────
        if tf == "1h" and self.levels:
            for lvl_name in APPROACH_LEVELS:
                lvl_price = self.levels.get(lvl_name)
                if lvl_price:
                    dist_pct = abs(close - lvl_price) / lvl_price
                    if dist_pct <= APPROACH_THRESHOLD:
                        last_alert = self.approach_alerts.get(lvl_name, 0)
                        if current_time - last_alert > APPROACH_COOLDOWN:
                            self.queue_alert({
                                "type": "APPROACHING LEVEL",
                                "note": f"Approaching {lvl_name} ({dist_pct*100:.2f}%)"
                            })
                            tg.send_approaching_level(lvl_name, lvl_price, close, dist_pct * 100)
                            self.approach_alerts[lvl_name] = current_time

        # ─── Liquidity Sweeps ────────────────────────────
        if self.levels:
            sweeps = check_liquidity_sweep(
                price_high, price_low, self.levels,
                prev_high=prev_high, prev_low=prev_low
            )
            for sw in sweeps:
                # Include candle timestamp in deduplication key
                sig_key = f"sweep_{sw['level']}_{sw['side']}_{candle_ts}"
                if sig_key not in self.sent_signals:
                    self.sent_signals.add(sig_key)
                    tg.send_liquidity_sweep(**sw)
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
                # Include candle timestamp in deduplication key
                sig_key = f"vol_{vt['level']}_{vt['side']}_{candle_ts}"
                if sig_key not in self.sent_signals:
                    self.sent_signals.add(sig_key)
                    tg.send_volatility_touch(**vt)
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
                self.confirmations.adxd_signal(sig)

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
                tg.send_scalp_open(tf, evt["side"], evt["price"], emoji=emoji)
                print(f"  [TG] Scalp Open [{tf}] {evt['side']}")

            elif evt["type"] == "PREPARE":
                tg.send_scalp_prepare(tf, evt["side"], emoji=emoji)
                print(f"  [TG] Prepare [{tf}] {evt['side']}")

            elif evt["type"] == "CONFIRMED":
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
                    emoji=emoji,
                )
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
                tg.send_scalp_closed(tf, evt["side"], evt["price"], emoji=emoji)
                print(f"  [TG] Scalp Closed [{tf}] {evt['side']}")

        # ─── Check Confirmation Aggregation ──────────────
        for side in ["LONG", "SHORT"]:
            conf_events = self.confirmations.check_confirmations(side)
            for ce in conf_events:
                if ce["type"] == "STRONG":
                    tg.send_strong(
                        side=ce["side"],
                        total_points=ce["points"],
                        confirmations=ce["confirmations"],
                        indicators_list=ce["indicators"],
                    )
                    print(f"  [TG] ✅ STRONG {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                elif ce["type"] == "EXTREME":
                    tg.send_extreme(
                        side=ce["side"],
                        total_points=ce["points"],
                        confirmations=ce["confirmations"],
                        indicators_list=ce["indicators"],
                    )
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
    bot = PonchBot()
    bot.run()
