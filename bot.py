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
    TIMEFRAME_PROFILES,
)
from data import fetch_klines, fetch_daily, fetch_weekly, fetch_monthly
from levels import calculate_levels, check_liquidity_sweep, check_volatility_touch
from channels import calculate_channels, check_channel_signals
from momentum import calculate_momentum, ScalpTracker
from signals import check_momentum_confirm, check_range_confirm, check_flow_confirm
from confirmation import ConfirmationTracker
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

    def run(self):
        """Main loop — fetches data and processes signals."""
        print(f"{'='*50}")
        print(f"  Ponch Signal System")
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
        print(f"\n[{now.strftime('%H:%M:%S')} UTC] Fetching data...")

        # ─── Check if new day → update levels & send report ──
        today = now.strftime("%d.%m.%Y")
        if today != self.last_levels_date:
            self._update_levels()
            self._send_daily_report(now)
            self.last_levels_date = today
            self.sent_signals.clear()  # Reset duplicate tracking

        # ─── Process each scalp timeframe ────────────────────
        for tf in SCALP_TIMEFRAMES:
            df = fetch_klines(interval=tf, limit=200)
            if df.empty:
                continue

            self._process_timeframe(tf, df)

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

        date_str = now.strftime("%d.%m.%Y")
        do = self.levels["DO"]

        tg.send_daily_levels(
            date_str=date_str,
            daily_open=do,
            resistance=self.levels["Resistance"],
            resistance_pct=self.levels["ResistancePct"],
            support=self.levels["Support"],
            support_pct=self.levels["SupportPct"],
            volatility=self.levels["Volatility"],
            volatility_pct=self.levels["VolatilityPct"],
            critical_high=self.levels["CriticalHigh"],
            critical_low=self.levels["CriticalLow"],
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

        # ─── Liquidity Sweeps ────────────────────────────
        if self.levels:
            sweeps = check_liquidity_sweep(
                price_high, price_low, self.levels,
                prev_high=prev_high, prev_low=prev_low
            )
            for sw in sweeps:
                # Include candle timestamp in deduplication key
                sig_key = f"sweep_{tf}_{sw['level']}_{sw['side']}_{candle_ts}"
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
                sig_key = f"vol_{tf}_{vt['level']}_{vt['side']}_{candle_ts}"
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
                tg.send_trade_signal(
                    tf=tf,
                    side=sig["side"],
                    signal=sig["signal"],
                    price=sig["price"],
                    indicator=sig["indicator"],
                    points=sig["points"],
                    strength=sig["strength"],
                    timestamp=candle_ts
                )
                print(f"  [TG] Trade Signal [{tf}] {sig['signal']} @ {sig['price']:,.2f}")
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
