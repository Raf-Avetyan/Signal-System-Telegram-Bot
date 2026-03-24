# в”Ђв”Ђв”Ђ Ponch Signal System вЂ” Main Bot в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

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
    FAST_MOVE_THRESHOLD, FAST_MOVE_WINDOW, FAST_MOVE_COOLDOWN,
    BITUNIX_REG_LINK, INVITE_LINK, COMMAND_POLL_INTERVAL,
    SCALP_TREND_FILTER_MODE, SCALP_COUNTERTREND_MIN_SCORE,
    SCALP_OPEN_ALERT_COOLDOWN, SCALP_COUNTERTREND_MAX_PER_WINDOW,
    SCALP_COUNTERTREND_WINDOW_SEC, SCALP_LOSS_STREAK_LIMIT,
    SCALP_LOSS_COOLDOWN_SEC, VOLATILITY_FILTER_ENABLED,
    VOLATILITY_MIN_ATR_PCT, VOLATILITY_MAX_ATR_PCT,
    SESSION_SCALP_MODE, ORDERFLOW_SAFETY_ENABLED,
    ORDERFLOW_ANOMALY_SCORE_MIN, ORDERFLOW_OI_PCT_ANOMALY,
    ORDERFLOW_LIQ_ANOMALY_USD, SCALP_MIN_SCORE_BY_TF,
    SCALP_ALLOWED_SESSIONS_BY_TF, SCALP_RELAXED_FILTERS,
    SCALP_TREND_FILTER_MODE_BY_TF, SCALP_COUNTERTREND_MIN_SCORE_BY_TF,
    SCALP_RELAX_MIN_SCORE_DELTA, SCALP_RELAX_VOL_MIN_MULT,
    SCALP_RELAX_VOL_MAX_MULT, SCALP_RELAX_COUNTERTREND_EXTRA,
    SCALP_RELAX_ALLOW_OFFSESSION, SCALP_REGIME_SWITCHING,
    SCALP_REGIME_PROFILES, SCALP_SELF_TUNING_ENABLED,
    SCALP_SELF_TUNE_LOOKBACK, SCALP_SELF_TUNE_MIN_CLOSED,
    SCALP_SELF_TUNE_LOW_WR, SCALP_SELF_TUNE_HIGH_WR,
    SCALP_SELF_TUNE_LOW_AVGR, SCALP_SELF_TUNE_HIGH_AVGR,
    SCALP_EXPOSURE_ENABLED, SCALP_MAX_OPEN_TOTAL,
    SCALP_MAX_OPEN_PER_SIDE, SCALP_MAX_OPEN_PER_TF,
    MOMENTUM_OS, MOMENTUM_OB,
    CONFIRMATION_RSI_EXHAUSTION_BUFFER, CONFLUENCE_OPPOSITE_LOCK_SEC,
    LIQ_POOL_ALERT_ENABLED, LIQ_POOL_MIN_USD, LIQ_POOL_ALERT_COOLDOWN,
    LIQ_POOL_BIAS_SCORE_BONUS, LIQ_POOL_MAX_DISTANCE_ATR_MULT,
    LIQ_POOL_MIN_DISTANCE_PCT, LIQ_POOL_HUGE_USD_OVERRIDE,
    TP_LIQUIDITY_MIN_USD, TP_LIQUIDITY_BAND_PCT,
    FALLING_KNIFE_FILTER_ENABLED, FALLING_KNIFE_LOOKBACK_5M, FALLING_KNIFE_LOOKBACK_15M,
    FALLING_KNIFE_MOVE_PCT_5M, FALLING_KNIFE_MOVE_PCT_15M,
    LIQ_POOL_REPORT_TIMEFRAMES, LIQ_POOL_MIN_USD_BY_TF, LIQ_POOL_MIN_DISTANCE_PCT_BY_TF,
    LIQ_POOL_NO_MOVE_RANGE_PCT_1H, LIQ_POOL_EXPANSION_PRICE_MOVE_PCT_1H,
    LIQ_POOL_EXPANSION_VOLUME_MULT, LIQ_POOL_EXPANSION_BOOK_MULT, LIQ_POOL_EXPANSION_COOLDOWN
)
from levels import calculate_levels, check_liquidity_sweep, check_volatility_touch
from channels import calculate_channels, check_channel_signals
from momentum import calculate_momentum, ScalpTracker, detect_trend
from scoring import calculate_signal_score
from signals import check_momentum_confirm, check_range_confirm, check_flow_confirm, check_rsi_divergence
from confirmation import ConfirmationTracker
from charting import generate_daily_levels_chart
from data import (
    fetch_klines, fetch_all_timeframes, fetch_daily, fetch_weekly, fetch_monthly, 
    fetch_funding_rate, fetch_open_interest, fetch_liquidations, fetch_global_indicators, fetch_order_book
)
from tracker import SignalTracker
from bitunix import verify_bitunix_user
from liquidity_map import detect_liquidity_event
import telegram as tg


class PonchBot:
    """Main Ponch Signal System bot."""

    def __init__(self):
        # Scalp trackers вЂ” one per timeframe
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

        # в”Ђв”Ђв”Ђ New Features в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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
        self.last_scalp_open_alert = state.get("last_scalp_open_alert", {})
        self.scalp_countertrend_hits = state.get("scalp_countertrend_hits", {"LONG": [], "SHORT": []})
        self.scalp_loss_streak = state.get("scalp_loss_streak", {"LONG": 0, "SHORT": 0})
        self.scalp_side_cooldown_until = state.get("scalp_side_cooldown_until", {"LONG": 0, "SHORT": 0})
        self.confluence_side_lock_until = state.get("confluence_side_lock_until", {"LONG": 0, "SHORT": 0})
        self.liq_pool_alerts = state.get("liq_pool_alerts", {})
        self.liquidity_bias = state.get("liquidity_bias", {})
        self.last_liq_pool_report_hour = state.get("last_liq_pool_report_hour")
        self.last_liq_session_reports = state.get("last_liq_session_reports", {})
        self.last_liq_expansion_alert = float(state.get("last_liq_expansion_alert", 0) or 0)
        self.last_book_total_usd = float(state.get("last_book_total_usd", 0) or 0)
        self.last_liq_candidates = []
        self.last_order_book = None
        self.last_session_update = time.time()
        self.last_daily_update   = time.time()
        self.last_update_id      = state.get("last_update_id", 0)
        self.last_command_check  = 0

        # Macro Trend & Context
        self.macro_trend = "Ranging"
        self.market_regime = "RANGE"
        self.scalp_tuning_state = "NEUTRAL"
        self.last_oi = 0
        self.last_oi_base = 0
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

    def _get_current_session_name(self, now):
        """Return active session name (ASIA/LONDON/NY) or None."""
        sessions = get_adjusted_sessions(now)
        current_float_hour = now.hour + now.minute / 60.0
        for s_name, s_times in sessions.items():
            s_open = s_times["open"]
            s_close = s_times["close"]
            if s_open < s_close:
                if s_open <= current_float_hour < s_close:
                    return s_name
            else:
                if current_float_hour >= s_open or current_float_hour < s_close:
                    return s_name
        return None

    def _is_orderflow_anomaly(self):
        """Check if order-flow context is anomalous and risky for fresh scalp entries."""
        oi_pct = 0.0
        if self.last_oi and self.last_oi_base:
            oi_pct = abs((self.last_oi / self.last_oi_base - 1) * 100)
        liq_spike = self.last_liqs >= ORDERFLOW_LIQ_ANOMALY_USD
        oi_spike = oi_pct >= ORDERFLOW_OI_PCT_ANOMALY
        return oi_spike or liq_spike, oi_pct

    def _detect_market_regime(self, df_1h):
        """Classify current market regime for scalp filters."""
        if df_1h is None or df_1h.empty or len(df_1h) < 80:
            return "RANGE"
        close = df_1h["Close"]
        ema21 = close.ewm(span=21, adjust=False).mean()
        ema55 = close.ewm(span=55, adjust=False).mean()
        atr_series = df_1h["ATR"] if "ATR" in df_1h.columns else None

        curr_close = float(close.iloc[-1])
        spread_pct = abs(float(ema21.iloc[-1] - ema55.iloc[-1])) / curr_close * 100 if curr_close else 0.0
        slope_pct = abs(float(ema21.iloc[-1] - ema21.iloc[-6])) / curr_close * 100 if len(ema21) >= 6 and curr_close else 0.0
        atr_pct = float(atr_series.iloc[-1] / curr_close * 100) if atr_series is not None and curr_close else 0.0

        if atr_pct >= 1.2:
            return "HIGH_VOL"
        if spread_pct >= 0.35 and slope_pct >= 0.20:
            return "TREND"
        return "RANGE"

    def _update_liquidity_pool_context(self, data, latest_price, now_ts):
        """
        Build multi-timeframe liquidity-pool context from OKX order book and update bias.
        Does not send alerts directly; reporting is handled by _maybe_send_liquidity_pool_report().
        """
        self.liquidity_bias = {}
        self.last_liq_candidates = []
        if not LIQ_POOL_ALERT_ENABLED or latest_price is None or latest_price <= 0:
            return

        order_book = fetch_order_book()
        if not order_book:
            return
        self.last_order_book = order_book
        self.last_book_total_usd = (
            sum(float(px) * float(sz) for px, sz in order_book.get("bids", []))
            + sum(float(px) * float(sz) for px, sz in order_book.get("asks", []))
        )

        candidates = []
        for tf in LIQ_POOL_REPORT_TIMEFRAMES:
            df_tf = data.get(tf)
            if df_tf is None or df_tf.empty:
                continue
            try:
                df_tf_ch = calculate_channels(df_tf)
                atr_val = float(df_tf_ch.iloc[-1].get("ATR", 0) or 0)
            except Exception:
                atr_val = 0
            if atr_val <= 0:
                continue

            tf_min_usd = float(LIQ_POOL_MIN_USD_BY_TF.get(tf, LIQ_POOL_MIN_USD))
            tf_min_dist = float(LIQ_POOL_MIN_DISTANCE_PCT_BY_TF.get(tf, LIQ_POOL_MIN_DISTANCE_PCT))
            event = detect_liquidity_event(
                order_book=order_book,
                price=float(latest_price),
                atr=atr_val,
                timeframe=tf,
                min_usd=tf_min_usd,
                max_distance_atr_mult=float(LIQ_POOL_MAX_DISTANCE_ATR_MULT.get(tf, 1.5)),
                min_distance_pct=tf_min_dist,
                huge_usd_override=float(LIQ_POOL_HUGE_USD_OVERRIDE),
            )
            if not event:
                continue
            candidates.append(event)

        if not candidates:
            return

        self.last_liq_candidates = candidates
        best_event = max(candidates, key=lambda e: e.get("score", 0))
        self.liquidity_bias = {
            "side": best_event["side"],
            "timeframe": best_event["timeframe"],
            "level_price": best_event["level_price"],
            "probability_pct": best_event["probability_pct"],
            "size_usd": best_event["size_usd"],
        }

    def _maybe_send_liquidity_pool_report(self, data, latest_price, now, now_ts):
        """Send liquidity pool reports on hourly/range, session open, and expansion triggers."""
        candidates = list(self.last_liq_candidates or [])
        if not candidates or latest_price is None or latest_price <= 0:
            return

        by_tf = {}
        for evt in candidates:
            tf = evt.get("timeframe")
            if tf not in by_tf or evt.get("score", 0) > by_tf[tf].get("score", 0):
                by_tf[tf] = evt

        if not by_tf:
            return

        trigger = None

        # 1) Hourly report only when market is relatively flat (no movement).
        df5 = data.get("5m")
        if df5 is not None and len(df5) >= 13:
            hour_range = abs(float(df5["Close"].iloc[-1]) / float(df5["Close"].iloc[-13]) - 1.0) * 100.0
            hour_key = now.strftime("%Y-%m-%d %H")
            if hour_range <= float(LIQ_POOL_NO_MOVE_RANGE_PCT_1H) and self.last_liq_pool_report_hour != hour_key:
                trigger = f"Hourly (Range {hour_range:.2f}%)"
                self.last_liq_pool_report_hour = hour_key

        # 2) Session open report (ASIA/LONDON/NY), once per session per date.
        sessions = get_adjusted_sessions(now)
        now_h = now.hour + now.minute / 60.0
        for s_name, s_times in sessions.items():
            s_open = float(s_times["open"])
            near_open = abs(now_h - s_open) <= (10.0 / 60.0)  # first 10 minutes after open
            sid = f"{s_name}_{now.strftime('%Y-%m-%d')}"
            if near_open and self.last_liq_session_reports.get(sid) != 1:
                trigger = f"{s_name} Session Open"
                self.last_liq_session_reports[sid] = 1
                break

        # 3) Expansion trigger: price move + volume spike + order-book growth.
        if df5 is not None and len(df5) >= 13:
            move_1h = abs(float(df5["Close"].iloc[-1]) / float(df5["Close"].iloc[-13]) - 1.0) * 100.0
            vol_now = float(df5["Volume"].iloc[-1])
            vol_avg = float(df5["Volume"].iloc[-21:-1].mean()) if len(df5) >= 21 else max(1.0, vol_now)
            vol_mult = (vol_now / vol_avg) if vol_avg > 0 else 1.0
            prev_book = float(self.last_book_total_usd or 0)
            # last_book_total_usd already updated in _update_liquidity_pool_context this tick;
            # for expansion comparison use previous cached value from state var backup.
            book_mult = 1.0
            if hasattr(self, "_prev_book_total_usd") and float(getattr(self, "_prev_book_total_usd") or 0) > 0:
                book_mult = float(self.last_book_total_usd) / float(getattr(self, "_prev_book_total_usd"))
            self._prev_book_total_usd = float(self.last_book_total_usd)

            if (
                move_1h >= float(LIQ_POOL_EXPANSION_PRICE_MOVE_PCT_1H)
                and vol_mult >= float(LIQ_POOL_EXPANSION_VOLUME_MULT)
                and book_mult >= float(LIQ_POOL_EXPANSION_BOOK_MULT)
                and (now_ts - float(self.last_liq_expansion_alert or 0)) >= float(LIQ_POOL_EXPANSION_COOLDOWN)
            ):
                trigger = f"Expansion (move {move_1h:.2f}% | vol {vol_mult:.1f}x | book {book_mult:.2f}x)"
                self.last_liq_expansion_alert = now_ts

        if not trigger:
            return

        lines = [f"<b>🧲 LIQUIDITY POOL REPORT</b>", f"<pre>Trigger: {trigger}"]
        for tf in LIQ_POOL_REPORT_TIMEFRAMES:
            evt = by_tf.get(tf)
            if not evt:
                lines.append(f"{tf:>3} | {'-':<5} | No valid big pool in range")
                continue
            lines.append(
                f"{evt['timeframe']:>3} | {evt['side']:<5} | Px {evt['level_price']:,.0f} | "
                f"${evt['size_usd']/1e6:,.0f}M | D {evt['distance_pct']:.2f}% | P {evt['probability_pct']:.0f}%"
            )
        lines.append("</pre>")
        msg = "\n".join(lines)
        if not self.is_booting:
            tg.send(msg, parse_mode="HTML", chat_id=PRIVATE_CHAT_ID)
        self._save_state()

    def _estimate_tp_liquidity(self, side, entry, tp1, tp2, tp3):
        """
        Estimate TP hit confidence from visible liquidity around TP levels.
        Returns {"prob": float, "size_usd": float, "target": "TPx"} or None.
        """
        book = self.last_order_book
        if not isinstance(book, dict):
            return None
        rows = book.get("asks", []) if side == "LONG" else book.get("bids", [])
        if not rows:
            return None

        targets = [("TP1", tp1), ("TP2", tp2), ("TP3", tp3)]
        best = None
        for name, tp in targets:
            if not tp or tp <= 0:
                continue
            band = max(tp * (TP_LIQUIDITY_BAND_PCT / 100.0), entry * 0.0004)
            near_usd = 0.0
            for px, sz in rows:
                if abs(float(px) - float(tp)) <= band:
                    near_usd += float(px) * float(sz)
            if near_usd < TP_LIQUIDITY_MIN_USD:
                continue

            size_ratio = min(3.0, near_usd / TP_LIQUIDITY_MIN_USD)
            prob = max(35.0, min(95.0, 30.0 + size_ratio * 22.0))
            item = {"prob": prob, "size_usd": near_usd, "target": name}
            if best is None or item["size_usd"] > best["size_usd"]:
                best = item
        return best

    def _is_unstable_impulse(self, data, side):
        """
        Block counter-impulse confluence entries:
        - LONG during sharp downside impulse without stabilization
        - SHORT during sharp upside impulse without stabilization
        """
        if not FALLING_KNIFE_FILTER_ENABLED:
            return False, ""

        checks = [
            ("5m", int(FALLING_KNIFE_LOOKBACK_5M), float(FALLING_KNIFE_MOVE_PCT_5M)),
            ("15m", int(FALLING_KNIFE_LOOKBACK_15M), float(FALLING_KNIFE_MOVE_PCT_15M)),
        ]
        for tf, lookback, move_thr in checks:
            df = data.get(tf)
            if df is None or df.empty or len(df) < lookback + 1:
                continue

            closes = df["Close"]
            opens = df["Open"]
            prev_close = float(closes.iloc[-(lookback + 1)])
            curr_close = float(closes.iloc[-1])
            if prev_close <= 0:
                continue

            move_pct = (curr_close / prev_close - 1.0) * 100.0
            c1, c2, c3 = float(closes.iloc[-1]), float(closes.iloc[-2]), float(closes.iloc[-3])
            o1, o2, o3 = float(opens.iloc[-1]), float(opens.iloc[-2]), float(opens.iloc[-3])
            red_count = int(c1 < o1) + int(c2 < o2) + int(c3 < o3)
            green_count = int(c1 > o1) + int(c2 > o2) + int(c3 > o3)
            down_streak = c1 < c2 < c3
            up_streak = c1 > c2 > c3
            bounce = c1 > c2 > c3
            pullback = c1 < c2 < c3

            if side == "LONG":
                knife = (move_pct <= -abs(move_thr)) and (down_streak or red_count >= 2)
                if knife and not bounce:
                    return True, f"{tf} impulse {move_pct:+.2f}% (no base)"
            else:  # SHORT
                blowoff = (move_pct >= abs(move_thr)) and (up_streak or green_count >= 2)
                if blowoff and not pullback:
                    return True, f"{tf} impulse {move_pct:+.2f}% (no top)"

        return False, ""

    def _get_scalp_tuning_state(self):
        """Adapt scalp strictness from recent closed scalp performance."""
        if not SCALP_SELF_TUNING_ENABLED:
            return "NEUTRAL", {"trades": 0, "win_rate": 0.0, "avg_r": 0.0}
        health = self.tracker.get_recent_signal_health("SCALP", limit=SCALP_SELF_TUNE_LOOKBACK)
        if health["trades"] < SCALP_SELF_TUNE_MIN_CLOSED:
            return "NEUTRAL", health
        if health["win_rate"] <= SCALP_SELF_TUNE_LOW_WR or health["avg_r"] <= SCALP_SELF_TUNE_LOW_AVGR:
            return "TIGHTEN", health
        if health["win_rate"] >= SCALP_SELF_TUNE_HIGH_WR and health["avg_r"] >= SCALP_SELF_TUNE_HIGH_AVGR:
            return "LOOSEN", health
        return "NEUTRAL", health

    def _get_scalp_exposure(self):
        """Return currently open scalp exposure counts."""
        if not SCALP_EXPOSURE_ENABLED:
            return {"total": 0, "by_side": {}, "by_tf": {}}
        return self.tracker.get_open_signal_counts("SCALP")


    def run(self):
        """Main loop вЂ” fetches data and processes signals."""

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
                # 1. Process Telegram commands
                if time.time() - self.last_command_check > COMMAND_POLL_INTERVAL:
                    self._process_commands()
                    self.last_command_check = time.time()
                
                # 2. Main tick
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
            yesterday = today - timedelta(days=1)
            df["float_hour"] = df.index.hour + df.index.minute / 60.0

            # Filter session candles
            if start_hour < end_hour:
                mask = (df.index.date == today) & (df["float_hour"] >= start_hour - 0.01) & (df["float_hour"] < end_hour - 0.01)
            else: # Crosses midnight
                mask = ((df.index.date == yesterday) & (df["float_hour"] >= start_hour - 0.01)) | \
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
                "last_scalp_open_alert": self.last_scalp_open_alert,
                "scalp_countertrend_hits": self.scalp_countertrend_hits,
                "scalp_loss_streak": self.scalp_loss_streak,
                "scalp_side_cooldown_until": self.scalp_side_cooldown_until,
                "confluence_side_lock_until": self.confluence_side_lock_until,
                "liq_pool_alerts": self.liq_pool_alerts,
                "liquidity_bias": self.liquidity_bias,
                "last_liq_pool_report_hour": self.last_liq_pool_report_hour,
                "last_liq_session_reports": self.last_liq_session_reports,
                "last_liq_expansion_alert": self.last_liq_expansion_alert,
                "last_book_total_usd": self.last_book_total_usd,
                "last_update_id": self.last_update_id,
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

        # 3. Fetch all timeframes (+1d for liquidity-pool context)
        fetch_tfs = list(dict.fromkeys(SIGNAL_TIMEFRAMES + ["1d"]))
        data = fetch_all_timeframes(timeframes=fetch_tfs)
        if not data: return

        # 3.1 Market Alert (Fast Move)
        if "1h" in data:
            df_1h = data["1h"]
            if len(df_1h) >= FAST_MOVE_WINDOW + 1:
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
            self.market_regime = self._detect_market_regime(data["1h"])
            self.scalp_tuning_state, tuning_stats = self._get_scalp_tuning_state()
            print(
                f"  Trend: {self.macro_trend} | Regime: {self.market_regime} | "
                f"SelfTune: {self.scalp_tuning_state} "
                f"(wr={tuning_stats['win_rate']:.1f}% avgR={tuning_stats['avg_r']:+.2f} n={tuning_stats['trades']})"
            )

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
        base_candle_high = None
        base_candle_low = None
        base_candle_ts = None

        # Track reference values for confluence alerts
        ref_atr = 0
        conf_ts = now.strftime("%Y-%m-%d %H:%M")  # Minute-level key for alerts
        ref_ts  = conf_ts
        confluence_rsi = None
        # Collect current candle timestamps for ALL timeframes (for TP protection)
        current_candle_ts_set: set = set()

        for tf in SIGNAL_TIMEFRAMES:
            if tf not in data: continue
            df = data[tf]

            # Update session-level H/L tracking using the current candle's wicks
            last_c = df.iloc[-1]
            current_candle_high = max(current_candle_high, float(last_c["High"]))
            current_candle_low = min(current_candle_low, float(last_c["Low"]))
            if tf == "5m":
                base_candle_high = float(last_c["High"])
                base_candle_low = float(last_c["Low"])

            if latest_price is None:
                latest_price = float(df.iloc[-1]["Close"])

            # Process timeframe and capture ATR/TS for confluence reference
            tf_atr, tf_ts, tf_rsi = self._process_timeframe(tf, df, now, entry_protection_ts=base_candle_ts)

            # Collect this TF's current candle ts for TP skip protection
            if tf_ts:
                current_candle_ts_set.add(tf_ts)
                if tf == "5m":
                    base_candle_ts = tf_ts
                    confluence_rsi = tf_rsi

            # Use 1h ATR for confluence targets if available, otherwise fallback
            if tf == "1h" or ref_atr == 0:
                if tf_atr > 0:
                    ref_atr = tf_atr
                    ref_ts  = tf_ts

        # Check confirmation aggregation (once per tick)
        if latest_price is not None:
            self._update_liquidity_pool_context(data, latest_price, current_time)
            self._maybe_send_liquidity_pool_report(data, latest_price, now, current_time)
            for side in ["LONG", "SHORT"]:
                lock_until = float(self.confluence_side_lock_until.get(side, 0) or 0)
                if current_time < lock_until:
                    wait_m = int((lock_until - current_time) / 60) + 1
                    print(f"  [CONFLUENCE] Blocked {side}: opposite lock active ({wait_m}m left)")
                    continue

                conf_events = self.confirmations.check_confirmations(side)
                for ce in conf_events:
                    # BLOCK DUPLICATES: use a minute-level key for confluence
                    conf_key = f"conf_{side}_{ce['type']}_{conf_ts}"
                    if conf_key in self.sent_signals:
                        continue
                    
                    self.sent_signals.add(conf_key)
                    
                    # --- CONFLUENCE FILTERS & QUALITY CONTROL ---
                    # 1. Trend Alignment: Confluence must match the macro trend
                    trend_ok = self.macro_trend == "Ranging"
                    if side == "LONG" and self.macro_trend in ["Bullish", "Trending Bullish", "Strong Bullish"]: trend_ok = True
                    if side == "SHORT" and self.macro_trend in ["Bearish", "Trending Bearish", "Strong Bearish"]: trend_ok = True
                    
                    if not trend_ok:
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: Against Macro Trend ({self.macro_trend})")
                        continue

                    # 1.5 Falling-knife / blow-off safety filter.
                    impulse_blocked, impulse_note = self._is_unstable_impulse(data, side)
                    if impulse_blocked:
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: {impulse_note}")
                        continue

                    # 2. Momentum exhaustion guard: avoid SHORT when RSI already very low
                    # and LONG when RSI already very high.
                    if confluence_rsi is not None:
                        if side == "SHORT" and confluence_rsi <= (MOMENTUM_OS + CONFIRMATION_RSI_EXHAUSTION_BUFFER):
                            print(
                                f"  [CONFLUENCE] Blocked SHORT {ce['type']}: "
                                f"RSI exhausted low ({confluence_rsi:.1f})"
                            )
                            continue
                        if side == "LONG" and confluence_rsi >= (MOMENTUM_OB - CONFIRMATION_RSI_EXHAUSTION_BUFFER):
                            print(
                                f"  [CONFLUENCE] Blocked LONG {ce['type']}: "
                                f"RSI exhausted high ({confluence_rsi:.1f})"
                            )
                            continue

                    # 3. Proximity Protection: Don't enter Long near levels, etc.
                    proximity_blocked = False
                    block_threshold = 0.0015 # 0.15%
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
                        strong_size = round(min(ce["points"] * 0.3, 5.0), 1)
                        tp_liq = self._estimate_tp_liquidity(side, latest_price, tp1_c, tp2_c, tp3_c)
                        resp = tg.send_strong(
                            side=ce["side"],
                            total_points=ce["points"],
                            confirmations=ce["confirmations"],
                            indicators_list=ce["indicators"],
                            price=latest_price,
                            sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            size=strong_size,
                            tp_liq_prob=tp_liq["prob"] if tp_liq else None,
                            tp_liq_usd=tp_liq["size_usd"] if tp_liq else None,
                            tp_liq_target=tp_liq["target"] if tp_liq else None,
                            chat_id=PRIVATE_CHAT_ID
                        )
                        msg_id = resp.get("result", {}).get("message_id") if resp else None
                        self.tracker.log_signal(
                            side=ce["side"], entry=latest_price, sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            tf="Confluence", timestamp=base_candle_ts or conf_ts,
                            msg_id=msg_id, chat_id=PRIVATE_CHAT_ID, signal_type="STRONG",
                            meta={
                                "indicators": ce["indicators"],
                                "size": strong_size,
                                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                            }
                        )
                        self._save_state()
                        print(f"  [CONFLUENCE] вњ… STRONG {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                    elif ce["type"] == "EXTREME":
                        extreme_size = round(min(ce["points"] * 0.3, 5.0), 1)
                        tp_liq = self._estimate_tp_liquidity(side, latest_price, tp1_c, tp2_c, tp3_c)
                        resp = tg.send_extreme(
                            side=ce["side"],
                            total_points=ce["points"],
                            confirmations=ce["confirmations"],
                            indicators_list=ce["indicators"],
                            price=latest_price,
                            sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            size=extreme_size,
                            tp_liq_prob=tp_liq["prob"] if tp_liq else None,
                            tp_liq_usd=tp_liq["size_usd"] if tp_liq else None,
                            tp_liq_target=tp_liq["target"] if tp_liq else None,
                            chat_id=PRIVATE_CHAT_ID
                        )
                        msg_id = resp.get("result", {}).get("message_id") if resp else None
                        self.tracker.log_signal(
                            side=ce["side"], entry=latest_price, sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            tf="Confluence", timestamp=base_candle_ts or conf_ts,
                            msg_id=msg_id, chat_id=PRIVATE_CHAT_ID, signal_type="EXTREME",
                            meta={
                                "indicators": ce["indicators"],
                                "size": extreme_size,
                                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                            }
                        )
                        self._save_state()
                        print(f"  [CONFLUENCE] рџ”Ґ EXTREME {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                    # Prevent immediate opposite-side flip from stale queued confirmations.
                    opposite_side = "SHORT" if ce["side"] == "LONG" else "LONG"
                    self.confirmations.reset(opposite_side)
                    self.confluence_side_lock_until[opposite_side] = current_time + CONFLUENCE_OPPOSITE_LOCK_SEC
                    self._save_state()

        # в”Ђв”Ђв”Ђ Update Performance Tracker & Success Teasers в”Ђв”Ђв”Ђв”Ђ
        if latest_price is not None:
            # 1. Success Teasers (Public Marketing FOMO)
            # Use the base 5m candle wick range when available to avoid
            # cross-timeframe false hits from long-duration candles (1h/4h).
            outcome_high = base_candle_high if base_candle_high is not None else current_candle_high
            outcome_low = base_candle_low if base_candle_low is not None else current_candle_low
            trade_events = self.tracker.check_outcomes(
                latest_price,
                high=outcome_high,
                low=outcome_low,
                current_candle_ts_set=current_candle_ts_set
            )
            today_str = now.strftime("%Y-%m-%d")

            for event in trade_events:
                sig = event["sig"]
                evt_type = event["type"] # "TP1", "TP2", "TP3", "SL"
                side = sig.get("side")
                risk_state_changed = False

                # --- Loss-streak protection state updates ---
                if side in ("LONG", "SHORT"):
                    if evt_type == "SL":
                        current_streak = int(self.scalp_loss_streak.get(side, 0)) + 1
                        self.scalp_loss_streak[side] = current_streak
                        risk_state_changed = True
                        if current_streak >= SCALP_LOSS_STREAK_LIMIT:
                            self.scalp_side_cooldown_until[side] = current_time + SCALP_LOSS_COOLDOWN_SEC
                            risk_state_changed = True
                            print(
                                f"  [RISK] {side} cooldown armed for {SCALP_LOSS_COOLDOWN_SEC//60}m "
                                f"after streak={current_streak}"
                            )
                    elif evt_type in ("TP3", "PROFIT_SL"):
                        self.scalp_loss_streak[side] = 0
                        risk_state_changed = True
                    elif evt_type == "ENTRY_CLOSE" and int(self.scalp_loss_streak.get(side, 0)) > 0:
                        self.scalp_loss_streak[side] = max(0, int(self.scalp_loss_streak.get(side, 0)) - 1)
                        risk_state_changed = True
                
                # --- LIVE MESSAGE UPDATE ---
                # Update the original signal message with hit markers
                if sig.get("msg_id") and sig.get("chat_id"):
                    tg.update_signal_message(sig["chat_id"], sig["msg_id"], sig)

                    # NEW: Reply if TP2 or TP3 targets hit
                    if evt_type == "TP2":
                        tg.send_tp2_hit_congrats(
                            sig["chat_id"],
                            sig["msg_id"],
                            sig.get("tf", "Unknown"),
                            side=sig.get("side"),
                            lock_price=sig.get("tp1"),
                            entry=sig.get("entry"),
                            sl=sig.get("sl"),
                            tp1=sig.get("tp1"),
                            tp2=sig.get("tp2"),
                            size=(sig.get("meta", {}) or {}).get("size")
                        )
                    elif evt_type == "TP3":
                        tg.send_tp3_hit_congrats(sig["chat_id"], sig["msg_id"], sig.get("tf", "Unknown"))
                    elif evt_type == "ENTRY_CLOSE":
                        tg.send_breakeven_alert(sig["chat_id"], sig["msg_id"], sig.get("tf", "Unknown"))
                    elif evt_type == "PROFIT_SL":
                        tg.send_profit_sl_alert(sig["chat_id"], sig["msg_id"], sig.get("tf", "Unknown"))

                if risk_state_changed:
                    self._save_state()

            # 2. Liquidation Squeezes
            if self.last_liqs >= LIQ_SQUEEZE_THRESHOLD:
                if current_time - self.last_liq_alert_time > LIQ_ALERT_COOLDOWN:
                    if not self.is_booting:
                        tg.send_squeeze_alert(self.last_liqs, latest_price, chat_id=PRIVATE_CHAT_ID)
                    self.last_liq_alert_time = current_time
                    print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} рџљЁ Liquidation Squeeze: ${self.last_liqs/1e6:.1f}M")

            # 3. OI Divergence
            if self.last_oi and self.last_oi_price:
                price_chg = (latest_price / self.last_oi_price) - 1
                oi_chg = (self.last_oi / self.last_oi_base) - 1 if self.last_oi_base else 0
                
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
                            print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} вљ пёЏ OI Divergence: {note}")

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

        # в”Ђв”Ђв”Ђ Flush Batched Alerts в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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

        # в”Ђв”Ђв”Ђ Periodic Chart Updates в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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
        # в”Ђв”Ђв”Ђ End of Tick в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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
            self.scalp_loss_streak = {"LONG": 0, "SHORT": 0}
            self.scalp_side_cooldown_until = {"LONG": 0, "SHORT": 0}
            self.confluence_side_lock_until = {"LONG": 0, "SHORT": 0}
            self.liq_pool_alerts = {}
            self.liquidity_bias = {}
            self.last_liq_pool_report_hour = None
            self.last_liq_session_reports = {}
            self.last_liq_expansion_alert = 0
            self.last_book_total_usd = 0
            self._save_state()
        
        self._reconstruct_session_history(now.hour + now.minute / 60.0)

    def _update_levels(self):
        """Fetch daily/weekly/monthly/hourly data and calculate levels."""
        print("[LEVELS] Updating daily/weekly/monthly/hourly levels...")

        daily_df   = fetch_daily()
        weekly_df  = fetch_weekly()
        monthly_df = fetch_monthly()
        # Fetch 1200 hours to cover a full month + extra buffer for stable UTC reconstruction
        hourly_df  = fetch_klines(interval="1h", limit=1200)

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

    def _process_commands(self):
        """Fetch and handle incoming Telegram messages."""
        updates = tg.get_updates(offset=self.last_update_id + 1)
        if not updates or not updates.get("ok"):
            return

        for up in updates.get("result", []):
            self.last_update_id = up["update_id"]
            
            message = up.get("message")
            if not message or "text" not in message:
                continue

            user_id = message["from"]["id"]
            text = message["text"].strip()
            
            if text == "/start":
                welcome_msg = (
                    f"<b>How to Join:</b>\n\n"
                    f"1. Sign up on Bitunix to start trading:\n"
                    f"рџ”— {BITUNIX_REG_LINK}\n\n"
                    f"2. <b>Send your unique UID here.</b>\n\n"
                    f"3. Once verified, youвЂ™ll receive an invite link to join."
                )
                tg.send(welcome_msg, parse_mode="HTML", chat_id=user_id)
            elif text.startswith("/analytics"):
                try:
                    days = 30
                    parts = text.split()
                    if len(parts) > 1 and parts[1].isdigit():
                        days = max(1, min(180, int(parts[1])))
                    stats = self.tracker.get_analytics(days=days)
                    totals = stats["totals"]
                    by_type = stats.get("by_signal_type", {})

                    def fmt_type_line(name):
                        b = by_type.get(name, {})
                        generated = int(b.get("generated", 0))
                        closed = int(b.get("trades", 0))
                        wr = float(b.get("win_rate", 0.0))
                        hit = float(b.get("hit_rate", 0.0))
                        avg_r = float(b.get("avg_r", 0.0))
                        return (
                            f"{name:<7} g={generated:<3} c={closed:<3} "
                            f"wr={wr:>5.1f}% hit={hit:>5.1f}% avgR={avg_r:+.2f}"
                        )

                    msg = (
                        f"рџ“Љ <b>SIGNAL ANALYTICS ({days}d)</b>\n\n"
                        f"<pre>"
                        f"Generated:   {totals['generated']}\n"
                        f"Closed:      {totals['trades']}\n"
                        f"Open:        {totals['open']}\n"
                        f"Wins:        {totals['wins']}\n"
                        f"Losses:      {totals['losses']}\n"
                        f"Breakeven:   {totals['breakeven']}\n"
                        f"Win Rate:    {totals['win_rate']:.1f}%\n"
                        f"Hit Rate:    {totals['hit_rate']:.1f}%\n"
                        f"Avg R:       {totals['avg_r']:.2f}\n"
                        f"Expectancy:  {totals['expectancy_r']:.2f}R\n"
                        f"------------------------------\n"
                        f"{fmt_type_line('SCALP')}\n"
                        f"{fmt_type_line('STRONG')}\n"
                        f"{fmt_type_line('EXTREME')}"
                        f"</pre>"
                    )
                    tg.send(msg, parse_mode="HTML", chat_id=user_id)
                except Exception as e:
                    tg.send(f"Analytics failed: {e}", chat_id=user_id)

            elif text.isdigit():
                # User sent their UID
                uid = text
                print(f"[ONBOARDING] Checking UID: {uid} for user {user_id}")

                is_referral = verify_bitunix_user(uid)

                if not is_referral:
                    error_msg = (
                        f"вљ пёЏвќ—пёЏ Hi there, the account you provided is not under this partner. "
                        f"please use the link below to sign up\n"
                        f"рџ”—: {BITUNIX_REG_LINK}"
                    )
                    tg.send(error_msg, parse_mode="HTML", chat_id=user_id)
                else:
                    success_msg = (
                        f"вњ… <b>Verification Successful!</b>\n\n"
                        f"Welcome to the team. You can now access our private signal channel:\n"
                        f"рџ”— {INVITE_LINK}\n\n"
                        f"See you inside!"
                    )
                    tg.send(success_msg, parse_mode="HTML", chat_id=user_id)
            
            # Save state after each update to avoid re-processing on crash
            self._save_state()

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


    def _process_timeframe(self, tf, df, now, entry_protection_ts=None):
        """Process one timeframe: channels, momentum, signals."""

        # в”Ђв”Ђв”Ђ Calculate indicators в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
        df = calculate_channels(df)
        df = calculate_momentum(df)

        if df.empty or len(df) < 2:
            return 0, "", None

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        price_high = float(curr["High"])
        price_low  = float(curr["Low"])
        close      = float(curr["Close"])
        atr_val    = float(curr["ATR"]) if "ATR" in curr else 0
        zone       = curr["MomentumZone"] if "MomentumZone" in curr else "NEUTRAL"
        rsi_raw    = float(curr["RSI"]) if "RSI" in curr else 50
        rsi_smooth = float(curr["MomentumSmooth"]) if "MomentumSmooth" in curr else 50
        # Local trend (timeframe-relative): used as directional bias for entries.
        local_trend = "Ranging"
        if "EMA2" in curr and "EMA3" in curr:
            ema_fast = float(curr["EMA2"])
            ema_slow = float(curr["EMA3"])
            if close > ema_fast > ema_slow:
                local_trend = "Bullish"
            elif close < ema_fast < ema_slow:
                local_trend = "Bearish"

        # в”Ђв”Ђв”Ђ REAL-TIME MONITOR (Debug) в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

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

        # в”Ђв”Ђв”Ђ Volume Spike Detection в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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


        # в”Ђв”Ђв”Ђ Liquidity Sweeps в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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

        # в”Ђв”Ђв”Ђ Volatility Zone Touches в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
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

        # в”Ђв”Ђв”Ђ Trade Signals (Channels) в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
        ch_sigs = check_channel_signals(df)
        for sig in ch_sigs:
            sig_key = f"tr_sig_{tf}_{sig['signal']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # 2. Momentum confirmation
        mom_sigs = check_momentum_confirm(df)
        div_sigs = check_rsi_divergence(df)
        divergence_sides = {s.get("side") for s in div_sigs}
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

        # 5. RSI divergence confirmation
        for sig in div_sigs:
            sig_key = f"rsi_div_{tf}_{sig['side']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # в”Ђв”Ђв”Ђ Scalp Momentum System в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
        tracker = self.scalp_trackers[tf]
        events = tracker.update(zone, close, atr_val, candle_ts=candle_ts, rsi_raw=rsi_raw, rsi_smooth=rsi_smooth)

        profile = TIMEFRAME_PROFILES.get(tf, TIMEFRAME_PROFILES["5m"])
        emoji = profile["emoji"]

        for evt in events:
            # Scalp signals usually only one of each type per candle
            evt_key = f"scalp_{tf}_{evt['type']}_{evt['side']}_{candle_ts}"

            if evt_key in self.sent_signals:
                continue

            self.sent_signals.add(evt_key)

            if evt["type"] == "OPEN":
                open_key = f"{tf}_{evt['side']}"
                last_open_alert_ts = self.last_scalp_open_alert.get(open_key, 0)
                can_send_open = (current_time - last_open_alert_ts) >= SCALP_OPEN_ALERT_COOLDOWN

                if not self.is_booting and can_send_open:
                    tg.send_scalp_open(tf, evt["side"], evt["price"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                    self.last_scalp_open_alert[open_key] = current_time
                self._save_state()
                if self.is_booting:
                    print(f"  [TG] Skipped Scalp Open [{tf}] {evt['side']} (booting)")
                elif not can_send_open:
                    print(f"  [SCALP] Suppressed Open [{tf}] {evt['side']} (cooldown)")
                else:
                    print(f"  [TG] Sent Scalp Open [{tf}] {evt['side']}")

            elif evt["type"] == "PREPARE":
                if not self.is_booting:
                    tg.send_scalp_prepare(tf, evt["side"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self._save_state()
                print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Prepare [{tf}] {evt['side']}")

            elif evt["type"] == "CONFIRMED":
                # --- Calculate Signal Strength Score ---
                score, reasons = calculate_signal_score(
                    evt, df, self.levels, self.macro_trend, self.last_oi, self.last_liqs
                )
                side = evt["side"]
                if side in divergence_sides:
                    score += 1
                    reasons.append("RSI Divergence")

                # Local trend bias: prefer signals aligned with current TF direction.
                if local_trend in ("Bullish", "Bearish"):
                    aligned_local = (
                        (side == "LONG" and local_trend == "Bullish") or
                        (side == "SHORT" and local_trend == "Bearish")
                    )
                    if aligned_local:
                        score += 1
                        reasons.append(f"Local {local_trend}")
                    else:
                        score -= 2
                        reasons.append(f"Local {local_trend} (counter)")

                # Small directional bias toward strongest nearby liquidity pool.
                lb_side = self.liquidity_bias.get("side")
                if lb_side in ("LONG", "SHORT"):
                    lb_tf = self.liquidity_bias.get("timeframe", "N/A")
                    lb_prob = float(self.liquidity_bias.get("probability_pct", 0) or 0)
                    if side == lb_side:
                        score += int(LIQ_POOL_BIAS_SCORE_BONUS)
                        reasons.append(f"Liquidity Pull {lb_tf} ({lb_prob:.0f}%)")
                    else:
                        score -= int(LIQ_POOL_BIAS_SCORE_BONUS)
                        reasons.append(f"Against Liquidity Pull {lb_tf}")

                session_name = self._get_current_session_name(now) or "ASIA"
                session_cfg = SESSION_SCALP_MODE.get(session_name, {})
                session_countertrend_max = session_cfg.get("countertrend_max", SCALP_COUNTERTREND_MAX_PER_WINDOW)
                session_score_boost = session_cfg.get("score_boost", 0)
                relaxed_filters = bool(SCALP_RELAXED_FILTERS)
                regime_name = self.market_regime if SCALP_REGIME_SWITCHING else "RANGE"
                regime_cfg = SCALP_REGIME_PROFILES.get(regime_name, {})
                score_delta = int(regime_cfg.get("score_delta", 0))
                regime_vol_min_mult = float(regime_cfg.get("vol_min_mult", 1.0))
                regime_vol_max_mult = float(regime_cfg.get("vol_max_mult", 1.0))
                size_mult = float(regime_cfg.get("size_mult", 1.0))
                tuning_delta = 0
                if self.scalp_tuning_state == "TIGHTEN":
                    tuning_delta = 1
                elif self.scalp_tuning_state == "LOOSEN":
                    tuning_delta = -1

                # --- Losing streak cooldown per side ---
                cooldown_until = self.scalp_side_cooldown_until.get(side, 0)
                if current_time < cooldown_until:
                    wait_m = int((cooldown_until - current_time) / 60) + 1
                    print(f"  [SCALP] Blocked {tf} {side}: side cooldown active ({wait_m}m left)")
                    continue

                # --- Volatility regime filter ---
                if VOLATILITY_FILTER_ENABLED and close > 0 and atr_val > 0:
                    atr_pct = atr_val / close * 100
                    min_pct = VOLATILITY_MIN_ATR_PCT.get(tf, 0.0)
                    max_pct = VOLATILITY_MAX_ATR_PCT.get(tf, 99.0)
                    if relaxed_filters:
                        min_pct *= float(SCALP_RELAX_VOL_MIN_MULT)
                        max_pct *= float(SCALP_RELAX_VOL_MAX_MULT)
                    min_pct *= regime_vol_min_mult
                    max_pct *= regime_vol_max_mult
                    if atr_pct < min_pct or atr_pct > max_pct:
                        print(
                            f"  [SCALP] Blocked {tf} {side}: ATR% {atr_pct:.3f} "
                            f"outside [{min_pct:.3f}, {max_pct:.3f}]"
                        )
                        continue

                # --- Order-flow safety filter ---
                if ORDERFLOW_SAFETY_ENABLED:
                    anomaly, oi_pct = self._is_orderflow_anomaly()
                    if anomaly and score < ORDERFLOW_ANOMALY_SCORE_MIN:
                        print(
                            f"  [SCALP] Blocked {tf} {side}: order-flow anomaly "
                            f"(OI {oi_pct:.2f}%, LIQ ${self.last_liqs:,.0f}) and score {score}"
                        )
                        continue

                # --- Session whitelist per timeframe ---
                allowed_sessions = SCALP_ALLOWED_SESSIONS_BY_TF.get(tf)
                if allowed_sessions and session_name not in allowed_sessions and not (relaxed_filters and SCALP_RELAX_ALLOW_OFFSESSION):
                    print(
                        f"  [SCALP] Blocked {tf} {side}: session {session_name} "
                        f"not in {allowed_sessions}"
                    )
                    continue

                # --- Minimum score gate by timeframe ---
                min_score_tf = SCALP_MIN_SCORE_BY_TF.get(tf, 0)
                if relaxed_filters:
                    min_score_tf = max(0, int(min_score_tf) - int(SCALP_RELAX_MIN_SCORE_DELTA))
                min_score_tf = max(0, int(min_score_tf) + score_delta + tuning_delta)
                if score < min_score_tf:
                    print(
                        f"  [SCALP] Blocked {tf} {side}: score {score}<{min_score_tf} "
                        f"(tf quality gate)"
                    )
                    continue

                # --- Exposure control ---
                if SCALP_EXPOSURE_ENABLED:
                    exposure = self._get_scalp_exposure()
                    open_total = int(exposure["total"])
                    open_side = int(exposure["by_side"].get(side, 0))
                    open_tf = int(exposure["by_tf"].get(tf, 0))
                    tf_limit = int(SCALP_MAX_OPEN_PER_TF.get(tf, 1))
                    if open_total >= SCALP_MAX_OPEN_TOTAL:
                        print(f"  [SCALP] Blocked {tf} {side}: total exposure {open_total}/{SCALP_MAX_OPEN_TOTAL}")
                        continue
                    if open_side >= SCALP_MAX_OPEN_PER_SIDE:
                        print(f"  [SCALP] Blocked {tf} {side}: side exposure {open_side}/{SCALP_MAX_OPEN_PER_SIDE}")
                        continue
                    if open_tf >= tf_limit:
                        print(f"  [SCALP] Blocked {tf} {side}: tf exposure {open_tf}/{tf_limit}")
                        continue

                # --- Trend gate for scalp confirms ---
                trend_name = self.macro_trend or "Ranging"
                bullish_trends = {"Bullish", "Trending Bullish", "Strong Bullish"}
                bearish_trends = {"Bearish", "Trending Bearish", "Strong Bearish"}
                trend_aligned = (
                    trend_name == "Ranging"
                    or (evt["side"] == "LONG" and trend_name in bullish_trends)
                    or (evt["side"] == "SHORT" and trend_name in bearish_trends)
                )
                filter_mode = str(SCALP_TREND_FILTER_MODE_BY_TF.get(tf, SCALP_TREND_FILTER_MODE)).strip().lower()
                countertrend_min_score = int(SCALP_COUNTERTREND_MIN_SCORE_BY_TF.get(tf, SCALP_COUNTERTREND_MIN_SCORE))

                if not trend_aligned:
                    if filter_mode == "hard":
                        if relaxed_filters:
                            print(
                                f"  [SCALP] Relaxed override {tf} {side}: "
                                f"counter-trend allowed in hard mode vs {trend_name}"
                            )
                        else:
                            print(f"  [SCALP] Blocked {tf} {side}: counter-trend vs {trend_name} (mode=hard)")
                            continue
                    if filter_mode == "soft":
                        required_score = countertrend_min_score + session_score_boost
                        if score < required_score:
                            print(
                                f"  [SCALP] Blocked {tf} {side}: "
                                f"counter-trend score {score}<{required_score} vs {trend_name} (mode=soft)"
                            )
                            continue

                        side_hits = self.scalp_countertrend_hits.get(side, [])
                        if not isinstance(side_hits, list):
                            side_hits = []
                        cutoff = current_time - SCALP_COUNTERTREND_WINDOW_SEC
                        side_hits = [
                            ts for ts in side_hits
                            if isinstance(ts, (int, float)) and ts >= cutoff
                        ]
                        ct_extra = int(SCALP_RELAX_COUNTERTREND_EXTRA) if relaxed_filters else 0
                        ct_limit = session_countertrend_max + ct_extra
                        if len(side_hits) >= ct_limit:
                            print(
                                f"  [SCALP] Blocked {tf} {side}: "
                                f"counter-trend quota reached ({len(side_hits)}/"
                                f"{ct_limit} in {SCALP_COUNTERTREND_WINDOW_SEC}s)"
                            )
                            self.scalp_countertrend_hits[side] = side_hits
                            continue

                        side_hits.append(current_time)
                        self.scalp_countertrend_hits[side] = side_hits
                    elif filter_mode == "hard" and relaxed_filters:
                        side_hits = self.scalp_countertrend_hits.get(side, [])
                        if not isinstance(side_hits, list):
                            side_hits = []
                        cutoff = current_time - SCALP_COUNTERTREND_WINDOW_SEC
                        side_hits = [
                            ts for ts in side_hits
                            if isinstance(ts, (int, float)) and ts >= cutoff
                        ]
                        ct_extra = int(SCALP_RELAX_COUNTERTREND_EXTRA)
                        ct_limit = session_countertrend_max + ct_extra
                        if len(side_hits) >= ct_limit:
                            print(
                                f"  [SCALP] Blocked {tf} {side}: "
                                f"relaxed hard-mode quota reached ({len(side_hits)}/"
                                f"{ct_limit} in {SCALP_COUNTERTREND_WINDOW_SEC}s)"
                            )
                            self.scalp_countertrend_hits[side] = side_hits
                            continue
                        side_hits.append(current_time)
                        self.scalp_countertrend_hits[side] = side_hits

                # Dynamic size: scale base size by score, min 0.5%
                dyn_base = max(0.5, (score / 10) * profile["size"]) if score else profile["size"]
                dyn_size = round(max(0.5, dyn_base * size_mult), 1)
                tp_liq = self._estimate_tp_liquidity(evt["side"], evt["entry"], evt["tp1"], evt["tp2"], evt["tp3"])

                resp = None
                if not self.is_booting:
                    resp = tg.send_scalp_confirmed(
                        timeframe=tf,
                        side=evt["side"],
                        entry=evt["entry"],
                        sl=evt["sl"],
                        tp1=evt["tp1"],
                        tp2=evt["tp2"],
                        tp3=evt["tp3"],
                        strength=profile["strength"],
                        size=dyn_size,
                        score=score,
                        trend=self.macro_trend,
                        reasons=reasons,
                        tp_liq_prob=tp_liq["prob"] if tp_liq else None,
                        tp_liq_usd=tp_liq["size_usd"] if tp_liq else None,
                        tp_liq_target=tp_liq["target"] if tp_liq else None,
                        chat_id=PRIVATE_CHAT_ID
                    )
                msg_id = resp.get("result", {}).get("message_id") if resp else None
                self._save_state()
                print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Scalp Confirmed [{tf}] {evt['side']} @ {evt['entry']:,.2f}")

                # Log signal for performance tracking
                self.tracker.log_signal(
                    side=evt["side"],
                    entry=evt["entry"],
                    sl=evt["sl"],
                    tp1=evt["tp1"],
                    tp2=evt["tp2"],
                    tp3=evt["tp3"],
                    tf=tf,
                    timestamp=entry_protection_ts or candle_ts,
                    msg_id=msg_id,
                    chat_id=PRIVATE_CHAT_ID,
                    signal_type="SCALP",
                    meta={
                        "score": score,
                        "trend": self.macro_trend,
                        "reasons": reasons,
                        "size": dyn_size,
                        "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                        "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                        "tp_liq_target": tp_liq["target"] if tp_liq else None,
                    }
                )

            elif evt["type"] == "CLOSED":
                if not self.is_booting:
                    tg.send_scalp_closed(tf, evt["side"], evt["price"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self._save_state()
                print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Scalp Closed [{tf}] {evt['side']}")


        # в”Ђв”Ђв”Ђ Store prev candle data в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
        self.prev_candles[tf] = {
            "High": price_high,
            "Low":  price_low,
        }

        return atr_val, candle_ts, rsi_raw


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# ENTRY POINT
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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

