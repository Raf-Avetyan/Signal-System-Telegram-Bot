# Ponch Signal System - Main Bot

"""
Main entry point. Monitors BTCUSDT across multiple timeframes,
detects signals, and sends formatted Telegram alerts.
"""

import time
import traceback
from datetime import datetime, timezone, timedelta
import json
import re
import html
import requests
from zoneinfo import ZoneInfo

from config import (
    BOT_TOKEN,
    SYMBOL, SIGNAL_TIMEFRAMES, POLL_INTERVAL,
    TIMEFRAME_PROFILES, FUNDING_THRESHOLD, FUNDING_CHECK_INTERVAL,
    FUNDING_COOLDOWN, VOLUME_SPIKE_MULT, VOLUME_SPIKE_TIMEFRAMES,
    VOLUME_AVG_PERIOD, APPROACH_THRESHOLD, APPROACH_COOLDOWN,
    APPROACH_LEVELS, SESSIONS, get_adjusted_sessions, ALERT_BATCH_WINDOW,
    OI_CHANGE_THRESHOLD, LIQ_SQUEEZE_THRESHOLD, LIQ_ALERT_COOLDOWN, CHAT_ID,
    SIGNAL_CHAT_ID, SIGNAL_TRADING_THREAD_ID, SIGNAL_GENERAL_THREAD_ID,
    SIGNAL_SESSIONS_THREAD_ID, SIGNAL_ACTIVE_TRADES_THREAD_ID,
    SIGNAL_SCENARIOS_THREAD_ID, SIGNAL_SUCCESS_TRADES_THREAD_ID, SIGNAL_BLOCKED_THREAD_IDS,
    BOT_MENTION_ALIASES,
    FAST_MOVE_THRESHOLD, FAST_MOVE_WINDOW, FAST_MOVE_COOLDOWN,
    BITUNIX_REG_LINK, INVITE_LINK, COMMAND_POLL_INTERVAL,
    BITUNIX_SCENARIO_TRADING_ENABLED, BITUNIX_SCENARIO_TRADING_MODE,
    BITUNIX_SCENARIO_MIN_PROBABILITY, BITUNIX_SCENARIO_SCAN_INTERVAL_SEC,
    BITUNIX_SCENARIO_TRIGGER_COOLDOWN_SEC, BITUNIX_AUTO_HEDGE_ENABLED,
    BITUNIX_AUTO_HEDGE_MIN_PROBABILITY, BITUNIX_AUTO_HEDGE_TRIGGER_R,
    BITUNIX_AUTO_HEDGE_MAX_PROGRESS_R, BITUNIX_AUTO_HEDGE_SIZE_MULT,
    BITUNIX_AUTO_HEDGE_COOLDOWN_SEC,
    SCALP_TREND_FILTER_MODE, SCALP_COUNTERTREND_MIN_SCORE,
    SCALP_OPEN_ALERT_COOLDOWN, SCALP_COUNTERTREND_MAX_PER_WINDOW,
    SCALP_COUNTERTREND_WINDOW_SEC, SCALP_LOSS_STREAK_LIMIT,
    SCALP_LOSS_COOLDOWN_SEC, VOLATILITY_FILTER_ENABLED,
    VOLATILITY_MIN_ATR_PCT, VOLATILITY_MAX_ATR_PCT,
    SESSION_SCALP_MODE, ORDERFLOW_SAFETY_ENABLED,
    ORDERFLOW_ANOMALY_SCORE_MIN, ORDERFLOW_OI_PCT_ANOMALY,
    ORDERFLOW_LIQ_ANOMALY_USD, SCALP_MIN_SCORE_BY_TF,
    SCALP_HARD_MIN_SCORE_BY_TF, SCALP_MOMENTUM_EXIT_MIN_SCORE_BY_TF,
    SCALP_ALLOWED_SESSIONS_BY_TF, SCALP_RELAXED_FILTERS,
    SCALP_TREND_FILTER_MODE_BY_TF, SCALP_COUNTERTREND_MIN_SCORE_BY_TF,
    SCALP_RELAX_MIN_SCORE_DELTA, SCALP_RELAX_VOL_MIN_MULT,
    SCALP_RELAX_VOL_MAX_MULT, SCALP_RELAX_COUNTERTREND_EXTRA,
    SCALP_RELAX_ALLOW_OFFSESSION, SCALP_REGIME_SWITCHING,
    SCALP_REGIME_PROFILES, SCALP_SELF_TUNING_ENABLED,
    SCALP_SELF_TUNE_LOOKBACK, SCALP_SELF_TUNE_MIN_CLOSED,
    SCALP_SELF_TUNE_LOW_WR, SCALP_SELF_TUNE_HIGH_WR,
    SCALP_SELF_TUNE_LOW_AVGR, SCALP_SELF_TUNE_HIGH_AVGR,
    SCALP_EXPOSURE_ENABLED, BLOCK_OPPOSITE_SIDE_SIGNALS, SCALP_MAX_OPEN_TOTAL,
    SCALP_MAX_OPEN_PER_SIDE, SCALP_MAX_OPEN_PER_TF,
    MOMENTUM_OS, MOMENTUM_OB,
    BASE_MOMENTUM_ENABLED_TFS,
    CONFIRMATION_RSI_EXHAUSTION_BUFFER, CONFLUENCE_OPPOSITE_LOCK_SEC,
    LIQ_POOL_ALERT_ENABLED, LIQ_POOL_MIN_USD, LIQ_POOL_ALERT_COOLDOWN,
    LIQ_POOL_BIAS_SCORE_BONUS, LIQ_POOL_MAX_DISTANCE_ATR_MULT,
    LIQ_POOL_MIN_DISTANCE_PCT, LIQ_POOL_HUGE_USD_OVERRIDE,
    TP_LIQUIDITY_MIN_USD, TP_LIQUIDITY_BAND_PCT,
    FALLING_KNIFE_FILTER_ENABLED, FALLING_KNIFE_LOOKBACK_5M, FALLING_KNIFE_LOOKBACK_15M,
    FALLING_KNIFE_MOVE_PCT_5M, FALLING_KNIFE_MOVE_PCT_15M,
    FAST_EXPANSION_GUARD_ENABLED, FAST_EXPANSION_LOOKBACK_5M, FAST_EXPANSION_LOOKBACK_15M,
    FAST_EXPANSION_MOVE_PCT_5M, FAST_EXPANSION_MOVE_PCT_15M,
    FAST_EXPANSION_VOLUME_MULT, FAST_EXPANSION_BODY_ATR, FAST_EXPANSION_EMA2_ATR,
    LIQ_POOL_REPORT_TIMEFRAMES, LIQ_POOL_MIN_USD_BY_TF, LIQ_POOL_MIN_DISTANCE_PCT_BY_TF,
    LIQ_POOL_TARGET_DISTANCE_PCT_BY_TF, LIQ_POOL_LEVEL_DEDUP_GAP_PCT,
    LIQ_POOL_PROGRESSIVE_MIN_STEP_PCT,
    LIQ_POOL_AGG_WINDOW_PCT_BY_TF,
    LIQ_POOL_NO_MOVE_RANGE_PCT_1H, LIQ_POOL_EXPANSION_PRICE_MOVE_PCT_1H,
    LIQ_POOL_EXPANSION_VOLUME_MULT, LIQ_POOL_EXPANSION_BOOK_MULT, LIQ_POOL_EXPANSION_COOLDOWN,
    SMART_MONEY_ENABLED, SMART_MONEY_EXECUTION_TFS, SMART_MONEY_RISK_PCT,
    MIN_SIGNAL_SIZE_PCT, MAX_SIGNAL_SIZE_PCT, PRIVATE_EXEC_CHAT_ID, EXECUTION_UPDATES_PRIVATE_ONLY,
    PRIVATE_EXEC_AI_CONTROL_ENABLED, PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC,
    GEMINI_API_KEY, GEMINI_MODEL, TIMEFRAME_RISK_MULTIPLIERS, BITUNIX_DEFAULT_LEVERAGE,
    BITUNIX_POSITION_MODE,
    NEWS_FILTER_ENABLED, get_active_news_blackout, is_ny_market_holiday,
    TRADING_ECONOMICS_NEWS_ENABLED, TRADING_ECONOMICS_API_KEY,
    TRADING_ECONOMICS_COUNTRIES, TRADING_ECONOMICS_MIN_IMPORTANCE,
    TRADING_ECONOMICS_REFRESH_SEC, TRADING_ECONOMICS_BLOCK_BEFORE_MIN,
    TRADING_ECONOMICS_BLOCK_AFTER_MIN, FIVE_MIN_STRICT_NEWS_FILTER,
    FIVE_MIN_NEWS_BLOCK_BEFORE_MIN, FIVE_MIN_NEWS_BLOCK_AFTER_MIN,
    FIVE_MIN_REQUIRE_15M_PERMISSION, MARKETTWITS_NEWS_ENABLED,
    MARKETTWITS_CHANNEL_URL, MARKETTWITS_REFRESH_SEC,
    MARKETTWITS_LOOKBACK_HOURS, MARKETTWITS_BLOCK_AFTER_MIN,
    MARKETTWITS_MIN_BLOCK_SCORE, REVERSAL_OVERRIDE_ENABLED,
    REVERSAL_OVERRIDE_MIN_SCORE, REVERSAL_OVERRIDE_MIN_PROOFS,
    WEEKEND_TRADING_ENABLED, BOS_GUARD_ENABLED, BOS_GUARD_SWING_LOOKBACK,
    BOS_GUARD_RECENT_BARS, BOS_GUARD_RECLAIM_BARS, RSI_PULLBACK_SCALP_ENABLED,
    RSI_PULLBACK_SCALP_TFS, RSI_PULLBACK_SCALP_OB, RSI_PULLBACK_SCALP_OS,
    RSI_PULLBACK_SCALP_MIN_FILTERS, RSI_PULLBACK_SCALP_MIN_EMA_ATR_DISTANCE,
    RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR, RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR,
    RSI_PULLBACK_SCALP_MIN_WICK_BODY_RATIO, RSI_PULLBACK_SCALP_TP1_R,
    RSI_PULLBACK_SCALP_TP2_R, RSI_PULLBACK_SCALP_TP3_R,
    STRUCTURE_GUARD_MODE_BY_TF, LATE_CONFIRM_MAX_EMA2_ATR_DISTANCE_BY_TF,
    LATE_CONFIRM_MAX_BODY_ATR_BY_TF
)
from education_posts import PROFESSIONAL_MEMBER_EDUCATION_POSTS
from levels import calculate_levels, check_liquidity_sweep, check_volatility_touch
from channels import calculate_channels, check_channel_signals
from momentum import calculate_momentum, ScalpTracker, detect_trend, classify_momentum_zone, check_htf_pullback_entry, check_one_h_reclaim_entry
from scoring import calculate_signal_score
from signals import check_momentum_confirm, check_range_confirm, check_flow_confirm, check_rsi_divergence
from confirmation import ConfirmationTracker
from charting import generate_daily_levels_chart, generate_signal_setup_chart, generate_liquidation_map_chart
from data import (
    fetch_klines, fetch_all_timeframes, fetch_daily, fetch_weekly, fetch_monthly, 
    fetch_funding_rate, fetch_open_interest, fetch_liquidations, fetch_global_indicators, fetch_order_book,
    fetch_trading_economics_calendar, parse_gemini_trade_instruction, ask_gemini_trade_question,
    ask_gemini_trade_question_with_image,
    fetch_last_price, fetch_markettwits_posts
)
from tracker import SignalTracker
from bitunix import verify_bitunix_user
from bitunix_trade import TradeExecutor, new_signal_id
from liquidity_map import detect_liquidity_event, detect_liquidity_candidates
from smart_money import detect_smart_money_entry
from market_report import build_btc_market_report, build_btc_scenarios_payload, build_liquidation_map_snapshot
import telegram as tg


class PonchBot:
    """Main Ponch Signal System bot."""

    def __init__(self, quiet_init=False):
        # Scalp trackers - one per timeframe
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
        self.trade_executor = TradeExecutor()
        self.latest_data = {}
        self.approach_alerts = {}      # { "Pump": timestamp }
        self.last_funding_check = 0
        self.last_funding_alert = 0
        self.sent_sessions = set()     # "session_LONDON_2023-10-14"
        self.session_data = {}         # { "LONDON_2024-03-15": {"open": 70000, "levels": set()} }
        self.session_history = {}      # { "ASIA": "Asia recap text" }
        self.state_file = "bot_state.json"
        self.local_tz = ZoneInfo("Asia/Yerevan")
        
        state = self._load_state()
        self.daily_report_msg_id = state.get("daily_report_msg_id")
        self.session_msg_ids     = state.get("session_msg_ids", {})
        self.session_thread_message_ids = state.get("session_thread_message_ids", [])
        self.confirmations.from_dict(state.get("confirmations", {})) # Restore confirmation state
        self.session_data        = state.get("session_data", {})
        self.last_levels_date    = state.get("last_levels_date")
        self.sent_signals        = set(state.get("sent_signals", []))
        self.sent_sessions       = set(state.get("sent_sessions", [])) # New: Persist session summaries
        self.approach_alerts     = state.get("approach_alerts", {})    # New: Persist approaching level cooldowns
        self.last_funding_alert  = state.get("last_funding_alert", 0)   
        self.last_market_alert   = state.get("last_market_alert", 0)
        self.last_summary_date   = state.get("last_summary_date")      # New: Track summary schedule
        self.last_daily_report_date = state.get("last_daily_report_date")
        self.last_session_thread_cleanup_date = state.get("last_session_thread_cleanup_date")
        self.last_exec_snapshot_date = state.get("last_exec_snapshot_date")
        self.last_liquidation_map_date = state.get("last_liquidation_map_date")
        self.last_education_post_date = state.get("last_education_post_date")
        self.last_education_post_slot = state.get("last_education_post_slot")
        self.last_today_wins_batch_date = state.get("last_today_wins_batch_date")
        self.pending_stop_liq_watches = state.get("pending_stop_liq_watches", {})
        self.scenario_trade_cooldowns = state.get("scenario_trade_cooldowns", {})
        if not self.last_education_post_slot and self.last_education_post_date:
            self.last_education_post_slot = f"{self.last_education_post_date} 08"
        self.education_post_index = int(state.get("education_post_index", 0) or 0)
        self.pending_exec_action = state.get("pending_exec_action")
        self.last_exec_suggested_action = state.get("last_exec_suggested_action")
        self.private_exec_focus = state.get("private_exec_focus", {})
        self.group_chat_contexts = state.get("group_chat_contexts", {})
        self.private_todo_items = state.get("private_todo_items", [])
        self.signal_debug_stats = state.get("signal_debug_stats", {})
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
        self.live_news_events = []
        self.last_live_news_refresh = 0.0
        self.last_live_news_error = None
        self.markettwits_events = []
        self.last_markettwits_refresh = 0.0
        self.last_markettwits_error = None
        self.live_exchange_context_cache = None
        self.live_exchange_context_cache_ts = 0.0
        self.live_exchange_history_cache = {}
        self.last_session_update = time.time()
        self.last_daily_update   = time.time()
        self.last_liq_heatmap_capture = 0.0
        self.last_scenario_trade_scan_at = 0.0
        self.last_position_snapshot_refresh_slot = None
        self.last_update_id      = state.get("last_update_id", 0)
        self.last_command_check  = 0

        # Macro Trend & Context
        self.macro_trend = "Ranging"
        self.market_regime = "RANGE"
        self.scalp_tuning_state = "NEUTRAL"
        self.last_oi = 0
        self.last_oi_base = 0
        self.last_liqs = 0
        print(f"[TRADE] Bitunix executor {self.trade_executor.status_line()}")
        trade_check = self.trade_executor.startup_self_check()
        print(
            f"[TRADE] Startup check: auth_ok={trade_check.get('auth_ok')} "
            f"free={float(trade_check.get('balance_available', 0) or 0):.2f} "
            f"total={float(trade_check.get('balance_total', 0) or 0):.2f} "
            f"used={float(trade_check.get('balance_used', 0) or 0):.2f} "
            f"margin_mode={trade_check.get('margin_mode')} "
            f"leverage={int(trade_check.get('leverage', 0) or 0)} "
            f"open_positions={int(trade_check.get('open_positions', 0) or 0)} "
            f"sides={trade_check.get('position_sides', [])}"
        )
        for err in trade_check.get("errors", []):
            print(f"[TRADE] Startup detail: {err}")
        if trade_check.get("balance_endpoint"):
            print(f"[TRADE] Balance endpoint: {trade_check.get('balance_endpoint')}")
        if trade_check.get("balance_response"):
            print(f"[TRADE] Balance response: {trade_check.get('balance_response')}")
        if trade_check.get("positions_endpoint"):
            print(f"[TRADE] Positions endpoint: {trade_check.get('positions_endpoint')}")
        if trade_check.get("positions_response"):
            print(f"[TRADE] Positions response: {trade_check.get('positions_response')}")
        reconcile = self.trade_executor.reconcile_execution_state(self.tracker.signals)
        if reconcile.get("inactive_marked"):
            self.tracker.persist()
        if reconcile.get("inactive_marked") or reconcile.get("state_updated"):
            self._cleanup_finished_active_trade_cards()
        print(
            f"[TRADE] Reconcile: matched={int(reconcile.get('matched', 0) or 0)} "
            f"inactive_marked={int(reconcile.get('inactive_marked', 0) or 0)} "
            f"orphans={len(reconcile.get('orphan_positions', []) or [])} "
            f"missing_protection={len(reconcile.get('missing_protection', []) or [])}"
        )
        for err in reconcile.get("errors", []):
            print(f"[TRADE] Reconcile detail: {err}")
        self.last_oi_price = 0
        self.last_liq_alert_time = 0
        self.is_booting = True         # Start in quiet mode for first check

        # Alert Batching
        self.pending_alerts = []
        self.batch_timer_start = None

        # Mute state
        self.muted_until = None
        self.chat_member_status_cache = {}
        self.last_execution_reconcile_at = 0

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

    def _execution_chat_id(self):
        return PRIVATE_EXEC_CHAT_ID or None

    def _signal_chat_id(self):
        return SIGNAL_CHAT_ID or None

    def _trading_signal_thread_id(self):
        try:
            return int(SIGNAL_TRADING_THREAD_ID or 0)
        except Exception:
            return 0

    def _general_thread_id(self):
        try:
            return int(SIGNAL_GENERAL_THREAD_ID or 0)
        except Exception:
            return 0

    def _sessions_thread_id(self):
        try:
            return int(SIGNAL_SESSIONS_THREAD_ID or 0)
        except Exception:
            return 0

    def _active_trades_thread_id(self):
        try:
            return int(SIGNAL_ACTIVE_TRADES_THREAD_ID or 0)
        except Exception:
            return 0

    def _scenarios_thread_id(self):
        try:
            return int(SIGNAL_SCENARIOS_THREAD_ID or 0)
        except Exception:
            return 0

    def _success_trades_thread_id(self):
        try:
            return int(SIGNAL_SUCCESS_TRADES_THREAD_ID or 0)
        except Exception:
            return 0

    def _important_thread_id(self):
        return 0

    def _normalized_signal_thread_id(self, chat_id, message_thread_id):
        signal_chat = str(self._signal_chat_id() or "").strip()
        current_chat = str(chat_id or "").strip()
        if signal_chat and current_chat == signal_chat:
            try:
                thread_val = int(message_thread_id or 0)
            except Exception:
                thread_val = 0
            # Telegram forum "General" topic may arrive without message_thread_id.
            if thread_val <= 0:
                return 1
            return thread_val
        try:
            return int(message_thread_id or 0)
        except Exception:
            return 0

    def _is_specific_signal_topic(self, chat_id, message_thread_id, target_thread_id):
        signal_chat = str(self._signal_chat_id() or "").strip()
        current_chat = str(chat_id or "").strip()
        current_thread = self._normalized_signal_thread_id(chat_id, message_thread_id)
        target_thread = int(target_thread_id or 0)
        return bool(signal_chat and current_chat == signal_chat and target_thread > 0 and current_thread == target_thread)

    def _is_scenarios_topic(self, chat_id, message_thread_id):
        return self._is_specific_signal_topic(chat_id, message_thread_id, self._scenarios_thread_id())

    def _is_analytics_topic(self, chat_id, message_thread_id):
        return self._is_specific_signal_topic(chat_id, message_thread_id, self._trading_signal_thread_id())

    def _is_liqmap_topic(self, chat_id, message_thread_id):
        return self._is_specific_signal_topic(chat_id, message_thread_id, self._general_thread_id())

    def _is_signal_command_topic(self, chat_id, message_thread_id):
        return self._is_specific_signal_topic(chat_id, message_thread_id, self._trading_signal_thread_id())

    def _is_blocked_signal_topic(self, chat_id, message_thread_id):
        signal_chat = str(self._signal_chat_id() or "").strip()
        current_chat = str(chat_id or "").strip()
        current_thread = self._normalized_signal_thread_id(chat_id, message_thread_id)
        blocked_threads = {int(x) for x in (SIGNAL_BLOCKED_THREAD_IDS or ()) if int(x) > 0}
        return bool(signal_chat and current_chat == signal_chat and current_thread in blocked_threads)

    def _should_delete_signal_chat_message(self, chat_id, message_thread_id):
        signal_chat = str(self._signal_chat_id() or "").strip()
        current_chat = str(chat_id or "").strip()
        current_thread = self._normalized_signal_thread_id(chat_id, message_thread_id)
        blocked_threads = {int(x) for x in (SIGNAL_BLOCKED_THREAD_IDS or ()) if int(x) > 0}
        if not signal_chat or current_chat != signal_chat or not blocked_threads:
            return False
        return current_thread in blocked_threads

    def _bot_mention_aliases(self):
        aliases = []
        for alias in (BOT_MENTION_ALIASES or ()):
            norm = str(alias or "").strip().lstrip("@").lower()
            if norm and norm not in aliases:
                aliases.append(norm)
        if not aliases:
            aliases = ["mrponchvvip_bot", "mrponch"]
        return tuple(aliases)

    def _extract_group_mention_prompt(self, message):
        chat_type = str(((message or {}).get("chat") or {}).get("type") or "").strip().lower()
        if chat_type not in {"group", "supergroup"}:
            return ""
        text = str((message or {}).get("text") or "").strip()
        if not text or text.startswith("/"):
            return ""

        found = False
        cleaned = text
        for alias in self._bot_mention_aliases():
            pattern = rf"(?<!\w)@{re.escape(alias)}\b"
            if re.search(pattern, cleaned, flags=re.IGNORECASE):
                cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
                found = True

        if not found:
            return ""

        cleaned = re.sub(r"\s+", " ", cleaned).strip(" \n\t,:-")
        return cleaned

    def _handle_group_mention_message(self, message):
        prompt = self._extract_group_mention_prompt(message)
        if prompt == "" and not any(
            re.search(rf"(?<!\w)@{re.escape(alias)}\b", str((message or {}).get("text") or ""), flags=re.IGNORECASE)
            for alias in self._bot_mention_aliases()
        ):
            return False

        chat_obj = message.get("chat") or {}
        chat_id = chat_obj.get("id")
        message_thread_id = message.get("message_thread_id")
        reply_to_message_id = message.get("message_id")

        if not self._is_specific_signal_topic(chat_id, message_thread_id, self._general_thread_id()):
            self._silence_restricted_command(message)
            return True

        if not GEMINI_API_KEY:
            self._send_text_chunks(
                chat_id,
                "I’m here, but the Gemini key is missing in the bot config right now.",
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
            return True

        if not prompt:
            self._send_text_chunks(
                chat_id,
                "I’m here. Mention me with your question and I’ll answer in this group.",
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
            return True

        group_context = (
            f"Group mention chat title: {chat_obj.get('title') or 'Unknown'}\n"
            f"Thread id: {self._normalized_signal_thread_id(chat_id, message_thread_id)}\n\n"
            f"{self._build_gemini_trade_context()}"
        )
        answer = self._ask_private_chat_question(prompt, context_text=group_context)
        answer = str(answer or "").strip()
        if not answer:
            answer = "I couldn’t form a clean answer this time. Send it again a little more simply and I’ll retry."

        self._send_text_chunks(
            chat_id,
            answer,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
        )
        return True

    def _should_answer_general_group_message(self, message):
        text = str((message or {}).get("text") or "").strip()
        if not text or text.startswith("/"):
            return False

        if self._extract_group_mention_prompt(message):
            return True

        reply_to = (message or {}).get("reply_to_message") or {}
        reply_from = (reply_to.get("from") or {})
        if bool(reply_from.get("is_bot")):
            return True

        return self._contains_general_bot_name(text)

    def _contains_general_bot_name(self, text):
        lower = str(text or "").lower().strip()
        if not lower:
            return False

        exact_names = list(self._bot_mention_aliases()) + ["ponch", "mr ponch", "big yahoo"]
        if any(re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", lower) for alias in exact_names):
            return True

        fuzzy_patterns = [
            r"\bmr\s*p[o0]n+c+h?\b",
            r"\bmrp[o0]n+c+h?\b",
            r"\bp[o0]n+c+h?\b",
            r"\bmr\s*p[o0]n+h\b",
            r"\bp[o0]n+h\b",
            r"\bp[o0]n+s+h\b",
            r"\bp[uo]n+c+h\b",
            r"\bb[i1]g\s+y[a@]h+o+o?\b",
            r"\bb[i1]gy[a@]h+o+o?\b",
        ]
        return any(re.search(pattern, lower, flags=re.IGNORECASE) for pattern in fuzzy_patterns)

    def _group_context_key(self, message):
        chat_id = str(((message or {}).get("chat") or {}).get("id") or "")
        thread_id = str(self._normalized_signal_thread_id(((message or {}).get("chat") or {}).get("id"), (message or {}).get("message_thread_id")) or "")
        user_id = str(((message or {}).get("from") or {}).get("id") or "")
        return f"{chat_id}:{thread_id}:{user_id}"

    def _recent_group_chat_context(self, message):
        key = self._group_context_key(message)
        ctx = dict((self.group_chat_contexts or {}).get(key) or {})
        ts = float(ctx.get("updated_at") or 0)
        if not ts or (time.time() - ts) > 3600:
            if key in (self.group_chat_contexts or {}):
                self.group_chat_contexts.pop(key, None)
                self._save_state()
            return {}
        return ctx

    def _remember_group_chat_context(self, message, *, prompt=None, answer=None, symbol=None):
        key = self._group_context_key(message)
        payload = {
            "updated_at": time.time(),
            "prompt": str(prompt or "").strip(),
            "answer": str(answer or "").strip(),
            "symbol": str(symbol or "").strip().upper(),
            "message_id": (message or {}).get("message_id"),
        }
        self.group_chat_contexts[key] = payload
        self._save_state()

    def _handle_general_group_chat_message(self, message):
        chat_obj = message.get("chat") or {}
        chat_id = chat_obj.get("id")
        message_thread_id = message.get("message_thread_id")
        reply_to_message_id = message.get("message_id")
        from_obj = message.get("from") or {}
        if bool(from_obj.get("is_bot")):
            return False
        if not self._is_specific_signal_topic(chat_id, message_thread_id, self._general_thread_id()):
            return False

        text = str(message.get("text") or "").strip()
        if not self._should_answer_general_group_message(message):
            return False

        prompt = self._extract_group_mention_prompt(message) or text
        if not prompt:
            return False

        if not GEMINI_API_KEY:
            self._send_text_chunks(
                chat_id,
                "I’m here, but the Gemini key is missing in the bot config right now.",
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
            return True

        if self._looks_like_chart_question(prompt):
            try:
                answer = self._build_symbol_chart_chat_answer(prompt)
            except Exception as e:
                answer = f"I tried to load that chart from Bitunix/OKX, but it failed this time: {e}"
            self._send_text_chunks(
                chat_id,
                answer,
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
            return True

        group_context = (
            "You are answering inside the Mr. Ponch Telegram general discussion topic. "
            "Reply naturally and conversationally, like a helpful trading assistant in group chat. "
            "Keep answers clear and not too long. "
            "If the topic is politics, religion, ethnicity, nationality, or identity, stay neutral, respectful, and factual. "
            "Do not express partisan loyalty, hatred, or favoritism.\n\n"
            f"Group chat title: {chat_obj.get('title') or 'Unknown'}\n"
            f"Thread id: {self._normalized_signal_thread_id(chat_id, message_thread_id)}\n\n"
            f"{self._build_gemini_trade_context()}"
        )
        answer = self._ask_private_chat_question(prompt, context_text=group_context)
        answer = str(answer or "").strip()
        if not answer:
            answer = "I couldn’t form a clean answer this time. Send it again a little more simply and I’ll retry."

        self._send_text_chunks(
            chat_id,
            answer,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
        )
        return True

    def _is_chat_admin_user(self, chat_id, user_id):
        try:
            cache_key = (str(chat_id or "").strip(), str(user_id or "").strip())
            now_ts = time.time()
            cached = (self.chat_member_status_cache or {}).get(cache_key)
            if cached and (now_ts - float(cached.get("ts", 0) or 0)) < 60:
                return bool(cached.get("is_admin"))

            member = tg.get_chat_member(chat_id, user_id)
            status = str((member or {}).get("status") or "").strip().lower()
            is_admin = status in {"administrator", "creator"}
            self.chat_member_status_cache[cache_key] = {"ts": now_ts, "is_admin": is_admin}
            return is_admin
        except Exception:
            return False

    def _moderate_signal_group_message(self, message):
        chat_obj = message.get("chat") or {}
        chat_id = chat_obj.get("id")
        message_id = message.get("message_id")
        message_thread_id = message.get("message_thread_id")
        from_obj = message.get("from") or {}
        sender_is_bot = bool(from_obj.get("is_bot"))
        normalized_thread = self._normalized_signal_thread_id(chat_id, message_thread_id)

        if not self._should_delete_signal_chat_message(chat_id, message_thread_id):
            return False
        if not message_id or sender_is_bot:
            return False

        deleted = tg.delete_message(chat_id, message_id)
        print(
            f"[MOD] signal-group moderation chat={chat_id} raw_thread={message_thread_id} "
            f"normalized_thread={normalized_thread} message_id={message_id} deleted={deleted}"
        )
        return True

    def _silence_restricted_command(self, message):
        chat_obj = message.get("chat") or {}
        chat_id = chat_obj.get("id")
        message_id = message.get("message_id")
        chat_type = str(chat_obj.get("type") or "").strip().lower()
        if chat_id and message_id and chat_type in {"group", "supergroup"}:
            tg.delete_message(chat_id, message_id)
        return True

    def _handle_restricted_topic_command(self, message):
        text = str(message.get("text") or message.get("caption") or "").strip()
        if not text.startswith("/"):
            return False
        cmd = text.lower().split()[0] if text else ""
        cmd_base = cmd.split("@", 1)[0]
        chat_id = (message.get("chat") or {}).get("id")
        message_thread_id = message.get("message_thread_id")

        if cmd_base == "/intraday":
            return self._silence_restricted_command(message)
        if cmd_base == "/scenarios" and not self._is_scenarios_topic(chat_id, message_thread_id):
            return self._silence_restricted_command(message)
        if cmd_base == "/analytics" and not self._is_analytics_topic(chat_id, message_thread_id):
            return self._silence_restricted_command(message)
        if cmd_base == "/liqmap" and not self._is_liqmap_topic(chat_id, message_thread_id):
            return self._silence_restricted_command(message)
        return False

    def _execution_updates_private_only(self):
        return bool(EXECUTION_UPDATES_PRIVATE_ONLY and self._execution_chat_id())

    def _current_public_signal_chat(self):
        return str(self._signal_chat_id() or "").strip()

    def _is_current_public_signal(self, sig):
        return str((sig or {}).get("chat_id") or "").strip() == self._current_public_signal_chat()

    def _public_signal_message_id(self, sig):
        sig = sig or {}
        for key in ("trading_signal_msg_id", "msg_id"):
            try:
                msg_id = int(sig.get(key) or 0)
            except Exception:
                msg_id = 0
            if msg_id > 0:
                return msg_id
        return 0

    def _active_trade_card_message_id(self, sig):
        sig = sig or {}
        for key in ("active_signal_msg_id", "active_snapshot_msg_id"):
            try:
                msg_id = int(sig.get(key) or 0)
            except Exception:
                msg_id = 0
            if msg_id > 0:
                return msg_id
        for raw in reversed(list(sig.get("active_thread_message_ids") or [])):
            try:
                msg_id = int(raw or 0)
            except Exception:
                msg_id = 0
            if msg_id > 0:
                return msg_id
        return 0

    def _signal_timeframe_label(self, sig):
        sig = sig or {}
        tf_val = str(sig.get("tf") or "N/A")
        indicators = (sig.get("meta") or {}).get("indicators") or []
        if tf_val == "Confluence" and indicators:
            tfs = sorted({str(ind.get("tf") or "N/A") for ind in indicators})
            return ", ".join(tfs)
        return tf_val

    def _signal_effective_state(self, sig):
        sig = sig or {}
        execution = sig.get("execution") or {}
        missing_tp_indices = {
            int(i) for i in (execution.get("missing_tp_indices") or [])
            if str(i).strip().isdigit()
        }
        tp1_h = bool(sig.get("tp1_hit"))
        tp2_h = bool(sig.get("tp2_hit"))
        tp3_h = bool(sig.get("tp3_hit"))
        if tp2_h and 1 not in missing_tp_indices:
            tp1_h = True
        if tp3_h and 1 not in missing_tp_indices:
            tp1_h = True
        if tp3_h and 2 not in missing_tp_indices:
            tp2_h = True

        status = str(sig.get("status", "OPEN") or "OPEN").upper()
        entry = float(sig.get("entry", 0) or 0)
        sl = float(sig.get("sl", 0) or 0)
        initial_sl = float(sig.get("initial_sl", sl) or sl)
        sl_h = bool(sig.get("sl_hit"))

        if status == "SL" and tp1_h:
            if abs(sl - entry) < 1e-9:
                status = "ENTRY_CLOSE"
            elif abs(sl - initial_sl) > 1e-9:
                status = "PROFIT_SL"

        return {
            "status": status,
            "tp1_h": tp1_h,
            "tp2_h": tp2_h,
            "tp3_h": tp3_h,
            "sl_h": sl_h,
            "initial_sl": initial_sl,
        }

    def _track_active_trade_message(self, sig, message_id):
        try:
            msg_id = int(message_id or 0)
        except Exception:
            return
        if msg_id <= 0 or not sig:
            return
        existing = []
        for raw in list(sig.get("active_thread_message_ids") or []):
            try:
                existing.append(int(raw))
            except Exception:
                continue
        if msg_id not in existing:
            existing.append(msg_id)
        sig["active_thread_message_ids"] = existing

    def _cleanup_active_trade_messages(self, sig):
        if not sig:
            return
        target_chat = self._signal_chat_id()
        if not target_chat:
            sig["active_thread_message_ids"] = []
            sig["active_signal_msg_id"] = None
            sig["active_snapshot_msg_id"] = None
            return
        cleaned = False
        had_snapshot = bool(
            self._active_trade_card_message_id(sig)
            or list(sig.get("active_thread_message_ids") or [])
        )
        seen = set()
        raw_ids = list(sig.get("active_thread_message_ids") or [])
        raw_ids.extend([
            sig.get("active_signal_msg_id"),
            sig.get("active_snapshot_msg_id"),
        ])
        for raw_id in raw_ids:
            try:
                msg_id = int(raw_id)
            except Exception:
                continue
            if msg_id <= 0 or msg_id in seen:
                continue
            seen.add(msg_id)
            tg.delete_message(target_chat, msg_id)
            cleaned = True
        sig["active_thread_message_ids"] = []
        sig["active_signal_msg_id"] = None
        sig["active_snapshot_msg_id"] = None
        if cleaned or had_snapshot:
            self._save_state()

    def _compact_usd(self, value):
        try:
            value = float(value or 0)
        except Exception:
            value = 0.0
        if abs(value) >= 1_000_000_000:
            return f"${value/1_000_000_000:.2f}B"
        if abs(value) >= 1_000_000:
            return f"${value/1_000_000:.1f}M"
        if abs(value) >= 1_000:
            return f"${value/1_000:.0f}K"
        return f"${value:.0f}"

    def _signal_strategy_label(self, sig):
        return str((sig.get("meta") or {}).get("strategy") or sig.get("type") or "Setup").replace("_", " ").title()

    def _active_trade_snapshot_caption(self, sig, *, event_label=None, close_price=None):
        side = str(sig.get("side") or "").upper() or "BTC"
        strategy = self._signal_strategy_label(sig)
        tf_label = self._signal_timeframe_label(sig)
        state = self._signal_effective_state(sig)
        status = str(state["status"] or "OPEN")
        header_map = {
            "OPEN": "📍 <b>ENTRY CONFIRMED</b>",
            "TP1": "🟢 <b>TP1 HIT</b>",
            "TP2": "⚡️ <b>TP2 HIT</b>",
            "TP3": "🎯 <b>ALL TARGETS HIT</b>",
            "SL": "❌ <b>STOP LOSS HIT</b>",
            "ENTRY_CLOSE": "🟡 <b>BREAKEVEN EXIT</b>",
            "PROFIT_SL": "🛡 <b>PROTECTED EXIT</b>",
            "CLOSED": "🛑 <b>TRADE CLOSED</b>",
        }
        header = header_map.get(status, "📍 <b>BTC SIGNAL</b>")
        if event_label and status == "OPEN":
            header = f"📍 <b>{str(event_label).upper()} UPDATE</b>"
        levels_code = tg.get_signal_levels_code(
            float(sig.get("entry", 0) or 0),
            float(sig.get("sl", 0) or 0),
            float(sig.get("tp1", 0) or 0),
            float(sig.get("tp2", 0) or 0),
            float(sig.get("tp3", 0) or 0),
            status=status,
            tp1_h=state["tp1_h"],
            tp2_h=state["tp2_h"],
            tp3_h=state["tp3_h"],
            sl_h=state["sl_h"],
            initial_sl=state["initial_sl"],
        )
        lines = [
            f"{side} | {tf_label}",
            f"Setup: {strategy}",
        ]
        size = (sig.get("meta") or {}).get("size")
        if size is not None:
            try:
                lines.append(f"Size: {float(size):.1f}%")
            except Exception:
                lines.append(f"Size: {size}")
        reinf_count = int(((sig.get("meta") or {}).get("reinforcement_count", 0)) or 0)
        add_count = int(((sig.get("meta") or {}).get("same_side_add_count", 0)) or 0)
        if reinf_count > 0:
            lines.append(f"Reinforcement: {reinf_count}x")
        if add_count > 0:
            lines.append(f"Scale-ins: {add_count}x")
        if (sig.get("meta") or {}).get("runner_bias_confirmed"):
            lines.append("Bias: hold runner")
        if close_price is not None:
            lines.append(f"Current: {float(close_price):,.2f}")
        return f"{header} [{tf_label}]\n<blockquote>{chr(10).join(lines)}</blockquote>\n<pre>{levels_code}</pre>"

    def _public_signal_caption(self, sig, *, status_override="OPEN"):
        sig = sig or {}
        meta = sig.get("meta", {}) or {}
        if meta.get("reinforcement_owner_signal_id"):
            side = str(sig.get("side") or "").upper() or "BTC"
            tf_label = self._signal_timeframe_label(sig)
            side_emoji = "🟢" if side == "LONG" else "🔴"
            owner_tf = meta.get("reinforcement_owner_tf") or "N/A"
            action_line = (
                "Action: controlled add into the protected live position."
                if meta.get("same_side_add_allowed")
                else "Action: confirmation only. No new separate trade will be opened."
            )
            note = str(meta.get("same_side_add_reason") or "Same-side confirmation while the main trade is already live.").strip()
            levels_code = tg.get_signal_levels_code(
                float(sig.get("entry", 0) or 0),
                float(sig.get("sl", 0) or 0),
                float(sig.get("tp1", 0) or 0),
                float(sig.get("tp2", 0) or 0),
                float(sig.get("tp3", 0) or 0),
                status="OPEN",
                initial_sl=sig.get("initial_sl", sig.get("sl")),
            )
            header = f"<b>{side_emoji} {side} REINFORCEMENT</b> [{tf_label}]"
            body_lines = [
                f"Existing trade: {side} [{owner_tf}]",
                action_line,
                note,
            ]
            return f"{header}\n<blockquote>{chr(10).join(body_lines)}</blockquote>\n<pre>{levels_code}</pre>"
        signal_type = str(sig.get("type") or "SCALP").upper()
        signal_html_type = signal_type if signal_type in {"SCALP", "STRONG", "EXTREME"} else "SCALP"
        tf_val = sig.get("tf")
        indicators = meta.get("indicators")
        if tf_val == "Confluence" and indicators:
            tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators)))
            tf_val = ", ".join(tfs)
        return tg.get_signal_html(
            signal_type=signal_html_type,
            side=sig.get("side"),
            timeframe=tf_val or "N/A",
            entry=sig.get("entry"),
            sl=sig.get("sl"),
            initial_sl=sig.get("initial_sl", sig.get("sl")),
            tp1=sig.get("tp1"),
            tp2=sig.get("tp2"),
            tp3=sig.get("tp3"),
            status=status_override,
            score=meta.get("score"),
            trend=meta.get("trend"),
            indicators=indicators,
            reasons=meta.get("reasons"),
            size=sig.get("signal_size_pct", meta.get("size")),
            tp_liq_prob=meta.get("tp_liq_prob"),
            tp_liq_usd=meta.get("tp_liq_usd"),
            tp_liq_target=meta.get("tp_liq_target"),
            trigger_label=meta.get("trigger"),
        )

    def _send_public_signal_snapshot(self, sig, *, status_override="OPEN"):
        if not sig or self.is_booting:
            return None
        side = str(sig.get("side") or "").upper() or "BTC"
        chart_path = self._generate_signal_snapshot_chart(
            sig,
            output_name=f"public_signal_{str(sig.get('signal_id') or 'sig')}.png",
            title=f"{side} SIGNAL",
        )
        caption = self._public_signal_caption(sig, status_override=status_override)
        try:
            if chart_path:
                sig["public_signal_is_photo"] = True
                resp = tg.send_photo(
                    chart_path,
                    caption=caption,
                    chat_id=self._signal_chat_id(),
                    message_thread_id=self._trading_signal_thread_id(),
                )
                return resp
            sig["public_signal_is_photo"] = False
            return tg.send(
                caption,
                parse_mode="HTML",
                chat_id=self._signal_chat_id(),
                message_thread_id=self._trading_signal_thread_id(),
            )
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _refresh_public_signal_snapshot(self, sig, *, close_price=None, event_label=None):
        if not sig or self.is_booting:
            return
        if not self._is_current_public_signal(sig):
            return
        state = self._signal_effective_state(sig)
        status_override = str(state.get("status") or "OPEN")
        msg_id = self._public_signal_message_id(sig)
        if msg_id <= 0:
            resp = self._send_public_signal_snapshot(sig, status_override=status_override)
            public_msg_id = (resp or {}).get("result", {}).get("message_id") if resp else None
            if public_msg_id:
                sig["msg_id"] = public_msg_id
                sig["trading_signal_msg_id"] = public_msg_id
                self._save_state()
            return
        if not sig.get("public_signal_is_photo"):
            tg.update_signal_message(
                self._signal_chat_id(),
                msg_id,
                sig,
                use_caption=False,
            )
            return
        side = str(sig.get("side") or "").upper() or "BTC"
        title = f"{side} SIGNAL"
        if event_label:
            title = f"{str(event_label).upper()} UPDATE"
        chart_path = self._generate_signal_snapshot_chart(
            sig,
            output_name=f"public_signal_refresh_{str(sig.get('signal_id') or 'sig')}.png",
            close_price=close_price,
            event_label=event_label,
            title=title,
        )
        if not chart_path:
            return
        try:
            result = tg.edit_message_media(
                msg_id,
                chart_path,
                caption=self._public_signal_caption(sig, status_override=status_override),
                chat_id=self._signal_chat_id(),
            )
            if result is False:
                time.sleep(0.35)
                result = tg.edit_message_media(
                    msg_id,
                    chart_path,
                    caption=self._public_signal_caption(sig, status_override=status_override),
                    chat_id=self._signal_chat_id(),
                )
            if result == "DELETED":
                sig["msg_id"] = None
                sig["trading_signal_msg_id"] = None
                self._save_state()
                resp = self._send_public_signal_snapshot(sig, status_override=status_override)
                public_msg_id = (resp or {}).get("result", {}).get("message_id") if resp else None
                if public_msg_id:
                    sig["msg_id"] = public_msg_id
                    sig["trading_signal_msg_id"] = public_msg_id
                    self._save_state()
            elif result is False:
                print(f"  [TG] Public signal chart refresh failed for signal {self._signal_id_value(sig)}.")
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _trade_journal_caption(self, sig, outcome_label, r_mult, note, *, close_price=None):
        side = str(sig.get("side") or "").upper()
        strategy = self._signal_strategy_label(sig)
        lines = [
            f"{side} | {outcome_label} | {float(r_mult):+.2f}R",
            f"Setup: {strategy}",
            f"Entry: {float(sig.get('entry', 0) or 0):,.2f}",
        ]
        if close_price is not None:
            lines.append(f"Close: {float(close_price):,.2f}")
        return f"📘 <b>TRADE JOURNAL</b>\n<blockquote>{chr(10).join(lines)}</blockquote>\n{note}"

    def _success_trade_caption(self, sig, outcome_label, r_mult, note, *, close_price=None):
        side = str(sig.get("side") or "").upper()
        strategy = self._signal_strategy_label(sig)
        tf_label = self._signal_timeframe_label(sig)
        lines = [
            f"{side} | {tf_label} | {float(r_mult):+.2f}R",
            f"Setup: {strategy}",
            f"Entry: {float(sig.get('entry', 0) or 0):,.2f}",
        ]
        if close_price is not None:
            lines.append(f"Close: {float(close_price):,.2f}")
        if sig.get("tp1_hit"):
            lines.append("TP1: hit")
        if sig.get("tp2_hit"):
            lines.append("TP2: hit")
        if sig.get("tp3_hit"):
            lines.append("TP3: hit")
        return f"🏆 <b>TODAY'S SUCCESSFUL TRADE</b>\n<blockquote>{chr(10).join(lines)}</blockquote>\n{note}"

    def _signal_chart_timeframe(self, sig):
        tf = str((sig or {}).get("tf") or "").strip().lower()
        if tf == "5m":
            return "5m", 160
        if tf == "15m":
            return "15m", 140
        if tf == "1h":
            return "15m", 160
        return "1h", 120

    def _generate_signal_snapshot_chart(self, sig, *, output_name="signal_snapshot.png", close_price=None, event_label=None, title="Signal Setup"):
        if not sig:
            return None
        tf, limit = self._signal_chart_timeframe(sig)
        try:
            symbol_name = str((sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL)).upper()
            live_price = close_price
            if live_price is None:
                live_price = self._current_market_price(sig.get("tf"), symbol=symbol_name)
            df = self._snapshot_dataframe(sig, tf, limit, close_price=live_price)
            if df is None or df.empty:
                return None
            return generate_signal_setup_chart(
                df,
                side=sig.get("side"),
                entry=float(sig.get("entry", 0) or 0),
                sl=float(sig.get("sl", 0) or 0),
                tp1=float(sig.get("tp1", 0) or 0),
                tp2=float(sig.get("tp2", 0) or 0),
                tp3=float(sig.get("tp3", 0) or 0),
                symbol=symbol_name,
                timeframe=tf,
                output_path=output_name,
                title=title,
                close_price=live_price,
                event_label=event_label,
            )
        except Exception as e:
            print(f"[CHART] Signal snapshot failed: {e}")
            return None

    def _send_active_trade_snapshot(self, sig):
        if not sig or self.is_booting:
            return
        side = str(sig.get("side") or "").upper() or "BTC"
        chart_path = self._generate_signal_snapshot_chart(
            sig,
            output_name=f"active_trade_{str(sig.get('signal_id') or 'sig')}.png",
            title=f"{side} PLAN",
        )
        if not chart_path:
            return
        try:
            caption = self._active_trade_snapshot_caption(sig)
            resp = tg.send_photo(
                chart_path,
                caption=caption,
                chat_id=self._signal_chat_id(),
                message_thread_id=self._active_trades_thread_id(),
            )
            snapshot_msg_id = (resp or {}).get("result", {}).get("message_id")
            sig["active_signal_msg_id"] = snapshot_msg_id
            sig["active_snapshot_msg_id"] = snapshot_msg_id
            self._track_active_trade_message(sig, snapshot_msg_id)
            self._save_state()
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _refresh_active_trade_snapshot(self, sig, *, close_price=None, event_label=None):
        if not sig or self.is_booting:
            return
        msg_id = self._active_trade_card_message_id(sig)
        if msg_id <= 0:
            self._send_active_trade_snapshot(sig)
            return
        side = str(sig.get("side") or "").upper() or "BTC"
        title = f"{side} PLAN"
        if event_label:
            title = f"{str(event_label).upper()} UPDATE"
        chart_path = self._generate_signal_snapshot_chart(
            sig,
            output_name=f"active_trade_refresh_{str(sig.get('signal_id') or 'sig')}.png",
            close_price=close_price,
            event_label=event_label,
            title=title,
        )
        if not chart_path:
            return
        try:
            result = tg.edit_message_media(
                msg_id,
                chart_path,
                caption=self._active_trade_snapshot_caption(sig, event_label=event_label, close_price=close_price),
                chat_id=self._signal_chat_id(),
            )
            if result is False:
                time.sleep(0.35)
                result = tg.edit_message_media(
                    msg_id,
                    chart_path,
                    caption=self._active_trade_snapshot_caption(sig, event_label=event_label, close_price=close_price),
                    chat_id=self._signal_chat_id(),
                )
            if result == "DELETED":
                sig["active_signal_msg_id"] = None
                sig["active_snapshot_msg_id"] = None
                self._save_state()
                self._send_active_trade_snapshot(sig)
            elif result is False:
                print(f"  [TG] Active trade chart refresh failed for signal {self._signal_id_value(sig)}.")
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _cleanup_finished_active_trade_cards(self):
        terminal_statuses = {"TP3", "SL", "ENTRY_CLOSE", "PROFIT_SL", "CLOSED"}
        cleaned_any = False
        for sig in list(self.tracker.signals or []):
            execution = (sig or {}).get("execution") or {}
            has_active_messages = bool(
                self._active_trade_card_message_id(sig)
                or list(sig.get("active_thread_message_ids") or [])
            )
            if not has_active_messages:
                continue
            status = str(sig.get("status") or "").upper()
            if status in terminal_statuses or (execution and execution.get("active") is False):
                self._cleanup_active_trade_messages(sig)
                cleaned_any = True
        if cleaned_any:
            self._save_state()

    def _scenario_trading_enabled(self):
        return bool(BITUNIX_SCENARIO_TRADING_ENABLED and self.trade_executor.can_trade())

    def _smart_hedge_enabled(self):
        return bool(
            BITUNIX_AUTO_HEDGE_ENABLED
            and self._hedge_mode_enabled()
            and self.trade_executor.can_trade()
        )

    def _is_hedge_signal(self, sig):
        sig = sig or {}
        meta = sig.get("meta") or {}
        execution = sig.get("execution") or {}
        strategy = str(meta.get("strategy") or "").strip().upper()
        if strategy == "SMART_HEDGE":
            return True
        if execution.get("hedge_parent_signal_id"):
            return True
        return False

    def _execution_effective_entry(self, sig):
        execution = (sig or {}).get("execution") or {}
        for key in ("filled_entry_price", "entry"):
            try:
                price = float(execution.get(key) or 0)
            except Exception:
                price = 0.0
            if price > 0:
                return price
        try:
            return float((sig or {}).get("entry") or 0)
        except Exception:
            return 0.0

    def _signal_stop_progress_r(self, sig, current_price):
        sig = sig or {}
        side = str(sig.get("side") or "").upper()
        entry = self._execution_effective_entry(sig)
        execution = sig.get("execution") or {}
        try:
            stop_price = float(
                execution.get("sl_moved_to")
                or sig.get("sl")
                or sig.get("initial_sl")
                or 0
            )
        except Exception:
            stop_price = 0.0
        current = float(current_price or 0)
        if entry <= 0 or stop_price <= 0 or current <= 0:
            return 0.0
        risk = abs(entry - stop_price)
        if risk <= 0:
            return 0.0
        if side == "LONG":
            return max(0.0, (entry - current) / risk)
        if side == "SHORT":
            return max(0.0, (current - entry) / risk)
        return 0.0

    def _active_primary_execution_signals(self):
        return [sig for sig in self._active_execution_signals() if not self._is_hedge_signal(sig)]

    def _active_hedge_execution_signals(self):
        return [sig for sig in self._active_execution_signals() if self._is_hedge_signal(sig)]

    def _find_active_primary_opposite_execution(self, side, symbol=None):
        wanted_symbol = str(symbol or SYMBOL).upper()
        opposite = "SHORT" if str(side or "").upper() == "LONG" else "LONG"
        for sig in self._active_primary_execution_signals():
            sig_side = str(sig.get("side") or "").upper()
            sig_symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL).upper()
            if sig_side == opposite and sig_symbol == wanted_symbol:
                return sig
        return None

    def _find_active_primary_same_side_execution(self, side, symbol=None):
        wanted_symbol = str(symbol or SYMBOL).upper()
        wanted_side = str(side or "").upper()
        if not wanted_side:
            return None
        for sig in self._active_primary_execution_signals():
            sig_side = str(sig.get("side") or "").upper()
            sig_symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL).upper()
            if sig_side == wanted_side and sig_symbol == wanted_symbol:
                return sig
        return None

    def _signal_quality_score(self, sig):
        meta = (sig or {}).get("meta") or {}
        for raw in (meta.get("score"), meta.get("scenario_probability")):
            try:
                value = float(raw or 0)
            except Exception:
                value = 0.0
            if value > 10:
                value = value / 10.0
            if value > 0:
                return value
        return 0.0

    def _protected_stop_active(self, sig):
        sig = sig or {}
        side = str(sig.get("side") or "").upper()
        entry = float(self._execution_effective_entry(sig) or 0)
        execution = sig.get("execution") or {}
        try:
            stop_price = float(execution.get("sl_moved_to") or sig.get("sl") or sig.get("initial_sl") or 0)
        except Exception:
            stop_price = 0.0
        if entry <= 0 or stop_price <= 0:
            return False
        tolerance = max(entry * 0.0002, 5.0)
        if side == "LONG":
            return stop_price >= (entry - tolerance)
        if side == "SHORT":
            return stop_price <= (entry + tolerance)
        return False

    def _same_side_entry_improvement(self, candidate_sig, owner_sig):
        candidate_sig = candidate_sig or {}
        owner_sig = owner_sig or {}
        side = str(candidate_sig.get("side") or "").upper()
        try:
            candidate_entry = float(candidate_sig.get("entry") or 0)
        except Exception:
            candidate_entry = 0.0
        owner_entry = float(self._execution_effective_entry(owner_sig) or 0)
        if candidate_entry <= 0 or owner_entry <= 0:
            return False, 0.0, 0.0
        threshold = max(owner_entry * 0.0025, 150.0)
        if side == "LONG":
            improvement = owner_entry - candidate_entry
        elif side == "SHORT":
            improvement = candidate_entry - owner_entry
        else:
            improvement = 0.0
        return improvement >= threshold, improvement, threshold

    def _is_same_side_add_signal(self, sig, owner_sig=None):
        meta = (sig or {}).get("meta") or {}
        if not bool(meta.get("same_side_add_allowed")):
            return False
        owner_id = str(meta.get("same_side_owner_signal_id") or "").strip()
        if not owner_id:
            return False
        if owner_sig is None:
            return True
        return owner_id == str(self._signal_id_value(owner_sig) or "").strip()

    def _record_same_side_reinforcement(self, owner_sig, candidate_sig, *, add_candidate=False):
        if not owner_sig or not candidate_sig:
            return
        meta = owner_sig.setdefault("meta", {})
        execution = owner_sig.setdefault("execution", {})
        now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        count = int(meta.get("reinforcement_count", 0) or 0) + 1
        meta["reinforcement_count"] = count
        meta["reinforcement_last_at"] = now_text
        meta["reinforcement_last_tf"] = candidate_sig.get("tf")
        meta["reinforcement_last_side"] = candidate_sig.get("side")
        meta["reinforcement_last_score"] = round(self._signal_quality_score(candidate_sig), 1)
        meta["runner_bias_confirmed"] = True
        meta["confidence_reinforced"] = True
        if add_candidate:
            meta["same_side_add_candidate_count"] = int(meta.get("same_side_add_candidate_count", 0) or 0) + 1
        execution["reinforcement_count"] = count

    def _record_same_side_add_execution(self, owner_sig, candidate_sig):
        if not owner_sig or not candidate_sig:
            return
        meta = owner_sig.setdefault("meta", {})
        execution = owner_sig.setdefault("execution", {})
        count = int(meta.get("same_side_add_count", 0) or 0) + 1
        meta["same_side_add_count"] = count
        meta["same_side_last_add_signal_id"] = self._signal_id_value(candidate_sig)
        meta["same_side_last_add_tf"] = candidate_sig.get("tf")
        meta["same_side_last_add_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        execution["same_side_add_count"] = count

    def _prepare_same_side_signal_behavior(self, sig, owner_sig):
        if not sig or not owner_sig or self._is_hedge_signal(sig):
            return "normal", None
        owner_id = str(self._signal_id_value(owner_sig) or "").strip()
        if not owner_id:
            return "normal", None

        meta = sig.setdefault("meta", {})
        meta["reinforcement_only"] = True
        meta["reinforcement_owner_signal_id"] = owner_id
        meta["reinforcement_owner_tf"] = owner_sig.get("tf")

        owner_tp1 = bool(owner_sig.get("tp1_hit"))
        owner_protected = self._protected_stop_active(owner_sig)
        candidate_quality = self._signal_quality_score(sig)
        owner_quality = self._signal_quality_score(owner_sig)
        better_ok, improvement, threshold = self._same_side_entry_improvement(sig, owner_sig)
        owner_add_count = int(((owner_sig.get("meta") or {}).get("same_side_add_count", 0)) or 0)
        owner_execution = owner_sig.get("execution") or {}
        owner_stop = float(owner_execution.get("sl_moved_to") or owner_sig.get("sl") or owner_sig.get("initial_sl") or 0)
        owner_qty = float(owner_execution.get("qty") or 0)
        owner_entry = float(self._execution_effective_entry(owner_sig) or 0)

        add_allowed = bool(
            owner_add_count < 1
            and (owner_tp1 or owner_protected)
            and candidate_quality >= owner_quality
            and better_ok
            and owner_qty > 0
            and owner_entry > 0
            and owner_stop > 0
        )

        meta["same_side_owner_signal_id"] = owner_id
        meta["same_side_owner_qty"] = owner_qty
        meta["same_side_owner_entry"] = owner_entry
        meta["same_side_owner_stop"] = owner_stop
        meta["same_side_owner_quality"] = round(owner_quality, 1)
        meta["same_side_candidate_quality"] = round(candidate_quality, 1)
        meta["same_side_entry_improvement"] = round(improvement, 2)
        meta["same_side_entry_threshold"] = round(threshold, 2)
        meta["same_side_add_allowed"] = add_allowed
        if add_allowed:
            meta["same_side_add_reason"] = "Protected trade with a clearly better same-side entry."
        else:
            meta["same_side_add_reason"] = "Confirmation only. Existing trade stays primary."

        self._record_same_side_reinforcement(owner_sig, sig, add_candidate=add_allowed)
        return ("add" if add_allowed else "reinforcement"), owner_sig

    def _find_active_hedge_for_parent(self, parent_signal_id):
        parent_signal_id = str(parent_signal_id or "").strip()
        if not parent_signal_id:
            return None
        for sig in self._active_hedge_execution_signals():
            execution = sig.get("execution") or {}
            meta = sig.get("meta") or {}
            hedge_parent = str(
                execution.get("hedge_parent_signal_id")
                or meta.get("hedge_parent_signal_id")
                or ""
            ).strip()
            if hedge_parent == parent_signal_id:
                return sig
        return None

    def _scenario_execution_size_pct(self, scenario):
        style = str((scenario or {}).get("risk_style") or "").strip().lower()
        probability = float((scenario or {}).get("probability") or 0.0)
        base = float(MIN_SIGNAL_SIZE_PCT)
        target = base
        if style == "normal":
            target = base + 2.5
        elif style == "medium":
            target = base + 1.6
        elif style == "reduced":
            target = base + 0.8
        if probability >= 70:
            target += 0.4
        elif probability < 58:
            target -= 0.3
        return round(min(float(MAX_SIGNAL_SIZE_PCT), max(float(MIN_SIGNAL_SIZE_PCT), target)), 1)

    def _smart_hedge_size_pct(self, scenario):
        base = float(self._scenario_execution_size_pct(scenario))
        reduced = max(float(MIN_SIGNAL_SIZE_PCT), base * float(BITUNIX_AUTO_HEDGE_SIZE_MULT))
        # Keep hedges intentionally smaller than normal directional trades.
        return round(min(base, reduced), 1)

    def _scenario_reason_tokens(self, scenario, payload):
        scenario = scenario or {}
        payload = payload or {}
        reasons = [str(scenario.get("kind") or "Scenario").replace("_", " ").title()]
        if scenario.get("trend_aligned"):
            reasons.append("Trend Align")
        funding_bias = str(((payload.get("funding_ctx") or {}).get("bias")) or "")
        side = str(scenario.get("side") or "").upper()
        if side == "LONG" and funding_bias == "shorts paying":
            reasons.append("Funding Support")
        elif side == "SHORT" and funding_bias == "longs paying":
            reasons.append("Funding Support")
        dominant_side = str(payload.get("liq_map", {}).get("dominant_side") or "")
        if side == "LONG" and dominant_side == "shorts_vulnerable":
            reasons.append("Short Squeeze")
        elif side == "SHORT" and dominant_side == "longs_vulnerable":
            reasons.append("Long Flush")
        return reasons[:4]

    def _scenario_trigger_ready(self, scenario, payload):
        scenario = scenario or {}
        payload = payload or {}
        tf_map = payload.get("tf_map") or {}
        tf_15m = tf_map.get("15m") or {}
        tf_1h = tf_map.get("1h") or {}
        current_price = float(payload.get("current_price") or 0.0)
        entry_low = float(scenario.get("entry_low") or 0.0)
        entry_high = float(scenario.get("entry_high") or 0.0)
        entry_mid = float(scenario.get("entry_mid") or ((entry_low + entry_high) / 2.0 if (entry_low or entry_high) else 0.0))
        kind = str(scenario.get("kind") or "").strip().lower()
        side = str(scenario.get("side") or "").strip().upper()
        bias_15m = str(tf_15m.get("bias") or "")
        bias_1h = str(tf_1h.get("bias") or "")
        reclaim_15m = tf_15m.get("active_reclaim") or {}
        reclaim_1h = tf_1h.get("active_reclaim") or {}
        channel_15m = tf_15m.get("channel_signal") or {}
        atr_15m = float(tf_15m.get("atr") or current_price * 0.0025)
        close_15m = float(tf_15m.get("close") or current_price)
        zone_pad = max(current_price * 0.00045, atr_15m * 0.08)
        in_zone = (entry_low - zone_pad) <= current_price <= (entry_high + zone_pad)

        bullish_bias = bias_15m in {"Bullish", "Trending Bullish"} or bias_1h in {"Bullish", "Trending Bullish"}
        bearish_bias = bias_15m in {"Bearish", "Trending Bearish"} or bias_1h in {"Bearish", "Trending Bearish"}
        reclaim_long = str(reclaim_15m.get("side") or reclaim_1h.get("side") or "").upper() == "LONG"
        reclaim_short = str(reclaim_15m.get("side") or reclaim_1h.get("side") or "").upper() == "SHORT"
        channel_short = str(channel_15m.get("side") or "").upper() == "SHORT"
        channel_long = str(channel_15m.get("side") or "").upper() == "LONG"

        if side == "LONG" and kind in {"pullback", "major_flush"}:
            return in_zone and (reclaim_long or bullish_bias or close_15m >= entry_mid)
        if side == "SHORT" and kind in {"rejection", "major_squeeze"}:
            return in_zone and (channel_short or bearish_bias or close_15m <= entry_mid)
        if side == "LONG" and kind == "breakout":
            return current_price >= entry_low and close_15m >= entry_low and bullish_bias
        if side == "SHORT" and kind == "breakdown":
            return current_price <= entry_high and close_15m <= entry_high and (reclaim_short or bearish_bias)
        return in_zone and (channel_long or channel_short or bullish_bias or bearish_bias)

    def _send_scenario_execution_signal(
        self,
        scenario,
        payload,
        now=None,
        *,
        size_pct_override=None,
        strategy_name="SCENARIO_PLAN",
        trigger_label="Scenario Plan",
        signal_type="SCALP",
        extra_meta=None,
    ):
        scenario = scenario or {}
        payload = payload or {}
        extra_meta = extra_meta or {}
        now = now or datetime.now(timezone.utc)
        current_price = float(payload.get("current_price") or 0.0)
        side = str(scenario.get("side") or "").upper()
        if not side or current_price <= 0:
            return False

        exec_tf = str(scenario.get("execution_tf") or "15m")
        size_pct = float(size_pct_override) if size_pct_override is not None else self._scenario_execution_size_pct(scenario)
        reasons = self._scenario_reason_tokens(scenario, payload)
        if str(strategy_name or "").upper() == "SMART_HEDGE":
            reasons = ["Smart Hedge"] + [r for r in reasons if r != "Smart Hedge"]
        strength_label = "Hedge" if str(strategy_name or "").upper() == "SMART_HEDGE" else "Scenario"
        signal_id = new_signal_id()
        tp_liq = self._estimate_tp_liquidity(side, current_price, scenario.get("tp1"), scenario.get("tp2"), scenario.get("tp3"))

        self.tracker.log_signal(
            side=side,
            entry=current_price,
            sl=float(scenario.get("stop") or 0),
            tp1=float(scenario.get("tp1") or 0),
            tp2=float(scenario.get("tp2") or 0),
            tp3=float(scenario.get("tp3") or 0),
            tf=exec_tf,
            timestamp=now.strftime("%Y-%m-%d %H:%M"),
            msg_id=None,
            chat_id=self._signal_chat_id(),
            signal_type=signal_type,
            meta={
                "signal_id": signal_id,
                "score": int(round(float(scenario.get("probability") or 0.0) / 10.0)),
                "trend": self.macro_trend,
                "trigger": trigger_label,
                "strategy": strategy_name,
                "reasons": reasons,
                "size": size_pct,
                "scenario_kind": scenario.get("kind"),
                "scenario_probability": scenario.get("probability"),
                "scenario_mode": scenario.get("scenario_mode"),
                "scenario_trigger": scenario.get("trigger"),
                "scenario_note": scenario.get("note"),
                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                **dict(extra_meta or {}),
            },
        )
        sig = self.tracker.signals[-1]
        sig["signal_id"] = signal_id
        sig["signal_size_pct"] = size_pct
        sig["public_signal_is_photo"] = True
        reinforcement_mode, owner_sig = self._prepare_same_side_signal_behavior(
            sig,
            self._find_active_primary_same_side_execution(side, SYMBOL),
        )
        trading_resp = self._send_public_signal_snapshot(sig) if not self.is_booting else None
        public_msg_id = trading_resp.get("result", {}).get("message_id") if trading_resp else None
        sig["msg_id"] = public_msg_id
        sig["trading_signal_msg_id"] = public_msg_id
        sig["symbol"] = SYMBOL
        self._save_state()
        if reinforcement_mode == "normal":
            self._send_active_trade_snapshot(sig)
            self._execute_exchange_trade(sig)
        else:
            self._refresh_public_signal_snapshot(owner_sig, event_label="REINFORCED")
            self._refresh_active_trade_snapshot(owner_sig, event_label="REINFORCED")
            if reinforcement_mode == "add":
                self._execute_exchange_trade(sig)
            else:
                print(
                    f"  [TRADE] Reinforcement only {sig.get('type')} {side}: "
                    f"existing primary {owner_sig.get('side')} {owner_sig.get('tf')} remains the live trade"
                )
        return True

    def _pick_smart_hedge_scenario(self, owner_sig, payload):
        owner_sig = owner_sig or {}
        payload = payload or {}
        opposite_side = "SHORT" if str(owner_sig.get("side") or "").upper() == "LONG" else "LONG"
        preferred_kinds = {"breakdown", "rejection", "major_squeeze", "major_flush", "breakout", "pullback"}
        candidates = []
        for scenario in list(payload.get("scenarios") or []):
            side = str(scenario.get("side") or "").upper()
            probability = float(scenario.get("probability") or 0.0)
            if side != opposite_side:
                continue
            if probability < float(BITUNIX_AUTO_HEDGE_MIN_PROBABILITY):
                continue
            if not self._scenario_trigger_ready(scenario, payload):
                continue
            score = probability
            if scenario.get("trend_aligned"):
                score += 4.0
            if str(scenario.get("kind") or "").strip().lower() in preferred_kinds:
                score += 2.0
            candidates.append((score, scenario))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _maybe_execute_smart_hedge(self, now, current_time):
        if not self._smart_hedge_enabled():
            return
        primary_active = self._active_primary_execution_signals()
        if len(primary_active) != 1:
            return
        if self._active_hedge_execution_signals():
            return

        owner_sig = primary_active[0]
        owner_signal_id = str(self._signal_id_value(owner_sig) or "").strip()
        if not owner_signal_id:
            return
        if self._find_active_hedge_for_parent(owner_signal_id):
            return

        owner_status = str(owner_sig.get("status") or "OPEN").upper()
        if owner_status not in {"OPEN", "TP1"}:
            return

        cutoff = current_time - max(float(BITUNIX_AUTO_HEDGE_COOLDOWN_SEC), 300.0)
        hedge_key = f"HEDGE:{owner_signal_id}"
        if float((self.scenario_trade_cooldowns or {}).get(hedge_key) or 0) >= cutoff:
            return

        try:
            payload = build_btc_scenarios_payload(symbol=SYMBOL, mode=BITUNIX_SCENARIO_TRADING_MODE or "short_term")
        except Exception as e:
            print(f"  [HEDGE] Scenario payload failed: {e}")
            return

        current_price = float(payload.get("current_price") or 0.0)
        if current_price <= 0:
            return
        progress_r = self._signal_stop_progress_r(owner_sig, current_price)
        if progress_r < float(BITUNIX_AUTO_HEDGE_TRIGGER_R):
            return
        if progress_r > float(BITUNIX_AUTO_HEDGE_MAX_PROGRESS_R):
            return

        hedge_scenario = self._pick_smart_hedge_scenario(owner_sig, payload)
        if not hedge_scenario:
            return

        sent = self._send_scenario_execution_signal(
            hedge_scenario,
            payload,
            now=now,
            size_pct_override=self._smart_hedge_size_pct(hedge_scenario),
            strategy_name="SMART_HEDGE",
            trigger_label="Smart Hedge",
            signal_type="SCALP",
            extra_meta={
                "hedge_parent_signal_id": owner_signal_id,
                "hedge_parent_side": owner_sig.get("side"),
                "hedge_parent_tf": owner_sig.get("tf"),
                "hedge_trigger_progress_r": round(progress_r, 3),
            },
        )
        if sent:
            self.scenario_trade_cooldowns[hedge_key] = current_time
            self._record_signal_sent("SMART_HEDGE", now=now)
            self._save_state()
            print(
                f"  [HEDGE] Sent smart hedge against {owner_sig.get('side')} "
                f"{owner_sig.get('tf')} progress_r={progress_r:.2f}"
            )

    def _maybe_execute_scenario_trade(self, now, current_time):
        if not self._scenario_trading_enabled() or self.is_booting:
            return
        if (current_time - float(self.last_scenario_trade_scan_at or 0)) < float(BITUNIX_SCENARIO_SCAN_INTERVAL_SEC):
            return
        self.last_scenario_trade_scan_at = current_time

        retention_sec = max(
            float(BITUNIX_SCENARIO_TRIGGER_COOLDOWN_SEC),
            float(BITUNIX_AUTO_HEDGE_COOLDOWN_SEC),
            60.0,
        )
        cutoff = current_time - retention_sec
        self.scenario_trade_cooldowns = {
            k: float(v) for k, v in (self.scenario_trade_cooldowns or {}).items()
            if float(v or 0) >= cutoff
        }

        active_exec = self._active_execution_signals()
        if active_exec:
            self._maybe_execute_smart_hedge(now, current_time)

        try:
            payload = build_btc_scenarios_payload(symbol=SYMBOL, mode=BITUNIX_SCENARIO_TRADING_MODE or "short_term")
        except Exception as e:
            print(f"  [SCENARIO] Trading payload failed: {e}")
            return

        for scenario in list(payload.get("scenarios") or []):
            probability = float(scenario.get("probability") or 0.0)
            if probability < float(BITUNIX_SCENARIO_MIN_PROBABILITY):
                continue
            side = str(scenario.get("side") or "").upper()
            if not side:
                continue
            if self._has_active_opposite_signal(side, SYMBOL):
                continue
            if not self._scenario_trigger_ready(scenario, payload):
                continue

            key = f"{side}:{scenario.get('kind')}:{round(float(scenario.get('entry_mid') or 0), 2)}"
            if float((self.scenario_trade_cooldowns or {}).get(key) or 0) >= cutoff:
                continue

            sent = self._send_scenario_execution_signal(scenario, payload, now=now)
            if sent:
                self.scenario_trade_cooldowns[key] = current_time
                self._record_signal_sent(f"SCENARIO_{scenario.get('kind')}", now=now)
                self._save_state()
                print(
                    f"  [SCENARIO] {'Sent' if not self.is_booting else 'Skipped'} "
                    f"{side} {scenario.get('kind')} @ {float(payload.get('current_price') or 0):,.2f}"
                )
                break

    def _send_trade_journal(self, sig, evt_type, close_price=None):
        # Journal posts are intentionally disabled now.
        return

    def _send_success_trade_post(self, sig, evt_type, close_price=None):
        if not sig or self.is_booting or sig.get("success_trade_posted"):
            return
        target_chat = self._signal_chat_id()
        target_thread = self._success_trades_thread_id()
        if not target_chat or target_thread <= 0:
            return

        outcome_key, r_mult = self.tracker._metric_outcome(sig)
        evt_key = str(evt_type or "").upper()
        has_any_tp = bool(sig.get("tp1_hit") or sig.get("tp2_hit") or sig.get("tp3_hit"))
        success_like_close = outcome_key == "wins" or (evt_key in {"ENTRY_CLOSE", "PROFIT_SL", "TP3"} and has_any_tp)
        if not success_like_close:
            return

        outcome_label = {
            "TP3": "Full Target Win",
            "PROFIT_SL": "Protected Win",
            "ENTRY_CLOSE": "Managed Win",
        }.get(evt_key, "Win")
        chart_path = self._generate_signal_snapshot_chart(
            sig,
            output_name=f"success_trade_{str(sig.get('signal_id') or 'sig')}.png",
            close_price=float(close_price or sig.get("tp3") or sig.get("tp2") or sig.get("tp1") or sig.get("entry") or 0),
            event_label=evt_type,
            title="SUCCESSFUL TRADE",
        )
        try:
            note = "Closed green and managed according to plan."
            if evt_key == "ENTRY_CLOSE":
                note = "Locked a positive result after targets were touched and momentum cooled."
            elif evt_key == "PROFIT_SL":
                note = "Protected profit on the stop after the move already paid."
            elif evt_key == "TP3":
                note = "Full target sequence completed cleanly."

            caption = self._success_trade_caption(sig, outcome_label, r_mult, note, close_price=close_price)
            if chart_path:
                tg.send_photo(
                    chart_path,
                    caption=caption,
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                )
            else:
                tg.send(
                    caption,
                    parse_mode="HTML",
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                )
            sig["success_trade_posted"] = True
            self._save_state()
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _signal_logged_local_date(self, sig):
        sig = sig or {}
        raw_logged = sig.get("logged_at") or sig.get("closed_at") or sig.get("timestamp")
        dt = None
        if raw_logged:
            raw_text = str(raw_logged).strip()
            for candidate in (raw_text, raw_text.replace("Z", "+00:00")):
                try:
                    dt = datetime.fromisoformat(candidate)
                    break
                except Exception:
                    continue
            if dt is None:
                for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                    try:
                        dt = datetime.strptime(raw_text, fmt)
                        break
                    except Exception:
                        continue
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(self.local_tz).date()

    def _send_today_wins_batch_card(self, sig, batch_date_key):
        if not sig:
            return False
        target_chat = self._signal_chat_id()
        target_thread = self._success_trades_thread_id()
        if not target_chat or target_thread <= 0:
            return False

        status = str(sig.get("status") or "OPEN").upper()
        if status == "TP3":
            outcome_label = "Full Target Win"
            note = "Finished the full target sequence during the day."
        elif status == "PROFIT_SL":
            outcome_label = "Protected Win"
            note = "Locked profit on the stop after targets were reached."
        elif status == "ENTRY_CLOSE":
            outcome_label = "Managed Win"
            note = "Touched targets and later closed flat on the runner."
        elif status == "SL":
            outcome_label = "Partial Win"
            note = "Paid at least TP1 before the stop closed the rest."
        elif bool(sig.get("tp2_hit")):
            outcome_label = "TP2 Reached"
            note = "Reached TP2 during the day and stayed on the winners list."
        else:
            outcome_label = "TP1 Reached"
            note = "Reached TP1 during the day and qualifies for Today’s Wins."

        outcome_key, r_mult = self.tracker._metric_outcome(sig)
        if outcome_key == "open":
            r_mult = 0.0
        symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL).upper()
        live_price = self._current_market_price(sig.get("tf"), symbol=symbol)
        chart_path = self._generate_signal_snapshot_chart(
            sig,
            output_name=f"today_wins_{str(sig.get('signal_id') or 'sig')}.png",
            close_price=float(live_price or sig.get("tp2") or sig.get("tp1") or sig.get("entry") or 0),
            event_label="TODAY WIN",
            title="TODAY'S WIN",
        )
        try:
            caption = self._success_trade_caption(sig, outcome_label, r_mult, note, close_price=live_price)
            resp = None
            if chart_path:
                resp = tg.send_photo(
                    chart_path,
                    caption=caption,
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                )
            else:
                resp = tg.send(
                    caption,
                    parse_mode="HTML",
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                )
            if resp:
                sig["today_wins_batch_date"] = batch_date_key
                self._save_state()
                return True
            return False
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _send_today_wins_batch(self, now_utc):
        now_utc = now_utc.astimezone(timezone.utc) if now_utc.tzinfo else now_utc.replace(tzinfo=timezone.utc)
        target_local_date = (now_utc.astimezone(self.local_tz) - timedelta(days=1)).date()
        batch_date_key = target_local_date.isoformat()
        winners = []
        for sig in list(self.tracker.signals or []):
            if not bool(sig.get("tp1_hit")):
                continue
            if str(sig.get("today_wins_batch_date") or "").strip() == batch_date_key:
                continue
            sig_local_date = self._signal_logged_local_date(sig)
            if sig_local_date != target_local_date:
                continue
            winners.append(sig)
        winners.sort(key=lambda s: str(s.get("logged_at") or s.get("timestamp") or ""))
        sent = 0
        for sig in winners:
            if self._send_today_wins_batch_card(sig, batch_date_key):
                sent += 1
        self.last_today_wins_batch_date = batch_date_key
        self._save_state()
        return sent

    def _member_education_posts(self):
        return [
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Reclaim entry</blockquote>\n"
                "A reclaim is not just a wick through a level. Wait for price to lose the level, get back above it, and show acceptance before treating it as a long."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: With trend vs counter-trend</blockquote>\n"
                "With-trend setups usually need less proof and can travel farther. Counter-trend setups need cleaner rejection or reclaim confirmation."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Why no-trade is a real decision</blockquote>\n"
                "If price is trapped between levels with weak momentum, the best position is often no position. Waiting is part of risk control."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Funding context</blockquote>\n"
                "Negative funding means shorts are paying. That can support long squeeze ideas, but only if price action confirms it."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Liquidity sweep logic</blockquote>\n"
                "A sweep alone is not the entry. The entry is the reaction after the sweep: rejection, reclaim, or continuation failure."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Session behavior</blockquote>\n"
                "London and New York usually decide where liquidity gets taken. Asia often sets the range that later sessions attack."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Breakout confirmation</blockquote>\n"
                "A breakout is stronger when price closes above the level and then holds it. A quick wick above resistance is not enough by itself."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Retest entries</blockquote>\n"
                "After a breakout, the retest is often the cleaner entry. Chasing the first candle usually gives worse risk and more emotional mistakes."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Stop placement</blockquote>\n"
                "A stop should sit beyond invalidation, not at a random dollar amount. If the idea is still valid after your stop, the stop is in the wrong place."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Target selection</blockquote>\n"
                "Targets work better when they point to real liquidity or structure. Arbitrary take-profit numbers usually ignore where price actually wants to travel."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Range trading</blockquote>\n"
                "Inside a range, the best trades often come from the edges. Entries in the middle of the range usually offer weak reward and messy price action."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Trend continuation</blockquote>\n"
                "Continuation setups work best when the pullback is controlled and volume dries up into support. A violent pullback often means the move is losing quality."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Counter-trend rules</blockquote>\n"
                "Counter-trend trades need a stronger reason than with-trend trades. That usually means a sweep, rejection, and fast reclaim, not just a guess at the top or bottom."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Timeframe alignment</blockquote>\n"
                "When 1H and 4H point the same way, signals usually behave cleaner. Mixed timeframes often create noise and shorter-lived moves."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: News caution</blockquote>\n"
                "Right before major news, clean structure can break for no technical reason. Lower size or stay flat when volatility is event-driven."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Funding extremes</blockquote>\n"
                "Extreme funding can warn that one side is overcrowded. It is a clue about risk, not an automatic signal by itself."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Open interest context</blockquote>\n"
                "Price rising with open interest rising often means new positions are joining the move. Price rising while open interest falls can mean shorts are only closing."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Wick rejection</blockquote>\n"
                "A strong wick matters more when it hits a real level and the next candle confirms it. A wick in the middle of nowhere usually has less meaning."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Patience after sweep</blockquote>\n"
                "After a liquidity sweep, let the market show its reaction first. The sweep itself is the setup context, not always the exact entry trigger."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Failed breakout</blockquote>\n"
                "A failed breakout can become a sharp move the other way. When price loses the breakout level fast, trapped traders often fuel the reversal."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Failed breakdown</blockquote>\n"
                "A failed breakdown is powerful when price quickly reclaims the lost support. That tells you sellers could not keep control below the level."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Market structure break</blockquote>\n"
                "A structure break means more when it removes a meaningful swing, not just a tiny intraday pivot. Bigger breaks usually lead to cleaner continuation."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Higher-low logic</blockquote>\n"
                "In an uptrend, a higher low that holds is often more useful than a random bullish candle. It shows buyers are defending earlier than before."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Lower-high logic</blockquote>\n"
                "In a downtrend, a lower high helps confirm seller control. If price cannot reclaim the prior reaction high, continuation lower becomes more likely."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Risk-reward filter</blockquote>\n"
                "Even a good idea can be a bad trade if the stop is too wide for the target. Location matters as much as direction."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: First touch vs second touch</blockquote>\n"
                "The first touch of a level often gives the cleanest reaction. Each extra test can weaken the level as liquidity gets consumed."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Confluence</blockquote>\n"
                "A level becomes stronger when multiple reasons meet there: structure, liquidity, funding pressure, session timing, and trend alignment."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Session highs and lows</blockquote>\n"
                "Session highs and lows matter because they attract stops and liquidity. Price often visits them before the real directional move begins."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Daily open</blockquote>\n"
                "The daily open can act like a balance point. Holding above it often supports longs, while repeated rejection below it can help shorts."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Weekly open</blockquote>\n"
                "The weekly open is more meaningful than many traders realize. When price reclaims or loses it cleanly, that can shape the whole week."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Monthly levels</blockquote>\n"
                "Monthly highs, lows, and opens matter because they hold bigger liquidity. Reactions there usually carry more weight than small intraday levels."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: ATR awareness</blockquote>\n"
                "If your target sits inside normal hourly volatility, the move may happen too easily to be meaningful. If your stop is smaller than normal noise, it may be too tight."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Choppy market warning</blockquote>\n"
                "When candles overlap heavily and both sides keep getting rejected, the market is often in chop. Good traders preserve energy there instead of forcing trades."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Momentum candle trap</blockquote>\n"
                "Large momentum candles look exciting, but they often offer the worst entry if you are late. Let the market pull back or prove continuation first."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Acceptance above a level</blockquote>\n"
                "Acceptance means price spends time above the level and keeps holding it. One fast poke above resistance is not the same thing."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Acceptance below a level</blockquote>\n"
                "Acceptance below support tells you the market is comfortable lower. That is very different from a quick sweep that snaps right back up."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Best side of the market</blockquote>\n"
                "Sometimes the best edge is not in predicting both directions. It is in choosing the side that has trend, structure, and positioning behind it."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Trade location</blockquote>\n"
                "A mediocre setup from a great location can outperform a great-looking setup from a bad location. Price level still matters more than excitement."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Internal range traps</blockquote>\n"
                "Small local highs and lows inside a bigger range often trap impatient traders. Focus on the meaningful external liquidity first."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Strong support</blockquote>\n"
                "Support is stronger when it comes from a bigger timeframe and lines up with a liquidity zone. The best long reactions usually happen from those deeper clusters."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Strong resistance</blockquote>\n"
                "Resistance is stronger when it matches a major prior high and a crowded short-liquidation area above price. That is where rejection plans become more attractive."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Liquidity above price</blockquote>\n"
                "Liquidity above price can act like a magnet when the market is bullish. That does not mean long immediately, but it does help explain likely path."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Liquidity below price</blockquote>\n"
                "Liquidity below price becomes important when longs are crowded or support is weak. Flushes often happen before the market shows its real intention."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Compression</blockquote>\n"
                "When price compresses tightly under resistance or above support, expansion usually follows. The key is waiting for direction instead of guessing too early."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Impulse and pullback</blockquote>\n"
                "Healthy trends usually alternate between impulse and pullback. Entering after the impulse without waiting for the pullback often hurts the trade location."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Entry quality</blockquote>\n"
                "A better entry is not only about price. It also gives better invalidation, cleaner logic, and less emotional pressure after entering."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Invalidated setup</blockquote>\n"
                "When a setup is invalidated, the smart move is to accept it quickly. Staying loyal to a dead idea usually creates a bigger loss than planned."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Breakeven discipline</blockquote>\n"
                "Moving to breakeven too early can remove a good trade for no reason. Moving too late can give back a solid position. Let structure guide the adjustment."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Scaling out</blockquote>\n"
                "Taking partials can reduce pressure and protect profit, but do not cut the whole trade too early. Leave room for the part of the move that pays best."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Strong close</blockquote>\n"
                "A candle closing near its high in a bullish move, or near its low in a bearish move, usually shows commitment. Weak closes often signal hesitation."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Fake strength</blockquote>\n"
                "Some moves look strong only because they are fueled by short covering or thin liquidity. Structure and follow-through tell you if the move is real."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Laddering levels</blockquote>\n"
                "When several nearby levels stack together, treat the area as a zone instead of pretending every line is separate. Markets often react to clusters, not exact numbers."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Good miss vs bad fill</blockquote>\n"
                "Missing a trade is frustrating, but forcing a late entry is often worse. A disciplined miss is still better than a low-quality fill."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Trade fatigue</blockquote>\n"
                "After several trades in a row, decision quality often drops. Overtrading usually starts when discipline gets replaced by the need to stay active."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Clean chart thinking</blockquote>\n"
                "More indicators do not always mean more clarity. The best read often comes from price, key levels, liquidity, and a few strong context tools."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Confirmation candle</blockquote>\n"
                "A confirmation candle should support the idea with close, location, and reaction. A random green or red candle is not enough confirmation by itself."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Sweep and hold</blockquote>\n"
                "The strongest sweeps are often the ones that reverse quickly and then hold the reclaimed area. That hold tells you the trap actually worked."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Map before entry</blockquote>\n"
                "Know the invalidation and target path before you enter. If the plan is unclear after entry, the trade was probably not ready."
            ),
            (
                "🎓 <b>Member Education</b>\n"
                "<blockquote>Topic: Waiting for the better trade</blockquote>\n"
                "Good trading is often the skill of rejecting average setups. The biggest edge usually comes from selectivity, not constant action."
            ),
        ]

    def _send_member_education_post(self):
        posts = list(PROFESSIONAL_MEMBER_EDUCATION_POSTS or []) or self._member_education_posts()
        if not posts or self.is_booting:
            return
        idx = int(self.education_post_index or 0) % len(posts)
        tg.send(
            posts[idx],
            parse_mode="HTML",
            chat_id=self._signal_chat_id(),
            message_thread_id=self._general_thread_id(),
        )
        self.education_post_index = idx + 1
        self._save_state()

    def _send_liquidation_map_post(self, *, chat_id=None, message_thread_id=None):
        if self.is_booting:
            return
        snapshot = build_liquidation_map_snapshot(symbol=SYMBOL)
        horizon_rows = list(snapshot.get("horizons", []) or [])
        lines = []
        for row in snapshot.get("horizons", []):
            horizon = str(row.get("horizon") or "")
            upside = row.get("upside")
            downside = row.get("downside")
            upside_zone = row.get("upside_zone") or {}
            downside_zone = row.get("downside_zone") or {}
            up_txt = f"{float(upside):,.0f} {self._compact_usd(upside_zone.get('size_usd'))}" if upside else "n/a"
            down_txt = f"{float(downside):,.0f} {self._compact_usd(downside_zone.get('size_usd'))}" if downside else "n/a"
            lines.append(f"{horizon}: ↑ {up_txt} | ↓ {down_txt}")
        compact_lines = lines[:4]
        if len(lines) > 4:
            compact_lines.append(f"1M: {lines[-1].split(': ', 1)[1]}")
        chart_path = generate_liquidation_map_chart(
            snapshot.get("chart_df"),
            current_price=float(snapshot.get("current_price", 0) or 0),
            horizon_rows=horizon_rows,
            heatmap_rows=snapshot.get("heatmap_rows"),
            heatmap_history=snapshot.get("heatmap_history"),
            symbol=SYMBOL,
            timeframe=str(snapshot.get("chart_timeframe") or "15m"),
            output_path="liquidation_map_post.png",
        )
        message = (
            f"🟡 <b>BTC Liquidation Map</b>\n\n"
            f"<blockquote>"
            f"Price: {float(snapshot.get('current_price', 0) or 0):,.2f}\n"
            f"Funding: {float(snapshot.get('funding_rate', 0) or 0):+.6f}%"
            f"</blockquote>\n"
            f"<blockquote>{chr(10).join(compact_lines)}</blockquote>"
        )
        try:
            if chart_path:
                tg.send_photo(
                    chart_path,
                    caption=message,
                    chat_id=chat_id or self._signal_chat_id(),
                    message_thread_id=message_thread_id if message_thread_id is not None else self._scenarios_thread_id(),
                )
            else:
                tg.send(
                    message,
                    parse_mode="HTML",
                    chat_id=chat_id or self._signal_chat_id(),
                    message_thread_id=message_thread_id if message_thread_id is not None else self._scenarios_thread_id(),
                )
        finally:
            try:
                import os
                if chart_path and os.path.exists(chart_path):
                    os.remove(chart_path)
            except Exception:
                pass

    def _build_demo_signal(self, side="LONG"):
        side_text = str(side or "LONG").upper()
        try:
            price = float(fetch_last_price(SYMBOL) or 0)
        except Exception:
            price = 0.0
        if price <= 0:
            try:
                df = fetch_klines(interval="15m", limit=4)
                if not df.empty:
                    price = float(df["Close"].iloc[-1])
            except Exception:
                price = 0.0
        if price <= 0:
            price = 80000.0

        if side_text == "SHORT":
            sl = price + 420.0
            tp1 = price - 550.0
            tp2 = price - 900.0
            tp3 = price - 1400.0
        else:
            sl = price - 420.0
            tp1 = price + 550.0
            tp2 = price + 900.0
            tp3 = price + 1400.0

        return {
            "signal_id": f"TEST-{int(time.time())}",
            "type": "SCALP",
            "side": side_text,
            "entry": price,
            "sl": sl,
            "initial_sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "tf": "15m",
            "status": "OPEN",
            "signal_size_pct": 0.5,
            "meta": {
                "strategy": "TEST_FEATURE",
                "size": 0.5,
                "trigger": "manual feature test",
            },
            "journal_posted": False,
        }

    def _hedge_mode_enabled(self):
        return str(BITUNIX_POSITION_MODE or "").strip().upper() == "HEDGE"

    def _should_update_public_signal(self, sig):
        if not self._execution_updates_private_only():
            return True
        return str(sig.get("chat_id") or "") == str(self._execution_chat_id())

    def _has_real_exchange_execution(self, sig):
        execution = (sig or {}).get("execution") or {}
        if not execution:
            return False
        if execution.get("position_id"):
            return True
        if execution.get("active"):
            return True
        if execution.get("sl_order"):
            return True
        if execution.get("tp_orders"):
            return True
        return False

    def _send_private_execution_notice(self, title, lines=None, icon="🔐"):
        exec_chat = self._execution_chat_id()
        if not exec_chat:
            return
        if str(title or "").strip() == "Bitunix Startup Check":
            tg.send_execution_notice(title, lines=lines, chat_id=exec_chat, icon=icon)
            return
        body_lines = [str(line) for line in (lines or []) if line is not None and str(line).strip() and str(line).strip() != "None"]
        title_text = str(title or "").strip()
        if title_text and title_text not in {"Exec Control", "Confirm Exec Action", "Exec Action Result"}:
            body_lines.insert(0, title_text)
        if not body_lines:
            body_lines = [title_text] if title_text else []
        if body_lines:
            tg.send("\n".join(body_lines), chat_id=exec_chat, parse_mode="HTML")

    def _send_private_execution_signal_card(self, sig):
        exec_chat = self._execution_chat_id()
        if not exec_chat or not sig:
            return None
        meta = sig.get("meta", {}) or {}
        signal_type = str(sig.get("type") or "SCALP").upper()
        signal_html_type = signal_type if signal_type in {"SCALP", "STRONG", "EXTREME"} else "SCALP"
        tf_val = sig.get("tf")
        indicators = meta.get("indicators")
        if tf_val == "Confluence" and indicators:
            tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators)))
            tf_val = ", ".join(tfs)
        html_text = tg.get_signal_html(
            signal_type=signal_html_type,
            side=sig.get("side"),
            timeframe=tf_val or "N/A",
            entry=sig.get("entry"),
            sl=sig.get("sl"),
            initial_sl=sig.get("initial_sl", sig.get("sl")),
            tp1=sig.get("tp1"),
            tp2=sig.get("tp2"),
            tp3=sig.get("tp3"),
            status="OPEN",
            score=meta.get("score"),
            trend=meta.get("trend"),
            indicators=indicators,
            reasons=meta.get("reasons"),
            size=sig.get("signal_size_pct", meta.get("size")),
            tp_liq_prob=meta.get("tp_liq_prob"),
            tp_liq_usd=meta.get("tp_liq_usd"),
            tp_liq_target=meta.get("tp_liq_target"),
            trigger_label=meta.get("trigger"),
        )
        return tg.send(html_text, parse_mode="HTML", chat_id=exec_chat)

    def _send_private_execution_position_id_reply(self, sig, merged=False, owner_sig=None):
        exec_chat = self._execution_chat_id()
        execution = (sig or {}).get("execution") or {}
        msg_id = execution.get("exec_msg_id")
        position_id = execution.get("position_id")
        if not exec_chat or not msg_id or not position_id:
            return None
        if merged:
            owner_tf = (owner_sig or {}).get("tf") or "n/a"
            owner_side = str((owner_sig or {}).get("side") or "N/A").upper()
            text = (
                f"🔗 <b>MERGED INTO EXISTING POSITION</b>\n"
                f"{'🟢' if str(sig.get('side') or '').upper() == 'LONG' else '🔴'} <b>{str(sig.get('side') or '').upper()} [{sig.get('tf', 'N/A')}]</b>\n\n"
                f"Following the existing <b>{owner_side} [{owner_tf}]</b> Bitunix position.\n"
                f"Bitunix Position ID:\n<pre>{html.escape(str(position_id))}</pre>"
            )
        else:
            text = (
                f"✅ <b>LIVE ON BITUNIX</b>\n"
                f"Bitunix Position ID:\n<pre>{html.escape(str(position_id))}</pre>"
            )
        return tg.send(text, parse_mode="HTML", chat_id=exec_chat, reply_to_message_id=msg_id)

    def _send_private_execution_lifecycle_reply(self, sig, event_type):
        exec_chat = self._execution_chat_id()
        execution = (sig or {}).get("execution") or {}
        msg_id = execution.get("exec_msg_id")
        if not exec_chat or not msg_id:
            return None
        evt_type = str(event_type or "").upper()
        if evt_type == "TP1":
            return tg.send_tp1_hit_congrats(
                exec_chat,
                msg_id,
                sig.get("tf", "Unknown"),
                side=sig.get("side"),
                lock_price=sig.get("entry"),
                entry=sig.get("entry"),
                sl=sig.get("sl"),
                tp1=sig.get("tp1"),
                tp2=sig.get("tp2"),
                size=(sig.get("meta", {}) or {}).get("size"),
            )
        if evt_type == "TP2":
            return tg.send_tp2_hit_congrats(
                exec_chat,
                msg_id,
                sig.get("tf", "Unknown"),
                side=sig.get("side"),
                lock_price=(execution.get("sl_moved_to") or sig.get("sl")),
                entry=sig.get("entry"),
                sl=sig.get("sl"),
                tp1=sig.get("tp1"),
                tp2=sig.get("tp2"),
                size=(sig.get("meta", {}) or {}).get("size"),
                single_full=self._is_single_full_tp_execution(sig),
            )
        if evt_type == "TP3":
            return tg.send_tp3_hit_congrats(exec_chat, msg_id, sig.get("tf", "Unknown"))
        if evt_type == "ENTRY_CLOSE":
            return tg.send_breakeven_alert(exec_chat, msg_id, sig.get("tf", "Unknown"))
        if evt_type == "PROFIT_SL":
            return tg.send_profit_sl_alert(exec_chat, msg_id, sig.get("tf", "Unknown"))
        return None

    def _format_private_answer_for_telegram(self, text):
        answer = str(text or "").strip()
        if not answer:
            return ""
        if "<pre>" in answer or "<code>" in answer or "<b>" in answer or "<i>" in answer:
            return answer

        lines = answer.splitlines()
        rendered = []
        markdown_block = []

        def _clean_markdown_line(line):
            cleaned = str(line or "").rstrip()
            cleaned = re.sub(r"^\s*[*-]\s*", "", cleaned)
            cleaned = re.sub(r"\*\*(.*?)\*\*", r"\1", cleaned)
            cleaned = cleaned.replace("`", "")
            return cleaned.strip()

        def _flush_markdown_block():
            nonlocal markdown_block
            if not markdown_block:
                return
            block_text = "\n".join(_clean_markdown_line(line) for line in markdown_block if str(line).strip())
            if block_text.strip():
                rendered.append(f"<pre>{html.escape(block_text)}</pre>")
            markdown_block = []

        for raw_line in lines:
            line = str(raw_line or "").rstrip()
            stripped = line.strip()
            is_markdownish = bool(re.match(r"^\s*[*-]\s+", line)) or ("**" in line)
            if is_markdownish:
                markdown_block.append(line)
                continue
            _flush_markdown_block()
            if stripped:
                rendered.append(html.escape(line))
            else:
                rendered.append("")

        _flush_markdown_block()
        return "\n".join(rendered).strip()

    def _send_private_execution_answer(self, text):
        exec_chat = self._execution_chat_id()
        if not exec_chat:
            return
        answer = self._format_private_answer_for_telegram(text)
        if not answer:
            return
        tg.send(answer, chat_id=exec_chat, parse_mode="HTML")

    def _todo_extract_item(self, text):
        raw = str(text or "").strip()
        patterns = [
            r"^(?:remember|save|add)\s+(?:this\s+)?(?:to[- ]?do|task|note)?\s*:?\s*(.+)$",
            r"^(?:my\s+)?to[- ]?do\s*:?\s*(.+)$",
            r"^(?:task|note)\s*:?\s*(.+)$",
        ]
        for pattern in patterns:
            match = re.match(pattern, raw, flags=re.I)
            if match:
                item = str(match.group(1) or "").strip(" .")
                if item:
                    return item
        return ""

    def _todo_add(self, text):
        item = self._todo_extract_item(text)
        if not item:
            return False
        todo = {
            "text": item,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "done": False,
        }
        self.private_todo_items.append(todo)
        self._save_state()
        self._send_private_execution_answer(
            "I saved that to your to-do list.\n\n<pre>" + html.escape(item) + "</pre>"
        )
        return True

    def _todo_render(self, include_done=True):
        items = list(self.private_todo_items or [])
        if not include_done:
            items = [item for item in items if not bool(item.get("done"))]
        if not items:
            return "Your to-do list is empty right now."
        lines = []
        for idx, item in enumerate(items, start=1):
            mark = "done" if bool(item.get("done")) else "open"
            lines.append(f"{idx}. [{mark}] {str(item.get('text') or '').strip()}")
        return "Here is your to-do list.\n\n<pre>" + html.escape("\n".join(lines)) + "</pre>"

    def _todo_show(self, open_only=False):
        self._send_private_execution_answer(self._todo_render(include_done=not open_only))
        return True

    def _todo_clear(self):
        if not self.private_todo_items:
            self._send_private_execution_answer("Your to-do list is already empty.")
            return True
        self.private_todo_items = []
        self._save_state()
        self._send_private_execution_answer("I cleared your to-do list.")
        return True

    def _todo_update_by_index(self, text, action):
        match = re.search(r"\b(?:task|todo|to[- ]?do|item)?\s*(\d+)\b", str(text or ""), flags=re.I)
        if not match:
            return False
        idx = int(match.group(1))
        if idx < 1 or idx > len(self.private_todo_items):
            self._send_private_execution_answer(f"I could not find task {idx} in your to-do list.")
            return True
        item = self.private_todo_items[idx - 1]
        if action == "remove":
            removed = str(item.get("text") or "").strip()
            self.private_todo_items.pop(idx - 1)
            self._save_state()
            self._send_private_execution_answer(
                "I removed that task.\n\n<pre>" + html.escape(removed) + "</pre>"
            )
            return True
        if action == "done":
            item["done"] = True
            self._save_state()
            self._send_private_execution_answer(
                "I marked that task as done.\n\n<pre>" + html.escape(str(item.get("text") or "").strip()) + "</pre>"
            )
            return True
        return False

    def _handle_private_todo_message(self, text):
        normalized = self._normalize_intent_text(text)
        if not normalized:
            return False
        if self._intent_has_any_normalized(normalized, [
            "show my to do", "show my todo", "show to do", "show todo",
            "what is on my to do", "what is on my todo", "my to do list", "my todo list",
            "list my tasks", "show my tasks", "what tasks do i have", "what todos do i have"
        ]):
            return self._todo_show()
        if self._intent_has_any_normalized(normalized, [
            "show open tasks", "show open to do", "show open todo", "open tasks", "open to do",
            "unfinished tasks", "undone tasks", "pending tasks"
        ]):
            return self._todo_show(open_only=True)
        if self._intent_has_any_normalized(normalized, [
            "clear my to do", "clear my todo", "delete all tasks", "clear tasks", "remove all tasks",
            "delete my todo", "wipe my tasks"
        ]):
            return self._todo_clear()
        if self._intent_has_any_normalized(normalized, ["mark task", "complete task", "done task", "finish task"]):
            return self._todo_update_by_index(text, "done")
        if self._intent_has_any_normalized(normalized, ["remove task", "delete task"]):
            return self._todo_update_by_index(text, "remove")
        if self._intent_has_any_normalized(normalized, [
            "remember this", "remember that", "add to do", "add todo", "save this task", "save this note",
            "add task", "add note", "todo:", "to do:", "task:", "note:"
        ]):
            return self._todo_add(text)
        return False

    def _is_private_exec_chat(self, chat_id):
        exec_chat = str(self._execution_chat_id() or "").strip()
        return bool(exec_chat and str(chat_id or "").strip() == exec_chat)

    def _ensure_signal_debug_day(self, now=None):
        now = now or datetime.now(timezone.utc)
        today = now.astimezone(timezone.utc).strftime("%Y-%m-%d")
        stats = self.signal_debug_stats or {}
        if str(stats.get("date") or "") != today:
            stats = {
                "date": today,
                "blocked": {},
                "suppressed": {},
                "sent": {},
            }
            self.signal_debug_stats = stats
        return stats

    def _record_signal_debug(self, bucket, key, now=None):
        if not key:
            return
        stats = self._ensure_signal_debug_day(now)
        section = stats.setdefault(bucket, {})
        section[key] = int(section.get(key, 0) or 0) + 1

    def _record_signal_block(self, key, now=None):
        self._record_signal_debug("blocked", key, now=now)

    def _record_signal_suppressed(self, key, now=None):
        self._record_signal_debug("suppressed", key, now=now)

    def _record_signal_sent(self, key, now=None):
        self._record_signal_debug("sent", key, now=now)

    def _normalize_intent_text(self, text):
        raw = str(text or "").strip().lower()
        if not raw:
            return ""
        replacements = {
            "what's": "what is",
            "whats": "what is",
            "can't": "can not",
            "dont": "do not",
            "doesnt": "does not",
            "wont": "will not",
            "stop lose": "stop loss",
            "stoplose": "stop loss",
            "set stop lose": "set stop loss",
            "move stop lose": "move stop loss",
            "stp loss": "stop loss",
            "stp": "stop",
            "singals": "signals",
            "singal": "signal",
            "baalnce": "balance",
            "balalnce": "balance",
            "wallet": "wallet",
            "posiiton": "position",
            "poisition": "position",
            "poistion": "position",
            "psoition": "position",
            "posiition": "position",
            "contructions": "constructions",
            "markettwits": "markettwits",
        }
        for src, dst in replacements.items():
            raw = raw.replace(src, dst)
        raw = re.sub(r"[^a-z0-9]+", " ", raw)
        tokens = []
        for token in raw.split():
            if not token:
                continue
            tokens.append(token)
            if token.endswith("ies") and len(token) > 4:
                tokens.append(token[:-3] + "y")
            elif token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
                tokens.append(token[:-1])
        deduped = []
        seen = set()
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            deduped.append(token)
        return " ".join(deduped)

    def _intent_has_phrase_in_normalized(self, normalized_text, phrase):
        target = self._normalize_intent_text(phrase)
        if not normalized_text or not target:
            return False
        return f" {target} " in f" {normalized_text} "

    def _intent_has_any_normalized(self, normalized_text, phrases):
        return any(self._intent_has_phrase_in_normalized(normalized_text, phrase) for phrase in (phrases or []))

    def _intent_has_any(self, text, phrases):
        return self._intent_has_any_normalized(self._normalize_intent_text(text), phrases)

    def _intent_has_all(self, text, groups):
        normalized = self._normalize_intent_text(text)
        if not normalized:
            return False
        for group in groups:
            variants = list(group) if isinstance(group, (list, tuple, set)) else [group]
            if not any(self._intent_has_phrase_in_normalized(normalized, phrase) for phrase in variants):
                return False
        return True

    def _looks_like_block_reasons_text(self, text):
        normalized = self._normalize_intent_text(text)
        if not normalized:
            return False
        if self._intent_has_any_normalized(normalized, [
            "blocked reasons", "block reasons", "what is blocking", "what is blockin",
            "what blocks signals", "what blocked signals", "why nothing today"
        ]):
            return True
        if any(self._intent_has_phrase_in_normalized(normalized, phrase) for phrase in ["why", "how come"]):
            no_output = self._intent_has_any_normalized(normalized, ["no", "not getting", "nothing", "zero"])
            signal_words = self._intent_has_any_normalized(normalized, ["signal", "scalp", "entry", "trade", "alert"])
            timing_words = self._intent_has_any_normalized(normalized, ["today", "right now", "so far"])
            if signal_words and (no_output or timing_words):
                return True
        return False

    def _sanitize_exec_text(self, text):
        clean = str(text or "").strip()
        if not clean:
            return clean
        replacements = {
            "stop lose": "stop loss",
            "stoplose": "stop loss",
            "set stop lose": "set stop loss",
            "move stop lose": "move stop loss",
            "posiition": "position",
            "posiiton": "position",
            "poisition": "position",
            "poistion": "position",
            "psoition": "position",
        }
        lowered = clean.lower()
        for src, dst in replacements.items():
            lowered = lowered.replace(src, dst)
        return lowered

    def _looks_like_bitcoin_plans_request(self, text):
        normalized = self._normalize_intent_text(text)
        if not normalized:
            return False
        phrases = [
            "bitcoin plans", "btc plans", "give me bitcoin plans", "give me btc plans",
            "bitcoin scenarios", "btc scenarios", "bitcoin plan", "btc plan",
            "plans for bitcoin", "plans for btc", "give me bitcoin scenario", "give me btc scenario",
        ]
        return self._intent_has_any_normalized(normalized, phrases)

    def _extract_tp_split_values(self, text):
        raw_text = str(text or "").strip().lower()
        if not raw_text:
            return None
        compact = raw_text.replace("%", "")
        slash_match = re.search(r'(\d{1,3}(?:[.,]\d+)?)\s*[/\\-]\s*(\d{1,3}(?:[.,]\d+)?)\s*[/\\-]\s*(\d{1,3}(?:[.,]\d+)?)', compact)
        values = None
        if slash_match:
            values = [float(slash_match.group(i).replace(",", ".")) for i in range(1, 4)]
        else:
            nums = re.findall(r'(?<!\w)(\d{1,3}(?:[.,]\d+)?)(?:\s*%?)', compact)
            picked = []
            for token in nums:
                try:
                    value = float(token.replace(",", "."))
                except Exception:
                    continue
                if 0 < value <= 100:
                    picked.append(value)
                if len(picked) == 3:
                    break
            if len(picked) == 3:
                values = picked
        if not values or len(values) != 3:
            return None
        total = sum(values)
        if total <= 0:
            return None
        if total <= 1.01:
            return [float(v) for v in values]
        return [float(v / total) for v in values]

    def _infer_basic_exec_action(self, text):
        clean_text = self._sanitize_exec_text(text)
        normalized = self._normalize_intent_text(clean_text)
        if not normalized:
            return None
        raw_lower = str(clean_text or "").lower()
        if self._intent_has_any_normalized(normalized, [
            "tp split", "tp splits", "take profit split", "take profit splits",
            "partials", "tp percentages", "take profit percentages",
        ]):
            splits = self._extract_tp_split_values(clean_text)
            action = {"action": "set_tp_split"}
            if splits:
                action["splits"] = splits
            return action
        if self._intent_has_any_normalized(normalized, ["rebuild protection", "refresh protection", "rebuild tp orders", "refresh tp orders"]):
            return {"action": "rebuild_protection"}
        if self._intent_has_any_normalized(normalized, ["set one take profit", "single take profit", "single tp", "one tp"]):
            return {"action": "set_single_tp"}
        if self._intent_has_any_normalized(normalized, ["move stop to tp1", "set stop to tp1", "sl to tp1"]):
            return {"action": "move_sl_tp", "tp_index": 1}
        if self._intent_has_any_normalized(normalized, ["move stop to tp2", "set stop to tp2", "sl to tp2"]):
            return {"action": "move_sl_tp", "tp_index": 2}
        if self._intent_has_any_normalized(normalized, ["move stop to entry", "set stop to entry", "stop to entry", "move sl to entry"]):
            return {"action": "move_sl_entry"}
        if self._intent_has_any_normalized(normalized, ["move stop to breakeven", "set stop to breakeven", "move sl to breakeven", "break even stop"]):
            return {"action": "move_sl_entry"}
        if self._intent_has_any_normalized(normalized, ["set tp1", "move tp1", "change tp1"]):
            return {"action": "set_tp", "tp_index": 1}
        if self._intent_has_any_normalized(normalized, ["set tp2", "move tp2", "change tp2"]):
            return {"action": "set_tp", "tp_index": 2}
        if self._intent_has_any_normalized(normalized, ["set tp3", "move tp3", "change tp3"]):
            return {"action": "set_tp", "tp_index": 3}
        if self._intent_has_any_normalized(normalized, ["cancel tp1", "remove tp1", "delete tp1"]):
            return {"action": "cancel_tp", "tp_index": 1}
        if self._intent_has_any_normalized(normalized, ["cancel tp2", "remove tp2", "delete tp2"]):
            return {"action": "cancel_tp", "tp_index": 2}
        if self._intent_has_any_normalized(normalized, ["cancel tp3", "remove tp3", "delete tp3"]):
            return {"action": "cancel_tp", "tp_index": 3}
        if self._intent_has_any_normalized(normalized, ["cancel take profit", "cancel take profits", "cancel tp", "remove tp", "remove take profit"]):
            return {"action": "cancel_tp"}
        if self._intent_has_any_normalized(normalized, ["set stop loss", "move stop loss", "change stop loss", "set sl", "move sl", "change sl"]):
            return {"action": "move_sl"}
        if any(word in raw_lower for word in ["close", "take off", "reduce", "trim"]) and any(mark in raw_lower for mark in ["%", "percent", "half", "quarter", "third"]):
            action = {"action": "close_partial"}
            if "half" in raw_lower:
                action["fraction"] = 0.5
            elif "quarter" in raw_lower:
                action["fraction"] = 0.25
            elif "third" in raw_lower:
                action["fraction"] = 1.0 / 3.0
            else:
                match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', raw_lower)
                if not match:
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:percent)', raw_lower)
                if match:
                    try:
                        action["fraction"] = max(0.0, min(1.0, float(match.group(1).replace(",", ".")) / 100.0))
                    except Exception:
                        pass
            return action
        if self._intent_has_any_normalized(normalized, ["close position", "close my position", "close this position", "close trade", "close this trade"]):
            return {"action": "close_full"}
        if self._intent_has_any_normalized(normalized, ["open positions", "show my positions", "show open positions"]):
            return {"action": "status", "reason": text}
        return None

    def _send_signal_debug_summary(self):
        now = datetime.now(timezone.utc)
        stats = self._ensure_signal_debug_day(now)
        sent = stats.get("sent") or {}
        blocked = stats.get("blocked") or {}
        suppressed = stats.get("suppressed") or {}
        total_sent = sum(int(v or 0) for v in sent.values())

        lines = [f"Today in UTC I have sent {total_sent} confirmed signal{'s' if total_sent != 1 else ''} so far."]
        if sent:
            sent_lines = [f"{k}: {int(v or 0)}" for k, v in sorted(sent.items(), key=lambda item: (-int(item[1] or 0), item[0]))]
            lines.append("")
            lines.append("Confirmed sends:")
            lines.append("<pre>" + html.escape("\n".join(sent_lines)) + "</pre>")
        if blocked:
            top_blocked = sorted(blocked.items(), key=lambda item: (-int(item[1] or 0), item[0]))[:8]
            block_lines = [f"{k}: {int(v or 0)}" for k, v in top_blocked]
            lines.append("")
            lines.append("Main blockers:")
            lines.append("<pre>" + html.escape("\n".join(block_lines)) + "</pre>")
        if suppressed:
            top_suppressed = sorted(suppressed.items(), key=lambda item: (-int(item[1] or 0), item[0]))[:6]
            suppress_lines = [f"{k}: {int(v or 0)}" for k, v in top_suppressed]
            lines.append("")
            lines.append("Other suppressions:")
            lines.append("<pre>" + html.escape("\n".join(suppress_lines)) + "</pre>")
        if not blocked and not suppressed and total_sent == 0:
            lines.append("")
            lines.append("I do not have any blocker history recorded for today yet. That usually means the bot has not been running long enough today, or there really have not been any signal attempts yet.")
        self._send_private_execution_answer("\n".join(lines))


    def _refresh_private_execution_state(self):
        reconcile = self.trade_executor.reconcile_execution_state(self.tracker.signals)
        if reconcile.get("inactive_marked") or reconcile.get("state_updated"):
            self.tracker.persist()
            self._cleanup_finished_active_trade_cards()
        return reconcile

    def _reconcile_public_execution_state_if_due(self, current_time):
        if not self.trade_executor.can_trade():
            return
        if (current_time - float(self.last_execution_reconcile_at or 0)) < 90:
            return
        self.last_execution_reconcile_at = current_time
        reconcile = self.trade_executor.reconcile_execution_state(self.tracker.signals)
        if reconcile.get("inactive_marked") or reconcile.get("state_updated"):
            self.tracker.persist()
            self._cleanup_finished_active_trade_cards()

    def _refresh_position_snapshot_cards_if_due(self, now):
        if self.is_booting:
            return
        active = self._active_execution_signals()
        if not active:
            self.last_position_snapshot_refresh_slot = now.strftime("%Y-%m-%d %H:%M")
            return
        slot_key = now.strftime("%Y-%m-%d %H:%M")
        if self.last_position_snapshot_refresh_slot == slot_key:
            return
        self.last_position_snapshot_refresh_slot = slot_key
        refreshed = 0
        for sig in active:
            symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL).upper()
            current_price = self._current_market_price(sig.get("tf"), symbol=symbol)
            self._refresh_public_signal_snapshot(sig, close_price=current_price)
            self._refresh_active_trade_snapshot(sig, close_price=current_price)
            refreshed += 1
        if refreshed:
            print(f"  [TG] Refreshed {refreshed} active position chart card{'s' if refreshed != 1 else ''}.")

    def _active_execution_signals(self):
        active = []
        for sig in reversed(self.tracker.signals):
            execution = (sig or {}).get("execution") or {}
            if execution.get("active"):
                active.append(sig)
        return active

    def _ambiguous_active_position_message(self):
        return "I can see more than one active position right now. Tell me which one, for example: long, short, 5m, 15m, or the exact signal."

    def _signal_id_value(self, sig):
        sig = sig or {}
        return sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (((sig.get("execution") or {}).get("signal_id")))

    def _execution_position_id(self, sig):
        execution = (sig or {}).get("execution") or {}
        return str(execution.get("position_id") or "").strip()

    def _find_position_owner_signal(self, position_id, exclude_signal_id=None):
        position_id = str(position_id or "").strip()
        if not position_id:
            return None
        excluded = str(exclude_signal_id or "").strip()
        for sig in self.tracker.signals:
            sig_id = str(self._signal_id_value(sig) or "").strip()
            if excluded and sig_id == excluded:
                continue
            execution = (sig or {}).get("execution") or {}
            if not execution.get("active"):
                continue
            if execution.get("merge_shadow"):
                continue
            if self._execution_position_id(sig) == position_id:
                return sig
        return None

    def _mark_execution_as_merge_shadow(self, execution, owner_sig):
        execution = dict(execution or {})
        owner_sig = owner_sig or {}
        execution["merge_shadow"] = True
        execution["merged_into_existing"] = True
        execution["active"] = False
        execution["merged_owner_signal_id"] = self._signal_id_value(owner_sig)
        execution["merged_owner_tf"] = owner_sig.get("tf")
        execution["merged_owner_side"] = owner_sig.get("side")
        return execution

    def _recent_unexecuted_signals(self):
        pending = []
        for sig in reversed(self.tracker.signals):
            if str(sig.get("status", "")).upper() != "OPEN":
                continue
            execution = (sig or {}).get("execution") or {}
            if execution.get("active"):
                continue
            if execution.get("merge_shadow") or execution.get("merged_into_existing"):
                continue
            pending.append(sig)
        return pending

    def _control_signal_label(self, sig):
        signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (((sig.get("execution") or {}).get("signal_id")))
        symbol = str((sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or (sig.get("execution") or {}).get("symbol") or SYMBOL)).upper()
        return (
            f"id={signal_id or 'N/A'} symbol={symbol} type={sig.get('type', 'SCALP')} tf={sig.get('tf', 'N/A')} "
            f"side={sig.get('side', 'N/A')} entry={float(sig.get('entry', 0) or 0):.2f}"
        )

    def _current_market_price(self, tf_hint=None, symbol=None):
        symbol_name = str(symbol or SYMBOL).upper()
        try:
            live_price = fetch_last_price(symbol_name)
            if live_price and float(live_price) > 0:
                return float(live_price)
        except Exception:
            pass
        preferred = []
        tf_name = str(tf_hint or "").strip()
        if tf_name and symbol_name == str(SYMBOL).upper():
            preferred.append(tf_name)
        if symbol_name == str(SYMBOL).upper():
            preferred.extend([tf for tf in ["5m", "15m", "1h", "4h"] if tf not in preferred])
        for tf in preferred:
            df = self.latest_data.get(tf)
            try:
                if df is not None and not df.empty and "Close" in df.columns:
                    return float(df.iloc[-1]["Close"])
            except Exception:
                continue
        return None

    def _snapshot_dataframe(self, sig, tf, limit, close_price=None):
        symbol_name = str((sig or {}).get("symbol") or ((sig or {}).get("meta") or {}).get("symbol") or SYMBOL).upper()
        df = None
        cached = self.latest_data.get(tf) if symbol_name == str(SYMBOL).upper() else None
        try:
            if cached is not None and not cached.empty:
                df = cached.tail(limit).copy()
        except Exception:
            df = None
        if df is None or df.empty:
            df = fetch_klines(symbol=symbol_name, interval=tf, limit=limit)
        if df is None or df.empty:
            return df
        if close_price is not None:
            try:
                live_close = float(close_price)
            except Exception:
                live_close = 0.0
            if live_close > 0:
                idx = df.index[-1]
                last_open = float(df.iloc[-1]["Open"] or 0)
                last_high = float(df.iloc[-1]["High"] or 0)
                last_low = float(df.iloc[-1]["Low"] or 0)
                df.at[idx, "Close"] = live_close
                df.at[idx, "High"] = max(last_high, last_open, live_close)
                df.at[idx, "Low"] = min(last_low, last_open, live_close)
        return df

    def _position_live_metrics(self, sig, current_price_override=None):
        sig = sig or {}
        execution = sig.get("execution") or {}
        entry = float(sig.get("entry") or 0)
        qty = float(execution.get("qty", 0) or 0)
        side = str(sig.get("side") or "").upper()
        symbol = str((sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or execution.get("symbol") or SYMBOL)).upper()
        leverage = float(execution.get("leverage") or execution.get("target_leverage") or 0)
        try:
            current_price = float(current_price_override) if current_price_override is not None else self._current_market_price(sig.get("tf"), symbol=symbol)
        except Exception:
            current_price = self._current_market_price(sig.get("tf"), symbol=symbol)
        if entry <= 0 or qty <= 0 or current_price is None or side not in {"LONG", "SHORT"}:
            return None
        pnl_usd = (current_price - entry) * qty if side == "LONG" else (entry - current_price) * qty
        notional = entry * qty
        est_margin = (notional / leverage) if leverage > 0 else None
        roi_pct = (pnl_usd / est_margin * 100.0) if est_margin and est_margin > 0 else None
        move_pct = (((current_price - entry) / entry) * 100.0) if side == "LONG" else (((entry - current_price) / entry) * 100.0)
        return {
            "current_price": float(current_price),
            "pnl_usd": float(pnl_usd),
            "move_pct": float(move_pct),
            "est_margin": float(est_margin) if est_margin else None,
            "roi_pct": float(roi_pct) if roi_pct is not None else None,
        }

    def _remember_exec_suggestion(self, action):
        action = dict(action or {})
        if not action or str(action.get("action") or "").lower() in {"", "unsupported", "status"}:
            self.last_exec_suggested_action = None
        else:
            self.last_exec_suggested_action = {
                "created_at": time.time(),
                "action": action,
            }
        self._save_state()

    def _recent_exec_suggestion(self):
        payload = self.last_exec_suggested_action or {}
        created_at = float(payload.get("created_at") or 0)
        if not created_at or (time.time() - created_at) > max(60, PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC):
            self.last_exec_suggested_action = None
            self._save_state()
            return None
        action = payload.get("action") or {}
        return dict(action)

    def _remember_private_focus(self, sig=None, payload=None):
        payload = dict(payload or {})
        focus = {
            "updated_at": time.time(),
            "signal_id": None,
            "side": None,
            "tf": None,
            "symbol": None,
        }
        if sig:
            execution = (sig or {}).get("execution") or {}
            focus.update({
                "signal_id": sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or execution.get("signal_id"),
                "side": sig.get("side"),
                "tf": sig.get("tf"),
                "symbol": sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or execution.get("symbol") or SYMBOL,
            })
        for key in ["signal_id", "side", "tf", "symbol"]:
            if payload.get(key):
                focus[key] = payload.get(key)
        self.private_exec_focus = focus
        self._save_state()

    def _recent_private_focus(self):
        focus = dict(self.private_exec_focus or {})
        updated_at = float(focus.get("updated_at") or 0)
        if not updated_at or (time.time() - updated_at) > max(300, PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC * 4):
            self.private_exec_focus = {}
            self._save_state()
            return {}
        return focus

    def _extract_symbol_from_text(self, text):
        raw = str(text or "").upper()
        match = re.search(r"\b([A-Z]{2,10}USDT)\b", raw)
        if match:
            return match.group(1)
        aliases = {
            "BTC": "BTCUSDT",
            "BITCOIN": "BTCUSDT",
            "ETH": "ETHUSDT",
            "ETHEREUM": "ETHUSDT",
            "SOL": "SOLUSDT",
            "SOLANA": "SOLUSDT",
            "XRP": "XRPUSDT",
            "RIPPLE": "XRPUSDT",
            "DOGE": "DOGEUSDT",
            "BNB": "BNBUSDT",
            "ADA": "ADAUSDT",
            "CARDANO": "ADAUSDT",
            "AVAX": "AVAXUSDT",
            "AVALANCHE": "AVAXUSDT",
            "LINK": "LINKUSDT",
            "CHAINLINK": "LINKUSDT",
        }
        for key, value in aliases.items():
            if re.search(rf"\b{key}\b", raw):
                return value
        return None

    def _looks_like_chart_question(self, text):
        text_str = str(text or "").strip()
        if not text_str:
            return False
        symbol = self._extract_symbol_from_text(text_str)
        if not symbol:
            return False
        lower = text_str.lower()
        chart_cues = (
            "chart", "analysis", "analyse", "analyze", "think about", "thoughts on",
            "what do you think", "bullish", "bearish", "long", "short", "support",
            "resistance", "trend", "setup", "entry", "pump", "dump", "move", "direction",
        )
        return ("?" in text_str) or any(cue in lower for cue in chart_cues)

    def _build_symbol_chart_chat_answer(self, text, fallback_symbol=None):
        symbol = self._extract_symbol_from_text(text) or str(fallback_symbol or "").strip().upper()
        if not symbol:
            return ""
        payload = build_btc_scenarios_payload(symbol=symbol, mode="short_term")
        tf_map = payload.get("tf_map") or {}
        funding_ctx = payload.get("funding_ctx") or {}
        ticker_ctx = payload.get("ticker_ctx") or {}
        book_ctx = payload.get("book_ctx") or {}
        liq_ctx = payload.get("liq_ctx") or {}
        okx_ctx = payload.get("okx_ctx") or {}
        liq_map = payload.get("liq_map") or {}
        scenarios = list(payload.get("scenarios") or [])
        root = str(symbol).replace("USDT", "")

        longs = [row for row in scenarios if str(row.get("side") or "").upper() == "LONG"]
        shorts = [row for row in scenarios if str(row.get("side") or "").upper() == "SHORT"]
        best_long = max(longs, key=lambda row: float(row.get("probability") or 0), default=None)
        best_short = max(shorts, key=lambda row: float(row.get("probability") or 0), default=None)

        bias_1h = str((tf_map.get("1h") or {}).get("bias") or "n/a")
        bias_4h = str((tf_map.get("4h") or {}).get("bias") or "n/a")
        bias_1d = str((tf_map.get("1d") or {}).get("bias") or "n/a")
        day_change = float((ticker_ctx.get("day_change_pct") or 0.0))
        current_price = float(payload.get("current_price") or 0.0)
        funding_rate = float(payload.get("funding_rate") or 0.0)
        funding_bias = str(funding_ctx.get("bias") or "flat")
        top_above = str(book_ctx.get("above_text") or "n/a")
        top_below = str(book_ctx.get("below_text") or "n/a")

        def _liq_text_from_row(row):
            if not row:
                return "n/a"
            price = float(row.get("level_price") or row.get("price") or 0.0)
            size_usd = float(row.get("size_usd") or 0.0)
            bucket = str(row.get("bucket") or "").strip().lower()
            bucket_tag = f" {bucket}" if bucket else ""
            if price <= 0:
                return "n/a"
            if size_usd > 0:
                return f"{price:,.2f} (${size_usd/1e6:.1f}M{bucket_tag})"
            return f"{price:,.2f}{bucket_tag}"

        if top_above == "n/a":
            top_above = (
                _liq_text_from_row(liq_ctx.get("top_above"))
                if liq_ctx.get("top_above")
                else _liq_text_from_row(okx_ctx.get("mid_above") or okx_ctx.get("far_above"))
            )
            if top_above == "n/a":
                top_above = _liq_text_from_row(((liq_map.get("short_liq_zones") or [])[:1] or [None])[0])
        if top_below == "n/a":
            top_below = (
                _liq_text_from_row(liq_ctx.get("top_below"))
                if liq_ctx.get("top_below")
                else _liq_text_from_row(okx_ctx.get("mid_below") or okx_ctx.get("far_below"))
            )
            if top_below == "n/a":
                top_below = _liq_text_from_row(((liq_map.get("long_liq_zones") or [])[:1] or [None])[0])

        def _scenario_line(label, row):
            if not row:
                return f"{label}: n/a"
            entry_low = float(row.get("entry_low") or 0.0)
            entry_high = float(row.get("entry_high") or 0.0)
            entry_text = f"{entry_low:,.2f}-{entry_high:,.2f}" if abs(entry_high - entry_low) > 1e-9 else f"{entry_low:,.2f}"
            side_note = "with trend" if row.get("trend_aligned") else "counter-trend"
            return (
                f"{label}: {str(row.get('title') or '').upper()} | "
                f"entry {entry_text} | chance {float(row.get('probability') or 0):.0f}% | {side_note}"
            )

        bias_line = "mixed"
        if "bullish" in bias_4h.lower() and "bullish" in bias_1h.lower():
            bias_line = "bullish intraday bias"
        elif "bearish" in bias_4h.lower() and "bearish" in bias_1h.lower():
            bias_line = "bearish intraday bias"

        lines = [
            f"<b>{root} quick chart read</b>",
            (
                "<blockquote>"
                f"Price: {current_price:,.2f}\n"
                f"Funding: {funding_rate:+.6f}% ({funding_bias})\n"
                f"1H: {bias_1h} | 4H: {bias_4h} | 1D: {bias_1d}\n"
                f"24H: {day_change:+.2f}%"
                "</blockquote>"
            ),
            f"Bias: <b>{bias_line}</b>",
            f"Liquidity: above {top_above} | below {top_below}",
            f"<blockquote>{_scenario_line('Long plan', best_long)}</blockquote>",
            f"<blockquote>{_scenario_line('Short plan', best_short)}</blockquote>",
        ]
        return "\n".join(lines)

    def _extract_manual_preset(self, text):
        lower = str(text or "").lower()
        if "runner" in lower:
            return "runner"
        if "aggressive" in lower or "aggro" in lower:
            return "aggressive"
        if "safe" in lower or "conservative" in lower:
            return "safe"
        return None

    def _apply_context_to_action(self, action, text=""):
        action = dict(action or {})
        symbol = self._extract_symbol_from_text(text)
        preset = self._extract_manual_preset(text)
        if symbol and not action.get("symbol"):
            action["symbol"] = symbol
        if preset and not action.get("preset"):
            action["preset"] = preset
        focus = self._recent_private_focus()
        if focus:
            for key in ["signal_id", "side", "tf", "symbol"]:
                if not action.get(key) and focus.get(key):
                    action[key] = focus.get(key)
        return action

    def _infer_exec_suggestion_from_text(self, text):
        lower = str(text or "").strip().lower()
        if not lower:
            return None
        active = self._active_execution_signals()
        if len(active) != 1:
            return None
        sig = active[0]
        signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (((sig.get("execution") or {}).get("signal_id")))
        if "close" in lower:
            return {
                "action": "close_full",
                "signal_id": signal_id,
                "side": sig.get("side"),
                "tf": sig.get("tf"),
                "reason": "Recent conversation was about closing the active position",
            }
        return None

    @staticmethod
    def _safe_float_text(value, decimals=2, default="n/a"):
        try:
            val = float(value)
        except Exception:
            return default
        if abs(val) < 1e-12:
            return f"{0:.{decimals}f}"
        return f"{val:.{decimals}f}"

    def _build_live_exchange_context(self, now=None, force=False):
        now = now or datetime.now(timezone.utc)
        cache_age = time.time() - float(self.live_exchange_context_cache_ts or 0)
        if not force and self.live_exchange_context_cache and cache_age < 8:
            return self.live_exchange_context_cache

        lines = ["Live Bitunix exchange snapshot:"]
        trade_check, reconcile, _ = self._build_trade_check_bundle(now)
        lines.append(
            f"- mode={trade_check.get('mode')} auth_ok={bool(trade_check.get('auth_ok'))} "
            f"configured={bool(trade_check.get('configured'))}"
        )
        lines.append(
            f"- balance_total={self._safe_float_text(trade_check.get('balance_total'), 2)} "
            f"balance_free={self._safe_float_text(trade_check.get('balance_available'), 2)} "
            f"balance_used={self._safe_float_text(trade_check.get('balance_used'), 2)} "
            f"margin_mode={trade_check.get('margin_mode') or 'n/a'} "
            f"leverage={int(trade_check.get('leverage', 0) or 0)}x"
        )
        lines.append(
            f"- exchange_open_positions={int(trade_check.get('open_positions', 0) or 0)} "
            f"tracker_matched={int(reconcile.get('matched', 0) or 0)} "
            f"orphans={len(reconcile.get('orphan_positions', []) or [])} "
            f"missing_protection={len(reconcile.get('missing_protection', []) or [])}"
        )

        if self.trade_executor.mode != "live" or not self.trade_executor.client.is_configured():
            text = "\n".join(lines)
            self.live_exchange_context_cache = text
            self.live_exchange_context_cache_ts = time.time()
            return text

        active_by_position = {}
        for sig in self._active_execution_signals():
            execution = sig.get("execution") or {}
            position_id = str(execution.get("position_id") or "").strip()
            if position_id:
                active_by_position[position_id] = sig

        position_rows = []
        try:
            position_rows = self.trade_executor._pending_positions_list(None)
        except Exception as e:
            lines.append(f"- positions_error={e}")

        live_positions = []
        for pos in position_rows or []:
            try:
                qty = float(pos.get("qty") or pos.get("positionQty") or 0)
            except Exception:
                qty = 0.0
            if qty > 0:
                live_positions.append(pos)

        if live_positions:
            lines.append("Exchange positions:")
            unique_symbols = set()
            for pos in live_positions[:10]:
                position_id = str(pos.get("positionId") or "").strip() or "n/a"
                symbol = str(pos.get("symbol") or pos.get("instId") or SYMBOL).upper()
                unique_symbols.add(symbol)
                side = str(pos.get("side") or pos.get("positionSide") or "").upper() or "n/a"
                qty = self._safe_float_text(pos.get("qty") or pos.get("positionQty"), 6)
                entry = self._safe_float_text(
                    pos.get("avgPrice") or pos.get("averageOpenPrice") or pos.get("openPrice"),
                    2,
                )
                mark = self._safe_float_text(pos.get("markPrice") or pos.get("lastPrice"), 2)
                margin = self._safe_float_text(
                    pos.get("positionMargin") or pos.get("margin") or pos.get("initialMargin"),
                    4,
                )
                pnl = self._safe_float_text(
                    pos.get("unrealizedPnl") or pos.get("unrealizedProfit") or pos.get("floatingProfit"),
                    4,
                )
                liq = self._safe_float_text(pos.get("liquidationPrice") or pos.get("liqPrice"), 2)
                tracked = active_by_position.get(position_id)
                tracked_label = "untracked"
                if tracked:
                    tracked_label = f"{tracked.get('type', 'SCALP')} {tracked.get('side', '')} [{tracked.get('tf', 'n/a')}]"
                lines.append(
                    f"- position_id={position_id} symbol={symbol} side={side} qty={qty} "
                    f"entry={entry} mark={mark} pnl={pnl} margin={margin} liq={liq} tracked={tracked_label}"
                )

            pending_order_total = 0
            try:
                pending_orders = self.trade_executor.client.get_pending_orders().get("data", []) or []
                if isinstance(pending_orders, dict):
                    pending_orders = pending_orders.get("orderList") or pending_orders.get("data") or []
                pending_order_total = len(list(pending_orders or []))
            except Exception as e:
                lines.append(f"- open_orders_error={e}")
                pending_orders = []

            if pending_order_total or pending_orders == []:
                lines.append(f"- open_reduce_or_limit_orders={pending_order_total}")

            tpsl_summary_parts = []
            for symbol in sorted(unique_symbols):
                try:
                    symbol_tpsl = self.trade_executor.client.get_pending_tpsl(symbol).get("data", []) or []
                    if isinstance(symbol_tpsl, dict):
                        symbol_tpsl = symbol_tpsl.get("orderList") or symbol_tpsl.get("data") or []
                    rows = list(symbol_tpsl or [])
                    tp_count = 0
                    sl_count = 0
                    for row in rows:
                        try:
                            if float(row.get("tpPrice") or 0) > 0:
                                tp_count += 1
                        except Exception:
                            pass
                        try:
                            if float(row.get("slPrice") or 0) > 0:
                                sl_count += 1
                        except Exception:
                            pass
                    tpsl_summary_parts.append(f"{symbol}: tp_rows={tp_count} sl_rows={sl_count}")
                except Exception as e:
                    tpsl_summary_parts.append(f"{symbol}: error={e}")
            if tpsl_summary_parts:
                lines.append("- pending_tpsl=" + " | ".join(tpsl_summary_parts))
        else:
            lines.append("Exchange positions:")
            lines.append("- none")

        errors = []
        errors.extend([str(err) for err in (trade_check.get("errors") or [])[:2]])
        errors.extend([str(err) for err in (reconcile.get("errors") or [])[:2]])
        if errors:
            lines.append("Exchange issues:")
            for err in errors[:4]:
                lines.append(f"- {err}")

        text = "\n".join(lines)
        self.live_exchange_context_cache = text
        self.live_exchange_context_cache_ts = time.time()
        return text

    def _get_live_history_snapshot(self, *, start_dt=None, end_dt=None, force=False):
        end_dt = end_dt.astimezone(timezone.utc) if isinstance(end_dt, datetime) else datetime.now(timezone.utc)
        start_dt = start_dt.astimezone(timezone.utc) if isinstance(start_dt, datetime) else (end_dt - timedelta(days=7))
        key = (
            int(start_dt.timestamp()),
            int(end_dt.timestamp()),
        )
        cached = self.live_exchange_history_cache.get(key) or {}
        cache_age = time.time() - float(cached.get("ts") or 0)
        if not force and cached and cache_age < 15:
            return cached.get("payload") or {}

        payload = self.trade_executor.get_history_snapshot(
            start_time=int(start_dt.timestamp() * 1000),
            end_time=int(end_dt.timestamp() * 1000),
        )
        self.live_exchange_history_cache[key] = {
            "ts": time.time(),
            "payload": payload,
        }
        if len(self.live_exchange_history_cache) > 4:
            oldest_key = min(self.live_exchange_history_cache.keys(), key=lambda item: self.live_exchange_history_cache[item].get("ts", 0))
            if oldest_key != key:
                self.live_exchange_history_cache.pop(oldest_key, None)
        return payload

    def _build_live_history_context(self, now=None, force=False):
        now = now or datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        history = self._get_live_history_snapshot(start_dt=day_start, end_dt=now, force=force)
        summary = history.get("summary") or {}
        lines = ["Bitunix history since UTC midnight:"]
        if history.get("errors"):
            lines.extend(f"- {err}" for err in (history.get("errors") or [])[:4])
        if not any(int(summary.get(key, 0) or 0) for key in ("orders_total", "trades_total", "positions_total", "tpsl_total")):
            lines.append("- no history rows returned yet")
            return "\n".join(lines)

        lines.append(
            f"- history_orders={int(summary.get('orders_total', 0) or 0)} "
            f"filled={int(summary.get('orders_filled', 0) or 0)} "
            f"cancelled={int(summary.get('orders_cancelled', 0) or 0)}"
        )
        lines.append(
            f"- history_trades={int(summary.get('trades_total', 0) or 0)} "
            f"realized_pnl={float(summary.get('trades_realized_pnl', 0.0) or 0.0):+.4f} "
            f"fees={float(summary.get('trades_fees', 0.0) or 0.0):+.4f}"
        )
        lines.append(
            f"- history_positions={int(summary.get('positions_total', 0) or 0)} "
            f"realized_pnl={float(summary.get('positions_realized_pnl', 0.0) or 0.0):+.4f} "
            f"fees={float(summary.get('positions_fees', 0.0) or 0.0):+.4f} "
            f"funding={float(summary.get('positions_funding', 0.0) or 0.0):+.4f}"
        )
        lines.append(
            f"- history_tpsl={int(summary.get('tpsl_total', 0) or 0)} "
            f"tp_rows={int(summary.get('tpsl_tp_rows', 0) or 0)} "
            f"sl_rows={int(summary.get('tpsl_sl_rows', 0) or 0)}"
        )
        symbols = summary.get("symbols") or []
        if symbols:
            lines.append(f"- history_symbols={','.join(symbols[:10])}")

        recent_positions = list(history.get("positions") or [])[:5]
        if recent_positions:
            lines.append("Recent closed positions:")
            for row in recent_positions:
                symbol = str(row.get("symbol") or "n/a").upper()
                side = str(row.get("side") or row.get("positionSide") or "n/a").upper()
                realized = self._safe_float_text(
                    row.get("realizedPnl") or row.get("realizedPNL") or row.get("closeProfit") or row.get("profit") or row.get("pnl"),
                    4,
                )
                fee = self._safe_float_text(
                    row.get("fee") or row.get("closeFee") or row.get("tradingFee") or row.get("tradeFee"),
                    4,
                )
                close_price = self._safe_float_text(
                    row.get("closePrice") or row.get("avgClosePrice") or row.get("markPrice"),
                    2,
                )
                lines.append(
                    f"- symbol={symbol} side={side} realized={realized} fee={fee} close_price={close_price}"
                )

        recent_trades = list(history.get("trades") or [])[:5]
        if recent_trades:
            lines.append("Recent filled trades:")
            for row in recent_trades:
                symbol = str(row.get("symbol") or "n/a").upper()
                side = str(row.get("side") or row.get("tradeSide") or "n/a").upper()
                realized = self._safe_float_text(
                    row.get("realizedPnl") or row.get("realizedPNL") or row.get("profit") or row.get("pnl"),
                    4,
                )
                fee = self._safe_float_text(
                    row.get("fee") or row.get("tradeFee") or row.get("makerFee") or row.get("takerFee"),
                    4,
                )
                price = self._safe_float_text(
                    row.get("price") or row.get("avgPrice") or row.get("tradePrice"),
                    2,
                )
                lines.append(
                    f"- symbol={symbol} side={side} price={price} realized={realized} fee={fee}"
                )
        return "\n".join(lines)

    @staticmethod
    def _assistant_answer_is_weak(answer):
        text = str(answer or "").strip().lower()
        if not text:
            return True
        weak_markers = [
            "i don't have access",
            "i do not have access",
            "i can only see",
            "i only see",
            "i don't have a",
            "i do not have a",
            "i can't access",
            "i cannot access",
            "not available in this view",
            "i'd need to know",
            "i would need to know",
            "user message does not correspond",
        ]
        return any(marker in text for marker in weak_markers)

    def _ask_private_chat_question(self, user_text, *, context_text=None):
        context = context_text or self._build_gemini_trade_context()
        answer = ask_gemini_trade_question(
            GEMINI_API_KEY,
            GEMINI_MODEL,
            user_text,
            context,
        )
        if not self._assistant_answer_is_weak(answer):
            return answer

        stronger_context = context
        try:
            stronger_context = self._build_gemini_trade_context() + "\n\n" + self._build_live_exchange_context(force=True)
        except Exception:
            stronger_context = context

        retry_prompt = (
            "Please answer from the live Bitunix and bot context if possible. "
            "Do not say you lack access if the context already contains exchange state. "
            "If exact history is not available, give the closest truthful answer or estimate and say what it is based on.\n\n"
            f"User question: {user_text}"
        )
        retry = ask_gemini_trade_question(
            GEMINI_API_KEY,
            GEMINI_MODEL,
            retry_prompt,
            stronger_context,
        )
        return retry or answer

    def _build_gemini_trade_context(self):
        self._refresh_private_execution_state()
        now = datetime.now(timezone.utc)
        self._refresh_live_news_events(now)
        lines = ["Tracked execution view:"]
        active = self._active_execution_signals()
        if not active:
            lines.append("none")
        for sig in active[:8]:
            execution = sig.get("execution") or {}
            active_tps = self._format_active_tp_line(sig)
            live_sl = self._format_live_sl_value(sig)
            metrics = self._position_live_metrics(sig) or {}
            metrics_txt = ""
            if metrics:
                metrics_txt = (
                    f" current={float(metrics.get('current_price') or 0):.2f}"
                    f" pnl={float(metrics.get('pnl_usd') or 0):+.4f}"
                    f" roi={float(metrics.get('roi_pct') or 0):+.2f}%"
                )
            lines.append(
                f"- {self._control_signal_label(sig)} "
                f"status={sig.get('status')} sl={live_sl} "
                f"{active_tps} qty={float(execution.get('qty', 0) or 0):.6f}"
                f"{metrics_txt}"
            )
        lines.append("Recent tracked signals not opened on exchange:")
        pending = self._recent_unexecuted_signals()
        if not pending:
            lines.append("none")
        for sig in pending[:8]:
            lines.append(f"- {self._control_signal_label(sig)} status={sig.get('status')}")
        today_metrics = self._daily_trade_metrics(now)
        lines.append("Today's tracker summary (UTC):")
        lines.append(
            f"- opened={int(today_metrics.get('opened_today', 0) or 0)} "
            f"closed={int(today_metrics.get('closed_today', 0) or 0)} "
            f"wins={int(today_metrics.get('wins', 0) or 0)} "
            f"losses={int(today_metrics.get('losses', 0) or 0)} "
            f"breakeven={int(today_metrics.get('breakeven', 0) or 0)} "
            f"realized_r={float(today_metrics.get('realized_r', 0.0) or 0.0):+.2f}R"
        )
        if int(today_metrics.get("estimated_closed_count", 0) or 0) > 0:
            lines.append(
                f"- estimated_realized_usd={float(today_metrics.get('realized_est_usd', 0.0) or 0.0):+.4f} "
                f"from {int(today_metrics.get('estimated_closed_count', 0) or 0)} closed trade(s)"
            )
        lines.append(
            f"- current_open_unrealized_usd={float(today_metrics.get('open_unrealized_usd', 0.0) or 0.0):+.4f}"
        )
        lines.append("Live news status:")
        lines.append(f"- {self._format_news_status_line(now)}")
        upcoming = [event for event in self.live_news_events if event.get("datetime") and event["datetime"] >= now]
        if upcoming:
            next_event = upcoming[0]
            dt_text = next_event["datetime"].astimezone(timezone.utc).strftime("%d %b %H:%M UTC")
            lines.append(
                f"- Next event: {next_event.get('event', 'High Impact News')} at {dt_text} "
                f"(importance {next_event.get('importance', 'n/a')}, source Trading Economics)"
            )
        else:
            lines.append("- No upcoming high-impact live events are currently loaded.")
        lines.append("")
        lines.append(self._build_live_exchange_context(now=now))
        lines.append("")
        lines.append(self._build_live_history_context(now=now))
        return "\n".join(lines)

    def _answer_news_question(self, text: str) -> bool:
        normalized = self._normalize_intent_text(text)
        if not self._intent_has_any_normalized(normalized, ["news", "fomc", "cpi", "nfp", "pce", "event", "economic", "holiday"]):
            return False
        now = datetime.now(timezone.utc)
        self._refresh_live_news_events(now)
        status_line = self._format_news_status_line(now)
        answer_lines = [status_line]
        upcoming = [event for event in self.live_news_events if event.get("datetime") and event["datetime"] >= now]
        if upcoming:
            next_event = upcoming[0]
            dt_text = next_event["datetime"].astimezone(timezone.utc).strftime("%d %b %H:%M UTC")
            answer_lines.append(
                f"The next one I can see is {next_event.get('event', 'High Impact News')} at {dt_text}."
            )
        self._send_private_execution_answer(" ".join(answer_lines))
        return True

    def _telegram_download_file(self, file_id):
        file_id = str(file_id or "").strip()
        if not file_id or not BOT_TOKEN:
            return None, None
        try:
            meta = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                params={"file_id": file_id},
                timeout=20,
            ).json()
            file_path = (((meta or {}).get("result") or {}).get("file_path") or "").strip()
            if not file_path:
                return None, None
            content = requests.get(
                f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}",
                timeout=30,
            )
            content.raise_for_status()
            return content.content, file_path
        except Exception:
            return None, None

    def _handle_private_exec_image_message(self, message):
        if not PRIVATE_EXEC_AI_CONTROL_ENABLED or not self._is_private_exec_chat((message.get("chat") or {}).get("id")):
            return False
        file_id = None
        mime_type = "image/jpeg"
        photos = message.get("photo") or []
        document = message.get("document") or {}
        if photos:
            file_id = (photos[-1] or {}).get("file_id")
            mime_type = "image/jpeg"
        elif document and str(document.get("mime_type") or "").lower().startswith("image/"):
            file_id = document.get("file_id")
            mime_type = str(document.get("mime_type") or "image/jpeg")
        if not file_id:
            return False
        if not GEMINI_API_KEY:
            self._send_private_execution_answer("I can analyze screenshots here, but the Gemini key is missing in your .env.")
            return True
        image_bytes, _ = self._telegram_download_file(file_id)
        if not image_bytes:
            self._send_private_execution_answer("I tried to load that screenshot, but Telegram did not give it back cleanly. Send it again and I’ll try once more.")
            return True
        prompt = str(message.get("caption") or "").strip() or (
            "Please analyze this trading screenshot, explain what you see, tell me what matters, and suggest what I should do next."
        )
        answer = ask_gemini_trade_question_with_image(
            GEMINI_API_KEY,
            GEMINI_MODEL,
            prompt,
            self._build_gemini_trade_context(),
            image_bytes,
            mime_type=mime_type,
        )
        if answer:
            self._send_private_execution_answer(answer)
        else:
            self._send_private_execution_answer("I looked at the screenshot but I could not get a clean answer back this time. Send it again with a short caption and I’ll retry.")
        return True

    def _resolve_signal_for_action(self, payload, *, allow_unexecuted=False):
        signal_id = str(payload.get("signal_id") or "").strip()
        side = str(payload.get("side") or "").strip().upper()
        tf = str(payload.get("tf") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
        pool = self._recent_unexecuted_signals() if allow_unexecuted else self._active_execution_signals()

        if signal_id:
            for sig in pool:
                candidate_id = str(
                    sig.get("signal_id")
                    or ((sig.get("meta") or {}).get("signal_id"))
                    or (((sig.get("execution") or {}).get("signal_id")))
                    or ""
                ).strip()
                if candidate_id == signal_id:
                    return sig

        matches = []
        for sig in pool:
            candidate_symbol = str(
                sig.get("symbol")
                or ((sig.get("meta") or {}).get("symbol"))
                or (((sig.get("execution") or {}).get("symbol")))
                or SYMBOL
            ).strip().upper()
            if symbol and candidate_symbol != symbol:
                continue
            if side and str(sig.get("side", "")).upper() != side:
                continue
            if tf and str(sig.get("tf", "")) != tf:
                continue
            matches.append(sig)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return None
        if len(pool) == 1:
            return pool[0]
        return None

    def _resolve_signals_for_bulk_action(self, payload):
        side = str(payload.get("side") or "").strip().upper()
        tf = str(payload.get("tf") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
        matches = []
        for sig in self._active_execution_signals():
            candidate_symbol = str(
                sig.get("symbol")
                or ((sig.get("meta") or {}).get("symbol"))
                or (((sig.get("execution") or {}).get("symbol")))
                or SYMBOL
            ).strip().upper()
            if symbol and candidate_symbol != symbol:
                continue
            if side and str(sig.get("side", "")).upper() != side:
                continue
            if tf and str(sig.get("tf", "")) != tf:
                continue
            matches.append(sig)
        return matches

    def _preview_exec_action(self, action):
        kind = str(action.get("action") or "").lower()
        if kind == "move_sl_entry":
            return "Move stop to breakeven/entry"
        if kind == "move_sl":
            return f"Move stop to {float(action.get('price') or 0):.2f}"
        if kind == "set_tp":
            if int(action.get("tp_index") or 0) not in {1, 2, 3}:
                return f"Set take profit to {float(action.get('price') or 0):.2f}"
            return f"Set TP{int(action.get('tp_index') or 0)} to {float(action.get('price') or 0):.2f}"
        if kind == "set_single_tp":
            return f"Set one take profit to {float(action.get('price') or 0):.2f}"
        if kind == "set_tp_split":
            splits = list(action.get("splits") or [])
            if len(splits) == 3:
                pct_text = "/".join(f"{int(round(float(x) * 100))}" for x in splits)
                return f"Change TP split to {pct_text}"
            return "Change TP split"
        if kind == "cancel_tp":
            if int(action.get("tp_index") or 0) not in {1, 2, 3}:
                return "Cancel all take profits"
            return f"Cancel TP{int(action.get('tp_index') or 0)}"
        if kind == "move_sl_tp":
            return f"Move stop to TP{int(action.get('tp_index') or 0)}"
        if kind == "close_full":
            return "Close full position"
        if kind == "close_partial":
            return f"Close {float(action.get('fraction', 0) or 0) * 100:.1f}% of position"
        if kind == "close_all_positions":
            side = str(action.get("side") or "").upper()
            if side in {"LONG", "SHORT"}:
                return f"Close all {side.lower()} positions"
            return "Close all open positions"
        if kind == "move_all_sl_entry":
            side = str(action.get("side") or "").upper()
            if side in {"LONG", "SHORT"}:
                return f"Move all {side.lower()} stops to breakeven"
            return "Move all stops to breakeven"
        if kind == "status":
            if action.get("signal_id") or action.get("side") or action.get("tf"):
                return "Show position details"
            return "Show live status"
        if kind == "open_signal":
            return "Open tracked signal on exchange"
        if kind == "open_manual":
            side = str(action.get("side") or "").upper()
            tf = str(action.get("tf") or "").strip()
            symbol = str(action.get("symbol") or "").strip().upper()
            preset = str(action.get("preset") or "").strip().lower()
            margin = action.get("margin_usd")
            leverage = action.get("leverage")
            parts = ["Open manual market position"]
            if side in {"LONG", "SHORT"}:
                parts = [f"Open manual {side} position"]
            if tf:
                parts.append(f"on {tf}")
            if symbol:
                parts.append(f"for {symbol}")
            if preset:
                parts.append(f"using the {preset} preset")
            if margin not in (None, "", 0, 0.0):
                parts.append(f"with ${float(margin):.2f} margin")
            if leverage not in (None, "", 0, 0.0):
                parts.append(f"at {int(float(leverage))}x")
            return " ".join(parts)
        if kind == "cancel_all_positions_tps":
            side = str(action.get("side") or "").upper()
            if side in {"LONG", "SHORT"}:
                return f"Cancel all take profits on all {side.lower()} positions"
            return "Cancel all take profits on all open positions"
        if kind == "rebuild_protection":
            return "Rebuild the live stop and take-profit protection"
        return str(action.get("reason") or "Unsupported action")

    def _extract_followup_price(self, text, reference=None):
        raw_text = str(text or "").strip()
        if not raw_text:
            return None
        tokens = re.findall(r'(?<!\w)(\d[\d.,]{0,20})(?!\w)', raw_text)
        if not tokens:
            return None

        ref = 0.0
        try:
            ref = float(reference or 0)
        except Exception:
            ref = 0.0

        for token in reversed(tokens):
            token = token.strip()
            normalized = token.replace(" ", "")
            candidate = None

            try_forms = []
            if "," in normalized and "." in normalized:
                if normalized.rfind(",") > normalized.rfind("."):
                    try_forms.append(normalized.replace(".", "").replace(",", "."))
                try_forms.append(normalized.replace(",", ""))
            else:
                try_forms.append(normalized.replace(",", ""))
                if ref >= 10000 and re.fullmatch(r"\d{1,3}[.,]\d{3}", normalized):
                    try_forms.insert(0, normalized.replace(".", "").replace(",", ""))

            for form in try_forms:
                try:
                    value = float(form)
                except Exception:
                    continue
                if value <= 0:
                    continue
                if ref >= 10000 and value < 1000 and re.fullmatch(r"\d{1,3}[.,]\d{3}", normalized):
                    value = float(normalized.replace(".", "").replace(",", ""))
                candidate = value
                break

            if candidate and candidate > 0:
                return candidate
        return None

    def _apply_followup_to_pending_action(self, action, text):
        action = dict(action or {})
        lower = str(text or "").strip().lower()
        if not action or not lower:
            return action

        sig = self._resolve_signal_for_action(action, allow_unexecuted=False)
        reference = float((sig or {}).get("entry") or 0)

        if action.get("action") == "open_manual":
            if str(action.get("side") or "").upper() not in {"LONG", "SHORT"}:
                if "long" in lower:
                    action["side"] = "LONG"
                elif "short" in lower:
                    action["side"] = "SHORT"
            symbol = self._extract_symbol_from_text(text)
            if symbol:
                action["symbol"] = symbol
            preset = self._extract_manual_preset(text)
            if preset:
                action["preset"] = preset
            for tf_candidate in ["5m", "15m", "1h", "4h"]:
                if tf_candidate in lower:
                    action["tf"] = tf_candidate
                    break
            margin_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:\$|usd|usdt|dollar|dollars|margin)', lower)
            if margin_match:
                try:
                    action["margin_usd"] = float(margin_match.group(1).replace(",", "."))
                except Exception:
                    pass
            lev_match = re.search(r'(\d+(?:[.,]\d+)?)\s*x\b', lower)
            if lev_match:
                try:
                    action["leverage"] = int(float(lev_match.group(1).replace(",", ".")))
                except Exception:
                    pass

        if action.get("action") == "set_tp":
            if action.get("tp_index") in (None, "", 0, 0.0, "0"):
                if "tp1" in lower:
                    action["tp_index"] = 1
                elif "tp2" in lower:
                    action["tp_index"] = 2
                elif "tp3" in lower:
                    action["tp_index"] = 3

        if action.get("action") in {"move_sl", "set_tp"}:
            price = self._extract_followup_price(text, reference=reference)
            if price:
                action["price"] = price

        if action.get("action") == "set_single_tp":
            price = self._extract_followup_price(text, reference=reference)
            if price:
                action["price"] = price

        if action.get("action") == "set_tp_split":
            splits = self._extract_tp_split_values(text)
            if splits:
                action["splits"] = splits

        if action.get("action") == "close_partial":
            match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', lower)
            if match:
                try:
                    action["fraction"] = max(0.0, min(1.0, float(match.group(1).replace(",", ".")) / 100.0))
                except Exception:
                    pass
            else:
                match = re.search(r'(\d+(?:[.,]\d+)?)', lower)
                if match and any(word in lower for word in ["half", "percent", "portion", "part"]):
                    try:
                        value = float(match.group(1).replace(",", "."))
                        if value > 1:
                            value = value / 100.0
                        action["fraction"] = max(0.0, min(1.0, value))
                    except Exception:
                        pass
            if "half" in lower and not action.get("fraction"):
                action["fraction"] = 0.5

        return action

    def _needs_exec_clarification(self, action):
        action_type = str(action.get("action") or "").lower()
        active = self._active_execution_signals()

        if action_type == "open_manual":
            side = str(action.get("side") or "").upper()
            if side not in {"LONG", "SHORT"}:
                return "Do you want to open a LONG or a SHORT position?"
            return None

        if action_type == "open_signal":
            if not self._resolve_signal_for_action(action, allow_unexecuted=True):
                return "Which tracked signal do you want to open? Send the ID, or say the side and timeframe."
            return None

        if action_type in {"move_sl_entry", "move_sl", "set_tp", "set_single_tp", "set_tp_split", "cancel_tp", "close_full", "close_partial", "status", "move_sl_tp", "rebuild_protection"}:
            if not self._resolve_signal_for_action(action, allow_unexecuted=False):
                if len(active) > 1:
                    return "Which open position do you mean? Send the ID, or say the side and timeframe."
                if not active:
                    return "I do not see any active exchange position right now."

        if action_type in {"close_all_positions", "move_all_sl_entry", "cancel_all_positions_tps"}:
            matches = self._resolve_signals_for_bulk_action(action)
            if not matches:
                if not active:
                    return "I do not see any active exchange position right now."
                return "I could not find any active positions matching that filter."
            return None

        if action_type == "move_sl":
            if action.get("price") in (None, "", 0, 0.0, "0"):
                return "What stop-loss price do you want?"
            return None

        if action_type == "set_tp":
            tp_index = int(action.get("tp_index") or 0)
            sig = self._resolve_signal_for_action(action, allow_unexecuted=False)
            execution = (sig or {}).get("execution") or {}
            active_indices = [i + 1 for i, q in enumerate(list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])) if float(q or 0) > 0]
            if action.get("price") in (None, "", 0, 0.0, "0"):
                if tp_index in {1, 2, 3}:
                    return f"What price do you want for TP{tp_index}?"
                return "What take-profit price do you want?"
            if tp_index not in {1, 2, 3}:
                if len(active_indices) <= 1:
                    return None
                return "Which target do you want to change: TP1, TP2, or TP3?"
            return None

        if action_type == "set_single_tp":
            if action.get("price") in (None, "", 0, 0.0, "0"):
                return "What take-profit price do you want?"
            return None

        if action_type == "set_tp_split":
            splits = list(action.get("splits") or [])
            if len(splits) != 3:
                return "What TP split do you want? For example: 20/30/50."
            return None

        if action_type == "move_sl_tp":
            if int(action.get("tp_index") or 0) not in {1, 2, 3}:
                return "Which target should I use for the stop: TP1, TP2, or TP3?"
            return None

        if action_type == "rebuild_protection":
            return None

        if action_type == "close_partial":
            try:
                fraction = float(action.get("fraction") or 0)
            except Exception:
                fraction = 0.0
            if fraction <= 0 or fraction >= 1:
                return "How much do you want to close? For example: close 30 percent."
            return None

        return None

    def _derive_manual_trade_signal(self, action):
        side = str(action.get("side") or "").upper()
        if side not in {"LONG", "SHORT"}:
            raise ValueError("Manual open needs LONG or SHORT side.")
        tf = str(action.get("tf") or "5m")
        symbol = str(action.get("symbol") or SYMBOL).upper()
        df = self.latest_data.get(tf) if symbol == str(SYMBOL).upper() else None
        if df is None or df.empty:
            df = fetch_klines(symbol=symbol, interval=tf, limit=160)
        if df is None or df.empty:
            raise ValueError(f"No live market data available for {symbol} on {tf}.")
        df = calculate_channels(df.copy())
        curr = df.iloc[-1]
        entry = float(curr["Close"])
        atr = float(curr["ATR"]) if "ATR" in curr and curr["ATR"] else max(entry * 0.002, 1.0)
        risk_cfg = TIMEFRAME_RISK_MULTIPLIERS.get(tf, TIMEFRAME_RISK_MULTIPLIERS.get("5m", {}))
        sl_mult = float(risk_cfg.get("sl", 2.0))
        tp1_mult = float(risk_cfg.get("tp1", 1.0))
        tp2_mult = float(risk_cfg.get("tp2", 1.8))
        tp3_mult = float(risk_cfg.get("tp3", 2.5))
        preset = str(action.get("preset") or "").strip().lower()
        if preset == "safe":
            sl_mult *= 1.15
            tp1_mult *= 0.90
            tp2_mult *= 1.20
            tp3_mult *= 1.50
        elif preset == "aggressive":
            sl_mult *= 0.90
            tp1_mult *= 1.05
            tp2_mult *= 1.10
            tp3_mult *= 1.15
        elif preset == "runner":
            sl_mult *= 1.05
            tp1_mult *= 1.20
            tp2_mult *= 1.40
            tp3_mult *= 1.70

        sl = action.get("sl")
        tp1 = action.get("tp1")
        tp2 = action.get("tp2")
        tp3 = action.get("tp3")
        if sl is None:
            sl = entry - atr * sl_mult if side == "LONG" else entry + atr * sl_mult
        if tp1 is None:
            tp1 = entry + atr * tp1_mult if side == "LONG" else entry - atr * tp1_mult
        if tp2 is None:
            tp2 = entry + atr * tp2_mult if side == "LONG" else entry - atr * tp2_mult
        if tp3 is None:
            tp3 = entry + atr * tp3_mult if side == "LONG" else entry - atr * tp3_mult

        meta = {
            "strategy": "MANUAL_PRIVATE",
            "size": max(MIN_SIGNAL_SIZE_PCT, float(action.get("size_pct") or MIN_SIGNAL_SIZE_PCT)),
            "symbol": symbol,
        }
        if preset:
            meta["manual_preset"] = preset
        if action.get("margin_usd") is not None:
            meta["manual_margin_usd"] = float(action.get("margin_usd"))
        if action.get("leverage") is not None:
            meta["manual_leverage"] = int(float(action.get("leverage")))
        elif preset == "safe":
            meta["manual_leverage"] = min(int(BITUNIX_DEFAULT_LEVERAGE or 1), 10)
        elif preset == "aggressive":
            meta["manual_leverage"] = max(int(BITUNIX_DEFAULT_LEVERAGE or 1), 20)
        elif preset == "runner":
            meta["manual_leverage"] = min(max(int(BITUNIX_DEFAULT_LEVERAGE or 1), 8), 15)

        signal_id = new_signal_id()
        ts = curr.name.isoformat() if hasattr(curr, "name") else datetime.now(timezone.utc).isoformat()
        return {
            "signal_id": signal_id,
            "type": "MANUAL",
            "symbol": symbol,
            "side": side,
            "entry": float(entry),
            "sl": float(sl),
            "tp1": float(tp1),
            "tp2": float(tp2),
            "tp3": float(tp3),
            "tf": tf,
            "timestamp": ts,
            "status": "OPEN",
            "meta": meta,
        }

    def _apply_private_exec_action(self, action):
        action_type = str(action.get("action") or "").lower()
        if action_type == "status":
            reason_text = str(action.get("reason") or "").lower()
            if self._looks_like_balance_only_text(reason_text):
                self._send_simple_balance_answer()
                return True
            if self._looks_like_open_positions_text(reason_text):
                self._send_simple_open_positions_answer()
                return True
            if self._looks_like_account_mode_text(reason_text):
                self._send_simple_account_mode_answer()
                return True
            wants_account_status = self._looks_like_account_status_text(reason_text)
            sig = None if wants_account_status else self._resolve_signal_for_action(action, allow_unexecuted=False)
            if sig:
                self._remember_private_focus(sig=sig)
                self._send_single_position_snapshot(sig, title="Position Info")
            else:
                if len(self._active_execution_signals()) > 1 and not wants_account_status:
                    self._send_private_execution_answer(self._ambiguous_active_position_message())
                    return True
                self._send_execution_status_snapshot(datetime.now(timezone.utc), title="Bitunix Live Status")
            return True

        if action_type == "open_signal":
            sig = self._resolve_signal_for_action(action, allow_unexecuted=True)
            if not sig:
                self._send_private_execution_notice("Exec Control", ["No tracked pending signal matched your request."], icon="вљ пёЏ")
                return False
            self._remember_private_focus(sig=sig)
            self._execute_exchange_trade(sig)
            return True

        if action_type == "open_manual":
            sig = self._derive_manual_trade_signal(action)
            self.tracker.log_signal(
                side=sig["side"], entry=sig["entry"], sl=sig["sl"], tp1=sig["tp1"], tp2=sig["tp2"], tp3=sig["tp3"],
                tf=sig["tf"], timestamp=sig["timestamp"], msg_id=None, chat_id=self._execution_chat_id(),
                signal_type="MANUAL", meta=sig.get("meta") or {}
            )
            self.tracker.signals[-1]["signal_id"] = sig["signal_id"]
            self.tracker.signals[-1]["symbol"] = sig.get("symbol", SYMBOL)
            self._remember_private_focus(sig=self.tracker.signals[-1])
            self._execute_exchange_trade(self.tracker.signals[-1])
            return True

        if action_type in {"close_all_positions", "move_all_sl_entry", "cancel_all_positions_tps"}:
            targets = self._resolve_signals_for_bulk_action(action)
            if not targets:
                self._send_private_execution_answer("I do not see any matching active positions for that.")
                return False

            results = []
            for sig_obj in targets:
                if action_type == "close_all_positions":
                    result = self.trade_executor.manual_close_position(sig_obj, 1.0)
                elif action_type == "move_all_sl_entry":
                    result = self.trade_executor.manual_move_stop(sig_obj, float(sig_obj.get("entry") or 0))
                else:
                    result = self.trade_executor.manual_cancel_all_tps(sig_obj)
                if result.payload:
                    sig_obj["execution"] = result.payload
                results.append((sig_obj, result))

            self._save_state()

            done = [item for item in results if item[1].accepted]
            failed = [item for item in results if not item[1].accepted]
            if action_type == "close_all_positions":
                opener = (
                    f"I closed {len(done)} position{'s' if len(done) != 1 else ''}."
                    if done and not failed else
                    f"I worked on {len(results)} position{'s' if len(results) != 1 else ''}."
                )
            elif action_type == "move_all_sl_entry":
                opener = (
                    f"I moved {len(done)} stop{'s' if len(done) != 1 else ''} to breakeven."
                    if done and not failed else
                    f"I worked on {len(results)} stop{'s' if len(results) != 1 else ''}."
                )
            else:
                opener = (
                    f"I removed take profits from {len(done)} position{'s' if len(done) != 1 else ''}."
                    if done and not failed else
                    f"I worked on take profits for {len(results)} position{'s' if len(results) != 1 else ''}."
                )

            lines = [opener]
            if failed:
                lines.append(f"I could not finish {len(failed)} of them.")
                for sig_obj, result in failed[:5]:
                    signal_id = sig_obj.get("signal_id") or ((sig_obj.get("meta") or {}).get("signal_id")) or (((sig_obj.get("execution") or {}).get("signal_id"))) or "N/A"
                    lines.append(f"- {signal_id}: {result.message}")
            self._send_private_execution_answer("\n".join(lines))
            return len(done) > 0

        sig = self._resolve_signal_for_action(action, allow_unexecuted=False)
        if not sig:
            if len(self._active_execution_signals()) > 1:
                self._send_private_execution_notice("Exec Control", [self._ambiguous_active_position_message()], icon="⚠️")
            else:
                self._send_private_execution_notice("Exec Control", ["No active exchange position matched your request."], icon="⚠️")
            return False
        self._remember_private_focus(sig=sig)

        if action_type == "move_sl_entry":
            result = self.trade_executor.manual_move_stop(sig, float(sig.get("entry") or 0))
        elif action_type == "move_sl_tp":
            tp_index = int(action.get("tp_index") or 0)
            tp_price = float(sig.get(f"tp{tp_index}") or 0)
            if tp_index not in {1, 2, 3} or tp_price <= 0:
                self._send_private_execution_notice(
                    "Exec Control",
                    ["I could not find a valid TP level to use for that stop move."],
                    icon="⚠️",
                )
                return False
            result = self.trade_executor.manual_move_stop(sig, tp_price)
        elif action_type == "move_sl":
            if action.get("price") in (None, "", 0, 0.0, "0"):
                self._send_private_execution_notice(
                    "Exec Control",
                    [
                        "I understood a stop-loss change, but no valid stop price was found.",
                        "Please say the exact stop price, for example: move stop to 67120",
                    ],
                    icon="вљ пёЏ",
                )
                return False
            result = self.trade_executor.manual_move_stop(sig, float(action.get("price")))
        elif action_type == "set_tp":
            tp_index = int(action.get("tp_index") or 0)
            if action.get("price") in (None, "", 0, 0.0, "0"):
                self._send_private_execution_notice(
                    "Exec Control",
                    [
                        "I understood a take-profit change, but no valid price was found.",
                        "Please say the exact take-profit price, for example: set tp to 67120",
                    ],
                    icon="⚠️",
                )
                return False
            execution = (sig or {}).get("execution") or {}
            active_indices = [i + 1 for i, q in enumerate(list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])) if float(q or 0) > 0]
            if tp_index not in {1, 2, 3}:
                if len(active_indices) <= 1:
                    if len(active_indices) == 1:
                        result = self.trade_executor.manual_set_tp(sig, active_indices[0], float(action.get("price")))
                    else:
                        result = self.trade_executor.manual_set_single_tp(sig, float(action.get("price")))
                else:
                    self._send_private_execution_notice(
                        "Exec Control",
                        ["I need TP1, TP2, or TP3 to change a target."],
                        icon="⚠️",
                    )
                    return False
            else:
                if not active_indices:
                    result = self.trade_executor.manual_set_single_tp(sig, float(action.get("price")))
                else:
                    result = self.trade_executor.manual_set_tp(sig, tp_index, float(action.get("price")))
        elif action_type == "cancel_tp":
            tp_index = int(action.get("tp_index") or 0)
            if tp_index not in {1, 2, 3}:
                result = self.trade_executor.manual_cancel_all_tps(sig)
            else:
                result = self.trade_executor.manual_cancel_tp(sig, tp_index)
        elif action_type == "set_single_tp":
            if action.get("price") in (None, "", 0, 0.0, "0"):
                self._send_private_execution_notice(
                    "Exec Control",
                    [
                        "I understood a single take-profit change, but no valid price was found.",
                        "Please say the exact price, for example: set one tp to 67120",
                    ],
                    icon="⚠️",
                )
                return False
            result = self.trade_executor.manual_set_single_tp(sig, float(action.get("price")))
        elif action_type == "set_tp_split":
            splits = list(action.get("splits") or [])
            if len(splits) != 3:
                self._send_private_execution_notice(
                    "Exec Control",
                    ["I need three TP split values, for example: 20/30/50."],
                    icon="⚠️",
                )
                return False
            result = self.trade_executor.manual_set_tp_splits(sig, splits)
        elif action_type == "close_full":
            result = self.trade_executor.manual_close_position(sig, 1.0)
        elif action_type == "close_partial":
            result = self.trade_executor.manual_close_position(sig, float(action.get("fraction") or 0))
        elif action_type == "rebuild_protection":
            result = self.trade_executor.rebuild_position_protection(sig, reason="manual refresh")
        else:
            self._send_private_execution_notice("Exec Control", [f"Unsupported action: {action_type}"], icon="вљ пёЏ")
            return False

        current_sig = dict(sig or {})
        if result.payload:
            sig["execution"] = result.payload
            current_sig["execution"] = result.payload
        self._save_state()
        self._send_private_execution_notice(
            "Exec Action Result",
            self._format_execution_lines(
                current_sig,
                extra=[
                    f"I finished that for you. {result.message}" if result.accepted else f"I could not finish that. {result.message}",
                ],
            ),
            icon="✅" if result.accepted else "⚠️",
        )
        return result.accepted

    def _handle_private_exec_message(self, message):
        if not PRIVATE_EXEC_AI_CONTROL_ENABLED or not self._is_private_exec_chat((message.get("chat") or {}).get("id")):
            return False
        text = str(message.get("text") or message.get("caption") or "").strip()
        cmd = text.lower().split()[0] if text else ""
        cmd_base = cmd.split("@", 1)[0]
        if cmd_base == "/scenarios":
            if not self._is_scenarios_topic((message.get("chat") or {}).get("id"), message.get("message_thread_id")):
                return True
            return self._handle_btc_market_command(
                chat_id=(message.get("chat") or {}).get("id"),
                reply_to_message_id=message.get("message_id"),
                message_thread_id=message.get("message_thread_id"),
                mode="short_term",
            )
        if cmd_base == "/liqmap":
            return True
        if cmd_base == "/analytics":
            return True
        if (message.get("photo") or (message.get("document") or {}).get("mime_type")) and not text:
            return self._handle_private_exec_image_message(message)
        if message.get("photo") or str((message.get("document") or {}).get("mime_type") or "").lower().startswith("image/"):
            if self._handle_private_exec_image_message(message):
                return True
        if not text:
            return False

        def _ask_to_confirm(action_obj, chat_id, source_text=None):
            action_text = self._preview_exec_action(action_obj).strip().rstrip(".")
            preview_lines = [
                f"I can do that. {action_text}.",
                "If you want me to go ahead, reply YES. If you changed your mind, reply NO.",
            ]
            if action_obj.get("signal_id"):
                preview_lines.insert(1, f"Here is the position ID:\n<pre>{action_obj.get('signal_id')}</pre>")
            self.pending_exec_action = {
                "created_at": time.time(),
                "chat_id": str(chat_id or ""),
                "action": action_obj,
                "mode": "confirm",
            }
            if source_text is not None:
                self.pending_exec_action["source_text"] = source_text
            self._save_state()
            self._send_private_execution_notice("Confirm Exec Action", preview_lines, icon="🤖")

        lower = text.lower()
        normalized = self._normalize_intent_text(text)
        chat_id = str((message.get("chat") or {}).get("id") or "")
        if self._intent_has_any_normalized(normalized, ["still open", "is it open", "is this open", "is that open", "is it still open"]):
            focus_payload = self._apply_context_to_action({"action": "status"}, text)
            sig = self._resolve_signal_for_action(focus_payload, allow_unexecuted=False)
            if sig:
                execution = sig.get("execution") or {}
                if execution.get("active"):
                    self._send_private_execution_answer("Yes, that position is still open on the exchange.")
                else:
                    self._send_private_execution_answer("No, that position is not open anymore.")
                return True
            self._send_simple_open_positions_answer()
            return True
        if self._looks_like_open_positions_text(lower):
            self._send_open_positions_snapshot("Open Positions")
            return True

        if self._answer_news_question(text):
            return True

        pending_phrases = [
            "pending signals", "show pending signals", "what signals are pending",
            "show waiting signals", "waiting signals", "pending setups"
        ]
        if self._intent_has_any_normalized(normalized, pending_phrases):
            self._send_pending_signals_snapshot("Pending Signals")
            return True

        performance_phrases = [
            "show roi for all positions", "show pnl for all positions", "show performance",
            "position performance", "how are my positions doing", "show open position roi",
            "show open position pnl", "all position roi", "all position pnl"
        ]
        if self._intent_has_any_normalized(normalized, performance_phrases):
            self._send_positions_performance_snapshot("Position Performance")
            return True

        today_trade_phrases = [
            "how many trades did i open today", "how many trades opened today",
            "how many signals did i open today", "how many trades today",
            "how many positions did i open today", "trades i opened today",
            "opened today", "today trades", "today trade count"
        ]
        if self._intent_has_any_normalized(normalized, today_trade_phrases):
            self._send_today_trade_count_answer()
            return True

        today_pnl_words = ["gain", "gained", "lose", "lost", "loss", "profit", "pnl", "made", "earned", "result"]
        if self._intent_has_phrase_in_normalized(normalized, "today") and self._intent_has_any_normalized(normalized, today_pnl_words):
            self._send_today_pnl_answer()
            return True

        if self._looks_like_bitcoin_plans_request(text):
            return self._handle_btc_market_command(
                chat_id=(message.get("chat") or {}).get("id"),
                reply_to_message_id=message.get("message_id"),
                message_thread_id=message.get("message_thread_id"),
                mode="short_term",
            )

        if self._looks_like_balance_only_text(lower):
            self._send_simple_balance_answer()
            return True

        if self._looks_like_account_mode_text(lower):
            self._send_simple_account_mode_answer()
            return True

        if self._looks_like_account_status_text(lower):
            self._send_execution_status_snapshot(datetime.now(timezone.utc), title="Bitunix Live Status")
            return True

        if self._looks_like_block_reasons_text(text):
            self._send_signal_debug_summary()
            return True

        if self._handle_private_todo_message(text):
            return True

        open_latest_pending_phrases = [
            "open last pending signal", "open latest pending signal", "open the latest pending signal",
            "open the last pending signal", "open last waiting signal", "open latest waiting signal"
        ]
        if self._intent_has_any_normalized(normalized, open_latest_pending_phrases):
            pending = self._recent_unexecuted_signals()
            if not pending:
                self._send_private_execution_answer("I do not see any pending tracked signal right now.")
                return True
            signal_id = pending[0].get("signal_id") or ((pending[0].get("meta") or {}).get("signal_id"))
            action = self._apply_context_to_action({"action": "open_signal", "signal_id": signal_id}, text)
            self._remember_exec_suggestion(action)
            _ask_to_confirm(action, chat_id, source_text=text)
            return True

        if self._intent_has_any_normalized(normalized, ["close all", "close everything"]) and self._intent_has_any_normalized(normalized, ["position", "trade", "everything"]):
            action = {"action": "close_all_positions"}
            if self._intent_has_phrase_in_normalized(normalized, "long"):
                action["side"] = "LONG"
            elif self._intent_has_phrase_in_normalized(normalized, "short"):
                action["side"] = "SHORT"
            action = self._apply_context_to_action(action, text)
            self._remember_exec_suggestion(action)
            _ask_to_confirm(action, chat_id, source_text=text)
            return True

        if ("cancel all" in lower or "remove all" in lower or "delete all" in lower) and any(word in lower for word in ["tp", "tps", "take profit", "take profits"]):
            action = {"action": "cancel_all_positions_tps"}
            if "long" in lower:
                action["side"] = "LONG"
            elif "short" in lower:
                action["side"] = "SHORT"
            action = self._apply_context_to_action(action, text)
            self._remember_exec_suggestion(action)
            _ask_to_confirm(action, chat_id, source_text=text)
            return True

        if "all" in lower and any(word in lower for word in ["breakeven", "break even"]) and any(word in lower for word in ["stop", "stops", "sl"]):
            action = {"action": "move_all_sl_entry"}
            if "long" in lower:
                action["side"] = "LONG"
            elif "short" in lower:
                action["side"] = "SHORT"
            action = self._apply_context_to_action(action, text)
            self._remember_exec_suggestion(action)
            _ask_to_confirm(action, chat_id, source_text=text)
            return True

        affirmative_only = {"yes", "y", "ok", "okay", "ok do", "okay do", "do it", "yes do", "do", "continue"}
        if lower in affirmative_only and not self.pending_exec_action:
            suggested = self._recent_exec_suggestion()
            if suggested:
                _ask_to_confirm(suggested, chat_id, source_text=text)
                return True
            self._send_private_execution_answer("IвЂ™m ready. Just tell me what you want me to do.")
            return True

        live_metric_words = ["roi", "pnl", "profit", "loss", "unrealized", "return"]
        if any(word in lower for word in live_metric_words):
            payload = self._apply_context_to_action({}, text)
            id_match = re.search(r"\b[a-f0-9]{8,}\b", lower)
            if id_match:
                payload["signal_id"] = id_match.group(0)
            for tf_candidate in ["5m", "15m", "1h", "4h"]:
                if tf_candidate in lower:
                    payload["tf"] = tf_candidate
                    break
            if "long" in lower:
                payload["side"] = "LONG"
            elif "short" in lower:
                payload["side"] = "SHORT"
            sig = self._resolve_signal_for_action(payload, allow_unexecuted=False)
            manual_price = self._extract_followup_price(text, reference=float((sig or {}).get("entry") or 0)) if sig else None
            metrics = self._position_live_metrics(sig, current_price_override=manual_price) if sig else None
            if sig and metrics:
                answer_lines = [
                    f"For your {str(sig.get('type') or 'position').lower()} {str(sig.get('side') or '').lower()} on {sig.get('tf') or 'N/A'}:",
                    f"Current price: {float(metrics.get('current_price') or 0):.2f}",
                    f"PnL: {float(metrics.get('pnl_usd') or 0):+.4f} USDT",
                    f"Estimated ROI: {float(metrics.get('roi_pct') or 0):+.2f}%",
                ]
                self._send_private_execution_answer("\n".join(answer_lines))
                return True
            if sig:
                self._send_private_execution_answer(
                    "I can calculate that, but I do not have a live market price loaded right now. "
                    "If you want, send me the current price and IвЂ™ll use it."
                )
                return True


        if self.pending_exec_action:
            pending_chat = str((self.pending_exec_action or {}).get("chat_id") or "")
            expired = (time.time() - float((self.pending_exec_action or {}).get("created_at") or 0)) > max(30, PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC)
            if expired:
                self.pending_exec_action = None
                self._save_state()
                self._send_private_execution_notice("Exec Control", ["Pending action expired. Send the request again."], icon="⚠️")
                return True
            if pending_chat == chat_id:
                pending_mode = str((self.pending_exec_action or {}).get("mode") or "confirm").lower()
                pending_action = dict((self.pending_exec_action or {}).get("action") or {})
                pending_source = str((self.pending_exec_action or {}).get("source_text") or "")
                locally_updated = self._apply_followup_to_pending_action(pending_action, text)
                local_changed = json.dumps(locally_updated, sort_keys=True, default=str) != json.dumps(pending_action, sort_keys=True, default=str)

                if pending_mode == "clarify":
                    if lower in {"no", "n", "cancel", "stop"}:
                        self.pending_exec_action = None
                        self._save_state()
                        self._send_private_execution_notice("Exec Control", ["Pending request cancelled."], icon="❌")
                        return True
                    if local_changed:
                        need_more = self._needs_exec_clarification(locally_updated)
                        if need_more:
                            self.pending_exec_action = {
                                "created_at": time.time(),
                                "chat_id": chat_id,
                                "action": locally_updated,
                                "mode": "clarify",
                                "source_text": pending_source or text,
                            }
                            self._save_state()
                            self._send_private_execution_notice("Exec Control", [need_more], icon="⚠️")
                            return True
                        if str(locally_updated.get("action") or "").lower() == "status":
                            self.pending_exec_action = None
                            self._save_state()
                            self._apply_private_exec_action(locally_updated)
                            return True
                        _ask_to_confirm(locally_updated, chat_id, source_text=pending_source or text)
                        return True

                    combined_text = f"{pending_source}\nUser follow-up: {text}".strip()
                    self.pending_exec_action = None
                    self._save_state()
                    if not GEMINI_API_KEY:
                        self._send_private_execution_notice("Exec Control", ["GEMINI_API_KEY is missing in .env."], icon="⚠️")
                        return True
                    parsed = parse_gemini_trade_instruction(
                        GEMINI_API_KEY,
                        GEMINI_MODEL,
                        combined_text,
                        self._build_gemini_trade_context(),
                    )
                    parsed = self._apply_context_to_action(parsed or {}, combined_text) if parsed else parsed
                    if not parsed:
                        answer = self._ask_private_chat_question(combined_text)
                        if answer and "supported action" not in answer.lower():
                            self._send_private_execution_answer(answer)
                        else:
                            self._send_private_execution_answer("IвЂ™m still not fully sure what you want me to do. Say it in a simpler way and IвЂ™ll help.")
                        return True
                    action_type = str(parsed.get("action") or "unsupported").lower()
                    if action_type == "unsupported":
                        answer = self._ask_private_chat_question(combined_text)
                        if answer and "supported action" not in answer.lower():
                            self._send_private_execution_answer(answer)
                        else:
                            self._send_private_execution_answer("IвЂ™m not fully sure what you mean yet. Say it a little more simply and IвЂ™ll help.")
                        return True
                    if action_type == "status":
                        self._apply_private_exec_action(parsed)
                        return True
                    need_more = self._needs_exec_clarification(parsed)
                    if need_more:
                        self.pending_exec_action = {
                            "created_at": time.time(),
                            "chat_id": chat_id,
                            "action": parsed,
                            "mode": "clarify",
                            "source_text": combined_text,
                        }
                        self._save_state()
                        self._send_private_execution_notice("Exec Control", [need_more], icon="⚠️")
                        return True
                    _ask_to_confirm(parsed, chat_id, source_text=combined_text)
                    return True

                if lower in {"yes", "y", "confirm", "do it", "execute"}:
                    action = self.pending_exec_action.get("action") or {}
                    self.pending_exec_action = None
                    self._save_state()
                    self._apply_private_exec_action(action)
                    return True
                if lower in {"no", "n", "cancel", "stop"}:
                    self.pending_exec_action = None
                    self._save_state()
                    self._send_private_execution_notice("Exec Control", ["Pending action cancelled."], icon="❌")
                    return True
                if pending_mode == "confirm" and local_changed:
                    need_more = self._needs_exec_clarification(locally_updated)
                    if need_more:
                        self.pending_exec_action = {
                            "created_at": time.time(),
                            "chat_id": chat_id,
                            "action": locally_updated,
                            "mode": "clarify",
                            "source_text": pending_source or text,
                        }
                        self._save_state()
                        self._send_private_execution_notice("Exec Control", [need_more], icon="⚠️")
                        return True
                    _ask_to_confirm(locally_updated, chat_id, source_text=pending_source or text)
                    return True

        direct_action = self._apply_context_to_action(self._infer_basic_exec_action(text) or {}, text)
        if direct_action and str(direct_action.get("action") or "").lower():
            direct_type = str(direct_action.get("action") or "").lower()
            if direct_type == "status":
                self.last_exec_suggested_action = None
                self._save_state()
                self._apply_private_exec_action(direct_action)
                return True
            need_more = self._needs_exec_clarification(direct_action)
            if need_more:
                self._remember_exec_suggestion(direct_action)
                self.pending_exec_action = {
                    "created_at": time.time(),
                    "chat_id": chat_id,
                    "action": direct_action,
                    "mode": "clarify",
                    "source_text": text,
                }
                self._save_state()
                self._send_private_execution_notice("Exec Control", [need_more], icon="⚠️")
                return True
            self._remember_exec_suggestion(direct_action)
            _ask_to_confirm(direct_action, chat_id, source_text=text)
            return True

        if not GEMINI_API_KEY:
            self._send_private_execution_notice("Exec Control", ["GEMINI_API_KEY is missing in .env."], icon="⚠️")
            return True

        parsed = parse_gemini_trade_instruction(
            GEMINI_API_KEY,
            GEMINI_MODEL,
            text,
            self._build_gemini_trade_context(),
        )
        parsed = self._apply_context_to_action(parsed or {}, text) if parsed else parsed
        if not parsed:
            answer = None
            try:
                answer = self._ask_private_chat_question(text)
            except Exception:
                answer = None
            if answer and "supported action" not in answer.lower():
                self._send_private_execution_answer(answer)
            else:
                self._send_private_execution_answer("I didnвЂ™t fully understand that yet, but IвЂ™m here with you. Try asking it in a simpler way and IвЂ™ll help.")
            return True

        action_type = str(parsed.get("action") or "unsupported").lower()
        if action_type == "unsupported":
            inferred = self._infer_exec_suggestion_from_text(text)
            if inferred:
                self._remember_exec_suggestion(inferred)
            answer = None
            try:
                answer = self._ask_private_chat_question(text)
            except Exception:
                answer = None
            if answer and "supported action" not in answer.lower():
                self._send_private_execution_answer(answer)
            else:
                self._send_private_execution_answer("IвЂ™m here рџ™‚ Ask me in your own words and IвЂ™ll do my best to help.")
            return True

        if action_type == "status":
            self.last_exec_suggested_action = None
            self._save_state()
            self._apply_private_exec_action(parsed)
            return True

        need_more = self._needs_exec_clarification(parsed)
        if need_more:
            self._remember_exec_suggestion(parsed)
            self.pending_exec_action = {
                "created_at": time.time(),
                "chat_id": chat_id,
                "action": parsed,
                "mode": "clarify",
                "source_text": text,
            }
            self._save_state()
            self._send_private_execution_notice("Exec Control", [need_more], icon="⚠️")
            return True

        self._remember_exec_suggestion(parsed)
        _ask_to_confirm(parsed, chat_id, source_text=text)
        return True

    def _format_balance_line(self, trade_check):
        total = float(trade_check.get("balance_total", 0) or 0)
        free = float(trade_check.get("balance_available", 0) or 0)
        used = float(trade_check.get("balance_used", 0) or 0)
        if total <= 0 and free > 0:
            total = free
        if used <= 0 and total > 0:
            used = max(0.0, total - free)
        return f"Your balance is {total:.2f} total, with {free:.2f} free and {used:.2f} tied up in positions."

    def _looks_like_balance_only_text(self, text):
        normalized = self._normalize_intent_text(text)
        has_balance_signal = self._intent_has_any_normalized(normalized, [
            "balance", "wallet balance", "equity", "wallet", "how much money do i have"
        ])
        wants_full_status = self._intent_has_any_normalized(normalized, [
            "account status", "live status", "startup check", "balance summary",
            "wallet information", "account information", "account info", "full status", "summary"
        ])
        return has_balance_signal and not wants_full_status

    def _looks_like_open_positions_text(self, text):
        normalized = self._normalize_intent_text(text)
        return self._intent_has_any_normalized(normalized, [
            "open positions", "open position", "do i have any open", "which position is open",
            "which positions are open", "show my positions", "show my position", "list positions", "list position"
        ]) or (
            self._intent_has_any_normalized(normalized, ["position", "trade"]) and
            self._intent_has_any_normalized(normalized, ["open", "active"])
        )

    def _looks_like_account_mode_text(self, text):
        normalized = self._normalize_intent_text(text)
        return self._intent_has_any_normalized(normalized, [
            "margin mode", "what mode", "what is my mode", "leverage", "what leverage",
            "account mode", "cross mode", "isolated mode", "cross or isolated"
        ]) and not self._intent_has_any_normalized(normalized, ["position mode"])

    def _looks_like_account_status_text(self, text):
        normalized = self._normalize_intent_text(text)
        return self._intent_has_any_normalized(normalized, [
            "account status", "show account status", "show live status",
            "balance summary", "show balance summary", "startup check",
            "wallet information", "account information", "account info",
            "full status", "full account", "full snapshot", "live snapshot"
        ]) or self._intent_has_all(normalized, [("account", "wallet"), ("status", "summary", "snapshot")])

    def _format_news_status_line(self, now):
        if not NEWS_FILTER_ENABLED:
            return "The news filter is turned off right now."
        news_blackout = self._get_active_news_block(now)
        if news_blackout:
            label = str(news_blackout.get("label") or "High Impact News")
            source = str(news_blackout.get("source") or "manual")
            end = news_blackout.get("end")
            end_txt = end.astimezone(timezone.utc).strftime("%H:%M UTC") if isinstance(end, datetime) else "active"
            return f"There is an active news block for {label} from {source} until {end_txt}."
        if TRADING_ECONOMICS_NEWS_ENABLED:
            self._refresh_live_news_events(now)
            upcoming = [event for event in self.live_news_events if event.get("datetime") and event["datetime"] >= now]
            if upcoming:
                next_event = upcoming[0]
                event_dt = next_event["datetime"].astimezone(timezone.utc).strftime("%d %b %H:%M UTC")
                return f"The next high-impact US event is {next_event['event']} at {event_dt}."
            self._refresh_markettwits_events(now)
            if self.markettwits_events:
                latest = self.markettwits_events[0]
                event_dt = latest["datetime"].astimezone(timezone.utc).strftime("%d %b %H:%M UTC")
                return f"The latest MarketTwits headline shock was '{latest['event']}' at {event_dt}."
            if self.last_live_news_error:
                return f"The live news feed is having an issue right now: {self.last_live_news_error}."
            if self.last_markettwits_error:
                return f"MarketTwits is quiet for BTC right now: {self.last_markettwits_error}"
            return "There are no upcoming high-impact US events right now."
        manual = get_active_news_blackout(now)
        if manual:
            return f"A manual news block is active for {manual.get('label', 'active')}."
        return "Only the manual news blackout list is active right now."

    def _build_execution_status_lines(self, trade_check=None, reconcile=None, now=None):
        now = now or datetime.now(timezone.utc)
        trade_check = trade_check or self.trade_executor.startup_self_check()
        reconcile = reconcile or self.trade_executor.reconcile_execution_state(self.tracker.signals)
        lines = [
            f"The bot is running in {trade_check.get('mode')} mode on {SYMBOL}, and Bitunix auth is {'OK' if trade_check.get('auth_ok') else 'failing'}.",
            self._format_balance_line(trade_check),
            f"Your account is in {trade_check.get('margin_mode') or 'N/A'} mode at {int(trade_check.get('leverage', 0) or 0)}x, with {trade_check.get('position_mode') or BITUNIX_POSITION_MODE or 'UNKNOWN'} position mode.",
            f"You currently have {int(trade_check.get('open_positions', 0) or 0)} open position(s).",
        ]
        matched = int(reconcile.get("matched", 0) or 0)
        orphan_count = len(reconcile.get("orphan_positions", []) or [])
        missing_protection = len(reconcile.get("missing_protection", []) or [])
        if matched or orphan_count or missing_protection:
            lines.append(
                f"The tracker sees {matched} matched position(s), {orphan_count} orphan position(s), and {missing_protection} position(s) missing protection."
            )
        lines.append(self._format_news_status_line(now))

        errors = []
        errors.extend([str(err) for err in (trade_check.get("errors") or [])[:1]])
        errors.extend([str(err) for err in (reconcile.get("errors") or [])[:1]])
        if errors:
            lines.extend(errors[:2])
        return lines

    def _build_trade_check_bundle(self, now=None):
        now = now or datetime.now(timezone.utc)
        trade_check = self.trade_executor.startup_self_check()
        reconcile = self.trade_executor.reconcile_execution_state(self.tracker.signals)
        if reconcile.get("inactive_marked") or reconcile.get("state_updated"):
            self.tracker.persist()
            self._cleanup_finished_active_trade_cards()
        return trade_check, reconcile, now

    def _send_simple_balance_answer(self):
        trade_check, _, _ = self._build_trade_check_bundle()
        balance_line = self._format_balance_line(trade_check)
        self._send_private_execution_answer(balance_line)

    def _send_simple_open_positions_answer(self):
        trade_check, _, _ = self._build_trade_check_bundle()
        open_positions = int(trade_check.get("open_positions", 0) or 0)
        if open_positions <= 0:
            self._send_private_execution_answer("You do not have any open positions right now.")
            return
        self._send_private_execution_answer(
            f"You currently have {open_positions} open position{'s' if open_positions != 1 else ''}."
        )

    def _send_simple_account_mode_answer(self):
        trade_check, _, _ = self._build_trade_check_bundle()
        margin_mode = str(trade_check.get("margin_mode") or "N/A")
        position_mode = str(trade_check.get("position_mode") or BITUNIX_POSITION_MODE or "UNKNOWN")
        leverage = int(trade_check.get("leverage", 0) or 0)
        self._send_private_execution_answer(
            f"Your account is in {margin_mode} mode at {leverage}x, with {position_mode} position mode right now."
        )

    def _send_today_trade_count_answer(self):
        now_utc = datetime.now(timezone.utc)
        opened_today = int(self.tracker.count_signals_for_day(now_utc=now_utc) or 0)
        still_open_today = 0
        closed_today = 0
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        for sig in self.tracker.signals:
            logged_at = self.tracker._parse_iso_utc(sig.get("logged_at"))
            if not logged_at or logged_at < day_start or logged_at > now_utc:
                continue
            if str(sig.get("status") or "").upper() == "OPEN":
                still_open_today += 1
            else:
                closed_today += 1
        self._send_private_execution_answer(
            f"Today in UTC, you opened {opened_today} trade{'s' if opened_today != 1 else ''}. "
            f"{still_open_today} {'are' if still_open_today != 1 else 'is'} still open, "
            f"and {closed_today} {'have' if closed_today != 1 else 'has'} already closed."
        )

    def _daily_trade_metrics(self, now_utc=None):
        now_utc = now_utc.astimezone(timezone.utc) if now_utc else datetime.now(timezone.utc)
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        opened_today = int(self.tracker.count_signals_for_day(now_utc=now_utc) or 0)
        closed_today = 0
        wins = 0
        losses = 0
        breakeven = 0
        realized_r = 0.0
        realized_est_usd = 0.0
        estimated_closed_count = 0
        missing_risk_count = 0

        for sig in self.tracker.signals:
            closed_at = self.tracker._parse_iso_utc(sig.get("closed_at"))
            if not closed_at or closed_at < day_start or closed_at > now_utc:
                continue
            closed_today += 1
            outcome, r_mult = self.tracker._metric_outcome(sig)
            realized_r += float(r_mult or 0.0)
            if outcome == "wins":
                wins += 1
            elif outcome == "losses":
                losses += 1
            elif outcome == "breakeven":
                breakeven += 1
            execution = sig.get("execution") or {}
            try:
                risk_budget_usd = float(execution.get("risk_budget_usd") or 0)
            except Exception:
                risk_budget_usd = 0.0
            if risk_budget_usd > 0:
                realized_est_usd += float(r_mult or 0.0) * risk_budget_usd
                estimated_closed_count += 1
            else:
                missing_risk_count += 1

        open_unrealized_usd = 0.0
        active_positions = 0
        for sig in self._active_execution_signals():
            metrics = self._position_live_metrics(sig)
            if not metrics:
                continue
            open_unrealized_usd += float(metrics.get("pnl_usd") or 0.0)
            active_positions += 1

        return {
            "opened_today": opened_today,
            "closed_today": closed_today,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "realized_r": realized_r,
            "realized_est_usd": realized_est_usd,
            "estimated_closed_count": estimated_closed_count,
            "missing_risk_count": missing_risk_count,
            "open_unrealized_usd": open_unrealized_usd,
            "active_positions": active_positions,
        }

    def _send_today_pnl_answer(self):
        now_utc = datetime.now(timezone.utc)
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        exchange_history = self._get_live_history_snapshot(start_dt=day_start, end_dt=now_utc)
        history_summary = exchange_history.get("summary") or {}
        stats = self._daily_trade_metrics(now_utc)
        opened_today = int(stats.get("opened_today", 0) or 0)
        closed_today = int(stats.get("closed_today", 0) or 0)
        active_positions = int(stats.get("active_positions", 0) or 0)
        if opened_today <= 0 and closed_today <= 0 and active_positions <= 0:
            self._send_private_execution_answer("So far today in UTC, I do not see any trades or open positions, so there is no gain or loss to report.")
            return

        parts = []
        exchange_realized = float(history_summary.get("positions_realized_pnl", 0.0) or 0.0)
        exchange_fees = float(history_summary.get("positions_fees", 0.0) or 0.0)
        exchange_funding = float(history_summary.get("positions_funding", 0.0) or 0.0)
        exchange_positions_total = int(history_summary.get("positions_total", 0) or 0)
        if exchange_positions_total > 0:
            net_exchange = exchange_realized - abs(exchange_fees) + exchange_funding
            parts.append(
                f"So far today in UTC, Bitunix history shows about {net_exchange:+.4f} USDT net on {exchange_positions_total} closed position{'s' if exchange_positions_total != 1 else ''}."
            )
            if abs(exchange_realized) > 0 or abs(exchange_fees) > 0 or abs(exchange_funding) > 0:
                parts.append(
                    f"That comes from realized PnL {exchange_realized:+.4f}, fees {exchange_fees:+.4f}, and funding {exchange_funding:+.4f}."
                )
        if closed_today > 0:
            realized_r = float(stats.get("realized_r", 0.0) or 0.0)
            est_closed = int(stats.get("estimated_closed_count", 0) or 0)
            est_usd = float(stats.get("realized_est_usd", 0.0) or 0.0)
            wins = int(stats.get("wins", 0) or 0)
            losses = int(stats.get("losses", 0) or 0)
            breakeven = int(stats.get("breakeven", 0) or 0)
            if exchange_positions_total <= 0 and est_closed > 0:
                parts.append(
                    f"So far today in UTC, your closed trades are about {est_usd:+.4f} USDT, which is {realized_r:+.2f}R across {closed_today} closed trade{'s' if closed_today != 1 else ''}."
                )
            elif exchange_positions_total <= 0:
                parts.append(
                    f"So far today in UTC, your closed trades are {realized_r:+.2f}R across {closed_today} closed trade{'s' if closed_today != 1 else ''}."
                )
            parts.append(
                f"That includes {wins} win{'s' if wins != 1 else ''}, {losses} loss{'es' if losses != 1 else ''}, and {breakeven} breakeven trade{'s' if breakeven != 1 else ''}."
            )
        else:
            parts.append("You have not closed any trades yet today in UTC.")

        if active_positions > 0:
            parts.append(
                f"Right now your open position{'s' if active_positions != 1 else ''} are showing {float(stats.get('open_unrealized_usd', 0.0) or 0.0):+.4f} USDT unrealized."
            )

        if int(stats.get("missing_risk_count", 0) or 0) > 0:
            parts.append("For some older trades I can estimate today's result in R more reliably than in exact USD.")

        self._send_private_execution_answer(" ".join(parts))

    def _send_execution_status_snapshot(self, now=None, title="Bitunix Noon Check"):
        if not self._execution_chat_id():
            return
        trade_check, reconcile, now = self._build_trade_check_bundle(now)
        lines = self._build_execution_status_lines(trade_check=trade_check, reconcile=reconcile, now=now)
        self._send_private_execution_notice(title, lines)

    def _send_pending_signals_snapshot(self, title="Pending Signals"):
        pending = self._recent_unexecuted_signals()
        if not pending:
            self._send_private_execution_answer("You do not have any pending tracked signals right now.")
            return
        blocks = [f"I found {len(pending)} pending signal{'s' if len(pending) != 1 else ''}."]
        for sig in pending[:10]:
            signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or "N/A"
            symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL).upper()
            detail_lines = [
                f"Type: {sig.get('type', 'SCALP')} {sig.get('side', 'N/A')} [{sig.get('tf', 'N/A')}] {symbol}",
                f"Entry: {float(sig.get('entry', 0) or 0):.2f}",
                f"SL: {float(sig.get('sl', 0) or 0):.2f}",
                f"TPs: {float(sig.get('tp1', 0) or 0):.2f} / {float(sig.get('tp2', 0) or 0):.2f} / {float(sig.get('tp3', 0) or 0):.2f}",
            ]
            blocks.append(
                f"\n\nThis signal is waiting right now.\n"
                f"Here is the signal ID:\n<pre>{signal_id}</pre>\n"
                f"<pre>{chr(10).join(detail_lines)}</pre>"
            )
        self._send_private_execution_answer("".join(blocks))

    def _send_positions_performance_snapshot(self, title="Position Performance"):
        self._refresh_private_execution_state()
        active = self._active_execution_signals()
        if not active:
            self._send_private_execution_answer("You do not have any active exchange positions right now.")
            return

        total_pnl = 0.0
        roi_values = []
        blocks = ["Here is how your open positions are doing right now."]
        for sig in active[:10]:
            signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (((sig.get("execution") or {}).get("signal_id"))) or "N/A"
            symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or ((sig.get("execution") or {}).get("symbol")) or SYMBOL).upper()
            metrics = self._position_live_metrics(sig)
            if metrics:
                total_pnl += float(metrics.get("pnl_usd") or 0.0)
                if metrics.get("roi_pct") is not None:
                    roi_values.append(float(metrics.get("roi_pct")))
                detail_lines = [
                    f"Type: {sig.get('type', 'SCALP')} {sig.get('side', 'N/A')} [{sig.get('tf', 'N/A')}] {symbol}",
                    f"Entry: {float(sig.get('entry', 0) or 0):.2f}",
                    f"Current: {float(metrics.get('current_price') or 0):.2f}",
                    f"PnL: {float(metrics.get('pnl_usd') or 0):+.4f} USDT",
                    f"ROI: {float(metrics.get('roi_pct') or 0):+.2f}%",
                ]
            else:
                detail_lines = [
                    f"Type: {sig.get('type', 'SCALP')} {sig.get('side', 'N/A')} [{sig.get('tf', 'N/A')}] {symbol}",
                    "Live price is unavailable right now.",
                ]
            blocks.append(
                f"\n\nThis is one of your open positions.\n"
                f"Here is the position ID:\n<pre>{signal_id}</pre>\n"
                f"<pre>{chr(10).join(detail_lines)}</pre>"
            )

        summary_lines = [f"Open positions: {len(active)}", f"Total PnL: {total_pnl:+.4f} USDT"]
        if roi_values:
            summary_lines.append(f"Average ROI: {sum(roi_values) / len(roi_values):+.2f}%")
        blocks.insert(1, f"\n<pre>{chr(10).join(summary_lines)}</pre>")
        self._send_private_execution_answer("".join(blocks))

    def _send_open_positions_snapshot(self, title="Open Positions"):
        self._refresh_private_execution_state()
        active = self._active_execution_signals()
        if not active:
            self._send_private_execution_answer("You do not have any active exchange positions right now.")
            return
        blocks = [f"You currently have {len(active)} open position{'s' if len(active) != 1 else ''}."]
        for sig in active[:10]:
            execution = sig.get("execution") or {}
            signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (execution.get("signal_id"))
            sl_text = self._format_live_sl_value(sig)
            symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or execution.get("symbol") or SYMBOL).upper()
            header_block = f"{sig.get('type', 'SCALP')} {sig.get('side', 'N/A')} [{sig.get('tf', 'N/A')}] {symbol}"
            detail_lines = [
                f"Entry: {float(sig.get('entry', 0) or 0):.2f}",
                f"SL: {sl_text}",
                self._format_active_tp_line(sig),
                f"Qty: {float(execution.get('qty', 0) or 0):.6f}",
            ]
            block = (
                f"\n\nThis position is currently open.\n"
                f"<pre>{header_block}</pre>\n"
                f"Here is the position ID:\n<pre>{signal_id or 'N/A'}</pre>\n"
                f"<pre>{chr(10).join(detail_lines)}</pre>"
            )
            blocks.append(block)
        self._send_private_execution_answer("".join(blocks))

    def _send_single_position_snapshot(self, sig, title="Position Info"):
        self._refresh_private_execution_state()
        sig = sig or {}
        execution = sig.get("execution") or {}
        signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or execution.get("signal_id")
        side = str(sig.get("side") or "N/A")
        sig_type = str(sig.get("type") or "Signal")
        tf = str(sig.get("tf") or "N/A")
        symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or execution.get("symbol") or SYMBOL).upper()
        status = str(sig.get("status") or "OPEN").upper()
        sl_text = self._format_live_sl_value(sig)
        details_block = "\n".join([
            f"Symbol: {symbol}",
            f"Status: {status}",
            f"Entry: {float(sig.get('entry', 0) or 0):.2f}",
            f"SL: {sl_text}",
            self._format_active_tp_line(sig),
            f"Qty: {float(execution.get('qty', 0) or 0):.6f}",
        ])
        answer = (
            f"I found the position you asked about.\n"
            f"It is your {sig_type} {side} on {tf} for {symbol}.\n"
            f"Here is the position ID:\n<pre>{signal_id or 'N/A'}</pre>\n"
            f"<pre>{details_block}</pre>"
        )
        self._send_private_execution_answer(answer)

    def _format_live_sl_value(self, sig):
        execution = (sig or {}).get("execution") or {}
        if execution.get("active"):
            if not (execution.get("sl_order") or {}):
                return "none"
            moved = execution.get("sl_moved_to")
            try:
                moved_price = float(moved or 0)
            except Exception:
                moved_price = 0.0
            if moved_price > 0:
                return f"{moved_price:.2f}"
        try:
            sl_price = float((sig or {}).get("sl") or 0)
        except Exception:
            sl_price = 0.0
        return f"{sl_price:.2f}" if sl_price > 0 else "none"

    def _single_active_tp_index(self, sig):
        execution = (sig or {}).get("execution") or {}
        qtys = list(execution.get("tp_qtys") or [])
        active = []
        for idx, qty in enumerate(qtys[:3], start=1):
            try:
                if float(qty or 0) > 0:
                    active.append(idx)
            except Exception:
                pass
        return active[0] if len(active) == 1 else None

    def _is_single_full_tp_execution(self, sig):
        execution = (sig or {}).get("execution") or {}
        qtys = list(execution.get("tp_qtys") or [])
        total_qty = float(execution.get("qty", 0) or 0)
        active = [float(q or 0) for q in qtys[:3] if float(q or 0) > 0]
        if len(active) != 1 or total_qty <= 0:
            return False
        return abs(active[0] - total_qty) <= 1e-9

    def _format_active_tp_line(self, sig):
        execution = (sig or {}).get("execution") or {}
        tp_qtys = list(execution.get("tp_qtys") or [])
        tp_targets = list(execution.get("tp_targets") or [sig.get("tp1"), sig.get("tp2"), sig.get("tp3")])
        active_parts = []
        for idx, qty in enumerate(tp_qtys[:3], start=1):
            try:
                qty_val = float(qty or 0)
            except Exception:
                qty_val = 0.0
            if qty_val <= 0:
                continue
            target = float(tp_targets[idx - 1] or sig.get(f"tp{idx}") or 0)
            if target > 0:
                if self._is_single_full_tp_execution(sig):
                    active_parts.append(f"TP {target:.2f}")
                else:
                    active_parts.append(f"TP{idx} {target:.2f}")
        if active_parts:
            label = "Active TP" if len(active_parts) == 1 else "Active TPs"
            return f"{label}: " + " / ".join(active_parts)
        if execution.get("active") and (execution.get("tp_orders") is not None or tp_qtys):
            return "Active TPs: none"
        return (
            f"TPs: {float(sig.get('tp1', 0) or 0):.2f} / "
            f"{float(sig.get('tp2', 0) or 0):.2f} / "
            f"{float(sig.get('tp3', 0) or 0):.2f}"
        )

    def _format_execution_lines(self, sig=None, extra=None):
        sig = sig or {}
        lines = []
        tf = sig.get("tf")
        sig_type = sig.get("type")
        side = sig.get("side")
        signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (((sig.get("execution") or {}).get("signal_id")))
        symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or (sig.get("execution") or {}).get("symbol") or SYMBOL).upper()
        execution = sig.get("execution") or {}
        side_emoji = "🟢" if str(side or "").upper() == "LONG" else ("🔴" if str(side or "").upper() == "SHORT" else "⚪")
        if signal_id:
            lines.append("Signal ID:")
            lines.append(f"<pre>{signal_id}</pre>")
        if execution.get("position_id"):
            lines.append("Bitunix Position ID:")
            lines.append(f"<pre>{execution.get('position_id')}</pre>")
        detail_lines = []
        if tf or sig_type or side:
            detail_lines.append(f"{side_emoji} {sig_type or 'Signal'} {side or 'N/A'} [{tf or 'N/A'}] {symbol}")
        if sig.get("entry") is not None:
            detail_lines.append(f"Entry: {float(sig.get('entry') or 0):.2f}")
            detail_lines.append(f"SL: {self._format_live_sl_value(sig)}")
            detail_lines.append(self._format_active_tp_line(sig))
        if detail_lines:
            lines.append("<pre>" + "\n".join(detail_lines) + "</pre>")
        if extra:
            lines.extend([str(line) for line in extra if line is not None and str(line).strip() and str(line).strip() != "None"])
        return lines

    def _build_private_execution_update_html(self, sig, event_type, result_message=None):
        sig = sig or {}
        side = str(sig.get("side") or "N/A").upper()
        tf = str(sig.get("tf") or "N/A")
        trade_name = f"{side} trade on {tf}"
        event_key = str(event_type or "").upper()
        result_text = str(result_message or "").strip()
        is_single_full = self._is_single_full_tp_execution(sig)

        if event_key == "TP1":
            if is_single_full or "closed the full position" in result_text.lower():
                intro = f"I closed the full {trade_name} at take profit."
            else:
                intro = f"I took the first target on this {trade_name}."
        elif event_key == "TP2":
            if "closed the full position" in result_text.lower() or is_single_full:
                intro = f"I closed the full {trade_name} at take profit."
            else:
                intro = f"I took the second target on this {trade_name} and moved the stop to protected breakeven."
        elif event_key == "TP3":
            intro = f"This {trade_name} hit the final target and is fully closed."
        elif event_key == "ENTRY_CLOSE":
            intro = f"This {trade_name} closed at breakeven after protection was on."
        elif event_key == "PROFIT_SL":
            intro = f"This {trade_name} closed on the protected stop in profit."
        elif event_key == "SL":
            intro = f"This {trade_name} was closed by the stop loss."
        else:
            intro = f"I updated this {trade_name}."

        if event_key in {"TP1", "TP2"} and is_single_full:
            intro = f"I closed the full {trade_name} at take profit."

        detail_lines = self._format_execution_lines(
            sig,
            extra=[
                f"The new stop is {float((sig.get('execution') or {}).get('sl_moved_to') or 0):.2f}."
                if event_key in {"TP2", "PROFIT_SL", "ENTRY_CLOSE"} and not is_single_full and (sig.get("execution") or {}).get("sl_moved_to") is not None
                else None
            ],
        )
        rendered = [html.escape(intro)]
        if detail_lines:
            rendered.append("")
            rendered.extend(detail_lines)
        return "\n".join(part for part in rendered if part is not None and str(part).strip() != "")

    def _session_is_tradeable_today(self, session_name, now):
        if session_name == "NY" and is_ny_market_holiday(now):
            return False
        return True

    def _parse_te_datetime(self, value):
        if not value:
            return None
        raw = str(value).strip()
        for candidate in (raw, raw.replace("Z", "+00:00")):
            try:
                dt = datetime.fromisoformat(candidate)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
        return None

    def _score_markettwits_post(self, text):
        raw = str(text or "").strip()
        lower = raw.lower()
        if not lower:
            return None

        score = 0
        reasons = []

        def hit(words, points, label):
            nonlocal score
            if any(word in lower for word in words):
                score += points
                reasons.append(label)
                return True
            return False

        macro_hit = False
        macro_hit |= hit(["fomc", "federal reserve", "fed ", "powell", "ставк", "ставка", "фрс"], 4, "fed")
        macro_hit |= hit(["cpi", "инфляц", "pce", "ppi", "nfp", "payroll", "безработ", "jobless", "pmi", "gdp"], 4, "macro")

        geo_hit = False
        geo_hit |= hit(["иран", "iran", "израил", "israel", "геополит", "geopolit", "война", "war"], 3, "geopolitics")
        geo_hit |= hit(["ормуз", "hormuz", "oil", "нефть", "missile", "ракет", "удар", "attack", "санкц", "sanction"], 3, "oil-geo")

        crypto_hit = False
        crypto_hit |= hit(["bitcoin", "btc", "биткоин", "crypto", "крипт"], 2, "crypto")
        crypto_hit |= hit(["sec", "etf", "binance", "coinbase", "stablecoin", "usdt", "usdc"], 3, "crypto-market")

        policy_hit = hit(["tariff", "тариф", "трамп", "trump", "ultimatum", "ультимат"], 2, "policy")
        direct_btc_link = any(word in lower for word in [
            "bitcoin", "btc", "биткоин", "crypto", "крипт", "etf", "sec", "binance", "coinbase", "stablecoin"
        ])
        direct_macro_link = any(word in lower for word in [
            "fomc", "fed", "powell", "cpi", "pce", "ppi", "nfp", "jobless", "payroll", "ставка", "фрс", "инфляц"
        ])
        direct_shock_link = any(word in lower for word in [
            "oil", "нефть", "ormuz", "ормуз", "missile", "ракет", "attack", "удар", "sanction", "санкц",
            "war", "война", "iran", "иран", "israel", "израил"
        ])
        generic_macro_noise = any(word in lower for word in [
            "кризис", "crisis", "энергоресурс", "энергетическ", "поставк", "переговор", "серб", "венгр"
        ])
        if generic_macro_noise and not (direct_btc_link or direct_macro_link or direct_shock_link):
            return None

        if ("calendar" in lower or "календарь" in lower) and (direct_macro_link or direct_shock_link):
            score += 1
            reasons.append("calendar")

        impactful = False
        if macro_hit:
            impactful = True
        elif geo_hit and (direct_btc_link or "рын" in lower or "market" in lower or "oil" in lower or "нефть" in lower):
            impactful = True
        elif geo_hit and policy_hit:
            impactful = True
        elif crypto_hit and score >= 5:
            impactful = True
        elif score >= 6:
            impactful = True

        if not impactful:
            return None
        if score < int(MARKETTWITS_MIN_BLOCK_SCORE):
            return None

        headline = raw.splitlines()[0].strip()
        headline = re.sub(r"\s+", " ", headline)
        return {
            "score": score,
            "reasons": reasons[:4],
            "headline": headline[:180],
        }

    def _refresh_markettwits_events(self, now):
        if not MARKETTWITS_NEWS_ENABLED or not NEWS_FILTER_ENABLED:
            return
        current_ts = time.time()
        if self.markettwits_events and (current_ts - self.last_markettwits_refresh) < max(30, MARKETTWITS_REFRESH_SEC):
            return
        posts = fetch_markettwits_posts(MARKETTWITS_CHANNEL_URL, limit=30)
        parsed = []
        cutoff = (now - timedelta(hours=max(1, MARKETTWITS_LOOKBACK_HOURS))).astimezone(timezone.utc)
        for post in posts or []:
            event_dt = self._parse_te_datetime(post.get("datetime"))
            if not event_dt or event_dt < cutoff:
                continue
            scored = self._score_markettwits_post(post.get("text"))
            if not scored:
                continue
            parsed.append({
                "id": str(post.get("id") or "").strip(),
                "datetime": event_dt,
                "event": scored["headline"] or "MarketTwits headline shock",
                "score": int(scored["score"]),
                "reasons": list(scored["reasons"]),
                "url": str(post.get("url") or "").strip(),
                "source": "MARKETTWITS",
            })
        self.markettwits_events = sorted(parsed, key=lambda row: row["datetime"], reverse=True)
        self.last_markettwits_refresh = current_ts
        if parsed:
            self.last_markettwits_error = None
        elif posts:
            self.last_markettwits_error = "Feed loaded, but there were no recent BTC-relevant headlines strong enough to keep."
        else:
            self.last_markettwits_error = "No recent public posts were returned from the channel page."

    def _get_markettwits_blackout(self, now):
        if not MARKETTWITS_NEWS_ENABLED or not NEWS_FILTER_ENABLED:
            return None
        self._refresh_markettwits_events(now)
        now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
        block_after = timedelta(minutes=max(1, MARKETTWITS_BLOCK_AFTER_MIN))
        for event in self.markettwits_events:
            start = event["datetime"]
            end = event["datetime"] + block_after
            if start <= now_utc <= end:
                return {
                    "source": "MARKETTWITS",
                    "label": event["event"],
                    "start": start,
                    "end": end,
                    "event_time": event["datetime"],
                    "score": event.get("score"),
                    "url": event.get("url"),
                }
        return None

    def _refresh_live_news_events(self, now):
        if not TRADING_ECONOMICS_NEWS_ENABLED or not NEWS_FILTER_ENABLED:
            return
        current_ts = time.time()
        if self.live_news_events and (current_ts - self.last_live_news_refresh) < max(60, TRADING_ECONOMICS_REFRESH_SEC):
            return
        events = fetch_trading_economics_calendar(
            api_key=TRADING_ECONOMICS_API_KEY,
            countries=TRADING_ECONOMICS_COUNTRIES,
            importance=TRADING_ECONOMICS_MIN_IMPORTANCE,
        )
        parsed = []
        for item in events or []:
            event_dt = (
                self._parse_te_datetime(item.get("DateUtc"))
                or self._parse_te_datetime(item.get("date"))
                or self._parse_te_datetime(item.get("Date"))
            )
            if not event_dt:
                continue
            parsed.append({
                "datetime": event_dt,
                "event": str(item.get("Event") or item.get("event") or item.get("Category") or "High Impact News"),
                "country": str(item.get("Country") or item.get("country") or ""),
                "importance": int(item.get("Importance") or item.get("importance") or TRADING_ECONOMICS_MIN_IMPORTANCE),
            })
        self.live_news_events = sorted(parsed, key=lambda row: row["datetime"])
        self.last_live_news_refresh = current_ts
        self.last_live_news_error = None if parsed or events == [] else "No parsable events returned."

    def _get_live_news_blackout(self, now):
        if not TRADING_ECONOMICS_NEWS_ENABLED or not NEWS_FILTER_ENABLED:
            return None
        self._refresh_live_news_events(now)
        before = timedelta(minutes=max(0, TRADING_ECONOMICS_BLOCK_BEFORE_MIN))
        after = timedelta(minutes=max(0, TRADING_ECONOMICS_BLOCK_AFTER_MIN))
        now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
        for event in self.live_news_events:
            start = event["datetime"] - before
            end = event["datetime"] + after
            if start <= now_utc <= end:
                return {
                    "source": "TRADING_ECONOMICS",
                    "label": event["event"],
                    "country": event["country"],
                    "importance": event["importance"],
                    "start": start,
                    "end": end,
                    "event_time": event["datetime"],
                }
        return None

    def _get_active_news_block(self, now):
        manual = get_active_news_blackout(now)
        if manual:
            manual = dict(manual)
            manual["source"] = "MANUAL"
            return manual
        live_block = self._get_live_news_blackout(now)
        if live_block:
            return live_block
        return self._get_markettwits_blackout(now)

    def _get_5m_strict_news_block(self, now):
        if not NEWS_FILTER_ENABLED or not FIVE_MIN_STRICT_NEWS_FILTER:
            return None
        manual = get_active_news_blackout(now)
        if manual:
            manual = dict(manual)
            manual["source"] = "MANUAL"
            return manual
        if TRADING_ECONOMICS_NEWS_ENABLED:
            self._refresh_live_news_events(now)
            before = timedelta(minutes=max(0, FIVE_MIN_NEWS_BLOCK_BEFORE_MIN))
            after = timedelta(minutes=max(0, FIVE_MIN_NEWS_BLOCK_AFTER_MIN))
            now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
            for event in self.live_news_events:
                start = event["datetime"] - before
                end = event["datetime"] + after
                if start <= now_utc <= end:
                    return {
                        "source": "TRADING_ECONOMICS_5M",
                        "label": event["event"],
                        "country": event["country"],
                        "importance": event["importance"],
                        "start": start,
                        "end": end,
                        "event_time": event["datetime"],
                    }
        markettwits_block = self._get_markettwits_blackout(now)
        if markettwits_block:
            mt_block = dict(markettwits_block)
            mt_block["source"] = "MARKETTWITS_5M"
            return mt_block
        return None

    def _get_5m_higher_tf_guard_reason(self, side):
        if not FIVE_MIN_REQUIRE_15M_PERMISSION:
            return None
        df_15m = self.latest_data.get("15m")
        try:
            if df_15m is None or df_15m.empty or len(df_15m) < 3:
                return None
        except Exception:
            return None

        curr = df_15m.iloc[-1]
        prev = df_15m.iloc[-2]
        rsi_now = float(curr.get("RSI", 50) or 50)
        rsi_prev = float(prev.get("RSI", rsi_now) or rsi_now)
        smooth_now = float(curr.get("MomentumSmooth", rsi_now) or rsi_now)
        smooth_prev = float(prev.get("MomentumSmooth", smooth_now) or smooth_now)
        zone_15m = classify_momentum_zone(smooth_now, "15m")
        trend_15m = (getattr(self, "tf_trends", {}) or {}).get("15m") or detect_trend(df_15m)
        trend_side_15m = self._trend_side(trend_15m)
        side = str(side or "").upper()

        if side == "LONG":
            if trend_side_15m == "SHORT" and rsi_now < 52 and smooth_now <= smooth_prev:
                return f"15m trend is still bearish and RSI is not recovering yet ({rsi_now:.1f})"
            if zone_15m != "OS" and rsi_now < 50 and rsi_now <= rsi_prev and smooth_now <= smooth_prev:
                return f"15m RSI still has room to fall ({rsi_now:.1f})"
        elif side == "SHORT":
            if trend_side_15m == "LONG" and rsi_now > 48 and smooth_now >= smooth_prev:
                return f"15m trend is still bullish and RSI is not rolling over yet ({rsi_now:.1f})"
            if zone_15m != "OB" and rsi_now > 50 and rsi_now >= rsi_prev and smooth_now >= smooth_prev:
                return f"15m RSI still has room to rise ({rsi_now:.1f})"
        return None

    def _get_current_session_name(self, now):
        """Return active session name (ASIA/LONDON/NY) or None."""
        sessions = get_adjusted_sessions(now)
        current_float_hour = now.hour + now.minute / 60.0
        for s_name, s_times in sessions.items():
            if not self._session_is_tradeable_today(s_name, now):
                continue
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

    def _trend_side(self, trend_name):
        t = str(trend_name or "").lower()
        if "bullish" in t:
            return "LONG"
        if "bearish" in t:
            return "SHORT"
        return None

    def _get_anchor_trend(self, tf):
        """
        Hierarchical trend anchor:
        5m/15m -> 15m then 1h then 4h then 1d then 1w
        1h     -> 1h then 4h then 1d then 1w
        4h     -> 4h then 1d then 1w
        """
        order_map = {
            "5m":  ["15m", "1h", "4h", "1d", "1w"],
            "15m": ["15m", "1h", "4h", "1d", "1w"],
            "1h":  ["1h", "4h", "1d", "1w"],
            "4h":  ["4h", "1d", "1w"],
        }
        trends = getattr(self, "tf_trends", {}) or {}
        for k in order_map.get(tf, [tf, "1h", "4h", "1d", "1w"]):
            side = self._trend_side(trends.get(k))
            if side:
                return trends.get(k), k
        return "Ranging", None

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

        def pick_for_tf(tf_candidates, tf, tf_min_usd, tf_min_dist, used_prices, prev_tf_size, prev_tf_dist):
            if not tf_candidates:
                return None
            target_dist = float(LIQ_POOL_TARGET_DISTANCE_PCT_BY_TF.get(tf, tf_min_dist))
            dedup_gap = float(LIQ_POOL_LEVEL_DEDUP_GAP_PCT)
            step_pct = float(LIQ_POOL_PROGRESSIVE_MIN_STEP_PCT)
            progressive_floor = max(tf_min_dist, prev_tf_dist + step_pct)
            passes = [
                {
                    "usd": tf_min_usd,
                    "dist": max(progressive_floor, target_dist * 0.80),
                    "fallback": False,
                    "force": False
                },
                {
                    "usd": max(tf_min_usd * 0.65, 8_000_000.0),
                    "dist": max(progressive_floor * 0.85, tf_min_dist * 0.80, 0.05),
                    "fallback": True,
                    "force": False
                },
                {
                    "usd": max(tf_min_usd * 0.35, 4_000_000.0),
                    "dist": max(prev_tf_dist + step_pct * 0.50, 0.03),
                    "fallback": True,
                    "force": True
                },
            ]

            for p in passes:
                # Pass A: prefer equal-or-bigger pools than previous TF.
                for enforce_growth in (True, False):
                    for enforce_unique in (True, False):
                        best = None
                        best_rank = -1.0
                        for evt in tf_candidates:
                            usd = float(evt.get("size_usd", 0) or 0)
                            dist_pct = float(evt.get("distance_pct", 0) or 0)
                            px = float(evt.get("level_price", 0) or 0)
                            if usd < p["usd"] or dist_pct < p["dist"] or px <= 0:
                                continue
                            if enforce_growth and prev_tf_size > 0 and usd < (prev_tf_size * 0.95):
                                continue

                            duplicate = False
                            for upx in used_prices:
                                dd = abs(px - upx) / float(latest_price) * 100.0
                                if dd < dedup_gap:
                                    duplicate = True
                                    break
                            if enforce_unique and duplicate:
                                continue

                            # Prefer bigger pools while nudging each TF toward its own distance target.
                            rank = usd * (1.0 + dist_pct / max(target_dist, 0.05)) / (1.0 + abs(dist_pct - target_dist) * 2.0)
                            if rank > best_rank:
                                best_rank = rank
                                best = dict(evt)
                                size_ratio = usd / max(1.0, p["usd"])
                                dist_fit = max(0.0, 1.0 - (abs(dist_pct - target_dist) / max(target_dist, 0.05)))
                                prob = min(95.0, max(20.0, 25.0 + min(size_ratio, 6.0) * 10.0 + dist_fit * 20.0))
                                best["probability_pct"] = prob
                                best["score"] = rank
                                best["fallback"] = p["fallback"]
                                best["force"] = p["force"]
                        if best:
                            return best
            # Last resort: take farthest visible candidate (keeps higher TF farther than lower TF as much as possible).
            farthest = None
            farthest_dist = -1.0
            for evt in tf_candidates:
                px = float(evt.get("level_price", 0) or 0)
                dist_pct = float(evt.get("distance_pct", 0) or 0)
                if px <= 0:
                    continue
                if dist_pct > farthest_dist:
                    farthest = dict(evt)
                    farthest_dist = dist_pct
            if farthest:
                farthest["fallback"] = True
                farthest["force"] = True
                farthest["probability_pct"] = max(15.0, 55.0 - farthest_dist * 6.0)
                farthest["score"] = float(farthest.get("size_usd", 0) or 0) * 0.25
                return farthest
            return None

        def pick_structural_for_tf(tf, tf_min_usd, tf_min_dist, prev_tf_dist):
            levels = self.levels if isinstance(self.levels, dict) else {}
            step_pct = float(LIQ_POOL_PROGRESSIVE_MIN_STEP_PCT)
            target_dist = float(LIQ_POOL_TARGET_DISTANCE_PCT_BY_TF.get(tf, tf_min_dist))
            required_dist = max(tf_min_dist, prev_tf_dist + step_pct)

            raw = []
            if levels:
                for key in ["PDH", "PDL", "PWH", "PWL", "PMH", "PML", "PumpMax", "DumpMax"]:
                    lv = levels.get(key)
                    if not lv:
                        continue
                    px = float(lv)
                    dist_pct = abs(px - float(latest_price)) / float(latest_price) * 100.0
                    side = "LONG" if px > float(latest_price) else "SHORT"
                    raw.append((key, px, dist_pct, side))
            if not raw:
                bid_usd = sum(float(px) * float(sz) for px, sz in (order_book.get("bids", []) or []))
                ask_usd = sum(float(px) * float(sz) for px, sz in (order_book.get("asks", []) or []))
                side = "LONG" if ask_usd >= bid_usd else "SHORT"
                px = float(latest_price) * (1.0 + target_dist / 100.0) if side == "LONG" else float(latest_price) * (1.0 - target_dist / 100.0)
                raw.append(("Projected", px, target_dist, side))

            eligible = [r for r in raw if r[2] >= required_dist]
            if not eligible:
                eligible = sorted(raw, key=lambda x: x[2], reverse=True)[:1]
            chosen = min(eligible, key=lambda x: abs(x[2] - target_dist))
            key, px, dist_pct, side = chosen
            return {
                "timeframe": tf,
                "side": side,
                "level_price": px,
                "size_usd": float(tf_min_usd) * 1.1,
                "distance_pct": dist_pct,
                "probability_pct": max(20.0, 60.0 - abs(dist_pct - target_dist) * 8.0),
                "score": float(tf_min_usd) * (1.0 + dist_pct / 100.0),
                "fallback": True,
                "force": True,
                "synthetic": True,
                "source": key,
            }

        candidates = []
        used_prices = []
        prev_tf_size = 0.0
        prev_tf_dist = 0.0
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
            required_dist = max(tf_min_dist, prev_tf_dist + float(LIQ_POOL_PROGRESSIVE_MIN_STEP_PCT))
            tf_candidates = detect_liquidity_candidates(
                order_book=order_book,
                price=float(latest_price),
                atr=atr_val,
                timeframe=tf,
                max_distance_atr_mult=float(LIQ_POOL_MAX_DISTANCE_ATR_MULT.get(tf, 1.5)) * 2.5,
                bucket_pct=float(LIQ_POOL_AGG_WINDOW_PCT_BY_TF.get(tf, 0.0)),
            )
            event = pick_for_tf(tf_candidates, tf, tf_min_usd, tf_min_dist, used_prices, prev_tf_size, prev_tf_dist)
            if tf != "5m" and ((not event) or float(event.get("distance_pct", 0) or 0) < required_dist):
                structural = pick_structural_for_tf(tf, tf_min_usd, tf_min_dist, prev_tf_dist)
                if structural:
                    event = structural
            if not event:
                continue
            candidates.append(event)
            used_prices.append(float(event.get("level_price", 0) or 0))
            prev_tf_size = float(event.get("size_usd", 0) or prev_tf_size)
            prev_tf_dist = max(prev_tf_dist, float(event.get("distance_pct", 0) or prev_tf_dist))

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
            if not self._session_is_tradeable_today(s_name, now):
                continue
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

        lines = [f"<b>?? LIQUIDITY POOL REPORT</b>", f"<pre>Trigger: {trigger}"]
        for tf in LIQ_POOL_REPORT_TIMEFRAMES:
            evt = by_tf.get(tf)
            if not evt:
                lines.append(f"{tf:>3} | ? Waiting for clear pool")
                continue
            quality = "Estimated" if evt.get("synthetic") else ("Approx" if evt.get("force") or evt.get("fallback") else "Live")
            lines.append(
                f"{evt['timeframe']:>3} | {evt['side']:<5} | Price {evt['level_price']:,.0f} | "
                f"Pool ${evt['size_usd']/1e6:,.0f}M | Dist {evt['distance_pct']:.2f}% | Chance {evt['probability_pct']:.0f}% | {quality}"
            )
        lines.append("</pre>")
        msg = "\n".join(lines)
        if not self.is_booting:
            tg.send(msg, parse_mode="HTML", chat_id=self._signal_chat_id())
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

    def _stop_liq_watch_key(self, tf, side, symbol=None):
        symbol_name = str(symbol or SYMBOL).upper()
        return f"{symbol_name}:{str(tf or '').lower()}:{str(side or '').upper()}"

    def _prune_stop_liq_watches(self, now):
        now = now or datetime.now(timezone.utc)
        ts_now = now.timestamp()
        watches = dict(getattr(self, "pending_stop_liq_watches", {}) or {})
        changed = False
        for key, watch in list(watches.items()):
            try:
                expires_at = float((watch or {}).get("expires_at") or 0)
            except Exception:
                expires_at = 0.0
            if expires_at and expires_at < ts_now:
                watches.pop(key, None)
                changed = True
        if changed:
            self.pending_stop_liq_watches = watches
            self._save_state()

    def _register_stop_liq_watch(self, evt, tf, hazard, now):
        hazard = hazard or {}
        evt = evt or {}
        side = str(evt.get("side") or "").upper()
        if side not in {"LONG", "SHORT"}:
            return
        symbol_name = str(evt.get("symbol") or (evt.get("meta") or {}).get("symbol") or SYMBOL).upper()
        key = self._stop_liq_watch_key(tf, side, symbol_name)
        expiry_hours = {"5m": 6, "15m": 10, "1h": 18, "4h": 30}
        expires_at = now + timedelta(hours=int(expiry_hours.get(str(tf), 8)))
        self.pending_stop_liq_watches[key] = {
            "symbol": symbol_name,
            "tf": str(tf),
            "side": side,
            "watch_price": float(hazard.get("price") or 0),
            "entry": float(evt.get("entry") or 0),
            "stop": float(evt.get("sl") or 0),
            "kind": str(hazard.get("kind") or ""),
            "size_usd": float(hazard.get("size_usd") or 0),
            "created_at": now.isoformat(),
            "expires_at": expires_at.timestamp(),
            "reason": str(hazard.get("reason") or ""),
        }
        self._save_state()

    def _stop_liq_watch_trigger_note(self, evt, tf, df, now):
        self._prune_stop_liq_watches(now)
        evt = evt or {}
        side = str(evt.get("side") or "").upper()
        if side not in {"LONG", "SHORT"}:
            return ""
        symbol_name = str(evt.get("symbol") or (evt.get("meta") or {}).get("symbol") or SYMBOL).upper()
        key = self._stop_liq_watch_key(tf, side, symbol_name)
        watch = dict((getattr(self, "pending_stop_liq_watches", {}) or {}).get(key) or {})
        if not watch:
            return ""
        try:
            if df is None or df.empty or len(df) < 3:
                return ""
        except Exception:
            return ""
        watch_price = float(watch.get("watch_price") or 0)
        entry = float(watch.get("entry") or evt.get("entry") or 0)
        stop = float(watch.get("stop") or evt.get("sl") or 0)
        atr_val = float(df.iloc[-1].get("ATR", 0) or 0)
        if watch_price <= 0 or entry <= 0 or stop <= 0 or atr_val <= 0:
            return ""
        risk = abs(entry - stop)
        if risk <= 0:
            return ""

        recent = df.tail(3)
        curr = recent.iloc[-1]
        prev = recent.iloc[-2]
        open_now = float(curr.get("Open", 0) or 0)
        close_now = float(curr.get("Close", 0) or 0)
        high_now = float(curr.get("High", close_now) or close_now)
        low_now = float(curr.get("Low", close_now) or close_now)
        prev_close = float(prev.get("Close", close_now) or close_now)
        sweep_buffer = max(risk * 0.06, atr_val * 0.08, close_now * 0.00022)
        reclaim_buffer = max(risk * 0.10, atr_val * 0.10, close_now * 0.00030)

        if side == "LONG":
            swept = float(recent["Low"].min()) <= (watch_price + sweep_buffer)
            reclaimed = close_now >= (watch_price + reclaim_buffer) and close_now > open_now and close_now >= prev_close
        else:
            swept = float(recent["High"].max()) >= (watch_price - sweep_buffer)
            reclaimed = close_now <= (watch_price - reclaim_buffer) and close_now < open_now and close_now <= prev_close

        if not (swept and reclaimed):
            return ""

        self.pending_stop_liq_watches.pop(key, None)
        self._save_state()
        kind = str(watch.get("kind") or "sweep")
        return f"liquidity watch cleared after {kind.replace('_', ' ')} at {watch_price:,.2f}"

    def _get_stop_liquidity_hazard(self, evt, tf, df):
        if not LIQ_POOL_ALERT_ENABLED:
            return None
        book = getattr(self, "last_order_book", None)
        if not isinstance(book, dict):
            return None
        try:
            if df is None or df.empty:
                return None
        except Exception:
            return None

        side = str((evt or {}).get("side") or "").upper()
        if side not in {"LONG", "SHORT"}:
            return None
        try:
            entry = float((evt or {}).get("entry") or 0)
            stop = float((evt or {}).get("sl") or 0)
            close = float(df.iloc[-1].get("Close", entry) or entry)
            atr_val = float(df.iloc[-1].get("ATR", 0) or 0)
        except Exception:
            return None
        risk = abs(entry - stop)
        if entry <= 0 or stop <= 0 or risk <= 0 or atr_val <= 0 or close <= 0:
            return None

        try:
            rows = detect_liquidity_candidates(
                order_book=book,
                price=float(close),
                atr=float(atr_val),
                timeframe=str(tf or "5m"),
                max_distance_atr_mult=max(float(LIQ_POOL_MAX_DISTANCE_ATR_MULT.get(tf, 1.5) or 1.5) * 3.2, 3.0),
                bucket_pct=max(float(LIQ_POOL_AGG_WINDOW_PCT_BY_TF.get(tf, 0.0) or 0.0), 0.02),
            )
        except Exception:
            rows = []

        structural = []
        for row in list(getattr(self, "last_liq_candidates", []) or []):
            try:
                structural.append({
                    "side": row.get("side"),
                    "level_price": float(row.get("level_price") or 0),
                    "size_usd": float(row.get("size_usd") or 0),
                })
            except Exception:
                continue
        rows.extend(structural)

        danger_side = "SHORT" if side == "LONG" else "LONG"
        min_liq_usd = max(float(LIQ_POOL_MIN_USD or 0) * 0.80, 8_000_000.0)
        stop_band = max(risk * 0.22, atr_val * 0.16, close * 0.00055)
        sweep_gap = max(risk * 0.28, atr_val * 0.20, close * 0.0008)
        magnet_floor = max(risk * 0.35, atr_val * 0.30, close * 0.0018)
        magnet_ceiling = max(risk * 2.2, atr_val * 3.8, close * 0.0125)
        recent_move_thr = 0.30 if str(tf) == "5m" else (0.55 if str(tf) == "15m" else 0.75)
        best = None

        recent_move_pct = 0.0
        directional_pressure = False
        volume_mult = 1.0
        try:
            lookback = 3 if str(tf) == "5m" else 2
            if len(df) >= lookback + 1:
                prev_close = float(df["Close"].iloc[-(lookback + 1)] or 0)
                close_now = float(df["Close"].iloc[-1] or 0)
                if prev_close > 0 and close_now > 0:
                    recent_move_pct = (close_now / prev_close - 1.0) * 100.0
                c1 = float(df["Close"].iloc[-1] or close_now)
                c2 = float(df["Close"].iloc[-2] or c1)
                c3 = float(df["Close"].iloc[-3] or c2) if len(df) >= 3 else c2
                o1 = float(df["Open"].iloc[-1] or c1)
                o2 = float(df["Open"].iloc[-2] or c2)
                o3 = float(df["Open"].iloc[-3] or c3) if len(df) >= 3 else c3
                red_count = int(c1 < o1) + int(c2 < o2) + int(c3 < o3)
                green_count = int(c1 > o1) + int(c2 > o2) + int(c3 > o3)
                if side == "LONG":
                    directional_pressure = recent_move_pct <= -abs(recent_move_thr) and (c1 < c2 or red_count >= 2)
                else:
                    directional_pressure = recent_move_pct >= abs(recent_move_thr) and (c1 > c2 or green_count >= 2)
            if "Volume" in df.columns and len(df) >= 21:
                vol_now = float(df["Volume"].iloc[-1] or 0)
                vol_avg = float(df["Volume"].iloc[-21:-1].mean() or 0)
                if vol_now > 0 and vol_avg > 0:
                    volume_mult = vol_now / vol_avg
        except Exception:
            directional_pressure = False

        for row in rows:
            if str(row.get("side") or "").upper() != danger_side:
                continue
            try:
                px = float(row.get("level_price") or 0)
                usd = float(row.get("size_usd") or 0)
            except Exception:
                continue
            if px <= 0 or usd < min_liq_usd:
                continue

            if side == "LONG":
                in_stop_zone = (stop - stop_band) <= px <= (stop + stop_band)
                pre_sweep_zone = stop <= px < (entry - sweep_gap)
                magnet_zone = directional_pressure and (close - magnet_ceiling) <= px <= (close - magnet_floor)
            else:
                in_stop_zone = (stop - stop_band) <= px <= (stop + stop_band)
                pre_sweep_zone = (entry + sweep_gap) < px <= stop
                magnet_zone = directional_pressure and (close + magnet_floor) <= px <= (close + magnet_ceiling)

            if not (in_stop_zone or pre_sweep_zone or magnet_zone):
                continue

            score = usd
            if in_stop_zone:
                score *= 1.25
            if magnet_zone:
                score *= 1.15
                if volume_mult >= 1.4:
                    score *= 1.08
            if best is None or score > best["score"]:
                best = {
                    "price": px,
                    "size_usd": usd,
                    "score": score,
                    "kind": "stop_zone" if in_stop_zone else ("pre_sweep" if pre_sweep_zone else "magnet"),
                }

        if not best:
            return None

        price_text = f"{float(best['price']):,.2f}"
        if best["kind"] == "stop_zone":
            best["reason"] = (
                f"stop parked near major {'downside' if side == 'LONG' else 'upside'} liquidity "
                f"({price_text}, ${best['size_usd']/1e6:.1f}M)"
            )
            return best
        if best["kind"] == "magnet":
            move_text = f"{recent_move_pct:+.2f}%"
            best["reason"] = (
                f"major {'downside' if side == 'LONG' else 'upside'} liquidity still below the move "
                f"({price_text}, ${best['size_usd']/1e6:.1f}M) after {move_text}"
            )
            return best
        best["reason"] = (
            f"likely {'downside' if side == 'LONG' else 'upside'} sweep remains first "
            f"({price_text}, ${best['size_usd']/1e6:.1f}M)"
        )
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

    def _get_fast_move_volume_guard(self, data, side, preferred_tf=None):
        """
        Be careful around sudden expansions with heavy volume.
        Blocks:
        - same-direction late chase after a fast pump/dump
        - immediate counter-trend catches before any cooling / base appears
        """
        if not FAST_EXPANSION_GUARD_ENABLED:
            return ""
        if not isinstance(data, dict):
            return ""

        cfg_checks = [
            ("5m", int(FAST_EXPANSION_LOOKBACK_5M), float(FAST_EXPANSION_MOVE_PCT_5M)),
            ("15m", int(FAST_EXPANSION_LOOKBACK_15M), float(FAST_EXPANSION_MOVE_PCT_15M)),
        ]
        checks = []
        if preferred_tf in {"5m", "15m"}:
            for tf_name, lookback, move_thr in cfg_checks:
                if tf_name == preferred_tf:
                    checks.append((tf_name, lookback, move_thr))
                    break
        for tf_name, lookback, move_thr in cfg_checks:
            if tf_name != preferred_tf:
                checks.append((tf_name, lookback, move_thr))

        for tf_name, lookback, move_thr in checks:
            df = data.get(tf_name)
            if df is None or df.empty or len(df) < max(lookback + 1, 22):
                continue
            if "Volume" not in df.columns:
                continue

            curr = df.iloc[-1]
            prev = df.iloc[-(lookback + 1)]
            curr_close = float(curr.get("Close", 0) or 0)
            prev_close = float(prev.get("Close", 0) or 0)
            curr_open = float(curr.get("Open", curr_close) or curr_close)
            if curr_close <= 0 or prev_close <= 0:
                continue

            curr_vol = float(curr.get("Volume", 0) or 0)
            avg_vol = float(df["Volume"].iloc[-21:-1].mean() or 0)
            if curr_vol <= 0 or avg_vol <= 0:
                continue
            vol_mult = curr_vol / avg_vol
            if vol_mult < float(FAST_EXPANSION_VOLUME_MULT):
                continue

            move_pct = (curr_close / prev_close - 1.0) * 100.0
            if abs(move_pct) < abs(move_thr):
                continue

            atr_val = float(curr.get("ATR", 0) or 0)
            if atr_val <= 0:
                atr_val = max(curr_close * 0.002, 1.0)
            body_atr = abs(curr_close - curr_open) / atr_val
            ema2 = float(curr.get("EMA2", curr_close) or curr_close)
            stretch_atr = abs(curr_close - ema2) / atr_val
            if (
                body_atr < float(FAST_EXPANSION_BODY_ATR)
                and stretch_atr < float(FAST_EXPANSION_EMA2_ATR)
            ):
                continue

            c1 = float(df["Close"].iloc[-1])
            c2 = float(df["Close"].iloc[-2])
            c3 = float(df["Close"].iloc[-3])
            o1 = float(df["Open"].iloc[-1])
            o2 = float(df["Open"].iloc[-2])
            o3 = float(df["Open"].iloc[-3])
            green_count = int(c1 > o1) + int(c2 > o2) + int(c3 > o3)
            red_count = int(c1 < o1) + int(c2 < o2) + int(c3 < o3)

            if move_pct > 0:
                if not (curr_close > curr_open or green_count >= 2):
                    continue
                started_cooling = (c1 < c2) or (curr_close < curr_open and c2 <= c3)
                move_label = "pump"
                impulse_side = "LONG"
            else:
                if not (curr_close < curr_open or red_count >= 2):
                    continue
                started_cooling = (c1 > c2) or (curr_close > curr_open and c2 >= c3)
                move_label = "dump"
                impulse_side = "SHORT"

            if side == impulse_side:
                return (
                    f"fast {move_label} chase on {tf_name}: {move_pct:+.2f}% "
                    f"with {vol_mult:.1f}x volume"
                )

            if not started_cooling:
                return (
                    f"fast {move_label} on {tf_name}: wait for pullback/base "
                    f"after {move_pct:+.2f}% with {vol_mult:.1f}x volume"
                )

        return ""

    def _get_opposite_divergence_note(self, side):
        """Return note when any tracked timeframe has opposite-side RSI divergence."""
        div_map = getattr(self, "divergence_map", {}) or {}
        opposite = "SHORT" if side == "LONG" else "LONG"
        hits = []
        for tf in ["5m", "15m", "1h", "4h"]:
            sides = div_map.get(tf) or set()
            if opposite in sides:
                hits.append(tf)
        if not hits:
            return ""
        label = "bearish" if opposite == "SHORT" else "bullish"
        return f"opposite RSI divergence on {', '.join(hits)} ({label})"

    def _get_same_side_divergence_hits(self, side):
        div_map = getattr(self, "divergence_map", {}) or {}
        target = str(side or "").upper()
        hits = []
        for tf in ["5m", "15m", "1h", "4h"]:
            sides = div_map.get(tf) or set()
            if target in sides:
                hits.append(tf)
        return hits

    def _get_recent_liquidity_sweep_hits(self, side, preferred_tf=None):
        side = str(side or "").upper()
        if not self.levels:
            return []
        ordered_tfs = []
        if preferred_tf:
            ordered_tfs.append(preferred_tf)
        ordered_tfs.extend(["5m", "15m", "1h", "4h"])
        hits = []
        seen = set()
        for tf in ordered_tfs:
            if tf in seen:
                continue
            seen.add(tf)
            df = (getattr(self, "latest_data", {}) or {}).get(tf)
            try:
                if df is None or df.empty or len(df) < 2:
                    continue
            except Exception:
                continue
            curr = df.iloc[-1]
            prev = df.iloc[-2]
            sweeps = check_liquidity_sweep(
                float(curr.get("High", 0) or 0),
                float(curr.get("Low", 0) or 0),
                self.levels,
                prev_high=float(prev.get("High", 0) or 0),
                prev_low=float(prev.get("Low", 0) or 0),
            )
            side_sweeps = [sw for sw in sweeps if str(sw.get("side", "")).upper() == side]
            if side_sweeps:
                level_names = "/".join(str(sw.get("level", "")).upper() for sw in side_sweeps[:2] if sw.get("level"))
                hits.append(f"{tf} liquidity sweep" + (f" ({level_names})" if level_names else ""))
        return hits

    def _get_key_level_reaction_hits(self, side, preferred_tf=None):
        side = str(side or "").upper()
        if not self.levels:
            return []
        ordered_tfs = []
        if preferred_tf:
            ordered_tfs.append(preferred_tf)
        ordered_tfs.extend(["5m", "15m", "1h", "4h"])
        hits = []
        seen = set()
        target_levels = (
            ["DO", "PDL", "PWL", "PML", "Dump", "DumpMax"]
            if side == "LONG"
            else ["DO", "PDH", "PWH", "PMH", "Pump", "PumpMax"]
        )
        for tf in ordered_tfs:
            if tf in seen:
                continue
            seen.add(tf)
            df = (getattr(self, "latest_data", {}) or {}).get(tf)
            try:
                if df is None or df.empty:
                    continue
            except Exception:
                continue
            curr = df.iloc[-1]
            close = float(curr.get("Close", 0) or 0)
            open_ = float(curr.get("Open", close) or close)
            high = float(curr.get("High", close) or close)
            low = float(curr.get("Low", close) or close)
            atr_val = float(curr.get("ATR", 0) or 0)
            if close <= 0:
                continue
            band = max(close * 0.0010, atr_val * 0.35 if atr_val > 0 else 0.0)
            body = max(1e-9, abs(close - open_))
            if side == "LONG":
                wick_ok = max(0.0, min(open_, close) - low) / body >= 1.0 or close > open_
            else:
                wick_ok = max(0.0, high - max(open_, close)) / body >= 1.0 or close < open_
            if not wick_ok:
                continue
            for level_name in target_levels:
                level = float(self.levels.get(level_name, 0) or 0)
                if level <= 0:
                    continue
                if side == "LONG":
                    near_level = abs(low - level) <= band and close >= level
                else:
                    near_level = abs(high - level) <= band and close <= level
                if near_level:
                    hits.append(f"{tf} key-level reaction ({level_name})")
                    break
        return hits

    def _get_reversal_override(self, tf, side, evt=None, score=None):
        if not REVERSAL_OVERRIDE_ENABLED:
            return False, ""

        proofs = []
        divergence_hits = self._get_same_side_divergence_hits(side)
        if divergence_hits:
            proofs.append(f"RSI divergence on {', '.join(divergence_hits)}")

        sweep_hits = self._get_recent_liquidity_sweep_hits(side, preferred_tf=tf)
        if sweep_hits:
            proofs.extend(sweep_hits)
        level_hits = self._get_key_level_reaction_hits(side, preferred_tf=tf)
        if level_hits:
            proofs.extend(level_hits)

        trigger = str((evt or {}).get("trigger") or "").strip().upper()
        strategy = str((evt or {}).get("strategy") or "").strip().upper()
        if trigger == "ONE_H_RECLAIM":
            proofs.append("reclaim of key level")
        elif trigger == "HTF_PULLBACK":
            proofs.append("pullback reclaim")
        if strategy == "SMART_MONEY_LIQUIDITY":
            proofs.append("liquidity sweep structure break")

        if score is not None and float(score) >= float(REVERSAL_OVERRIDE_MIN_SCORE):
            proofs.append(f"score {int(score)}")

        unique_proofs = []
        for proof in proofs:
            if proof not in unique_proofs:
                unique_proofs.append(proof)

        has_strong_score = any(p.startswith("score ") for p in unique_proofs)
        if has_strong_score and len(unique_proofs) >= int(REVERSAL_OVERRIDE_MIN_PROOFS):
            return True, ", ".join(unique_proofs)
        return False, ""

    def _get_structure_anchor_tf(self, tf):
        return "15m" if str(tf) == "5m" else str(tf)

    def _get_structure_guard_mode(self, tf):
        return str((STRUCTURE_GUARD_MODE_BY_TF or {}).get(str(tf), "hard")).strip().lower()

    def _get_recent_bos_context(self, tf):
        if not BOS_GUARD_ENABLED:
            return {}
        anchor_tf = self._get_structure_anchor_tf(tf)
        df = (getattr(self, "latest_data", {}) or {}).get(anchor_tf)
        try:
            if df is None or df.empty:
                return {}
        except Exception:
            return {}

        swing_lb = int(max(3, BOS_GUARD_SWING_LOOKBACK))
        recent_bars = int(max(2, BOS_GUARD_RECENT_BARS))
        reclaim_bars = int(max(1, BOS_GUARD_RECLAIM_BARS))
        if len(df) < swing_lb + 3:
            return {}

        found = None
        start_idx = max(swing_lb, len(df) - recent_bars)
        for i in range(start_idx, len(df)):
            prior = df.iloc[i - swing_lb:i]
            if prior is None or prior.empty:
                continue
            prev_high = float(prior["High"].max())
            prev_low = float(prior["Low"].min())
            row = df.iloc[i]
            row_close = float(row.get("Close", 0) or 0)
            row_high = float(row.get("High", 0) or 0)
            row_low = float(row.get("Low", 0) or 0)
            if row_close > prev_high or row_high > prev_high:
                found = {"side": "LONG", "level": prev_high, "idx": i, "tf": anchor_tf}
            if row_close < prev_low or row_low < prev_low:
                found = {"side": "SHORT", "level": prev_low, "idx": i, "tf": anchor_tf}

        if not found:
            return {}

        bos_idx = int(found["idx"])
        pre_bos = df.iloc[max(0, bos_idx - swing_lb):bos_idx]
        bos_bar = df.iloc[bos_idx]
        latest = df.iloc[-1]
        latest_close = float(latest.get("Close", 0) or 0)
        latest_high = float(latest.get("High", latest_close) or latest_close)
        latest_low = float(latest.get("Low", latest_close) or latest_close)
        atr_ref = float(df.iloc[bos_idx].get("ATR", 0) or 0)
        tol = atr_ref * 0.15 if atr_ref > 0 else abs(float(found["level"])) * 0.0004
        follow = df.iloc[bos_idx:min(len(df), bos_idx + reclaim_bars + 1)]
        post_bos = df.iloc[min(len(df), bos_idx + 1):]
        reclaimed = False
        weak_follow = True
        lower_high = False
        lower_low = False
        higher_high = False
        higher_low = False
        continuation_after_reclaim = False
        bos_reference_high = float(pre_bos["High"].max()) if not pre_bos.empty else float(bos_bar.get("High", latest_high) or latest_high)
        bos_reference_low = float(pre_bos["Low"].min()) if not pre_bos.empty else float(bos_bar.get("Low", latest_low) or latest_low)
        if not follow.empty:
            if found["side"] == "SHORT":
                min_follow = float(follow["Low"].min())
                extension = max(0.0, float(found["level"]) - min_follow)
                weak_follow = atr_ref <= 0 or extension <= atr_ref * 0.8
                reclaimed = latest_close > float(found["level"]) and weak_follow
            else:
                max_follow = float(follow["High"].max())
                extension = max(0.0, max_follow - float(found["level"]))
                weak_follow = atr_ref <= 0 or extension <= atr_ref * 0.8
                reclaimed = latest_close < float(found["level"]) and weak_follow

        if not post_bos.empty:
            post_high = float(post_bos["High"].max())
            post_low = float(post_bos["Low"].min())
            lower_high = post_high < (bos_reference_high - tol)
            lower_low = post_low < (float(bos_bar.get("Low", post_low) or post_low) - tol)
            higher_high = post_high > (bos_reference_high + tol)
            higher_low = post_low > (float(found["level"]) + tol)
            if found["side"] == "SHORT":
                continuation_after_reclaim = reclaimed and higher_high and latest_close > bos_reference_high - tol
            else:
                continuation_after_reclaim = reclaimed and lower_low and latest_close < bos_reference_low + tol

        found["reclaimed"] = reclaimed
        found["weak_follow"] = weak_follow
        found["lower_high"] = lower_high
        found["lower_low"] = lower_low
        found["higher_high"] = higher_high
        found["higher_low"] = higher_low
        found["continuation_after_reclaim"] = continuation_after_reclaim
        if found["side"] == "SHORT":
            found["pullback_only"] = bool(reclaimed or (weak_follow and not lower_high and not lower_low) or continuation_after_reclaim)
            found["reversal_confirmed"] = bool(lower_high and lower_low and not reclaimed)
        else:
            found["pullback_only"] = bool(reclaimed or (weak_follow and not higher_high and not higher_low) or continuation_after_reclaim)
            found["reversal_confirmed"] = bool(higher_high and higher_low and not reclaimed)
        return found

    def _get_bos_guard_reason(self, tf, side):
        mode = self._get_structure_guard_mode(tf)
        if mode == "off":
            return ""
        ctx = self._get_recent_bos_context(tf)
        if not ctx:
            return ""
        bos_side = str(ctx.get("side", "")).upper()
        if not bos_side or bos_side == str(side or "").upper():
            return ""
        if ctx.get("reclaimed") or ctx.get("pullback_only") or ctx.get("continuation_after_reclaim"):
            return ""
        if mode == "soft" and not ctx.get("reversal_confirmed"):
            return ""
        state = "reversal follow-through" if ctx.get("reversal_confirmed") else "active BOS"
        return f"recent {bos_side.lower()} BOS on {ctx.get('tf', tf)} is still active ({state})"

    def _get_rsi_pullback_scalp_override(self, tf, side):
        if not RSI_PULLBACK_SCALP_ENABLED:
            return False, ""
        if str(tf) not in set(RSI_PULLBACK_SCALP_TFS):
            return False, ""

        df = (getattr(self, "latest_data", {}) or {}).get(tf)
        try:
            if df is None or df.empty or len(df) < 3:
                return False, ""
        except Exception:
            return False, ""

        curr = df.iloc[-1]
        prev = df.iloc[-2]
        side = str(side or "").upper()
        close = float(curr.get("Close", 0) or 0)
        open_ = float(curr.get("Open", close) or close)
        high = float(curr.get("High", close) or close)
        low = float(curr.get("Low", close) or close)
        prev_close = float(prev.get("Close", close) or close)
        prev_high = float(prev.get("High", high) or high)
        prev_low = float(prev.get("Low", low) or low)
        rsi = float(curr.get("RSI", 50) or 50)
        atr_val = float(curr.get("ATR", 0) or 0)
        ema2 = float(curr.get("EMA2", close) or close)

        if atr_val <= 0:
            return False, ""

        bos_ctx = self._get_recent_bos_context(tf)

        if side == "SHORT":
            if rsi < float(RSI_PULLBACK_SCALP_OB):
                return False, ""
        elif side == "LONG":
            if rsi > float(RSI_PULLBACK_SCALP_OS):
                return False, ""
        else:
            return False, ""

        body_atr = abs(close - open_) / atr_val
        displacement_atr = abs(close - prev_close) / atr_val
        if side == "SHORT":
            impulse_filter = (
                (close > open_ and body_atr >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
                or displacement_atr >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
            )
            ema_filter = close > ema2 and abs(close - ema2) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_EMA_ATR_DISTANCE)
            upper_wick = max(0.0, high - max(open_, close))
            body_abs = max(1e-9, abs(close - open_))
            wick_filter = upper_wick / body_abs >= float(RSI_PULLBACK_SCALP_MIN_WICK_BODY_RATIO)
            structure_shift = close < prev_low
        else:
            impulse_filter = (
                (close < open_ and body_atr >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
                or displacement_atr >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
            )
            ema_filter = close < ema2 and abs(close - ema2) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_EMA_ATR_DISTANCE)
            lower_wick = max(0.0, min(open_, close) - low)
            body_abs = max(1e-9, abs(close - open_))
            wick_filter = lower_wick / body_abs >= float(RSI_PULLBACK_SCALP_MIN_WICK_BODY_RATIO)
            structure_shift = close > prev_high

        sweep_filters = self._get_recent_liquidity_sweep_hits(side, preferred_tf=tf)
        level_reaction_filters = self._get_key_level_reaction_hits(side, preferred_tf=tf)
        filters = []
        if sweep_filters:
            filters.append("liquidity sweep")
        if level_reaction_filters:
            filters.append("key-level reaction")
        if impulse_filter:
            filters.append("impulse/displacement")
        if ema_filter:
            filters.append("EMA stretch")
        if wick_filter:
            filters.append("wick rejection")

        after_bos_or_impulse = impulse_filter or (bos_ctx and str(bos_ctx.get("side", "")).upper() == side)

        if not structure_shift:
            return False, ""
        if not after_bos_or_impulse:
            return False, ""
        if len(filters) < int(RSI_PULLBACK_SCALP_MIN_FILTERS):
            return False, ""

        return True, f"extreme RSI scalp ({', '.join(filters[:3])})"

    def _get_weekend_scalp_exception(self, tf, side):
        if str(tf) not in set(RSI_PULLBACK_SCALP_TFS):
            return False, ""
        df = (getattr(self, "latest_data", {}) or {}).get(tf)
        try:
            if df is None or df.empty or len(df) < 3:
                return False, ""
        except Exception:
            return False, ""

        curr = df.iloc[-1]
        prev = df.iloc[-2]
        side = str(side or "").upper()
        close = float(curr.get("Close", 0) or 0)
        open_ = float(curr.get("Open", close) or close)
        prev_close = float(prev.get("Close", close) or close)
        rsi = float(curr.get("RSI", 50) or 50)
        atr_val = float(curr.get("ATR", 0) or 0)
        if atr_val <= 0:
            return False, ""

        if side == "SHORT":
            extreme_ok = rsi >= float(RSI_PULLBACK_SCALP_OB)
            impulse_ok = (
                (close > open_ and abs(close - open_) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
                or abs(close - prev_close) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
            )
        elif side == "LONG":
            extreme_ok = rsi <= float(RSI_PULLBACK_SCALP_OS)
            impulse_ok = (
                (close < open_ and abs(close - open_) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_IMPULSE_BODY_ATR))
                or abs(close - prev_close) / atr_val >= float(RSI_PULLBACK_SCALP_MIN_DISPLACEMENT_ATR)
            )
        else:
            return False, ""

        sweep_ok = bool(self._get_recent_liquidity_sweep_hits(side, preferred_tf=tf))
        pullback_ok, pullback_note = self._get_rsi_pullback_scalp_override(tf, side)
        if extreme_ok and impulse_ok and sweep_ok and pullback_ok:
            return True, f"weekend extreme scalp ({pullback_note})"
        return False, ""

    def _apply_rsi_pullback_fast_targets(self, evt):
        if not evt:
            return evt
        entry = float(evt.get("entry", 0) or 0)
        sl = float(evt.get("sl", 0) or 0)
        side = str(evt.get("side", "")).upper()
        risk = abs(entry - sl)
        if risk <= 0 or side not in {"LONG", "SHORT"}:
            return evt
        mult = 1.0 if side == "LONG" else -1.0
        evt["tp1"] = float(entry + mult * risk * float(RSI_PULLBACK_SCALP_TP1_R))
        evt["tp2"] = float(entry + mult * risk * float(RSI_PULLBACK_SCALP_TP2_R))
        evt["tp3"] = float(entry + mult * risk * float(RSI_PULLBACK_SCALP_TP3_R))
        evt["trigger"] = "RSI_PULLBACK_SCALP"
        evt["trigger_label"] = "RSI Pullback Scalp"
        evt["fast_scalp_exit"] = True
        return evt

    def _get_late_confirm_reason(self, tf, side, df):
        tf_name = str(tf or "").lower()
        max_stretch = LATE_CONFIRM_MAX_EMA2_ATR_DISTANCE_BY_TF.get(tf_name)
        max_body = LATE_CONFIRM_MAX_BODY_ATR_BY_TF.get(tf_name)
        if max_stretch is None and max_body is None:
            return ""
        try:
            if df is None or df.empty:
                return ""
            curr = df.iloc[-1]
            close = float(curr.get("Close", 0) or 0)
            open_ = float(curr.get("Open", close) or close)
            atr_val = float(curr.get("ATR", 0) or 0)
            ema2 = float(curr.get("EMA2", 0) or 0)
            if close <= 0 or atr_val <= 0:
                return ""
            side = str(side or "").upper()
            if max_stretch is not None and ema2 > 0:
                stretch = ((close - ema2) / atr_val) if side == "LONG" else ((ema2 - close) / atr_val)
                if stretch > float(max_stretch):
                    return f"late chase: {stretch:.2f} ATR from EMA2"
            if max_body is not None:
                body_atr = abs(close - open_) / atr_val
                directional = (side == "LONG" and close > open_) or (side == "SHORT" and close < open_)
                if directional and body_atr > float(max_body):
                    return f"late chase: candle body {body_atr:.2f} ATR"
        except Exception:
            return ""
        return ""

    def _get_scalp_window_block_reason(self, tf, side, local_trend, local_trend_src, now=None):
        """
        Hard blockers for scalp OPEN/PREPARE visibility.
        We only suppress early alerts when the side is already invalid on hard structure.
        """
        if not WEEKEND_TRADING_ENABLED:
            check_now = now or datetime.now(timezone.utc)
            if check_now.weekday() >= 5:
                return "weekend trading disabled"

        divergence_note = self._get_opposite_divergence_note(side)
        if divergence_note:
            return divergence_note

        bos_guard = self._get_bos_guard_reason(tf, side)
        if bos_guard:
            return bos_guard

        local_side = self._trend_side(local_trend)
        if local_side and side != local_side:
            divergence_hits = self._get_same_side_divergence_hits(side)
            sweep_hits = self._get_recent_liquidity_sweep_hits(side, preferred_tf=tf)
            if divergence_hits or sweep_hits:
                return ""
            return f"local trend reversal ({local_trend} from {local_trend_src or tf})"

        macro_trend, macro_src = self._get_anchor_trend("1h")
        macro_side = self._trend_side(macro_trend)
        if macro_side and side != macro_side:
            divergence_hits = self._get_same_side_divergence_hits(side)
            sweep_hits = self._get_recent_liquidity_sweep_hits(side, preferred_tf=tf)
            if divergence_hits or sweep_hits:
                return ""
            return f"macro trend reversal ({macro_trend} from {macro_src or '1h'})"

        return ""

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

    def _has_active_opposite_signal(self, side, symbol=None):
        """Return the latest active opposite-side signal if one is still open."""
        if not BLOCK_OPPOSITE_SIDE_SIGNALS:
            return None
        if self._hedge_mode_enabled():
            return None
        wanted_symbol = str(symbol or SYMBOL).upper()
        opposite = "SHORT" if str(side).upper() == "LONG" else "LONG"
        terminal_statuses = {"SL", "TP3", "CLOSED", "ENTRY_CLOSE", "PROFIT_SL"}
        for sig in reversed(self.tracker.signals):
            sig_side = str(sig.get("side") or "").upper()
            sig_symbol = str(sig.get("symbol") or (sig.get("meta") or {}).get("symbol") or SYMBOL).upper()
            if sig_side != opposite or sig_symbol != wanted_symbol:
                continue
            execution = sig.get("execution") or {}
            status = str(sig.get("status") or "OPEN").upper()
            if execution:
                if execution.get("active"):
                    return sig
                if status in terminal_statuses:
                    continue
            elif status in terminal_statuses:
                continue
            else:
                return sig
        return None


    def run(self):
        """Main loop - fetches data and processes signals."""

        print(f"{'='*50}")
        print(f"  Ponch Signal System (v2)")
        print(f"  Symbol: {SYMBOL}")
        print(f"  Timeframes: {', '.join(SIGNAL_TIMEFRAMES)}")
        print(f"  Poll interval: {POLL_INTERVAL}s")
        print(f"  Chat: {CHAT_ID}")
        print(f"{'='*50}")

        tg.set_bot_commands()
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
            if not self._session_is_tradeable_today(s_name, datetime.now(timezone.utc)):
                continue
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
            # Keep tracker signals/execution info durable across restarts too.
            self.tracker.persist()

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
                "session_thread_message_ids": self.session_thread_message_ids,
                "confirmations": self.confirmations.to_dict(),
                "session_data": serializable_sessions,
                "last_levels_date": self.last_levels_date,
                "sent_signals": list(self.sent_signals),
                "sent_sessions": list(self.sent_sessions),
                "approach_alerts": self.approach_alerts,
                "last_funding_alert": self.last_funding_alert,
                "last_market_alert": self.last_market_alert,
                "last_summary_date": self.last_summary_date,
                "last_daily_report_date": self.last_daily_report_date,
                "last_session_thread_cleanup_date": self.last_session_thread_cleanup_date,
                "last_exec_snapshot_date": self.last_exec_snapshot_date,
                "last_liquidation_map_date": self.last_liquidation_map_date,
                "last_education_post_date": self.last_education_post_date,
                "last_education_post_slot": self.last_education_post_slot,
                "last_today_wins_batch_date": self.last_today_wins_batch_date,
                "pending_stop_liq_watches": self.pending_stop_liq_watches,
                "scenario_trade_cooldowns": self.scenario_trade_cooldowns,
                "education_post_index": self.education_post_index,
                "pending_exec_action": self.pending_exec_action,
                "last_exec_suggested_action": self.last_exec_suggested_action,
                "private_exec_focus": self.private_exec_focus,
                "group_chat_contexts": self.group_chat_contexts,
                "private_todo_items": self.private_todo_items,
                "signal_debug_stats": self.signal_debug_stats,
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

    def _track_session_thread_message_id(self, msg_id):
        try:
            msg_id = int(msg_id or 0)
        except Exception:
            return
        if msg_id <= 0:
            return
        existing = {int(x) for x in (self.session_thread_message_ids or []) if str(x).strip().isdigit()}
        if msg_id in existing:
            return
        self.session_thread_message_ids.append(msg_id)

    def _cleanup_sessions_thread_messages(self, today=None):
        target_chat = self._signal_chat_id()
        if not target_chat:
            self.session_thread_message_ids = []
            if today:
                self.last_session_thread_cleanup_date = today
            return

        cleaned = []
        for raw_id in list(self.session_thread_message_ids or []):
            try:
                msg_id = int(raw_id)
            except Exception:
                continue
            if msg_id <= 0:
                continue
            deleted = tg.delete_message(target_chat, msg_id)
            cleaned.append((msg_id, deleted))

        if cleaned:
            ok_count = sum(1 for _, deleted in cleaned if deleted)
            print(f"[SESSION] Daily cleanup removed {ok_count}/{len(cleaned)} session-thread messages.")

        self.session_thread_message_ids = []
        if today:
            self.last_session_thread_cleanup_date = today

    def _tick(self):
        """One iteration of the main loop."""
        now = datetime.now(timezone.utc)
        current_time = time.time()
        self._prune_stop_liq_watches(now)
        print(f"\n[{now.strftime('%H:%M:%S')} UTC] Fetching data...")

        # 1. Update Levels if new day
        self._update_levels_if_needed(now)

        now_local = now.astimezone(self.local_tz)
        if now_local.hour == 0 and now_local.minute == 0:
            batch_key = (now_local.date() - timedelta(days=1)).isoformat()
            if self.last_today_wins_batch_date != batch_key:
                sent_today_wins = self._send_today_wins_batch(now)
                print(f"  [TG] Today Wins midnight batch: {sent_today_wins} signal{'s' if sent_today_wins != 1 else ''}.")

        # 1.5 Scheduled Daily Summary (12:00 PM Local / 08:00 UTC)
        if now.hour == 8 and now.minute == 0:
            today_str = now.strftime("%d.%m.%Y")
            if self.last_summary_date != today_str:
                # Send summary for the last completed recap window (08:00 UTC -> 08:00 UTC)
                self._send_performance_summary(now)
                self.last_summary_date = today_str
                self._save_state()
            if self.last_daily_report_date != today_str:
                self._send_daily_report(now)
                self.last_daily_report_date = today_str
                self._save_state()
            if self.last_liquidation_map_date != today_str:
                self._send_liquidation_map_post()
                self.last_liquidation_map_date = today_str
                self._save_state()

        if now.minute == 0 and now.hour in {8, 12, 16}:
            slot_key = now.strftime("%d.%m.%Y %H")
            if self.last_education_post_slot != slot_key:
                self._send_member_education_post()
                self.last_education_post_slot = slot_key
                self.last_education_post_date = now.strftime("%d.%m.%Y")
                self._save_state()

        self._reconcile_public_execution_state_if_due(current_time)

        # 2. Fetch Global context
        self.last_oi = fetch_open_interest()
        self.last_liqs = fetch_liquidations()

        # 3. Fetch all timeframes (+1d/+1w for liquidity-pool and trend hierarchy context)
        fetch_tfs = list(dict.fromkeys(SIGNAL_TIMEFRAMES + ["1m", "1d", "1w"]))
        data = fetch_all_timeframes(timeframes=fetch_tfs)
        if not data: return
        self.latest_data = data

        # Build per-timeframe trend map for hierarchical gating.
        self.tf_trends = {}
        self.divergence_map = {}
        for tf_key in ["5m", "15m", "1h", "4h", "1d", "1w"]:
            df_tf = data.get(tf_key)
            if df_tf is not None and not df_tf.empty:
                self.tf_trends[tf_key] = detect_trend(df_tf)
                if tf_key in {"5m", "15m", "1h", "4h"}:
                    try:
                        df_div = calculate_channels(df_tf.copy())
                        df_div = calculate_momentum(df_div)
                        div_sigs = check_rsi_divergence(df_div, tf_key)
                        self.divergence_map[tf_key] = {
                            str(s.get("side")).upper() for s in div_sigs if s.get("side") and s.get("active", True)
                        }
                    except Exception:
                        self.divergence_map[tf_key] = set()

        self._refresh_position_snapshot_cards_if_due(now)

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
                            tg.send_market_alert(move_pct * 100, FAST_MOVE_WINDOW, past_p, curr_p, chat_id=self._signal_chat_id())
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

        self._maybe_execute_scenario_trade(now, current_time)

        # 5. Check Funding Rate
        if current_time - self.last_funding_check > FUNDING_CHECK_INTERVAL:
            self.last_funding_check = current_time
            rate = fetch_funding_rate()
            if rate is not None:
                if abs(rate) >= FUNDING_THRESHOLD:
                    if current_time - self.last_funding_alert > FUNDING_COOLDOWN:
                        direction = "POSITIVE" if rate > 0 else "NEGATIVE"
                        if not self.is_booting:
                            tg.send_funding_alert(rate, direction, chat_id=self._signal_chat_id())
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
            if current_time - float(self.last_liq_heatmap_capture or 0) >= 300:
                try:
                    build_liquidation_map_snapshot(symbol=SYMBOL)
                    self.last_liq_heatmap_capture = current_time
                except Exception as e:
                    print(f"[LIQMAP] history capture failed: {e}")
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
                    # 1. Trend Alignment: confluence must not fight either local
                    # (15m-led) or macro (1h-led) directional anchors.
                    local_conf_trend, local_conf_src = self._get_anchor_trend("15m")
                    macro_conf_trend, macro_conf_src = self._get_anchor_trend("1h")
                    blocked_trends = []
                    for trend_name, trend_src in (
                        (local_conf_trend, local_conf_src),
                        (macro_conf_trend, macro_conf_src),
                    ):
                        trend_side = self._trend_side(trend_name)
                        if trend_side and side != trend_side:
                            blocked_trends.append(f"{trend_name} ({trend_src or 'N/A'})")

                    if blocked_trends:
                        self._record_signal_block("confluence_trend_guard", now=now)
                        print(
                            f"  [CONFLUENCE] Blocked {side} {ce['type']}: "
                            f"against trend {', '.join(blocked_trends)}"
                        )
                        continue

                    divergence_note = self._get_opposite_divergence_note(side)
                    if divergence_note:
                        self._record_signal_block("confluence_opposite_divergence", now=now)
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: {divergence_note}")
                        continue

                    # 1.5 Falling-knife / blow-off safety filter.
                    impulse_blocked, impulse_note = self._is_unstable_impulse(data, side)
                    if impulse_blocked:
                        self._record_signal_block("confluence_unstable_impulse", now=now)
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: {impulse_note}")
                        continue

                    fast_move_note = self._get_fast_move_volume_guard(data, side, preferred_tf="5m")
                    if fast_move_note:
                        self._record_signal_block("confluence_fast_move_volume", now=now)
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: {fast_move_note}")
                        continue

                    # 2. Momentum exhaustion guard: avoid SHORT when RSI already very low
                    # and LONG when RSI already very high.
                    if confluence_rsi is not None:
                        if side == "SHORT" and confluence_rsi <= (MOMENTUM_OS + CONFIRMATION_RSI_EXHAUSTION_BUFFER):
                            self._record_signal_block("confluence_rsi_exhausted", now=now)
                            print(
                                f"  [CONFLUENCE] Blocked SHORT {ce['type']}: "
                                f"RSI exhausted low ({confluence_rsi:.1f})"
                            )
                            continue
                        if side == "LONG" and confluence_rsi >= (MOMENTUM_OB - CONFIRMATION_RSI_EXHAUSTION_BUFFER):
                            self._record_signal_block("confluence_rsi_exhausted", now=now)
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

                    opposite_sig = self._has_active_opposite_signal(side, SYMBOL)
                    if opposite_sig:
                        print(
                            f"  [CONFLUENCE] Blocked {side}: opposite active "
                            f"{opposite_sig.get('side')} [{opposite_sig.get('tf', 'N/A')}] still open"
                        )
                        continue

                    # 3. Calculate targets from timeframe ATR model.
                    risk_cfg = TIMEFRAME_RISK_MULTIPLIERS.get(tf, TIMEFRAME_RISK_MULTIPLIERS.get("5m", {}))
                    sl_m = float(risk_cfg.get("sl", 1.0))
                    tp1_m = float(risk_cfg.get("tp1", 1.0))
                    tp2_m = float(risk_cfg.get("tp2", 1.8))
                    tp3_m = float(risk_cfg.get("tp3", 2.5))
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
                        strong_size = round(
                            min(MAX_SIGNAL_SIZE_PCT, max(MIN_SIGNAL_SIZE_PCT, ce["points"] * 1.5)),
                            1,
                        )
                        tp_liq = self._estimate_tp_liquidity(side, latest_price, tp1_c, tp2_c, tp3_c)
                        signal_id = new_signal_id()
                        self.tracker.log_signal(
                            side=ce["side"], entry=latest_price, sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            tf="Confluence", timestamp=base_candle_ts or conf_ts,
                            msg_id=None, chat_id=self._signal_chat_id(), signal_type="STRONG",
                            meta={
                                "signal_id": signal_id,
                                "indicators": ce["indicators"],
                                "size": strong_size,
                                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                            }
                        )
                        self.tracker.signals[-1]["signal_id"] = signal_id
                        self.tracker.signals[-1]["signal_size_pct"] = strong_size
                        self.tracker.signals[-1]["public_signal_is_photo"] = True
                        reinforcement_mode, owner_sig = self._prepare_same_side_signal_behavior(
                            self.tracker.signals[-1],
                            self._find_active_primary_same_side_execution(ce["side"], SYMBOL),
                        )
                        trading_resp = self._send_public_signal_snapshot(self.tracker.signals[-1]) if not self.is_booting else None
                        public_msg_id = trading_resp.get("result", {}).get("message_id") if trading_resp else None
                        self.tracker.signals[-1]["msg_id"] = public_msg_id
                        self.tracker.signals[-1]["trading_signal_msg_id"] = public_msg_id
                        if reinforcement_mode == "normal":
                            self._send_active_trade_snapshot(self.tracker.signals[-1])
                        else:
                            self._refresh_public_signal_snapshot(owner_sig, event_label="REINFORCED")
                            self._refresh_active_trade_snapshot(owner_sig, event_label="REINFORCED")
                        self._record_signal_sent("STRONG", now=now)
                        if reinforcement_mode in {"normal", "add"}:
                            self._execute_exchange_trade(self.tracker.signals[-1])
                        self._save_state()
                        print(f"  [CONFLUENCE] STRONG {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                    elif ce["type"] == "EXTREME":
                        extreme_size = round(
                            min(MAX_SIGNAL_SIZE_PCT, max(MIN_SIGNAL_SIZE_PCT, ce["points"] * 2.0)),
                            1,
                        )
                        tp_liq = self._estimate_tp_liquidity(side, latest_price, tp1_c, tp2_c, tp3_c)
                        signal_id = new_signal_id()
                        self.tracker.log_signal(
                            side=ce["side"], entry=latest_price, sl=sl_c, tp1=tp1_c, tp2=tp2_c, tp3=tp3_c,
                            tf="Confluence", timestamp=base_candle_ts or conf_ts,
                            msg_id=None, chat_id=self._signal_chat_id(), signal_type="EXTREME",
                            meta={
                                "signal_id": signal_id,
                                "indicators": ce["indicators"],
                                "size": extreme_size,
                                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                            }
                        )
                        self.tracker.signals[-1]["signal_id"] = signal_id
                        self.tracker.signals[-1]["signal_size_pct"] = extreme_size
                        self.tracker.signals[-1]["public_signal_is_photo"] = True
                        reinforcement_mode, owner_sig = self._prepare_same_side_signal_behavior(
                            self.tracker.signals[-1],
                            self._find_active_primary_same_side_execution(ce["side"], SYMBOL),
                        )
                        trading_resp = self._send_public_signal_snapshot(self.tracker.signals[-1]) if not self.is_booting else None
                        public_msg_id = trading_resp.get("result", {}).get("message_id") if trading_resp else None
                        self.tracker.signals[-1]["msg_id"] = public_msg_id
                        self.tracker.signals[-1]["trading_signal_msg_id"] = public_msg_id
                        if reinforcement_mode == "normal":
                            self._send_active_trade_snapshot(self.tracker.signals[-1])
                        else:
                            self._refresh_public_signal_snapshot(owner_sig, event_label="REINFORCED")
                            self._refresh_active_trade_snapshot(owner_sig, event_label="REINFORCED")
                        self._record_signal_sent("EXTREME", now=now)
                        if reinforcement_mode in {"normal", "add"}:
                            self._execute_exchange_trade(self.tracker.signals[-1])
                        self._save_state()
                        print(f"  [CONFLUENCE] EXTREME {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

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
            signal_tick_events = {}
            terminal_event_types = {"TP3", "ENTRY_CLOSE", "PROFIT_SL", "SL"}
            for event in trade_events:
                sig = event.get("sig") or {}
                sig_key = str(sig.get("signal_id") or (sig.get("meta") or {}).get("signal_id") or id(sig))
                signal_tick_events.setdefault(sig_key, []).append(str(event.get("type") or "").upper())
            for event in trade_events:
                sig = event["sig"]
                evt_type = event["type"] # "TP1", "TP2", "TP3", "SL"
                side = sig.get("side")
                risk_state_changed = False
                sig_key = str(sig.get("signal_id") or (sig.get("meta") or {}).get("signal_id") or id(sig))
                tick_event_types = signal_tick_events.get(sig_key, [])
                suppress_intermediate_notice = evt_type in {"TP1", "TP2"} and any(t in terminal_event_types for t in tick_event_types)

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
                # Always keep the original signal card in sync if it exists.
                private_exec_paper_signal = (
                    str(sig.get("chat_id") or "") == str(self._execution_chat_id() or "")
                    and not self._has_real_exchange_execution(sig)
                )
                current_public_signal = self._is_current_public_signal(sig)
                public_msg_id = self._public_signal_message_id(sig)

                if public_msg_id and current_public_signal and not private_exec_paper_signal:
                    tg.update_signal_message(
                        self._signal_chat_id(),
                        public_msg_id,
                        sig,
                        use_caption=bool(sig.get("public_signal_is_photo")),
                    )

                if evt_type in {"TP1", "TP2"} and current_public_signal and not private_exec_paper_signal:
                    self._refresh_active_trade_snapshot(sig, close_price=latest_price, event_label=evt_type)

                # Public reply alerts keep the original public behavior.
                if public_msg_id and current_public_signal and not private_exec_paper_signal and not suppress_intermediate_notice:
                    if evt_type == "TP1":
                        tg.send_tp1_hit_congrats(
                            self._signal_chat_id(),
                            public_msg_id,
                            sig.get("tf", "Unknown"),
                            side=sig.get("side"),
                            lock_price=sig.get("entry"),
                            entry=sig.get("entry"),
                            sl=sig.get("sl"),
                            tp1=sig.get("tp1"),
                            tp2=sig.get("tp2"),
                            size=(sig.get("meta", {}) or {}).get("size"),
                            message_thread_id=self._trading_signal_thread_id(),
                        )
                    elif evt_type == "TP2":
                        tg.send_tp2_hit_congrats(
                            self._signal_chat_id(),
                            public_msg_id,
                            sig.get("tf", "Unknown"),
                            side=sig.get("side"),
                            lock_price=(sig.get("execution") or {}).get("sl_moved_to") or sig.get("sl"),
                            entry=sig.get("entry"),
                            sl=sig.get("sl"),
                            tp1=sig.get("tp1"),
                            tp2=sig.get("tp2"),
                            size=(sig.get("meta", {}) or {}).get("size"),
                            single_full=self._is_single_full_tp_execution(sig),
                            message_thread_id=self._trading_signal_thread_id(),
                        )
                    elif evt_type == "TP3":
                        tg.send_tp3_hit_congrats(
                            self._signal_chat_id(),
                            public_msg_id,
                            sig.get("tf", "Unknown"),
                            message_thread_id=self._trading_signal_thread_id(),
                        )
                    elif evt_type == "ENTRY_CLOSE":
                        tg.send_breakeven_alert(
                            self._signal_chat_id(),
                            public_msg_id,
                            sig.get("tf", "Unknown"),
                            message_thread_id=self._trading_signal_thread_id(),
                        )
                    elif evt_type == "PROFIT_SL":
                        tg.send_profit_sl_alert(
                            self._signal_chat_id(),
                            public_msg_id,
                            sig.get("tf", "Unknown"),
                            message_thread_id=self._trading_signal_thread_id(),
                        )

                exec_chat = self._execution_chat_id()
                exec_msg_id = ((sig.get("execution") or {}).get("exec_msg_id"))
                if exec_chat and exec_msg_id:
                    try:
                        tg.update_signal_message(exec_chat, exec_msg_id, sig)
                    except Exception:
                        pass
                    if not suppress_intermediate_notice:
                        self._send_private_execution_lifecycle_reply(sig, evt_type)

                if risk_state_changed:
                    self._save_state()

                if evt_type in terminal_event_types and current_public_signal and not private_exec_paper_signal:
                    self._send_success_trade_post(sig, evt_type, close_price=latest_price)
                    self._cleanup_active_trade_messages(sig)

                if evt_type in ("TP1", "TP2", "TP3", "ENTRY_CLOSE", "PROFIT_SL", "SL") and not suppress_intermediate_notice:
                    self._sync_exchange_trade_event(sig, evt_type)

            # 2. Liquidation Squeezes
            if self.last_liqs >= LIQ_SQUEEZE_THRESHOLD:
                if current_time - self.last_liq_alert_time > LIQ_ALERT_COOLDOWN:
                    if not self.is_booting:
                        tg.send_squeeze_alert(self.last_liqs, latest_price, chat_id=self._signal_chat_id())
                    self.last_liq_alert_time = current_time
                    print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Liquidation Squeeze: ${self.last_liqs/1e6:.1f}M")

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
                                tg.send_oi_divergence(price_chg*100, oi_chg*100, note, chat_id=self._signal_chat_id())
                            self._save_state()
                            print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} OI Divergence: {note}")

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
                    if not self._session_is_tradeable_today(s_name, now):
                        continue
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
                            chat_id=self._signal_chat_id(),
                            message_thread_id=self._sessions_thread_id(),
                        )
                        
                        if resp and "response" in resp:
                            msg_data = resp["response"]
                            if msg_data and "result" in msg_data:
                                msg_id = msg_data["result"]["message_id"]
                                self._track_session_thread_message_id(msg_id)
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
                                summary_resp = tg.send_session_summary(
                                    s_name,
                                    open_p,
                                    latest_price,
                                    stats["total"],
                                    levels,
                                    history=history_text,
                                    high=s_high,
                                    low=s_low,
                                    chart_path=chart_path,
                                    chat_id=self._signal_chat_id(),
                                    message_thread_id=self._sessions_thread_id(),
                                )
                                if summary_resp and "result" in summary_resp:
                                    self._track_session_thread_message_id(summary_resp["result"].get("message_id"))
                            
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
                            res = tg.edit_message_media(info["msg_id"], chart_path, caption=new_html, chat_id=self._signal_chat_id())
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
                            tg.edit_message_media(d_msg_id, chart_path, caption=new_html, chat_id=self._signal_chat_id())
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
            if self.last_session_thread_cleanup_date != today:
                self._cleanup_sessions_thread_messages(today=today)
            self.daily_report_msg_id = None 
            self._update_levels()
            
            self.last_levels_date = today
            self.sent_signals.clear()  # Reset duplicate tracking
            self.session_history.clear() # Reset session history for new day
            self.session_msg_ids.clear() # Reset message IDs for new day
            self.session_thread_message_ids.clear()
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
                chat_id=self._signal_chat_id(),
                message_thread_id=self._important_thread_id(),
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

    def _send_text_chunks(self, chat_id, text, reply_to_message_id=None, message_thread_id=None, max_chars=3200):
        raw = str(text or "").strip()
        if not raw or not chat_id:
            return

        parts = [p.strip() for p in re.split(r"\n\s*\n", raw) if str(p).strip()]
        if not parts:
            parts = [raw]

        chunks = []
        current = ""
        for part in parts:
            candidate = part if not current else f"{current}\n\n{part}"
            if len(candidate) <= max_chars:
                current = candidate
                continue
            if current:
                chunks.append(current)
                current = ""
            if len(part) <= max_chars:
                current = part
                continue
            lines = [ln.rstrip() for ln in part.splitlines()]
            line_chunk = ""
            for line in lines:
                candidate_line = line if not line_chunk else f"{line_chunk}\n{line}"
                if len(candidate_line) <= max_chars:
                    line_chunk = candidate_line
                    continue
                if line_chunk:
                    chunks.append(line_chunk)
                    line_chunk = ""
                if len(line) <= max_chars:
                    line_chunk = line
                else:
                    for idx in range(0, len(line), max_chars):
                        chunks.append(line[idx:idx + max_chars])
            if line_chunk:
                current = line_chunk
        if current:
            chunks.append(current)

        for idx, chunk in enumerate(chunks):
            formatted = self._format_private_answer_for_telegram(chunk)
            if not formatted:
                continue
            tg.send(
                formatted,
                chat_id=chat_id,
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id if idx == 0 else None,
                message_thread_id=message_thread_id,
            )

    def _handle_btc_market_command(self, chat_id, reply_to_message_id=None, message_thread_id=None, mode="short_term"):
        if not chat_id:
            return False
        try:
            report = build_btc_market_report(symbol=SYMBOL, mode=mode)
            self._send_text_chunks(
                chat_id,
                report,
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
        except Exception as e:
            tg.send(
                f"BTC analysis failed: {html.escape(str(e))}",
                chat_id=chat_id,
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
        return True

    def _process_commands(self):
        """Fetch and handle incoming Telegram messages."""
        updates = tg.get_updates(offset=self.last_update_id + 1)
        if not updates or not updates.get("ok"):
            return

        for up in updates.get("result", []):
            self.last_update_id = up["update_id"]
            
            message = up.get("message") or up.get("channel_post")
            if not message:
                continue

            chat_obj = message.get("chat") or {}
            chat_id = chat_obj.get("id")
            message_id = message.get("message_id")
            message_thread_id = message.get("message_thread_id")

            if self._moderate_signal_group_message(message):
                self._save_state()
                continue

            if self._handle_restricted_topic_command(message):
                self._save_state()
                continue

            if "text" not in message:
                continue

            if self._handle_private_exec_message(message):
                self._save_state()
                continue

            user_id = (message.get("from") or {}).get("id") or (message.get("chat") or {}).get("id")
            chat_id = chat_id or user_id
            reply_to_message_id = message_id
            text = message["text"].strip()
            cmd = text.lower().split()[0] if text else ""
            cmd_base = cmd.split("@", 1)[0]
            
            if cmd_base == "/scenarios":
                if not self._is_scenarios_topic(chat_id, message_thread_id):
                    self._silence_restricted_command(message)
                    self._save_state()
                    continue
                self._handle_btc_market_command(
                    chat_id=chat_id,
                    reply_to_message_id=reply_to_message_id,
                    message_thread_id=message_thread_id,
                    mode="short_term",
                )
            elif cmd_base == "/liqmap":
                if not self._is_liqmap_topic(chat_id, message_thread_id):
                    self._silence_restricted_command(message)
                    self._save_state()
                    continue
                self._send_liquidation_map_post(chat_id=chat_id, message_thread_id=message_thread_id)
            elif text == "/start":
                welcome_msg = (
                    f"<b>How to Join:</b>\n\n"
                    f"1. Sign up on Bitunix to start trading:\n"
                    f"{BITUNIX_REG_LINK}\n\n"
                    f"2. <b>Send your unique UID here.</b>\n\n"
                    f"3. Once verified, you'll receive an invite link to join."
                )
                tg.send(welcome_msg, parse_mode="HTML", chat_id=user_id)
            elif cmd_base == "/analytics":
                if not self._is_analytics_topic(chat_id, message_thread_id):
                    self._silence_restricted_command(message)
                    self._save_state()
                    continue
                try:
                    days = 30
                    parts = text.split()
                    if len(parts) > 1 and parts[1].isdigit():
                        days = max(1, min(180, int(parts[1])))
                    stats = self.tracker.get_analytics(days=days)
                    totals = stats["totals"]
                    best_tf = stats.get("best_timeframe")
                    best_strategy = stats.get("best_strategy")

                    def fmt_best(item):
                        if not item:
                            return "n/a"
                        return (
                            f"{item['name']} | wr {float(item.get('win_rate', 0.0)):.1f}%"
                            f" | {int(item.get('trades', 0) or 0)} trades"
                        )

                    msg = (
                        f"📊 <b>Signal Analytics</b>\n"
                        f"<blockquote>{days} day performance snapshot</blockquote>\n\n"
                        f"<blockquote>"
                        f"Win rate: {totals['win_rate']:.1f}%\n"
                        f"Avg R: {totals['avg_r']:+.2f}\n"
                        f"Expectancy: {totals['expectancy_r']:+.2f}R"
                        f"</blockquote>\n\n"
                        f"<pre>"
                        f"Generated  {totals['generated']}\n"
                        f"Closed     {totals['trades']}\n"
                        f"Open       {totals['open']}\n"
                        f"Wins       {totals['wins']}\n"
                        f"Losses     {totals['losses']}\n"
                        f"Breakeven  {totals['breakeven']}"
                        f"</pre>\n\n"
                        f"🏆 <b>Best TF</b>\n"
                        f"<blockquote>{fmt_best(best_tf)}</blockquote>\n\n"
                        f"🧠 <b>Best Model</b>\n"
                        f"<blockquote>{fmt_best(best_strategy)}</blockquote>"
                    )
                    tg.send(
                        msg,
                        parse_mode="HTML",
                        chat_id=chat_id,
                        reply_to_message_id=reply_to_message_id,
                        message_thread_id=message_thread_id,
                    )
                except Exception as e:
                    tg.send(
                        f"Analytics failed: {e}",
                        chat_id=chat_id,
                        reply_to_message_id=reply_to_message_id,
                        message_thread_id=message_thread_id,
                    )

            elif self._handle_group_mention_message(message):
                self._save_state()
                continue

            elif self._handle_general_group_chat_message(message):
                self._save_state()
                continue

            elif text.isdigit():
                # User sent their UID
                uid = text
                print(f"[ONBOARDING] Checking UID: {uid} for user {user_id}")

                is_referral = verify_bitunix_user(uid)

                if not is_referral:
                    error_msg = (
                        f"Account verification failed.\n\n"
                        f"The UID you provided is not under this partner.\n"
                        f"Please sign up using this link:\n"
                        f"{BITUNIX_REG_LINK}"
                    )
                    tg.send(error_msg, parse_mode="HTML", chat_id=user_id)
                else:
                    success_msg = (
                        f"<b>Verification Successful!</b>\n\n"
                        f"Welcome to the team. You can now access our private signal channel:\n"
                        f"{INVITE_LINK}\n\n"
                        f"See you inside!"
                    )
                    tg.send(success_msg, parse_mode="HTML", chat_id=user_id)
            
            # Save state after each update to avoid re-processing on crash
            self._save_state()

    def _send_performance_summary(self, window_end=None):
        """Fetch and send the performance summary for the last completed recap window."""
        try:
            stats = self.tracker.get_daily_summary(window_end)
            if stats:
                if not self.is_booting:
                    tg.send_performance_summary(
                        stats,
                        chat_id=self._signal_chat_id(),
                        message_thread_id=self._trading_signal_thread_id(),
                    )
                else:
                    print(f"  [TG] Skipped sending performance summary (booting)")
            else:
                print(f"  [TRACKER] No signals found for the completed recap window. Skipping summary.")
            
            # Clean up old signals (keep 7 days)
            self.tracker.cleanup_old(365)
        except Exception as e:
            print(f"[TRACKER ERROR] Failed to send performance summary: {e}")

    def _execute_exchange_trade(self, sig_obj):
        """Route a confirmed signal into the Bitunix executor if trading is enabled."""
        if not self.trade_executor.can_trade():
            print(f"  [TRADE] Skipped signal {sig_obj.get('type')} {sig_obj.get('side')}: executor disabled")
            return
        symbol = str(sig_obj.get("symbol") or (sig_obj.get("meta") or {}).get("symbol") or SYMBOL).upper()
        side = str(sig_obj.get("side") or "").upper()
        if not self._is_hedge_signal(sig_obj):
            same_side_live = self._find_active_primary_same_side_execution(side, symbol)
            if same_side_live and not self._is_same_side_add_signal(sig_obj, same_side_live):
                print(
                    f"  [TRADE] Skipped same-side auto-trade {sig_obj.get('type')} {side}: "
                    f"active primary {same_side_live.get('side')} {same_side_live.get('tf')} already exists; "
                    f"opening another would merge into the same Bitunix position"
                )
                return
        if self._hedge_mode_enabled() and not self._is_hedge_signal(sig_obj):
            opposite_live = self._find_active_primary_opposite_execution(side, symbol)
            if opposite_live:
                print(
                    f"  [TRADE] Skipped opposite auto-trade {sig_obj.get('type')} {side}: "
                    f"active primary {opposite_live.get('side')} {opposite_live.get('tf')} already exists; "
                    f"only SMART_HEDGE may open opposite in hedge mode"
                )
                return
        open_counts = self.tracker.get_open_signal_counts(signal_type=sig_obj.get("type", "SCALP"))
        try:
            result = self.trade_executor.execute_signal(sig_obj, open_positions_count=int(open_counts.get("total", 0)))
        except Exception as e:
            print(f"  [TRADE] Execution error for {sig_obj.get('type')} {sig_obj.get('side')}: {e}")
            endpoint = getattr(e, "endpoint", None)
            response_text = getattr(e, "response_text", None)
            plan = getattr(e, "plan", None) or {}
            if endpoint:
                print(f"  [TRADE] Endpoint: {endpoint}")
            if response_text:
                print(f"  [TRADE] Exchange response: {response_text}")
            if plan:
                print(
                    f"  [TRADE] Failed plan: mode={self.trade_executor.mode} symbol={plan.get('symbol')} "
                    f"side={plan.get('side', sig_obj.get('side'))} qty={float(plan.get('qty', 0) or 0):.6f} "
                    f"notional={float(plan.get('notional', 0) or 0):.4f} "
                    f"position_mode={plan.get('position_mode')} leverage={int(plan.get('leverage', 0) or 0)}"
                )
                print(
                    f"  [TRADE] Risk qty={float(plan.get('risk_qty', 0) or 0):.6f} "
                    f"affordable_qty={float(plan.get('affordable_qty', 0) or 0):.6f} "
                    f"affordable_notional={float(plan.get('affordable_notional', 0) or 0):.4f}"
                )
                if plan.get("required_margin_mode") or plan.get("margin_mode"):
                    print(
                        f"  [TRADE] Margin mode: current={plan.get('margin_mode')} "
                        f"required={plan.get('required_margin_mode')}"
                    )
                if "pre_liq_estimate" in plan or "pre_liq_reason" in plan:
                    print(
                        f"  [TRADE] Pre-liq safety: safe={plan.get('pre_liq_safe')} "
                        f"est_liq={plan.get('pre_liq_estimate')} note={plan.get('pre_liq_reason')}"
                    )
                if "liq_price" in plan or "liq_reason" in plan:
                    print(
                        f"  [TRADE] Liquidation safety: safe={plan.get('liq_safe')} "
                        f"liq_price={plan.get('liq_price')} note={plan.get('liq_reason')}"
                    )
                if plan.get("entry_order_id") or plan.get("entry_client_id"):
                    print(
                        f"  [TRADE] Entry order refs: order_id={plan.get('entry_order_id')} "
                        f"client_id={plan.get('entry_client_id')}"
                    )
                if plan.get("entry_status") or plan.get("entry_trade_qty") is not None:
                    print(
                        f"  [TRADE] Entry detail: status={plan.get('entry_status')} "
                        f"trade_qty={plan.get('entry_trade_qty')}"
                    )
            self._send_private_execution_notice(
                f"Exchange Error: {sig_obj.get('type')} {sig_obj.get('side')}",
                self._format_execution_lines(
                    sig_obj,
                    extra=[
                        f"I could not open it. {str(e)}",
                    ],
                ),
            )
            return

        status = "accepted" if result.accepted else "blocked"
        print(f"  [TRADE] {status.upper()} {sig_obj.get('type')} {sig_obj.get('side')}: {result.message}")
        details = result.payload or {}
        merged_owner_sig = None
        if result.accepted and result.mode == "live" and details.get("position_id"):
            merged_owner_sig = self._find_position_owner_signal(
                details.get("position_id"),
                exclude_signal_id=self._signal_id_value(sig_obj),
            )
            if merged_owner_sig:
                merged_owner_id = self._signal_id_value(merged_owner_sig)
                print(
                    f"  [TRADE] MERGED into existing position_id={details.get('position_id')} "
                    f"owner_signal_id={merged_owner_id}"
                )
                result.payload = self._mark_execution_as_merge_shadow(result.payload, merged_owner_sig)
                if self._is_same_side_add_signal(sig_obj, merged_owner_sig):
                    result.payload["same_side_add"] = True
                    result.payload["same_side_add_owner_signal_id"] = merged_owner_id
                    self._record_same_side_add_execution(merged_owner_sig, sig_obj)
                    rebuild_result = self.trade_executor.rebuild_position_protection(
                        merged_owner_sig,
                        reason="same-side add merge",
                    )
                    if rebuild_result.accepted:
                        merged_owner_sig["execution"] = rebuild_result.payload or (merged_owner_sig.get("execution") or {})
                        print(f"  [TRADE] {rebuild_result.message}")
                    else:
                        print(f"  [TRADE] Protection rebuild skipped: {rebuild_result.message}")
                    try:
                        self.tracker.persist()
                    except Exception:
                        pass
                    self._refresh_public_signal_snapshot(merged_owner_sig)
                    self._refresh_active_trade_snapshot(merged_owner_sig, event_label="REINFORCED")
                details = result.payload or {}
        actual_size_pct = details.get("signal_size_pct")
        try:
            actual_size_pct = float(actual_size_pct)
        except Exception:
            actual_size_pct = 0.0
        if actual_size_pct > 0:
            sig_obj["signal_size_pct"] = actual_size_pct
            meta = sig_obj.setdefault("meta", {})
            meta["size"] = actual_size_pct
            try:
                self.tracker.persist()
            except Exception:
                pass
        if "exchange_open_positions" in details:
            print(f"  [TRADE] Exchange open positions: {int(details.get('exchange_open_positions', 0) or 0)}")
        if details:
            print(
                f"  [TRADE] Plan: mode={result.mode} symbol={details.get('symbol')} side={details.get('side', sig_obj.get('side'))} "
                f"entry={float(details.get('entry', sig_obj.get('entry', 0)) or 0):.2f} "
                f"sl={float(details.get('sl', sig_obj.get('sl', 0)) or 0):.2f} "
                f"tp1={float(details.get('tp1', sig_obj.get('tp1', 0)) or 0):.2f} "
                f"tp2={float(details.get('tp2', sig_obj.get('tp2', 0)) or 0):.2f} "
                f"tp3={float(details.get('tp3', sig_obj.get('tp3', 0)) or 0):.2f}"
            )
        if "balance_available" in details:
            print(
                f"  [TRADE] Balance={float(details.get('balance_available', 0) or 0):.2f} "
                f"risk_budget={float(details.get('risk_budget_usd', 0) or 0):.2f} "
                f"configured_risk={float(details.get('configured_risk_budget_usd', 0) or 0):.2f} "
                f"signal_size={float(details.get('signal_size_pct', 0) or 0):.2f}% "
                f"qty={float(details.get('qty', 0) or 0):.6f} "
                f"notional={float(details.get('notional', 0) or 0):.4f}"
            )
            if details.get("position_mode"):
                print(
                    f"  [TRADE] PositionMode={details.get('position_mode')} "
                    f"margin_mode={details.get('margin_mode')} "
                    f"leverage={int(details.get('leverage', 0) or 0)} "
                    f"margin_budget={float(details.get('margin_budget_usd', 0) or 0):.2f} "
                    f"usage={float(details.get('margin_usage_pct', 0) or 0) * 100:.1f}% "
                    f"risk_qty={float(details.get('risk_qty', 0) or 0):.6f} "
                    f"affordable_qty={float(details.get('affordable_qty', 0) or 0):.6f} "
                    f"affordable_notional={float(details.get('affordable_notional', 0) or 0):.4f}"
                )
            if "pre_liq_estimate" in details or "pre_liq_reason" in details:
                print(
                    f"  [TRADE] Pre-liq safety: safe={details.get('pre_liq_safe')} "
                    f"est_liq={details.get('pre_liq_estimate')} note={details.get('pre_liq_reason')}"
                )
            if "liq_price" in details or "liq_reason" in details:
                print(
                    f"  [TRADE] Liquidation safety: safe={details.get('liq_safe')} "
                    f"liq_price={details.get('liq_price')} note={details.get('liq_reason')}"
                )
            if details.get("endpoint") or details.get("response_text") or details.get("error"):
                print(f"  [TRADE] Balance check error: {details.get('error')}")
                if details.get("endpoint"):
                    print(f"  [TRADE] Balance endpoint: {details.get('endpoint')}")
                if details.get("response_text"):
                    print(f"  [TRADE] Balance response: {details.get('response_text')}")
            if float(details.get("balance_available", 0) or 0) < 5:
                print("  [TRADE] Tiny balance mode: size is derived only from current balance and risk cap.")
        if result.mode == "demo" and result.accepted:
            print("  [TRADE] Demo mode only: no real Bitunix orders were sent.")
        if result.mode == "live" and result.accepted:
            exec_info = details
            print(
                f"  [TRADE] Live orders: position_id={exec_info.get('position_id')} "
                f"tp_orders={len(exec_info.get('tp_orders', []) or [])} "
                f"sl_order={bool(exec_info.get('sl_order'))}"
            )
            if "protection_ready" in exec_info:
                print(f"  [TRADE] Protection ready: {bool(exec_info.get('protection_ready'))}")
            for warning in exec_info.get("protection_warnings", []) or []:
                print(f"  [TRADE] Protection warning: {warning}")
        current_sig = dict(sig_obj or {})
        if result.accepted and result.payload:
            current_sig["execution"] = result.payload
        if result.accepted:
            if merged_owner_sig:
                self._send_private_execution_notice(
                    f"Exchange Merged: {sig_obj.get('type')} {sig_obj.get('side')}",
                    [
                        f"{'🟢' if str(sig_obj.get('side') or '').upper() == 'LONG' else '🔴'} {sig_obj.get('type', 'Signal')} {sig_obj.get('side', 'N/A')} [{sig_obj.get('tf', 'N/A')}] merged into the existing Bitunix position.",
                        "Bitunix Position ID:",
                        f"<pre>{details.get('position_id')}</pre>",
                    ],
                    icon="🔗",
                )
            else:
                exec_resp = self._send_private_execution_signal_card(current_sig)
                exec_msg_id = exec_resp.get("result", {}).get("message_id") if exec_resp else None
                if exec_msg_id:
                    result.payload["exec_msg_id"] = exec_msg_id
                    current_sig["execution"] = result.payload
                self._send_private_execution_position_id_reply(current_sig, merged=False)
        else:
            self._send_private_execution_notice(
                f"Exchange {status.title()}: {sig_obj.get('type')} {sig_obj.get('side')}",
                self._format_execution_lines(
                    sig_obj,
                    extra=[f"I could not open it. {result.message}"],
                ),
                icon="⚠️",
            )
        if result.accepted and result.payload:
            sig_obj["execution"] = result.payload
            self._save_state()

    def _sync_exchange_trade_event(self, sig_obj, event_type):
        """Apply TP/SL lifecycle changes to exchange-side protection orders."""
        execution = sig_obj.get("execution") or {}
        if not execution:
            return
        if execution.get("merge_shadow"):
            print(
                f"  [TRADE] Skip merged shadow sync for {event_type}: "
                f"signal_id={self._signal_id_value(sig_obj)} position_id={execution.get('position_id')}"
            )
            return
        try:
            result = self.trade_executor.sync_outcome(sig_obj, event_type)
        except Exception as e:
            print(f"  [TRADE] Sync error for {event_type} on {sig_obj.get('type')} {sig_obj.get('side')}: {e}")
            endpoint = getattr(e, "endpoint", None)
            response_text = getattr(e, "response_text", None)
            if endpoint:
                print(f"  [TRADE] Endpoint: {endpoint}")
            if response_text:
                print(f"  [TRADE] Exchange response: {response_text}")
            self._send_private_execution_notice(
                f"Sync Error: {event_type} {sig_obj.get('side')}",
                self._format_execution_lines(
                    sig_obj,
                    extra=[
                        str(e),
                    ],
                ),
            )
            return
        print(f"  [TRADE] {event_type}: {result.message}")
        if str(result.message or "").strip().lower() == "execution already inactive.":
            if result.payload:
                sig_obj["execution"] = result.payload
                self._save_state()
            return
        if result.payload:
            sig_obj["execution"] = result.payload
            self._save_state()


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
        zone       = classify_momentum_zone(float(curr["MomentumSmooth"]) if "MomentumSmooth" in curr else 50, tf)
        rsi_raw    = float(curr["RSI"]) if "RSI" in curr else 50
        rsi_smooth = float(curr["MomentumSmooth"]) if "MomentumSmooth" in curr else 50
        # Local trend anchor by hierarchy (5m/15m -> 15m, then 1h->4h->1d->1w).
        local_trend, local_trend_src = self._get_anchor_trend(tf)

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
                if not self._session_is_tradeable_today(s_name, now):
                    continue
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
                        chat_id=self._signal_chat_id()
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
                            chat_id=self._signal_chat_id()
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
                        tg.send_liquidity_sweep(**sw, chat_id=self._signal_chat_id())
                    
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
                        tg.send_volatility_touch(**vt, chat_id=self._signal_chat_id())
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

        divergence_sides = set()
        div_sigs = check_rsi_divergence(df, tf)
        divergence_sides = {str(s.get("side")).upper() for s in div_sigs if s.get("active", True) and s.get("side")}
        # 5m is scalp-only: skip non-scalp confirmation engines on this timeframe.
        if tf != "5m":
            # 1. Trade Signals (Channels)
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
        # 5. RSI divergence confirmation
        for sig in div_sigs:
            if not sig.get("active", True):
                continue
            sig_key = f"rsi_div_{tf}_{sig['side']}_{candle_ts}"
            if sig_key not in self.sent_signals:
                self.sent_signals.add(sig_key)
                sig["tf"] = tf
                self.confirmations.add_signal(sig)

        # в”Ђв”Ђв”Ђ Scalp Momentum System в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
        tracker = self.scalp_trackers[tf]
        events = []
        if SMART_MONEY_ENABLED and tf in SMART_MONEY_EXECUTION_TFS:
            smart_money_trades = self.tracker.count_signals_for_day(
                strategy="SMART_MONEY_LIQUIDITY",
                signal_type="SCALP",
                now_utc=now,
            )
            sm_evt = detect_smart_money_entry(
                self.latest_data or {},
                self.levels or {},
                now,
                trades_today=smart_money_trades,
                execution_tf=tf,
            )
            if sm_evt:
                events.append(sm_evt)
        if tf in BASE_MOMENTUM_ENABLED_TFS:
            events.extend(tracker.update(zone, close, atr_val, candle_ts=candle_ts, rsi_raw=rsi_raw, rsi_smooth=rsi_smooth))
        htf_evt = check_htf_pullback_entry(df, tf)
        if htf_evt:
            events.append(htf_evt)
        one_h_evt = check_one_h_reclaim_entry(df, tf)
        if one_h_evt:
            events.append(one_h_evt)

        profile = TIMEFRAME_PROFILES.get(tf, TIMEFRAME_PROFILES["5m"])
        emoji = profile["emoji"]

        for evt in events:
            # Scalp signals usually only one of each type per candle
            evt_signature = evt.get("event_id", candle_ts)
            evt_key = f"scalp_{tf}_{evt['type']}_{evt['side']}_{evt_signature}"

            if evt_key in self.sent_signals:
                continue

            self.sent_signals.add(evt_key)

            if evt["type"] == "OPEN":
                block_reason = self._get_scalp_window_block_reason(tf, evt["side"], local_trend, local_trend_src, now=now)
                if block_reason:
                    self._record_signal_suppressed("open_window_guard", now=now)
                    print(f"  [SCALP] Suppressed Open [{tf}] {evt['side']}: {block_reason}")
                    continue
                open_key = f"{tf}_{evt['side']}"
                last_open_alert_ts = self.last_scalp_open_alert.get(open_key, 0)
                can_send_open = (current_time - last_open_alert_ts) >= SCALP_OPEN_ALERT_COOLDOWN

                if not self.is_booting and can_send_open:
                    tg.send_scalp_open(tf, evt["side"], evt["price"], emoji=emoji, chat_id=self._signal_chat_id())
                    self.last_scalp_open_alert[open_key] = current_time
                self._save_state()
                if self.is_booting:
                    print(f"  [TG] Skipped Scalp Open [{tf}] {evt['side']} (booting)")
                elif not can_send_open:
                    self._record_signal_suppressed("open_cooldown", now=now)
                    print(f"  [SCALP] Suppressed Open [{tf}] {evt['side']} (cooldown)")
                else:
                    print(f"  [TG] Sent Scalp Open [{tf}] {evt['side']}")

            elif evt["type"] == "PREPARE":
                block_reason = self._get_scalp_window_block_reason(tf, evt["side"], local_trend, local_trend_src, now=now)
                if block_reason:
                    self._record_signal_suppressed("prepare_window_guard", now=now)
                    print(f"  [SCALP] Suppressed Prepare [{tf}] {evt['side']}: {block_reason}")
                    continue
                if not self.is_booting:
                    tg.send_scalp_prepare(tf, evt["side"], emoji=emoji, chat_id=self._signal_chat_id())
                self._save_state()
                print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Prepare [{tf}] {evt['side']}")

            elif evt["type"] == "CONFIRMED":
                trigger_label = str(evt.get("trigger_label") or evt.get("trigger") or "Momentum Exit")
                if str(evt.get("strategy", "")).upper() == "SMART_MONEY_LIQUIDITY":
                    score = max(8, len(evt.get("reasons", [])) + 3)
                    reasons = list(evt.get("reasons", []))
                else:
                    # --- Calculate Signal Strength Score ---
                    score, reasons = calculate_signal_score(
                        evt, df, self.levels, self.macro_trend, self.last_oi, self.last_liqs
                    )
                core_score = int(score)
                side = evt["side"]
                reversal_override_allowed, reversal_override_note = self._get_reversal_override(
                    tf, side, evt=evt, score=score
                )
                rsi_pullback_override, rsi_pullback_note = self._get_rsi_pullback_scalp_override(tf, side)
                weekend_exception_allowed, weekend_exception_note = self._get_weekend_scalp_exception(tf, side)
                if weekend_exception_allowed:
                    rsi_pullback_override = True
                    rsi_pullback_note = weekend_exception_note
                if rsi_pullback_override:
                    reversal_override_allowed = True
                    reversal_override_note = rsi_pullback_note
                    evt = self._apply_rsi_pullback_fast_targets(evt)
                    trigger_label = str(evt.get("trigger_label") or evt.get("trigger") or trigger_label)
                    if "RSI Pullback Scalp" not in reasons:
                        reasons.append("RSI Pullback Scalp")

                if not WEEKEND_TRADING_ENABLED and now.weekday() >= 5 and not weekend_exception_allowed:
                    self._record_signal_block("weekend_block", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: weekend trading disabled")
                    continue

                divergence_note = self._get_opposite_divergence_note(side)
                if divergence_note:
                    self._record_signal_block("opposite_divergence", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: {divergence_note}")
                    continue

                bos_guard_reason = self._get_bos_guard_reason(tf, side)
                if bos_guard_reason and not rsi_pullback_override:
                    self._record_signal_block("bos_guard", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: {bos_guard_reason}")
                    continue

                impulse_blocked, impulse_note = self._is_unstable_impulse(self.latest_data or {}, side)
                if impulse_blocked:
                    self._record_signal_block("unstable_impulse", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: {impulse_note}")
                    continue

                fast_move_note = self._get_fast_move_volume_guard(self.latest_data or {}, side, preferred_tf=tf)
                if fast_move_note:
                    self._record_signal_block("fast_move_volume", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: {fast_move_note}")
                    continue

                late_confirm_reason = self._get_late_confirm_reason(tf, side, df)
                if late_confirm_reason:
                    self._record_signal_block("late_confirm", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: {late_confirm_reason}")
                    continue

                liq_watch_note = self._stop_liq_watch_trigger_note(evt, tf, df, now)
                if liq_watch_note:
                    score += 1
                    reasons.append("Sweep Reclaim Watch")
                    print(f"  [SCALP] Allowed {tf} {side}: {liq_watch_note}")

                stop_liq_hazard = self._get_stop_liquidity_hazard(evt, tf, df)
                if stop_liq_hazard and not rsi_pullback_override and not liq_watch_note:
                    self._register_stop_liq_watch(evt, tf, stop_liq_hazard, now)
                    self._record_signal_block("stop_liquidity_hazard", now=now)
                    print(f"  [SCALP] Blocked {tf} {side}: {stop_liq_hazard.get('reason')}")
                    continue

                # Hard local-trend reversal guard (hierarchical source):
                # do not SHORT in bullish local trend, do not LONG in bearish local trend.
                local_side = self._trend_side(local_trend)
                if local_side and side != local_side:
                    if reversal_override_allowed:
                        print(
                            f"  [SCALP] Allowed {tf} {side}: reversal override "
                            f"({reversal_override_note})"
                        )
                    else:
                        self._record_signal_block("local_trend_reversal", now=now)
                        print(
                            f"  [SCALP] Blocked {tf} {side}: local trend reversal "
                            f"({local_trend} from {local_trend_src or tf})"
                        )
                        continue

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

                session_name = self._get_current_session_name(now) or "OFF_SESSION"
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
                    self._record_signal_block("side_cooldown", now=now)
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
                        self._record_signal_block("volatility_filter", now=now)
                        print(
                            f"  [SCALP] Blocked {tf} {side}: ATR% {atr_pct:.3f} "
                            f"outside [{min_pct:.3f}, {max_pct:.3f}]"
                        )
                        continue

                # --- Order-flow safety filter ---
                if ORDERFLOW_SAFETY_ENABLED:
                    anomaly, oi_pct = self._is_orderflow_anomaly()
                    if anomaly and score < ORDERFLOW_ANOMALY_SCORE_MIN:
                        self._record_signal_block("orderflow_anomaly", now=now)
                        print(
                            f"  [SCALP] Blocked {tf} {side}: order-flow anomaly "
                            f"(OI {oi_pct:.2f}%, LIQ ${self.last_liqs:,.0f}) and score {score}"
                        )
                        continue

                # --- Session whitelist per timeframe ---
                allowed_sessions = SCALP_ALLOWED_SESSIONS_BY_TF.get(tf)
                if allowed_sessions and session_name not in allowed_sessions and not (relaxed_filters and SCALP_RELAX_ALLOW_OFFSESSION):
                    self._record_signal_block("session_whitelist", now=now)
                    print(
                        f"  [SCALP] Blocked {tf} {side}: session {session_name} "
                        f"not in {allowed_sessions}"
                    )
                    continue

                news_blackout = self._get_active_news_block(now)
                if news_blackout:
                    self._record_signal_block("news_blackout", now=now)
                    label = str(news_blackout.get("label") or "High Impact News")
                    source = str(news_blackout.get("source") or "NEWS")
                    print(f"  [SCALP] Blocked {tf} {side}: news blackout active ({label} via {source})")
                    continue
                if tf == "5m":
                    news_blackout_5m = self._get_5m_strict_news_block(now)
                    if news_blackout_5m:
                        self._record_signal_block("strict_5m_news", now=now)
                        label = str(news_blackout_5m.get("label") or "High Impact News")
                        source = str(news_blackout_5m.get("source") or "NEWS")
                        print(f"  [SCALP] Blocked {tf} {side}: strict 5m news guard active ({label} via {source})")
                        continue

                opposite_sig = self._has_active_opposite_signal(side, SYMBOL)
                if opposite_sig:
                    self._record_signal_block("opposite_active_signal", now=now)
                    print(
                        f"  [SCALP] Blocked {tf} {side}: opposite active "
                        f"{opposite_sig.get('side')} [{opposite_sig.get('tf', 'N/A')}] still open"
                    )
                    continue
                if tf == "5m":
                    htf_guard_reason = self._get_5m_higher_tf_guard_reason(side)
                    if htf_guard_reason:
                        if reversal_override_allowed:
                            print(
                                f"  [SCALP] Allowed {tf} {side}: 5m reversal override "
                                f"({reversal_override_note})"
                            )
                        else:
                            self._record_signal_block("5m_higher_tf_guard", now=now)
                            print(f"  [SCALP] Blocked {tf} {side}: {htf_guard_reason}")
                            continue

                # --- Minimum score gate by timeframe ---
                min_score_tf = SCALP_MIN_SCORE_BY_TF.get(tf, 0)
                if relaxed_filters:
                    min_score_tf = max(0, int(min_score_tf) - int(SCALP_RELAX_MIN_SCORE_DELTA))
                min_score_tf = max(0, int(min_score_tf) + score_delta + tuning_delta)
                hard_min_score_tf = int(SCALP_HARD_MIN_SCORE_BY_TF.get(tf, 0))
                min_score_tf = max(min_score_tf, hard_min_score_tf)
                momentum_exit_min = int(SCALP_MOMENTUM_EXIT_MIN_SCORE_BY_TF.get(tf, 0))
                trigger_is_momentum_exit = str(trigger_label).strip().lower() == "momentum exit"
                if trigger_is_momentum_exit:
                    min_score_tf = max(min_score_tf, momentum_exit_min)
                    if core_score < momentum_exit_min:
                        self._record_signal_block("momentum_exit_core_score", now=now)
                        print(
                            f"  [SCALP] Blocked {tf} {side}: core score {core_score}<{momentum_exit_min} "
                            f"for Momentum Exit"
                        )
                        continue
                if score < min_score_tf:
                    self._record_signal_block("score_gate", now=now)
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
                        self._record_signal_block("exposure_total", now=now)
                        print(f"  [SCALP] Blocked {tf} {side}: total exposure {open_total}/{SCALP_MAX_OPEN_TOTAL}")
                        continue
                    if open_side >= SCALP_MAX_OPEN_PER_SIDE:
                        self._record_signal_block("exposure_side", now=now)
                        print(f"  [SCALP] Blocked {tf} {side}: side exposure {open_side}/{SCALP_MAX_OPEN_PER_SIDE}")
                        continue
                    if open_tf >= tf_limit:
                        self._record_signal_block("exposure_tf", now=now)
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
                        if reversal_override_allowed:
                            print(
                                f"  [SCALP] Allowed {tf} {side}: counter-trend override "
                                f"({reversal_override_note})"
                            )
                        elif relaxed_filters:
                            print(
                                f"  [SCALP] Relaxed override {tf} {side}: "
                                f"counter-trend allowed in hard mode vs {trend_name}"
                            )
                        else:
                            self._record_signal_block("countertrend_hard", now=now)
                            print(f"  [SCALP] Blocked {tf} {side}: counter-trend vs {trend_name} (mode=hard)")
                            continue
                    if filter_mode == "soft":
                        required_score = countertrend_min_score + session_score_boost
                        if score < required_score:
                            self._record_signal_block("countertrend_score", now=now)
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
                            self._record_signal_block("countertrend_quota", now=now)
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
                            self._record_signal_block("countertrend_quota", now=now)
                            print(
                                f"  [SCALP] Blocked {tf} {side}: "
                                f"relaxed hard-mode quota reached ({len(side_hits)}/"
                                f"{ct_limit} in {SCALP_COUNTERTREND_WINDOW_SEC}s)"
                            )
                            self.scalp_countertrend_hits[side] = side_hits
                            continue
                        side_hits.append(current_time)
                        self.scalp_countertrend_hits[side] = side_hits

                # Dynamic size: scale base size by score, with a global minimum size floor.
                if str(evt.get("strategy", "")).upper() == "SMART_MONEY_LIQUIDITY":
                    dyn_size = min(
                        float(MAX_SIGNAL_SIZE_PCT),
                        max(float(MIN_SIGNAL_SIZE_PCT), float(evt.get("size", SMART_MONEY_RISK_PCT))),
                    )
                else:
                    dyn_base = max(float(MIN_SIGNAL_SIZE_PCT), (score / 10) * profile["size"]) if score else max(float(MIN_SIGNAL_SIZE_PCT), profile["size"])
                    dyn_size = round(
                        min(float(MAX_SIGNAL_SIZE_PCT), max(float(MIN_SIGNAL_SIZE_PCT), dyn_base * size_mult)),
                        1,
                    )
                tp_liq = self._estimate_tp_liquidity(evt["side"], evt["entry"], evt["tp1"], evt["tp2"], evt["tp3"])
                signal_id = new_signal_id()

                self._save_state()
                sent_label = "Smart Money Confirmed" if str(evt.get("strategy", "")).upper() == "SMART_MONEY_LIQUIDITY" else "Scalp Confirmed"
                self._record_signal_sent("SMART_MONEY" if str(evt.get("strategy", "")).upper() == "SMART_MONEY_LIQUIDITY" else f"SCALP_{tf}", now=now)
                print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} {sent_label} [{tf}] {evt['side']} @ {evt['entry']:,.2f}")

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
                    msg_id=None,
                    chat_id=self._signal_chat_id(),
                    signal_type="SCALP",
                    meta={
                        "signal_id": signal_id,
                        "score": score,
                        "trend": self.macro_trend,
                        "trigger": trigger_label,
                        "strategy": evt.get("strategy", "MOMENTUM"),
                        "reasons": reasons,
                        "size": dyn_size,
                        "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                        "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                        "tp_liq_target": tp_liq["target"] if tp_liq else None,
                    }
                )
                self.tracker.signals[-1]["signal_id"] = signal_id
                self.tracker.signals[-1]["signal_size_pct"] = dyn_size
                self.tracker.signals[-1]["public_signal_is_photo"] = True
                reinforcement_mode, owner_sig = self._prepare_same_side_signal_behavior(
                    self.tracker.signals[-1],
                    self._find_active_primary_same_side_execution(evt["side"], SYMBOL),
                )
                trading_resp = self._send_public_signal_snapshot(self.tracker.signals[-1]) if not self.is_booting else None
                public_msg_id = trading_resp.get("result", {}).get("message_id") if trading_resp else None
                self.tracker.signals[-1]["msg_id"] = public_msg_id
                self.tracker.signals[-1]["trading_signal_msg_id"] = public_msg_id
                if reinforcement_mode == "normal":
                    self._send_active_trade_snapshot(self.tracker.signals[-1])
                else:
                    self._refresh_public_signal_snapshot(owner_sig, event_label="REINFORCED")
                    self._refresh_active_trade_snapshot(owner_sig, event_label="REINFORCED")
                self._save_state()
                if reinforcement_mode in {"normal", "add"}:
                    self._execute_exchange_trade(self.tracker.signals[-1])

            elif evt["type"] == "CLOSED":
                if not self.is_booting:
                    tg.send_scalp_closed(tf, evt["side"], evt["price"], emoji=emoji, chat_id=self._signal_chat_id())
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
    import time

    lock_file = os.path.join(os.path.dirname(__file__), "bot.lock")

    def _pid_is_running(pid):
        try:
            pid = int(pid)
        except Exception:
            return False
        if pid <= 0:
            return False
        if os.name == "nt":
            try:
                import ctypes
                PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                handle = ctypes.windll.kernel32.OpenProcess(
                    PROCESS_QUERY_LIMITED_INFORMATION,
                    False,
                    pid,
                )
                if not handle:
                    return False
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            except Exception:
                return False
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False

    if os.path.exists(lock_file):
        try:
            with open(lock_file, "r") as f:
                old_pid = int(f.read().strip())

            if _pid_is_running(old_pid) and (time.time() - os.path.getmtime(lock_file) < 60):
                print(f"\n[FATAL] Another instance (PID {old_pid}) is already running.")
                sys.exit(1)
            else:
                os.remove(lock_file)
        except Exception:
            if time.time() - os.getmtime(lock_file) < 60:
                print(f"\n[FATAL] Stale lock detected, but it's too fresh. Exiting.")
                sys.exit(1)
            else:
                try:
                    os.remove(lock_file)
                except Exception:
                    pass

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
