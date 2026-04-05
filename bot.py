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
    BASE_MOMENTUM_ENABLED_TFS,
    CONFIRMATION_RSI_EXHAUSTION_BUFFER, CONFLUENCE_OPPOSITE_LOCK_SEC,
    LIQ_POOL_ALERT_ENABLED, LIQ_POOL_MIN_USD, LIQ_POOL_ALERT_COOLDOWN,
    LIQ_POOL_BIAS_SCORE_BONUS, LIQ_POOL_MAX_DISTANCE_ATR_MULT,
    LIQ_POOL_MIN_DISTANCE_PCT, LIQ_POOL_HUGE_USD_OVERRIDE,
    TP_LIQUIDITY_MIN_USD, TP_LIQUIDITY_BAND_PCT,
    FALLING_KNIFE_FILTER_ENABLED, FALLING_KNIFE_LOOKBACK_5M, FALLING_KNIFE_LOOKBACK_15M,
    FALLING_KNIFE_MOVE_PCT_5M, FALLING_KNIFE_MOVE_PCT_15M,
    LIQ_POOL_REPORT_TIMEFRAMES, LIQ_POOL_MIN_USD_BY_TF, LIQ_POOL_MIN_DISTANCE_PCT_BY_TF,
    LIQ_POOL_TARGET_DISTANCE_PCT_BY_TF, LIQ_POOL_LEVEL_DEDUP_GAP_PCT,
    LIQ_POOL_PROGRESSIVE_MIN_STEP_PCT,
    LIQ_POOL_AGG_WINDOW_PCT_BY_TF,
    LIQ_POOL_NO_MOVE_RANGE_PCT_1H, LIQ_POOL_EXPANSION_PRICE_MOVE_PCT_1H,
    LIQ_POOL_EXPANSION_VOLUME_MULT, LIQ_POOL_EXPANSION_BOOK_MULT, LIQ_POOL_EXPANSION_COOLDOWN,
    SMART_MONEY_ENABLED, SMART_MONEY_EXECUTION_TFS, SMART_MONEY_RISK_PCT,
    MIN_SIGNAL_SIZE_PCT, PRIVATE_EXEC_CHAT_ID, EXECUTION_UPDATES_PRIVATE_ONLY,
    PRIVATE_EXEC_AI_CONTROL_ENABLED, PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC,
    GEMINI_API_KEY, GEMINI_MODEL, TIMEFRAME_RISK_MULTIPLIERS,
    NEWS_FILTER_ENABLED, get_active_news_blackout, is_ny_market_holiday,
    TRADING_ECONOMICS_NEWS_ENABLED, TRADING_ECONOMICS_API_KEY,
    TRADING_ECONOMICS_COUNTRIES, TRADING_ECONOMICS_MIN_IMPORTANCE,
    TRADING_ECONOMICS_REFRESH_SEC, TRADING_ECONOMICS_BLOCK_BEFORE_MIN,
    TRADING_ECONOMICS_BLOCK_AFTER_MIN
)
from levels import calculate_levels, check_liquidity_sweep, check_volatility_touch
from channels import calculate_channels, check_channel_signals
from momentum import calculate_momentum, ScalpTracker, detect_trend, classify_momentum_zone, check_htf_pullback_entry, check_one_h_reclaim_entry
from scoring import calculate_signal_score
from signals import check_momentum_confirm, check_range_confirm, check_flow_confirm, check_rsi_divergence
from confirmation import ConfirmationTracker
from charting import generate_daily_levels_chart
from data import (
    fetch_klines, fetch_all_timeframes, fetch_daily, fetch_weekly, fetch_monthly, 
    fetch_funding_rate, fetch_open_interest, fetch_liquidations, fetch_global_indicators, fetch_order_book,
    fetch_trading_economics_calendar, parse_gemini_trade_instruction, ask_gemini_trade_question,
    fetch_last_price
)
from tracker import SignalTracker
from bitunix import verify_bitunix_user
from bitunix_trade import TradeExecutor, new_signal_id
from liquidity_map import detect_liquidity_event, detect_liquidity_candidates
from smart_money import detect_smart_money_entry
import telegram as tg


class PonchBot:
    """Main Ponch Signal System bot."""

    def __init__(self):
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

        # ─── New Features ─────────────────────────────────
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
        self.last_daily_report_date = state.get("last_daily_report_date")
        self.last_exec_snapshot_date = state.get("last_exec_snapshot_date")
        self.pending_exec_action = state.get("pending_exec_action")
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
        print(
            f"[TRADE] Reconcile: matched={int(reconcile.get('matched', 0) or 0)} "
            f"inactive_marked={int(reconcile.get('inactive_marked', 0) or 0)} "
            f"orphans={len(reconcile.get('orphan_positions', []) or [])} "
            f"missing_protection={len(reconcile.get('missing_protection', []) or [])}"
        )
        for err in reconcile.get("errors", []):
            print(f"[TRADE] Reconcile detail: {err}")
        if self._execution_chat_id():
            startup_lines = self._build_execution_status_lines(
                trade_check=trade_check,
                reconcile=reconcile,
                now=datetime.now(timezone.utc),
            )
            self._send_private_execution_notice("Bitunix Startup Check", startup_lines)
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

    def _execution_chat_id(self):
        return PRIVATE_EXEC_CHAT_ID or PRIVATE_CHAT_ID

    def _execution_updates_private_only(self):
        return bool(EXECUTION_UPDATES_PRIVATE_ONLY and self._execution_chat_id())

    def _should_update_public_signal(self, sig):
        if not self._execution_updates_private_only():
            return True
        return str(sig.get("chat_id") or "") == str(self._execution_chat_id())

    def _send_private_execution_notice(self, title, lines=None, icon="🔐"):
        exec_chat = self._execution_chat_id()
        if not exec_chat:
            return
        if str(title or "").strip() == "Bitunix Startup Check":
            tg.send_execution_notice(title, lines=lines, chat_id=exec_chat, icon=icon)
            return
        body_lines = [str(line) for line in (lines or []) if str(line).strip()]
        title_text = str(title or "").strip()
        if title_text and title_text not in {"Exec Control", "Confirm Exec Action", "Exec Action Result"}:
            body_lines.insert(0, title_text)
        if not body_lines:
            body_lines = [title_text] if title_text else []
        if body_lines:
            tg.send("\n".join(body_lines), chat_id=exec_chat, parse_mode="HTML")

    def _send_private_execution_answer(self, text):
        exec_chat = self._execution_chat_id()
        if not exec_chat:
            return
        answer = str(text or "").strip()
        if not answer:
            return
        tg.send(answer, chat_id=exec_chat, parse_mode="HTML")

    def _is_private_exec_chat(self, chat_id):
        exec_chat = str(self._execution_chat_id() or "").strip()
        return bool(exec_chat and str(chat_id or "").strip() == exec_chat)


    def _refresh_private_execution_state(self):
        reconcile = self.trade_executor.reconcile_execution_state(self.tracker.signals)
        if reconcile.get("inactive_marked") or reconcile.get("state_updated"):
            self.tracker.persist()
        return reconcile

    def _active_execution_signals(self):
        active = []
        for sig in reversed(self.tracker.signals):
            execution = (sig or {}).get("execution") or {}
            if execution.get("active"):
                active.append(sig)
        return active

    def _recent_unexecuted_signals(self):
        pending = []
        for sig in reversed(self.tracker.signals):
            if str(sig.get("status", "")).upper() != "OPEN":
                continue
            execution = (sig or {}).get("execution") or {}
            if execution.get("active"):
                continue
            pending.append(sig)
        return pending

    def _control_signal_label(self, sig):
        signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (((sig.get("execution") or {}).get("signal_id")))
        return (
            f"id={signal_id or 'N/A'} type={sig.get('type', 'SCALP')} tf={sig.get('tf', 'N/A')} "
            f"side={sig.get('side', 'N/A')} entry={float(sig.get('entry', 0) or 0):.2f}"
        )

    def _current_market_price(self, tf_hint=None):
        preferred = []
        tf_name = str(tf_hint or "").strip()
        if tf_name:
            preferred.append(tf_name)
        preferred.extend([tf for tf in ["5m", "15m", "1h", "4h"] if tf not in preferred])
        for tf in preferred:
            df = self.latest_data.get(tf)
            try:
                if df is not None and not df.empty and "Close" in df.columns:
                    return float(df.iloc[-1]["Close"])
            except Exception:
                continue
        try:
            okx_price = fetch_last_price(SYMBOL)
            if okx_price and okx_price > 0:
                return float(okx_price)
        except Exception:
            pass
        return None

    def _position_live_metrics(self, sig, current_price_override=None):
        sig = sig or {}
        execution = sig.get("execution") or {}
        entry = float(sig.get("entry") or 0)
        qty = float(execution.get("qty", 0) or 0)
        side = str(sig.get("side") or "").upper()
        leverage = float(execution.get("leverage") or execution.get("target_leverage") or 0)
        try:
            current_price = float(current_price_override) if current_price_override is not None else self._current_market_price(sig.get("tf"))
        except Exception:
            current_price = self._current_market_price(sig.get("tf"))
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

    def _build_gemini_trade_context(self):
        self._refresh_private_execution_state()
        lines = ["Active exchange positions:"]
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
        return "\n".join(lines)

    def _resolve_signal_for_action(self, payload, *, allow_unexecuted=False):
        signal_id = str(payload.get("signal_id") or "").strip()
        side = str(payload.get("side") or "").strip().upper()
        tf = str(payload.get("tf") or "").strip()
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
            if side and str(sig.get("side", "")).upper() != side:
                continue
            if tf and str(sig.get("tf", "")) != tf:
                continue
            matches.append(sig)
        return matches[0] if matches else (pool[0] if pool else None)

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
        if kind == "cancel_tp":
            if int(action.get("tp_index") or 0) not in {1, 2, 3}:
                return "Cancel all take profits"
            return f"Cancel TP{int(action.get('tp_index') or 0)}"
        if kind == "close_full":
            return "Close full position"
        if kind == "close_partial":
            return f"Close {float(action.get('fraction', 0) or 0) * 100:.1f}% of position"
        if kind == "status":
            if action.get("signal_id") or action.get("side") or action.get("tf"):
                return "Show position details"
            return "Show live status"
        if kind == "open_signal":
            return "Open tracked signal on exchange"
        if kind == "open_manual":
            side = str(action.get("side") or "").upper()
            tf = str(action.get("tf") or "").strip()
            margin = action.get("margin_usd")
            leverage = action.get("leverage")
            parts = ["Open manual market position"]
            if side in {"LONG", "SHORT"}:
                parts = [f"Open manual {side} position"]
            if tf:
                parts.append(f"on {tf}")
            if margin not in (None, "", 0, 0.0):
                parts.append(f"with ${float(margin):.2f} margin")
            if leverage not in (None, "", 0, 0.0):
                parts.append(f"at {int(float(leverage))}x")
            return " ".join(parts)
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

        if action_type in {"move_sl_entry", "move_sl", "set_tp", "cancel_tp", "close_full", "close_partial", "status"}:
            if not self._resolve_signal_for_action(action, allow_unexecuted=False):
                if len(active) > 1:
                    return "Which open position do you mean? Send the ID, or say the side and timeframe."
                if not active:
                    return "I do not see any active exchange position right now."

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
        df = self.latest_data.get(tf)
        if df is None or df.empty:
            raise ValueError(f"No live market data available for {tf}.")
        df = calculate_channels(df.copy())
        curr = df.iloc[-1]
        entry = float(curr["Close"])
        atr = float(curr["ATR"]) if "ATR" in curr and curr["ATR"] else max(entry * 0.002, 1.0)
        risk_cfg = TIMEFRAME_RISK_MULTIPLIERS.get(tf, TIMEFRAME_RISK_MULTIPLIERS.get("5m", {}))
        sl_mult = float(risk_cfg.get("sl", 2.0))
        tp1_mult = float(risk_cfg.get("tp1", 1.0))
        tp2_mult = float(risk_cfg.get("tp2", 1.8))
        tp3_mult = float(risk_cfg.get("tp3", 2.5))

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
        }
        if action.get("margin_usd") is not None:
            meta["manual_margin_usd"] = float(action.get("margin_usd"))
        if action.get("leverage") is not None:
            meta["manual_leverage"] = int(float(action.get("leverage")))

        signal_id = new_signal_id()
        ts = curr.name.isoformat() if hasattr(curr, "name") else datetime.now(timezone.utc).isoformat()
        return {
            "signal_id": signal_id,
            "type": "MANUAL",
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
            sig = self._resolve_signal_for_action(action, allow_unexecuted=False)
            if sig:
                self._send_single_position_snapshot(sig, title="Position Info")
            else:
                self._send_execution_status_snapshot(datetime.now(timezone.utc), title="Bitunix Live Status")
            return True

        if action_type == "open_signal":
            sig = self._resolve_signal_for_action(action, allow_unexecuted=True)
            if not sig:
                self._send_private_execution_notice("Exec Control", ["No tracked pending signal matched your request."], icon="⚠️")
                return False
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
            self._execute_exchange_trade(self.tracker.signals[-1])
            return True

        sig = self._resolve_signal_for_action(action, allow_unexecuted=False)
        if not sig:
            self._send_private_execution_notice("Exec Control", ["No active exchange position matched your request."], icon="⚠️")
            return False

        if action_type == "move_sl_entry":
            result = self.trade_executor.manual_move_stop(sig, float(sig.get("entry") or 0))
        elif action_type == "move_sl":
            if action.get("price") in (None, "", 0, 0.0, "0"):
                self._send_private_execution_notice(
                    "Exec Control",
                    [
                        "I understood a stop-loss change, but no valid stop price was found.",
                        "Please say the exact stop price, for example: move stop to 67120",
                    ],
                    icon="⚠️",
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
                    icon="??",
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
                        icon="??",
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
        elif action_type == "close_full":
            result = self.trade_executor.manual_close_position(sig, 1.0)
        elif action_type == "close_partial":
            result = self.trade_executor.manual_close_position(sig, float(action.get("fraction") or 0))
        else:
            self._send_private_execution_notice("Exec Control", [f"Unsupported action: {action_type}"], icon="⚠️")
            return False

        if result.payload:
            sig["execution"] = result.payload
        self._save_state()
        self._send_private_execution_notice(
            "Exec Action Result",
            self._format_execution_lines(
                sig,
                extra=[
                    f"I did it. {result.message}" if result.accepted else f"I could not do that. {result.message}",
                ],
            ),
            icon="?" if result.accepted else "??",
        )
        return result.accepted

    def _handle_private_exec_message(self, message):
        if not PRIVATE_EXEC_AI_CONTROL_ENABLED or not self._is_private_exec_chat((message.get("chat") or {}).get("id")):
            return False
        text = str(message.get("text") or "").strip()
        if not text:
            return False

        def _ask_to_confirm(action_obj, chat_id, source_text=None):
            preview_lines = [
                f"I will {self._preview_exec_action(action_obj).lower()}.",
                "Reply YES to continue or NO to cancel.",
            ]
            if action_obj.get("signal_id"):
                preview_lines.insert(1, f"Position ID:\n<pre>{action_obj.get('signal_id')}</pre>")
            self.pending_exec_action = {
                "created_at": time.time(),
                "chat_id": str(chat_id or ""),
                "action": action_obj,
                "mode": "confirm",
            }
            if source_text is not None:
                self.pending_exec_action["source_text"] = source_text
            self._save_state()
            self._send_private_execution_notice("Confirm Exec Action", preview_lines, icon="??")

        lower = text.lower()
        chat_id = str((message.get("chat") or {}).get("id") or "")
        position_phrases = [
            "all open positions", "open positions", "show positions", "show my positions",
            "give me positions", "what positions", "what are open", "which positions are open",
            "list positions", "show all trades", "give me all trades"
        ]
        if any(phrase in lower for phrase in position_phrases):
            self._send_open_positions_snapshot("Open Positions")
            return True

        live_metric_words = ["roi", "pnl", "profit", "loss", "unrealized", "return"]
        if any(word in lower for word in live_metric_words):
            payload = {}
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
                    "If you want, send me the current price and I’ll use it."
                )
                return True


        if self.pending_exec_action:
            pending_chat = str((self.pending_exec_action or {}).get("chat_id") or "")
            expired = (time.time() - float((self.pending_exec_action or {}).get("created_at") or 0)) > max(30, PRIVATE_EXEC_CONFIRM_TIMEOUT_SEC)
            if expired:
                self.pending_exec_action = None
                self._save_state()
                self._send_private_execution_notice("Exec Control", ["Pending action expired. Send the request again."], icon="??")
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
                        self._send_private_execution_notice("Exec Control", ["Pending request cancelled."], icon="?")
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
                            self._send_private_execution_notice("Exec Control", [need_more], icon="??")
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
                        self._send_private_execution_notice("Exec Control", ["GEMINI_API_KEY is missing in .env."], icon="??")
                        return True
                    parsed = parse_gemini_trade_instruction(
                        GEMINI_API_KEY,
                        GEMINI_MODEL,
                        combined_text,
                        self._build_gemini_trade_context(),
                    )
                    if not parsed:
                        self._send_private_execution_notice("Exec Control", ["I still could not understand the request clearly."], icon="??")
                        return True
                    action_type = str(parsed.get("action") or "unsupported").lower()
                    if action_type == "unsupported":
                        self._send_private_execution_notice("Exec Control", [str(parsed.get("reason") or "Request is unclear or unsupported.")], icon="??")
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
                        self._send_private_execution_notice("Exec Control", [need_more], icon="??")
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
                    self._send_private_execution_notice("Exec Control", ["Pending action cancelled."], icon="?")
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
                        self._send_private_execution_notice("Exec Control", [need_more], icon="??")
                        return True
                    _ask_to_confirm(locally_updated, chat_id, source_text=pending_source or text)
                    return True

        if not GEMINI_API_KEY:
            self._send_private_execution_notice("Exec Control", ["GEMINI_API_KEY is missing in .env."], icon="??")
            return True

        parsed = parse_gemini_trade_instruction(
            GEMINI_API_KEY,
            GEMINI_MODEL,
            text,
            self._build_gemini_trade_context(),
        )
        if not parsed:
            answer = None
            try:
                answer = ask_gemini_trade_question(
                    GEMINI_API_KEY,
                    GEMINI_MODEL,
                    text,
                    self._build_gemini_trade_context(),
                )
            except Exception:
                answer = None
            if answer:
                self._send_private_execution_answer(answer)
            else:
                self._send_private_execution_answer("I didn’t fully understand that yet, but I’m here with you. Try asking it in a simpler way and I’ll help.")
            return True

        action_type = str(parsed.get("action") or "unsupported").lower()
        if action_type == "unsupported":
            answer = None
            try:
                answer = ask_gemini_trade_question(
                    GEMINI_API_KEY,
                    GEMINI_MODEL,
                    text,
                    self._build_gemini_trade_context(),
                )
            except Exception:
                answer = None
            if answer:
                self._send_private_execution_answer(answer)
            else:
                reason = str(parsed.get("reason") or "").strip()
                if reason:
                    self._send_private_execution_answer(reason)
                else:
                    self._send_private_execution_answer("I’m here 🙂 Ask me in your own words and I’ll do my best to help.")
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
                "source_text": text,
            }
            self._save_state()
            self._send_private_execution_notice("Exec Control", [need_more], icon="??")
            return True

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
        return f"Balance: {total:.2f} total, {free:.2f} free, {used:.2f} in positions"

    def _format_news_status_line(self, now):
        if not NEWS_FILTER_ENABLED:
            return "News: filter off"
        news_blackout = self._get_active_news_block(now)
        if news_blackout:
            label = str(news_blackout.get("label") or "High Impact News")
            source = str(news_blackout.get("source") or "manual")
            end = news_blackout.get("end")
            end_txt = end.astimezone(timezone.utc).strftime("%H:%M UTC") if isinstance(end, datetime) else "active"
            return f"News: BLOCK {label} via {source} until {end_txt}"
        if TRADING_ECONOMICS_NEWS_ENABLED:
            self._refresh_live_news_events(now)
            upcoming = [event for event in self.live_news_events if event.get("datetime") and event["datetime"] >= now]
            if upcoming:
                next_event = upcoming[0]
                event_dt = next_event["datetime"].astimezone(timezone.utc).strftime("%d %b %H:%M UTC")
                return f"News: next {next_event['event']} at {event_dt}"
            if self.last_live_news_error:
                return f"News: feed issue ({self.last_live_news_error})"
            return "News: no upcoming high-impact US events"
        manual = get_active_news_blackout(now)
        if manual:
            return f"News: manual block {manual.get('label', 'active')}"
        return "News: manual blackout only"

    def _build_execution_status_lines(self, trade_check=None, reconcile=None, now=None):
        now = now or datetime.now(timezone.utc)
        trade_check = trade_check or self.trade_executor.startup_self_check()
        reconcile = reconcile or self.trade_executor.reconcile_execution_state(self.tracker.signals)
        lines = [
            f"Bot is {trade_check.get('mode')} on {SYMBOL}. Auth: {'OK' if trade_check.get('auth_ok') else 'FAIL'}",
            self._format_balance_line(trade_check),
            f"Account mode: {trade_check.get('margin_mode') or 'N/A'} at {int(trade_check.get('leverage', 0) or 0)}x",
            f"Open positions: {int(trade_check.get('open_positions', 0) or 0)}",
        ]
        matched = int(reconcile.get("matched", 0) or 0)
        orphan_count = len(reconcile.get("orphan_positions", []) or [])
        missing_protection = len(reconcile.get("missing_protection", []) or [])
        if matched or orphan_count or missing_protection:
            lines.append(f"Tracker: {matched} matched, {orphan_count} orphan, {missing_protection} missing protection")
        lines.append(self._format_news_status_line(now))

        errors = []
        errors.extend([str(err) for err in (trade_check.get("errors") or [])[:1]])
        errors.extend([str(err) for err in (reconcile.get("errors") or [])[:1]])
        if errors:
            lines.extend(errors[:2])
        return lines

    def _send_execution_status_snapshot(self, now=None, title="Bitunix Noon Check"):
        if not self._execution_chat_id():
            return
        now = now or datetime.now(timezone.utc)
        trade_check = self.trade_executor.startup_self_check()
        reconcile = self.trade_executor.reconcile_execution_state(self.tracker.signals)
        if reconcile.get("inactive_marked") or reconcile.get("state_updated"):
            self.tracker.persist()
        lines = self._build_execution_status_lines(trade_check=trade_check, reconcile=reconcile, now=now)
        self._send_private_execution_notice(title, lines)

    def _send_open_positions_snapshot(self, title="Open Positions"):
        self._refresh_private_execution_state()
        active = self._active_execution_signals()
        if not active:
            self._send_private_execution_answer("You do not have any active exchange positions right now.")
            return
        blocks = [f"{title}\nYou currently have {len(active)} open position{'s' if len(active) != 1 else ''}."]
        for sig in active[:10]:
            execution = sig.get("execution") or {}
            signal_id = sig.get("signal_id") or ((sig.get("meta") or {}).get("signal_id")) or (execution.get("signal_id"))
            sl_text = self._format_live_sl_value(sig)
            header_block = f"{sig.get('type', 'SCALP')} {sig.get('side', 'N/A')} [{sig.get('tf', 'N/A')}]"
            detail_lines = [
                f"Entry: {float(sig.get('entry', 0) or 0):.2f}",
                f"SL: {sl_text}",
                self._format_active_tp_line(sig),
                f"Qty: {float(execution.get('qty', 0) or 0):.6f}",
            ]
            block = (
                f"\n\n{header_block}\n"
                f"Position ID:\n<pre>{signal_id or 'N/A'}</pre>\n"
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
        status = str(sig.get("status") or "OPEN").upper()
        sl_text = self._format_live_sl_value(sig)
        details_block = "\n".join([
            f"Status: {status}",
            f"Entry: {float(sig.get('entry', 0) or 0):.2f}",
            f"SL: {sl_text}",
            self._format_active_tp_line(sig),
            f"Qty: {float(execution.get('qty', 0) or 0):.6f}",
        ])
        answer = (
            f"{title}\n"
            f"This is your {sig_type} {side} on {tf}.\n"
            f"Position ID:\n<pre>{signal_id or 'N/A'}</pre>\n"
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
        if signal_id:
            lines.append("Position ID:")
            lines.append(f"<pre>{signal_id}</pre>")
        if tf or sig_type or side:
            lines.append(f"{sig_type or 'Signal'} {side or 'N/A'} [{tf or 'N/A'}]")
        if sig.get("entry") is not None:
            lines.append(f"Entry: {float(sig.get('entry') or 0):.2f}")
            lines.append(f"SL: {self._format_live_sl_value(sig)}")
            lines.append(self._format_active_tp_line(sig))
        if extra:
            lines.extend([str(line) for line in extra if str(line).strip()])
        return lines

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
        return self._get_live_news_blackout(now)

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

    def _get_scalp_window_block_reason(self, tf, side, local_trend, local_trend_src):
        """
        Hard blockers for scalp OPEN/PREPARE visibility.
        We only suppress early alerts when the side is already invalid on hard structure.
        """
        divergence_note = self._get_opposite_divergence_note(side)
        if divergence_note:
            return divergence_note

        local_side = self._trend_side(local_trend)
        if local_side and side != local_side:
            return f"local trend reversal ({local_trend} from {local_trend_src or tf})"

        macro_trend, macro_src = self._get_anchor_trend("1h")
        macro_side = self._trend_side(macro_trend)
        if macro_side and side != macro_side:
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


    def run(self):
        """Main loop - fetches data and processes signals."""

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
                "last_exec_snapshot_date": self.last_exec_snapshot_date,
                "pending_exec_action": self.pending_exec_action,
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
                # Send summary for the last completed recap window (08:00 UTC -> 08:00 UTC)
                self._send_performance_summary(now)
                self.last_summary_date = today_str
                self._save_state()
            if self.last_daily_report_date != today_str:
                self._send_daily_report(now)
                self.last_daily_report_date = today_str
                self._save_state()
            if self.last_exec_snapshot_date != today_str:
                self._send_execution_status_snapshot(now)
                self.last_exec_snapshot_date = today_str
                self._save_state()

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
                        print(
                            f"  [CONFLUENCE] Blocked {side} {ce['type']}: "
                            f"against trend {', '.join(blocked_trends)}"
                        )
                        continue

                    divergence_note = self._get_opposite_divergence_note(side)
                    if divergence_note:
                        print(f"  [CONFLUENCE] Blocked {side} {ce['type']}: {divergence_note}")
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
                        strong_size = round(max(MIN_SIGNAL_SIZE_PCT, min(ce["points"] * 0.3, 5.0)), 1)
                        tp_liq = self._estimate_tp_liquidity(side, latest_price, tp1_c, tp2_c, tp3_c)
                        signal_id = new_signal_id()
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
                                "signal_id": signal_id,
                                "indicators": ce["indicators"],
                                "size": strong_size,
                                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                            }
                        )
                        self.tracker.signals[-1]["signal_id"] = signal_id
                        self._execute_exchange_trade(self.tracker.signals[-1])
                        self._save_state()
                        print(f"  [CONFLUENCE] STRONG {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                    elif ce["type"] == "EXTREME":
                        extreme_size = round(max(MIN_SIGNAL_SIZE_PCT, min(ce["points"] * 0.3, 5.0)), 1)
                        tp_liq = self._estimate_tp_liquidity(side, latest_price, tp1_c, tp2_c, tp3_c)
                        signal_id = new_signal_id()
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
                                "signal_id": signal_id,
                                "indicators": ce["indicators"],
                                "size": extreme_size,
                                "tp_liq_prob": tp_liq["prob"] if tp_liq else None,
                                "tp_liq_usd": tp_liq["size_usd"] if tp_liq else None,
                                "tp_liq_target": tp_liq["target"] if tp_liq else None,
                            }
                        )
                        self.tracker.signals[-1]["signal_id"] = signal_id
                        self._execute_exchange_trade(self.tracker.signals[-1])
                        self._save_state()
                        print(f"  [CONFLUENCE] EXTREME {ce['side']} ({ce['points']}pts, {ce['confirmations']} conf)")

                    # Prevent immediate opposite-side flip from stale queued confirmations.
                    opposite_side = "SHORT" if ce["side"] == "LONG" else "LONG"
                    self.confirmations.reset(opposite_side)
                    self.confluence_side_lock_until[opposite_side] = current_time + CONFLUENCE_OPPOSITE_LOCK_SEC
                    self._save_state()

        # ─── Update Performance Tracker & Success Teasers ────
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
                if sig.get("msg_id") and sig.get("chat_id") and self._should_update_public_signal(sig):
                    tg.update_signal_message(sig["chat_id"], sig["msg_id"], sig)

                    # Reply on target progression and closure events.
                    if evt_type == "TP1":
                        tg.send_tp1_hit_congrats(
                            sig["chat_id"],
                            sig["msg_id"],
                            sig.get("tf", "Unknown"),
                            side=sig.get("side"),
                            lock_price=sig.get("entry"),
                            entry=sig.get("entry"),
                            sl=sig.get("sl"),
                            tp1=sig.get("tp1"),
                            tp2=sig.get("tp2"),
                            size=(sig.get("meta", {}) or {}).get("size")
                        )
                    elif evt_type == "TP2":
                        tg.send_tp2_hit_congrats(
                            sig["chat_id"],
                            sig["msg_id"],
                            sig.get("tf", "Unknown"),
                            side=sig.get("side"),
                            lock_price=sig.get("entry"),
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
                elif self._execution_updates_private_only():
                    self._send_private_execution_notice(
                        f"{evt_type} {sig.get('side', 'N/A')} [{sig.get('tf', 'N/A')}]",
                        self._format_execution_lines(
                            sig,
                            extra=[
                                f"status={sig.get('status')}",
                                f"lock={float(sig.get('sl') or 0):.2f}",
                            ],
                        ),
                    )

                if risk_state_changed:
                    self._save_state()

                if evt_type in ("TP1", "TP2", "TP3", "ENTRY_CLOSE", "PROFIT_SL", "SL"):
                    self._sync_exchange_trade_event(sig, evt_type)

            # 2. Liquidation Squeezes
            if self.last_liqs >= LIQ_SQUEEZE_THRESHOLD:
                if current_time - self.last_liq_alert_time > LIQ_ALERT_COOLDOWN:
                    if not self.is_booting:
                        tg.send_squeeze_alert(self.last_liqs, latest_price, chat_id=PRIVATE_CHAT_ID)
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
                                tg.send_oi_divergence(price_chg*100, oi_chg*100, note, chat_id=PRIVATE_CHAT_ID)
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
            
            message = up.get("message") or up.get("channel_post")
            if not message or "text" not in message:
                continue

            if self._handle_private_exec_message(message):
                self._save_state()
                continue

            user_id = (message.get("from") or {}).get("id") or (message.get("chat") or {}).get("id")
            text = message["text"].strip()
            
            if text == "/start":
                welcome_msg = (
                    f"<b>How to Join:</b>\n\n"
                    f"1. Sign up on Bitunix to start trading:\n"
                    f"{BITUNIX_REG_LINK}\n\n"
                    f"2. <b>Send your unique UID here.</b>\n\n"
                    f"3. Once verified, you'll receive an invite link to join."
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

                    def fmt_named_line(label, item):
                        if not item:
                            return f"{label}: n/a"
                        return (
                            f"{label}: {item['name']} | wr={float(item.get('win_rate', 0.0)):.1f}% "
                            f"avgR={float(item.get('avg_r', 0.0)):+.2f} trades={int(item.get('trades', 0) or 0)}"
                        )

                    by_strategy = stats.get("by_strategy", {})
                    strategy_rows = []
                    for name, bucket in sorted(
                        by_strategy.items(),
                        key=lambda item: (-float(item[1].get("win_rate", 0.0)), -int(item[1].get("trades", 0) or 0), item[0])
                    )[:4]:
                        strategy_rows.append(
                            f"{name:<22.22} wr={float(bucket.get('win_rate', 0.0)):>5.1f}% "
                            f"tr={int(bucket.get('trades', 0) or 0):<3} avgR={float(bucket.get('avg_r', 0.0)):+.2f}"
                        )

                    msg = (
                        f"<b>SIGNAL ANALYTICS ({days}d)</b>\n\n"
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
                        f"{fmt_type_line('EXTREME')}\n"
                        f"------------------------------\n"
                        f"{fmt_named_line('Best TF', stats.get('best_timeframe'))}\n"
                        f"{fmt_named_line('Worst TF', stats.get('worst_timeframe'))}\n"
                        f"{fmt_named_line('Best Mod', stats.get('best_strategy'))}\n"
                        f"{fmt_named_line('Worst Mod', stats.get('worst_strategy'))}"
                        f"</pre>"
                    )
                    if strategy_rows:
                        msg += "\n<pre>" + "\n".join(strategy_rows) + "</pre>"
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
                    tg.send_performance_summary(stats, chat_id=PRIVATE_CHAT_ID)
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
                        str(e),
                        f"endpoint={endpoint}" if endpoint else None,
                        f"response={response_text}" if response_text else None,
                    ],
                ),
            )
            return

        status = "accepted" if result.accepted else "blocked"
        print(f"  [TRADE] {status.upper()} {sig_obj.get('type')} {sig_obj.get('side')}: {result.message}")
        details = result.payload or {}
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
                f"signal_size={float(details.get('signal_size_pct', 0) or 0):.2f}% "
                f"qty={float(details.get('qty', 0) or 0):.6f} "
                f"notional={float(details.get('notional', 0) or 0):.4f}"
            )
            if details.get("position_mode"):
                print(
                    f"  [TRADE] PositionMode={details.get('position_mode')} "
                    f"margin_mode={details.get('margin_mode')} "
                    f"leverage={int(details.get('leverage', 0) or 0)} "
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
        notice_lines = self._format_execution_lines(
            sig_obj,
            extra=[
                f"Exchange status: {'accepted' if result.accepted else 'blocked'}",
                f"Result: {result.message}",
                f"Free balance: {float(details.get('balance_available', 0) or 0):.2f}" if "balance_available" in details else None,
                f"Risk budget: {float(details.get('risk_budget_usd', 0) or 0):.2f}" if "risk_budget_usd" in details else None,
                f"Order size: {float(details.get('qty', 0) or 0):.6f} ({float(details.get('notional', 0) or 0):.4f} notional)" if details else None,
                f"Bitunix position ID: {details.get('position_id')}" if details.get("position_id") else None,
                f"TP mode: {', '.join(sorted({str(o.get('kind', 'N/A')) for o in (details.get('tp_orders') or [])}))}" if details.get("tp_orders") else None,
            ] + list(details.get("protection_warnings", []) or [])
        )
        notice_lines = self._format_execution_lines(
            sig_obj,
            extra=[
                f"Done: {result.message}" if result.accepted else f"Could not do it: {result.message}",
                f"Position ID: {details.get('position_id')}" if details.get("position_id") else None,
            ] + list(details.get("protection_warnings", []) or [])
        )
        self._send_private_execution_notice(
            f"Exchange {status.title()}: {sig_obj.get('type')} {sig_obj.get('side')}",
            notice_lines,
            icon="?" if result.accepted else "??",
        )
        if result.accepted and result.payload:
            sig_obj["execution"] = result.payload
            self._save_state()

    def _sync_exchange_trade_event(self, sig_obj, event_type):
        """Apply TP/SL lifecycle changes to exchange-side protection orders."""
        execution = sig_obj.get("execution") or {}
        if not execution:
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
        self._send_private_execution_notice(
            f"Execution Update: {event_type}",
            self._format_execution_lines(
                sig_obj,
                extra=[
                    f"Done: {result.message}",
                    f"New stop: {float(result.payload.get('sl_moved_to') or 0):.2f}" if (result.payload or {}).get("sl_moved_to") is not None else None,
                ],
            ),
        )
        if result.payload:
            sig_obj["execution"] = result.payload
            self._save_state()
            self._save_state()


    def _process_timeframe(self, tf, df, now, entry_protection_ts=None):
        """Process one timeframe: channels, momentum, signals."""

        # ─── Calculate indicators ────────────────────────
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

        divergence_sides = set()
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
            div_sigs = check_rsi_divergence(df, tf)
            divergence_sides = {s.get("side") for s in div_sigs if s.get("active", True)}
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

        # ─── Scalp Momentum System ───────────────────────
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
                block_reason = self._get_scalp_window_block_reason(tf, evt["side"], local_trend, local_trend_src)
                if block_reason:
                    print(f"  [SCALP] Suppressed Open [{tf}] {evt['side']}: {block_reason}")
                    continue
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
                block_reason = self._get_scalp_window_block_reason(tf, evt["side"], local_trend, local_trend_src)
                if block_reason:
                    print(f"  [SCALP] Suppressed Prepare [{tf}] {evt['side']}: {block_reason}")
                    continue
                if not self.is_booting:
                    tg.send_scalp_prepare(tf, evt["side"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
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
                side = evt["side"]
                if side in divergence_sides:
                    score += 1
                    reasons.append("RSI Divergence")

                divergence_note = self._get_opposite_divergence_note(side)
                if divergence_note:
                    print(f"  [SCALP] Blocked {tf} {side}: {divergence_note}")
                    continue

                impulse_blocked, impulse_note = self._is_unstable_impulse(self.latest_data or {}, side)
                if impulse_blocked:
                    print(f"  [SCALP] Blocked {tf} {side}: {impulse_note}")
                    continue

                # Hard local-trend reversal guard (hierarchical source):
                # do not SHORT in bullish local trend, do not LONG in bearish local trend.
                local_side = self._trend_side(local_trend)
                if local_side and side != local_side:
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

                news_blackout = self._get_active_news_block(now)
                if news_blackout:
                    label = str(news_blackout.get("label") or "High Impact News")
                    source = str(news_blackout.get("source") or "NEWS")
                    print(f"  [SCALP] Blocked {tf} {side}: news blackout active ({label} via {source})")
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

                # Dynamic size: scale base size by score, with a global minimum size floor.
                if str(evt.get("strategy", "")).upper() == "SMART_MONEY_LIQUIDITY":
                    dyn_size = max(float(MIN_SIGNAL_SIZE_PCT), float(evt.get("size", SMART_MONEY_RISK_PCT)))
                else:
                    dyn_base = max(float(MIN_SIGNAL_SIZE_PCT), (score / 10) * profile["size"]) if score else max(float(MIN_SIGNAL_SIZE_PCT), profile["size"])
                    dyn_size = round(max(float(MIN_SIGNAL_SIZE_PCT), dyn_base * size_mult), 1)
                tp_liq = self._estimate_tp_liquidity(evt["side"], evt["entry"], evt["tp1"], evt["tp2"], evt["tp3"])
                signal_id = new_signal_id()

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
                        trigger_label=trigger_label,
                        chat_id=PRIVATE_CHAT_ID
                    )
                msg_id = resp.get("result", {}).get("message_id") if resp else None
                self._save_state()
                sent_label = "Smart Money Confirmed" if str(evt.get("strategy", "")).upper() == "SMART_MONEY_LIQUIDITY" else "Scalp Confirmed"
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
                    msg_id=msg_id,
                    chat_id=PRIVATE_CHAT_ID,
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
                self._execute_exchange_trade(self.tracker.signals[-1])

            elif evt["type"] == "CLOSED":
                if not self.is_booting:
                    tg.send_scalp_closed(tf, evt["side"], evt["price"], emoji=emoji, chat_id=PRIVATE_CHAT_ID)
                self._save_state()
                print(f"  [TG] {'Skipped' if self.is_booting else 'Sent'} Scalp Closed [{tf}] {evt['side']}")


        # ─── Store prev candle data ──────────────────────
        self.prev_candles[tf] = {
            "High": price_high,
            "Low":  price_low,
        }

        return atr_val, candle_ts, rsi_raw


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


