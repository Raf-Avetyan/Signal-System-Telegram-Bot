# ─── Ponch Signal Performance Tracker ─────────────────────────

"""
Tracks scalp CONFIRMED signals and checks if TP/SL were hit.
Persists data to signals_log.json for restart survival.
"""

import json
import os
from datetime import datetime, timezone, timedelta
from config import (
    BREAKEVEN_WIN_MIN_TP,
    BREAKEVEN_MOVE_AFTER_TP,
    BREAKEVEN_FEE_BUFFER_PCT,
    BITUNIX_TP_SPLITS,
    get_tp_splits_for_tf,
    SIGNAL_CHAT_ID,
    PRIVATE_EXEC_CHAT_ID,
)

LOG_FILE = os.path.join(os.path.dirname(__file__), "signals_log.json")


class SignalTracker:
    """Track signal outcomes (TP1/TP2/TP3 hit or SL hit)."""

    def __init__(self):
        self.signals = self._load()
        if self._normalize_loaded_signal_chats():
            self._save()
        self._new_this_tick: set = set()  # indices of signals logged this tick

    def _load(self):
        """Load signals from JSON file."""
        if os.path.exists(LOG_FILE):
            try:
                with open(LOG_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def _save(self):
        """Persist signals to JSON file."""
        try:
            with open(LOG_FILE, "w") as f:
                json.dump(self.signals, f, indent=2)
        except Exception as e:
            print(f"[TRACKER ERROR] Save failed: {e}")

    def _normalize_loaded_signal_chats(self):
        """Drop legacy public chat targets so old channels never receive updates again."""
        allowed_chats = {
            str(SIGNAL_CHAT_ID or "").strip(),
            str(PRIVATE_EXEC_CHAT_ID or "").strip(),
        }
        allowed_chats.discard("")
        changed = False
        for sig in self.signals:
            chat_id = str(sig.get("chat_id") or "").strip()
            if not chat_id or chat_id in allowed_chats:
                continue
            meta = sig.get("meta") or {}
            meta["legacy_chat_id"] = chat_id
            sig["meta"] = meta
            sig["chat_id"] = None
            sig["msg_id"] = None
            changed = True
        return changed

    def persist(self):
        """Public wrapper to force-save tracker state immediately."""
        self._save()

    def _breakeven_counts_as_win(self, sig):
        threshold = int(BREAKEVEN_WIN_MIN_TP)
        if threshold <= 1:
            return bool(sig.get("tp1_hit"))
        if threshold == 2:
            return bool(sig.get("tp2_hit"))
        return bool(sig.get("tp3_hit"))

    def _breakeven_trigger_index(self, sig, active_tp_indices):
        active_tp_indices = list(active_tp_indices or [])
        if len(active_tp_indices) <= 1:
            return None
        threshold = int(BREAKEVEN_MOVE_AFTER_TP)
        if threshold in active_tp_indices:
            return threshold
        return active_tp_indices[min(1, len(active_tp_indices) - 1)]

    @staticmethod
    def _breakeven_lock_price(sig):
        execution = (sig or {}).get("execution") or {}
        entry = float(
            execution.get("filled_entry_price")
            or execution.get("entry")
            or sig.get("entry", 0)
            or 0
        )
        if entry <= 0:
            return entry
        side = str(sig.get("side", "")).upper()
        buffer_pct = max(0.0, float(BREAKEVEN_FEE_BUFFER_PCT or 0.0))
        if side == "LONG":
            return entry * (1.0 + buffer_pct / 100.0)
        if side == "SHORT":
            return entry * (1.0 - buffer_pct / 100.0)
        return entry

    def _metric_outcome(self, sig):
        """Return metric outcome label and R-multiple for summaries/analytics."""
        status = str(sig.get("status", "")).upper()
        entry = float(sig.get("entry", 0))
        initial_sl = float(sig.get("initial_sl", sig.get("sl", 0)) or 0)
        current_sl = float(sig.get("sl", initial_sl) or 0)
        risk = abs(entry - initial_sl)

        def _tp_fracs():
            execution = sig.get("execution") or {}
            qtys = list(execution.get("tp_qtys") or [])
            if qtys:
                qtys = [max(0.0, float(q or 0)) for q in qtys[:3]]
                total = sum(qtys)
                if total > 0:
                    while len(qtys) < 3:
                        qtys.append(0.0)
                    return [q / total for q in qtys[:3]]
            strategy_name = str(sig.get("strategy") or (sig.get("meta") or {}).get("strategy") or "")
            tf_name = str(sig.get("tf") or "")
            base_splits = get_tp_splits_for_tf(tf_name, strategy_name)
            base = [max(0.0, float(x or 0)) for x in base_splits[:3]]
            while len(base) < 3:
                base.append(0.0)
            total = sum(base) or 1.0
            return [x / total for x in base[:3]]

        def _tp_prices():
            execution = sig.get("execution") or {}
            targets = list(execution.get("tp_targets") or [sig.get("tp1"), sig.get("tp2"), sig.get("tp3")])
            while len(targets) < 3:
                targets.append(entry)
            return [float(targets[i] or entry) for i in range(3)]

        def _stop_r():
            if risk <= 0:
                return 0.0
            side = str(sig.get("side", "")).upper()
            if side == "LONG":
                return (current_sl - entry) / risk
            if side == "SHORT":
                return (entry - current_sl) / risk
            return 0.0

        def _realized_partial_r():
            if risk <= 0:
                return 0.0
            fracs = _tp_fracs()
            prices = _tp_prices()
            hits = [
                bool(sig.get("tp1_hit")),
                bool(sig.get("tp2_hit")),
                bool(sig.get("tp3_hit")),
            ]
            realized = 0.0
            for frac, price, hit in zip(fracs, prices, hits):
                if hit:
                    realized += frac * (abs(price - entry) / risk)
            return realized

        def _remaining_frac():
            fracs = _tp_fracs()
            hits = [
                bool(sig.get("tp1_hit")),
                bool(sig.get("tp2_hit")),
                bool(sig.get("tp3_hit")),
            ]
            consumed = sum(frac for frac, hit in zip(fracs, hits) if hit)
            return max(0.0, 1.0 - consumed)

        if status == "SL":
            r_mult = _realized_partial_r() + (_remaining_frac() * _stop_r())
            return "losses", r_mult
        if status == "PROFIT_SL":
            r_mult = _realized_partial_r() + (_remaining_frac() * max(0.0, _stop_r()))
            return "wins", r_mult
        if status == "TP3":
            return "wins", _realized_partial_r()
        if status == "ENTRY_CLOSE":
            r_mult = _realized_partial_r()
            if self._breakeven_counts_as_win(sig):
                return "wins", r_mult
            return "breakeven", r_mult
        return "open", 0.0

    def _parse_iso_utc(self, value):
        """Parse ISO datetime string into UTC-aware datetime."""
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _daily_recap_window(self, now_utc=None, recap_hour_utc=8):
        """
        Return the last completed daily recap window.
        Default recap boundary is 08:00 UTC, which matches 12:00 local.
        """
        now_utc = now_utc.astimezone(timezone.utc) if now_utc else datetime.now(timezone.utc)
        window_end = now_utc.replace(hour=recap_hour_utc, minute=0, second=0, microsecond=0)
        if now_utc < window_end:
            window_end -= timedelta(days=1)
        window_start = window_end - timedelta(days=1)
        return window_start, window_end

    def log_signal(self, side, entry, sl, tp1, tp2, tp3, tf, timestamp, msg_id=None, chat_id=None, signal_type="SCALP", meta=None):
        """Log a new CONFIRMED scalp or confluence signal with TG data."""
        signal = {
            "type": signal_type,
            "side": side,
            "entry": entry,
            "sl": sl,
            "initial_sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "tf": tf,
            "timestamp": timestamp,
            "entry_candle_ts": timestamp,  # candle that produced this signal — skip TP checks on this candle
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "status": "OPEN",       # OPEN, TP1, TP2, TP3, SL, PROFIT_SL, CLOSED
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "sl_hit": False,
            "tp1_at": None,
            "tp2_at": None,
            "tp3_at": None,
            "sl_at": None,
            "msg_id": msg_id,
            "chat_id": chat_id,
            "meta": meta or {},
            "teaser_sent": False,
            "closed_at": None,
        }
        idx = len(self.signals)
        self.signals.append(signal)
        self._new_this_tick.add(idx)
        self._save()
        print(f"  [TRACKER] Logged {signal_type} {side} @ {entry:,.2f} [{tf}] (Msg: {msg_id})")

    def check_outcomes(self, current_price, high=None, low=None, current_candle_ts=None, current_candle_ts_set=None):
        """
        Check all OPEN signals against price movement.
        Uses high/low if provided to catch wicks (much more accurate).
        current_candle_ts_set: set of current candle timestamps across all timeframes.
          Any signal whose entry_candle_ts is in this set is skipped — the candle
          it was born on hasn't closed yet so TPs/SL on that candle are unreliable.
        current_candle_ts: legacy single-ts fallback (still supported).
        """
        changed = False
        events = []

        # Fallbacks to current_price if high/low not provided
        p_high = high if high is not None else current_price
        p_low = low if low is not None else current_price

        # Freshly logged signals should sit out one outcome pass so they cannot
        # instantly fire TP/SL from the same wick that created them.
        new_this_tick = set(self._new_this_tick)

        terminal_statuses = {"SL", "TP3", "CLOSED", "ENTRY_CLOSE", "PROFIT_SL", "EXPIRED"}

        for idx, sig in enumerate(self.signals):
            if idx in new_this_tick:
                continue
            if sig["status"] in terminal_statuses:
                continue

            execution = sig.get("execution") or {}
            raw_tp_qtys = list(execution.get("tp_qtys") or [])
            while len(raw_tp_qtys) < 3:
                raw_tp_qtys.append(0.0)
            active_tp_indices = [i + 1 for i, q in enumerate(raw_tp_qtys[:3]) if float(q or 0) > 0]
            if not active_tp_indices:
                active_tp_indices = [1, 2, 3]
            single_active_tp = active_tp_indices[0] if len(active_tp_indices) == 1 else None
            breakeven_trigger = self._breakeven_trigger_index(sig, active_tp_indices)
            breakeven_lock_price = self._breakeven_lock_price(sig)

            # Skip signal if we're still on the candle it was born on
            entry_ts = sig.get("entry_candle_ts")
            is_entry_candle = False
            if current_candle_ts_set and entry_ts and entry_ts in current_candle_ts_set:
                is_entry_candle = True
            if current_candle_ts and entry_ts and entry_ts == current_candle_ts:
                is_entry_candle = True

            is_long = sig["side"] == "LONG"
            sl_touched = (p_low <= sig["sl"]) if is_long else (p_high >= sig["sl"])
            tp1_touched = (p_high >= sig["tp1"]) if is_long else (p_low <= sig["tp1"])
            trigger_tp_touched = False
            if breakeven_trigger == 2:
                trigger_tp_touched = (p_high >= sig["tp2"]) if is_long else (p_low <= sig["tp2"])
            elif breakeven_trigger == 3:
                trigger_tp_touched = (p_high >= sig["tp3"]) if is_long else (p_low <= sig["tp3"])
            elif breakeven_trigger == 1:
                trigger_tp_touched = tp1_touched

            # Ambiguous one-candle case: TP1 and SL both touched for a fresh trade.
            # We treat this as TP reached and then return to entry (breakeven close),
            # never as direct SL, to preserve scalp lifecycle semantics.
            if not is_entry_candle and breakeven_trigger == 1 and not sig["tp1_hit"] and tp1_touched and sl_touched:
                sig["tp1_hit"] = True
                sig["tp1_at"] = sig.get("tp1_at") or datetime.now(timezone.utc).isoformat()
                sig["sl"] = breakeven_lock_price
                sig["status"] = "TP1"
                changed = True
                events.append({"type": "TP1", "sig": sig})

                sig["status"] = "ENTRY_CLOSE"
                sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                events.append({"type": "ENTRY_CLOSE", "sig": sig})
                continue

            # 1. Check TPs (progressive)
            # Longs use High to hit TP, Shorts use Low to hit TP
            tp_price = p_high if is_long else p_low

            if not is_entry_candle:
                if is_long:
                    if 1 in active_tp_indices and not sig["tp1_hit"] and tp_price >= sig["tp1"]:
                        sig["tp1_hit"] = True
                        sig["tp1_at"] = sig.get("tp1_at") or datetime.now(timezone.utc).isoformat()
                        sig["status"] = "TP1"
                        changed = True
                        events.append({"type": "TP1", "sig": sig})
                        if single_active_tp == 1:
                            sig["status"] = "CLOSED"
                            sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                            continue
                    if 2 in active_tp_indices and not sig["tp2_hit"] and tp_price >= sig["tp2"]:
                        sig["tp2_hit"] = True
                        sig["tp2_at"] = sig.get("tp2_at") or datetime.now(timezone.utc).isoformat()
                        if breakeven_trigger == 2:
                            sig["sl"] = breakeven_lock_price
                        sig["status"] = "TP2"
                        changed = True
                        events.append({"type": "TP2", "sig": sig})
                        if single_active_tp == 2:
                            sig["status"] = "CLOSED"
                            sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                            continue
                    if 3 in active_tp_indices and not sig["tp3_hit"] and tp_price >= sig["tp3"]:
                        sig["tp3_hit"] = True
                        sig["tp3_at"] = sig.get("tp3_at") or datetime.now(timezone.utc).isoformat()
                        if breakeven_trigger == 3:
                            sig["sl"] = breakeven_lock_price
                        sig["status"] = "TP3"
                        sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                        changed = True
                        events.append({"type": "TP3", "sig": sig})
                        continue
                else:
                    if 1 in active_tp_indices and not sig["tp1_hit"] and tp_price <= sig["tp1"]:
                        sig["tp1_hit"] = True
                        sig["tp1_at"] = sig.get("tp1_at") or datetime.now(timezone.utc).isoformat()
                        sig["status"] = "TP1"
                        changed = True
                        events.append({"type": "TP1", "sig": sig})
                        if single_active_tp == 1:
                            sig["status"] = "CLOSED"
                            sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                            continue
                    if 2 in active_tp_indices and not sig["tp2_hit"] and tp_price <= sig["tp2"]:
                        sig["tp2_hit"] = True
                        sig["tp2_at"] = sig.get("tp2_at") or datetime.now(timezone.utc).isoformat()
                        if breakeven_trigger == 2:
                            sig["sl"] = breakeven_lock_price
                        sig["status"] = "TP2"
                        changed = True
                        events.append({"type": "TP2", "sig": sig})
                        if single_active_tp == 2:
                            sig["status"] = "CLOSED"
                            sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                            continue
                    if 3 in active_tp_indices and not sig["tp3_hit"] and tp_price <= sig["tp3"]:
                        sig["tp3_hit"] = True
                        sig["tp3_at"] = sig.get("tp3_at") or datetime.now(timezone.utc).isoformat()
                        if breakeven_trigger == 3:
                            sig["sl"] = breakeven_lock_price
                        sig["status"] = "TP3"
                        sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                        changed = True
                        events.append({"type": "TP3", "sig": sig})
                        continue

            # 2. Check Entry Return when stop has been moved to breakeven.
            stop_at_entry = abs(float(sig.get("sl", sig["entry"])) - float(breakeven_lock_price)) < 1e-9
            trigger_hit = (
                (breakeven_trigger == 1 and sig.get("tp1_hit"))
                or (breakeven_trigger == 2 and sig.get("tp2_hit"))
                or (breakeven_trigger == 3 and sig.get("tp3_hit"))
            )
            if trigger_hit and stop_at_entry and sig["status"] != "ENTRY_CLOSE":
                entry_hit = False
                if is_long and p_low <= breakeven_lock_price:
                    entry_hit = True
                elif not is_long and p_high >= breakeven_lock_price:
                    entry_hit = True
                
                if entry_hit:
                    sig["status"] = "ENTRY_CLOSE"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "ENTRY_CLOSE", "sig": sig})
                    continue

            # 3. Check SL
            # Longs use Low to hit SL, Shorts use High to hit SL
            sl_price = p_low if is_long else p_high

            if (is_long and sl_price <= sig["sl"]) or (not is_long and sl_price >= sig["sl"]):
                if trigger_hit and abs(float(sig.get("sl", sig["entry"])) - float(sig.get("initial_sl", sig["entry"]))) >= 1e-9:
                    # Stop was moved into profit after targets; this is a protected win.
                    sig["status"] = "PROFIT_SL"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "PROFIT_SL", "sig": sig})
                else:
                    sig["sl_hit"] = True
                    sig["sl_at"] = sig.get("sl_at") or datetime.now(timezone.utc).isoformat()
                    sig["status"] = "SL"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "SL", "sig": sig})
                continue

        self._new_this_tick.clear()

        if changed:
            self._save()

        return events

    def get_daily_summary(self, window_end=None):
        """Get performance stats for the last completed recap window (08:00 UTC -> 08:00 UTC)."""
        window_start, window_end = self._daily_recap_window(window_end)

        generated = []
        tp1_hits = 0
        tp2_hits = 0
        tp3_hits = 0
        sl_hits = 0

        for sig in self.signals:
            logged_at = self._parse_iso_utc(sig.get("logged_at"))
            if logged_at and window_start <= logged_at < window_end:
                generated.append(sig)

            tp1_at = self._parse_iso_utc(sig.get("tp1_at"))
            if tp1_at and window_start <= tp1_at < window_end:
                tp1_hits += 1

            tp2_at = self._parse_iso_utc(sig.get("tp2_at"))
            if tp2_at and window_start <= tp2_at < window_end:
                tp2_hits += 1

            tp3_at = self._parse_iso_utc(sig.get("tp3_at"))
            if tp3_at and window_start <= tp3_at < window_end:
                tp3_hits += 1

            sl_at = self._parse_iso_utc(sig.get("sl_at"))
            if sl_at and window_start <= sl_at < window_end:
                sl_hits += 1

        total = len(generated)
        still_open = sum(1 for s in generated if s.get("status") == "OPEN")
        if total == 0 and tp1_hits == 0 and tp2_hits == 0 and tp3_hits == 0 and sl_hits == 0:
            return None

        wins = losses = breakeven = 0
        by_tf = {}
        by_strategy = {}

        def ensure_bucket(bucket, key):
            if key not in bucket:
                bucket[key] = {"trades": 0, "wins": 0, "losses": 0, "breakeven": 0}

        for sig in generated:
            outcome, _ = self._metric_outcome(sig)
            if outcome == "wins":
                wins += 1
            elif outcome == "losses":
                losses += 1
            elif outcome == "breakeven":
                breakeven += 1
            tf = str(sig.get("tf", "N/A"))
            strategy = str((sig.get("meta") or {}).get("strategy", "UNKNOWN"))
            ensure_bucket(by_tf, tf)
            ensure_bucket(by_strategy, strategy)
            for bucket in (by_tf[tf], by_strategy[strategy]):
                bucket["trades"] += 1
                if outcome in bucket:
                    bucket[outcome] += 1
        resolved = wins + losses
        win_rate = (wins / resolved * 100.0) if resolved > 0 else 0.0

        def classify_best_worst(bucket):
            ranked = []
            for key, stats in bucket.items():
                closed = int(stats["wins"]) + int(stats["losses"])
                wr = (float(stats["wins"]) / closed * 100.0) if closed else 0.0
                ranked.append((key, wr, int(stats["trades"]), closed))
            ranked = [item for item in ranked if item[2] > 0]
            if not ranked:
                return None, None
            best = max(ranked, key=lambda item: (item[1], item[2], -item[3]))
            worst = min(ranked, key=lambda item: (item[1], -item[2], item[3]))
            return (
                {"name": best[0], "win_rate": best[1], "trades": best[2], "closed": best[3]},
                {"name": worst[0], "win_rate": worst[1], "trades": worst[2], "closed": worst[3]},
            )

        best_tf, worst_tf = classify_best_worst(by_tf)
        best_strategy, worst_strategy = classify_best_worst(by_strategy)

        return {
            "total": total,
            "tp1_hits": tp1_hits,
            "tp2_hits": tp2_hits,
            "tp3_hits": tp3_hits,
            "sl_hits": sl_hits,
            "still_open": still_open,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate": win_rate,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "by_timeframe": by_tf,
            "by_strategy": by_strategy,
            "best_timeframe": best_tf,
            "worst_timeframe": worst_tf,
            "best_strategy": best_strategy,
            "worst_strategy": worst_strategy,
        }

    def get_session_stats(self, session_start_hour, session_end_hour):
        """Get stats for signals fired during a specific session window."""
        now = datetime.now(timezone.utc)
        today = now.date()
        yesterday = today - timedelta(days=1)

        session_signals = []
        for s in self.signals:
            try:
                logged = datetime.fromisoformat(s["logged_at"])
                if logged.tzinfo is None:
                    logged = logged.replace(tzinfo=timezone.utc)
                else:
                    logged = logged.astimezone(timezone.utc)

                logged_hour = (
                    logged.hour
                    + logged.minute / 60.0
                    + logged.second / 3600.0
                )

                if session_start_hour < session_end_hour:
                    # Session is within today's UTC date window.
                    if logged.date() == today and session_start_hour <= logged_hour < session_end_hour:
                        session_signals.append(s)
                else:
                    # Overnight session: yesterday[start..24) + today[0..end).
                    in_prev_leg = logged.date() == yesterday and logged_hour >= session_start_hour
                    in_today_leg = logged.date() == today and logged_hour < session_end_hour
                    if in_prev_leg or in_today_leg:
                        session_signals.append(s)
            except Exception:
                continue

        total = len(session_signals)
        tp1_hits = sum(1 for s in session_signals if s["tp1_hit"])
        sl_hits = sum(1 for s in session_signals if s["sl_hit"])

        return {
            "total": total,
            "tp1_hits": tp1_hits,
            "sl_hits": sl_hits,
        }

    def _session_name_for_dt(self, dt_utc):
        """Return active session name for a UTC datetime."""
        from config import get_adjusted_sessions
        sessions = get_adjusted_sessions(dt_utc)
        float_hour = dt_utc.hour + dt_utc.minute / 60.0
        for s_name, s_times in sessions.items():
            s_open = s_times["open"]
            s_close = s_times["close"]
            if s_open < s_close:
                if s_open <= float_hour < s_close:
                    return s_name
            else:
                if float_hour >= s_open or float_hour < s_close:
                    return s_name
        return "OFF_SESSION"

    def get_analytics(self, days=30):
        """Return analytics dashboard summary for recent signals (all types)."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        totals = {
            "generated": 0,
            "trades": 0,
            "open": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "avg_r": 0.0,
            "expectancy_r": 0.0,
            "win_rate": 0.0,
            "hit_rate": 0.0,
        }
        by_tf = {}
        by_side = {"LONG": {"trades": 0, "avg_r": 0.0}, "SHORT": {"trades": 0, "avg_r": 0.0}}
        by_session = {}
        by_type = {}
        by_strategy = {}
        r_values = []

        def ensure_bucket(bucket, key):
            if key not in bucket:
                bucket[key] = {"trades": 0, "wins": 0, "losses": 0, "breakeven": 0, "avg_r": 0.0}

        def ensure_type_bucket(key):
            if key not in by_type:
                by_type[key] = {
                    "generated": 0,
                    "trades": 0,
                    "open": 0,
                    "wins": 0,
                    "losses": 0,
                    "breakeven": 0,
                    "avg_r": 0.0,
                    "expectancy_r": 0.0,
                    "win_rate": 0.0,
                    "hit_rate": 0.0,
                    "_r_values": [],
                }

        def ensure_strategy_bucket(key):
            if key not in by_strategy:
                by_strategy[key] = {
                    "generated": 0,
                    "trades": 0,
                    "open": 0,
                    "wins": 0,
                    "losses": 0,
                    "breakeven": 0,
                    "avg_r": 0.0,
                    "expectancy_r": 0.0,
                    "win_rate": 0.0,
                    "hit_rate": 0.0,
                    "_r_values": [],
                }

        for sig in self.signals:
            status = sig.get("status")
            try:
                logged = datetime.fromisoformat(sig["logged_at"])
                if logged.tzinfo is None:
                    logged = logged.replace(tzinfo=timezone.utc)
                else:
                    logged = logged.astimezone(timezone.utc)
            except Exception:
                continue

            if logged < cutoff:
                continue

            sig_type = str(sig.get("type", "SCALP")).upper()
            strategy = str((sig.get("meta") or {}).get("strategy", "UNKNOWN"))
            ensure_type_bucket(sig_type)
            ensure_strategy_bucket(strategy)
            by_type[sig_type]["generated"] += 1
            by_strategy[strategy]["generated"] += 1
            totals["generated"] += 1

            closed_statuses = {"SL", "TP3", "ENTRY_CLOSE", "PROFIT_SL"}
            if status not in closed_statuses:
                by_type[sig_type]["open"] += 1
                by_strategy[strategy]["open"] += 1
                totals["open"] += 1
                continue

            outcome, r_mult = self._metric_outcome(sig)

            totals["trades"] += 1
            totals[outcome] += 1
            r_values.append(r_mult)
            by_type[sig_type]["trades"] += 1
            by_type[sig_type][outcome] += 1
            by_type[sig_type]["_r_values"].append(r_mult)
            by_strategy[strategy]["trades"] += 1
            by_strategy[strategy][outcome] += 1
            by_strategy[strategy]["_r_values"].append(r_mult)

            tf = sig.get("tf", "N/A")
            side = sig.get("side", "N/A")
            session = self._session_name_for_dt(logged)

            ensure_bucket(by_tf, tf)
            ensure_bucket(by_session, session)
            for bucket in (by_tf[tf], by_session[session]):
                bucket["trades"] += 1
                bucket[outcome] += 1
                bucket["avg_r"] = ((bucket["avg_r"] * (bucket["trades"] - 1)) + r_mult) / bucket["trades"]

            if side in by_side:
                prev = by_side[side]
                prev["trades"] += 1
                prev["avg_r"] = ((prev["avg_r"] * (prev["trades"] - 1)) + r_mult) / prev["trades"]

        if totals["trades"] > 0:
            totals["avg_r"] = sum(r_values) / len(r_values)
            totals["expectancy_r"] = totals["avg_r"]
            closed = totals["wins"] + totals["losses"]
            totals["win_rate"] = (totals["wins"] / closed * 100.0) if closed else 0.0
            totals["hit_rate"] = (totals["wins"] / totals["generated"] * 100.0) if totals["generated"] else 0.0

        for key, bucket in by_type.items():
            if bucket["trades"] > 0 and bucket["_r_values"]:
                bucket["avg_r"] = sum(bucket["_r_values"]) / len(bucket["_r_values"])
                bucket["expectancy_r"] = bucket["avg_r"]
                closed = bucket["wins"] + bucket["losses"]
                bucket["win_rate"] = (bucket["wins"] / closed * 100.0) if closed else 0.0
                bucket["hit_rate"] = (bucket["wins"] / bucket["generated"] * 100.0) if bucket["generated"] else 0.0
            bucket.pop("_r_values", None)

        for key, bucket in by_strategy.items():
            if bucket["trades"] > 0 and bucket["_r_values"]:
                bucket["avg_r"] = sum(bucket["_r_values"]) / len(bucket["_r_values"])
                bucket["expectancy_r"] = bucket["avg_r"]
                closed = bucket["wins"] + bucket["losses"]
                bucket["win_rate"] = (bucket["wins"] / closed * 100.0) if closed else 0.0
                bucket["hit_rate"] = (bucket["wins"] / bucket["generated"] * 100.0) if bucket["generated"] else 0.0
            bucket.pop("_r_values", None)

        def best_worst(bucket):
            ranked = []
            for key, stats in bucket.items():
                closed = int(stats.get("wins", 0)) + int(stats.get("losses", 0))
                trades = int(stats.get("trades", 0))
                wr = float(stats.get("win_rate", 0.0))
                avg_r = float(stats.get("avg_r", 0.0))
                if trades <= 0:
                    continue
                ranked.append((key, wr, avg_r, trades, closed))
            if not ranked:
                return None, None
            best = max(ranked, key=lambda item: (item[1], item[2], item[3]))
            worst = min(ranked, key=lambda item: (item[1], item[2], -item[3]))
            return (
                {"name": best[0], "win_rate": best[1], "avg_r": best[2], "trades": best[3], "closed": best[4]},
                {"name": worst[0], "win_rate": worst[1], "avg_r": worst[2], "trades": worst[3], "closed": worst[4]},
            )

        best_tf, worst_tf = best_worst(by_tf)
        best_strategy, worst_strategy = best_worst(by_strategy)

        return {
            "period_days": days,
            "totals": totals,
            "by_signal_type": by_type,
            "by_strategy": by_strategy,
            "by_timeframe": by_tf,
            "by_side": by_side,
            "by_session": by_session,
            "best_timeframe": best_tf,
            "worst_timeframe": worst_tf,
            "best_strategy": best_strategy,
            "worst_strategy": worst_strategy,
        }

    def get_open_signal_counts(self, signal_type="SCALP"):
        """Count currently open signals for exposure control."""
        terminal = {"SL", "TP3", "CLOSED", "ENTRY_CLOSE", "PROFIT_SL"}
        counts = {"total": 0, "by_side": {}, "by_tf": {}}
        target_type = str(signal_type).upper()

        for sig in self.signals:
            if str(sig.get("type", "")).upper() != target_type:
                continue
            if sig.get("status") in terminal:
                continue

            counts["total"] += 1
            side = str(sig.get("side", "N/A")).upper()
            tf = str(sig.get("tf", "N/A"))
            counts["by_side"][side] = counts["by_side"].get(side, 0) + 1
            counts["by_tf"][tf] = counts["by_tf"].get(tf, 0) + 1

        return counts

    def count_signals_for_day(self, strategy=None, signal_type=None, now_utc=None):
        """Count signals logged since current UTC midnight with optional filters."""
        now_utc = now_utc.astimezone(timezone.utc) if now_utc else datetime.now(timezone.utc)
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        count = 0

        for sig in self.signals:
            logged_at = self._parse_iso_utc(sig.get("logged_at"))
            if not logged_at or logged_at < day_start or logged_at > now_utc:
                continue
            if signal_type and str(sig.get("type", "")).upper() != str(signal_type).upper():
                continue
            meta = sig.get("meta") or {}
            if strategy and str(meta.get("strategy", "")) != str(strategy):
                continue
            count += 1

        return count

    def get_recent_signal_health(self, signal_type="SCALP", limit=25):
        """Return recent closed performance for adaptive scalp tuning."""
        target_type = str(signal_type).upper()
        closed = []
        for sig in reversed(self.signals):
            if str(sig.get("type", "")).upper() != target_type:
                continue
            status = sig.get("status")
            if status not in {"SL", "TP3", "ENTRY_CLOSE", "PROFIT_SL"}:
                continue
            closed.append(sig)
            if len(closed) >= limit:
                break

        trades = len(closed)
        if trades == 0:
            return {"trades": 0, "wins": 0, "losses": 0, "breakeven": 0, "win_rate": 0.0, "avg_r": 0.0}

        wins = losses = breakeven = 0
        r_values = []
        for sig in closed:
            outcome, r_mult = self._metric_outcome(sig)
            if outcome == "wins":
                wins += 1
            elif outcome == "losses":
                losses += 1
            elif outcome == "breakeven":
                breakeven += 1
            r_values.append(r_mult)

        closed_only = wins + losses
        return {
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate": (wins / closed_only * 100.0) if closed_only else 0.0,
            "avg_r": (sum(r_values) / len(r_values)) if r_values else 0.0,
        }

    def cleanup_old(self, days=365):
        """Remove signals older than N days."""
        cutoff = datetime.now(timezone.utc)
        keep = []
        for s in self.signals:
            try:
                logged = datetime.fromisoformat(s["logged_at"])
                diff = (cutoff - logged).days
                if diff <= days:
                    keep.append(s)
            except Exception:
                keep.append(s)
        self.signals = keep
        self._save()
