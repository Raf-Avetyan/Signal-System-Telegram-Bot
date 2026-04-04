# ─── Ponch Signal Performance Tracker ─────────────────────────

"""
Tracks scalp CONFIRMED signals and checks if TP/SL were hit.
Persists data to signals_log.json for restart survival.
"""

import json
import os
from datetime import datetime, timezone, timedelta
from config import BREAKEVEN_WIN_MIN_TP

LOG_FILE = os.path.join(os.path.dirname(__file__), "signals_log.json")


class SignalTracker:
    """Track signal outcomes (TP1/TP2/TP3 hit or SL hit)."""

    def __init__(self):
        self.signals = self._load()
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

    def _metric_outcome(self, sig):
        """Return metric outcome label and R-multiple for summaries/analytics."""
        status = str(sig.get("status", "")).upper()
        entry = float(sig.get("entry", 0))
        sl = float(sig.get("sl", 0))
        tp1 = float(sig.get("tp1", entry))
        tp3 = float(sig.get("tp3", entry))
        risk = abs(entry - sl)

        if status == "SL":
            return "losses", -1.0
        if status == "PROFIT_SL":
            r_mult = max(0.2, abs(tp1 - entry) / risk * 0.6) if risk > 0 else 0.2
            return "wins", r_mult
        if status == "TP3":
            r_mult = (abs(tp3 - entry) / risk) if risk > 0 else 0.0
            return "wins", r_mult
        if status == "ENTRY_CLOSE":
            if self._breakeven_counts_as_win(sig):
                return "wins", 0.0
            return "breakeven", 0.0
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

        # Clear new-this-tick set each call
        self._new_this_tick.clear()

        terminal_statuses = {"SL", "TP3", "CLOSED", "ENTRY_CLOSE", "PROFIT_SL"}

        for sig in self.signals:
            if sig["status"] in terminal_statuses:
                continue

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

            # Ambiguous one-candle case: TP1 and SL both touched for a fresh trade.
            # We treat this as TP reached and then return to entry (breakeven close),
            # never as direct SL, to preserve scalp lifecycle semantics.
            if not is_entry_candle and not sig["tp1_hit"] and tp1_touched and sl_touched:
                sig["tp1_hit"] = True
                sig["tp1_at"] = sig.get("tp1_at") or datetime.now(timezone.utc).isoformat()
                sig["sl"] = float(sig["entry"])
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
                    if not sig["tp1_hit"] and tp_price >= sig["tp1"]:
                        sig["tp1_hit"] = True
                        sig["tp1_at"] = sig.get("tp1_at") or datetime.now(timezone.utc).isoformat()
                        sig["sl"] = float(sig["entry"])
                        sig["status"] = "TP1"
                        changed = True
                        events.append({"type": "TP1", "sig": sig})
                    if not sig["tp2_hit"] and tp_price >= sig["tp2"]:
                        sig["tp2_hit"] = True
                        sig["tp2_at"] = sig.get("tp2_at") or datetime.now(timezone.utc).isoformat()
                        sig["status"] = "TP2"
                        changed = True
                        events.append({"type": "TP2", "sig": sig})
                    if not sig["tp3_hit"] and tp_price >= sig["tp3"]:
                        sig["tp3_hit"] = True
                        sig["tp3_at"] = sig.get("tp3_at") or datetime.now(timezone.utc).isoformat()
                        sig["status"] = "TP3"
                        sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                        changed = True
                        events.append({"type": "TP3", "sig": sig})
                        continue
                else:
                    if not sig["tp1_hit"] and tp_price <= sig["tp1"]:
                        sig["tp1_hit"] = True
                        sig["tp1_at"] = sig.get("tp1_at") or datetime.now(timezone.utc).isoformat()
                        sig["sl"] = float(sig["entry"])
                        sig["status"] = "TP1"
                        changed = True
                        events.append({"type": "TP1", "sig": sig})
                    if not sig["tp2_hit"] and tp_price <= sig["tp2"]:
                        sig["tp2_hit"] = True
                        sig["tp2_at"] = sig.get("tp2_at") or datetime.now(timezone.utc).isoformat()
                        sig["status"] = "TP2"
                        changed = True
                        events.append({"type": "TP2", "sig": sig})
                    if not sig["tp3_hit"] and tp_price <= sig["tp3"]:
                        sig["tp3_hit"] = True
                        sig["tp3_at"] = sig.get("tp3_at") or datetime.now(timezone.utc).isoformat()
                        sig["status"] = "TP3"
                        sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                        changed = True
                        events.append({"type": "TP3", "sig": sig})
                        continue

            # 2. Check Entry Return when stop has been moved to breakeven.
            stop_at_entry = abs(float(sig.get("sl", sig["entry"])) - float(sig["entry"])) < 1e-9
            if sig.get("tp1_hit") and stop_at_entry and sig["status"] != "ENTRY_CLOSE":
                entry_hit = False
                if is_long and p_low <= sig["entry"]:
                    entry_hit = True
                elif not is_long and p_high >= sig["entry"]:
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
                if sig["tp1_hit"] and abs(float(sig.get("sl", sig["entry"])) - float(sig["entry"])) >= 1e-9:
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
        for sig in generated:
            outcome, _ = self._metric_outcome(sig)
            if outcome == "wins":
                wins += 1
            elif outcome == "losses":
                losses += 1
            elif outcome == "breakeven":
                breakeven += 1
        resolved = wins + losses
        win_rate = (wins / resolved * 100.0) if resolved > 0 else 0.0

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
            ensure_type_bucket(sig_type)
            by_type[sig_type]["generated"] += 1
            totals["generated"] += 1

            closed_statuses = {"SL", "TP3", "ENTRY_CLOSE", "PROFIT_SL"}
            if status not in closed_statuses:
                by_type[sig_type]["open"] += 1
                totals["open"] += 1
                continue

            outcome, r_mult = self._metric_outcome(sig)

            totals["trades"] += 1
            totals[outcome] += 1
            r_values.append(r_mult)
            by_type[sig_type]["trades"] += 1
            by_type[sig_type][outcome] += 1
            by_type[sig_type]["_r_values"].append(r_mult)

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

        return {
            "period_days": days,
            "totals": totals,
            "by_signal_type": by_type,
            "by_timeframe": by_tf,
            "by_side": by_side,
            "by_session": by_session,
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
