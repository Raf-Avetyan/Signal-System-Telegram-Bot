# ─── Ponch Signal Performance Tracker ─────────────────────────

"""
Tracks scalp CONFIRMED signals and checks if TP/SL were hit.
Persists data to signals_log.json for restart survival.
"""

import json
import os
from datetime import datetime, timezone

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

    def log_signal(self, side, entry, sl, tp1, tp2, tp3, tf, timestamp, msg_id=None, chat_id=None, signal_type="SCALP", meta=None):
        """Log a new CONFIRMED scalp or confluence signal with TG data."""
        signal = {
            "type": signal_type,
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "tf": tf,
            "timestamp": timestamp,
            "entry_candle_ts": timestamp,  # candle that produced this signal — skip TP checks on this candle
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "status": "OPEN",       # OPEN, TP1, TP2, TP3, SL, CLOSED
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "sl_hit": False,
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

    def check_outcomes(self, current_price, high=None, low=None, current_candle_ts=None):
        """
        Check all OPEN signals against price movement.
        Uses high/low if provided to catch wicks (much more accurate).
        current_candle_ts: skip signals whose entry candle matches this — price
        hasn't closed yet so TPs/SL on that candle are unreliable.
        """
        changed = False
        events = []

        # Fallbacks to current_price if high/low not provided
        p_high = high if high is not None else current_price
        p_low = low if low is not None else current_price

        # Clear new-this-tick set each call
        self._new_this_tick.clear()

        for sig in self.signals:
            if sig["status"] in ("SL", "TP3", "CLOSED"):
                continue

            # Skip signal if we're still on the candle it was born on
            if current_candle_ts and sig.get("entry_candle_ts") == current_candle_ts:
                continue

            is_long = sig["side"] == "LONG"

            # 1. Check TPs (progressive)
            # Longs use High to hit TP, Shorts use Low to hit TP
            tp_price = p_high if is_long else p_low

            if is_long:
                if not sig["tp1_hit"] and tp_price >= sig["tp1"]:
                    sig["tp1_hit"] = True
                    sig["status"] = "TP1"
                    changed = True
                    events.append({"type": "TP1", "sig": sig})
                if not sig["tp2_hit"] and tp_price >= sig["tp2"]:
                    sig["tp2_hit"] = True
                    sig["status"] = "TP2"
                    changed = True
                    events.append({"type": "TP2", "sig": sig})
                if not sig["tp3_hit"] and tp_price >= sig["tp3"]:
                    sig["tp3_hit"] = True
                    sig["status"] = "TP3"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "TP3", "sig": sig})
                    continue
            else:
                if not sig["tp1_hit"] and tp_price <= sig["tp1"]:
                    sig["tp1_hit"] = True
                    sig["status"] = "TP1"
                    changed = True
                    events.append({"type": "TP1", "sig": sig})
                if not sig["tp2_hit"] and tp_price <= sig["tp2"]:
                    sig["tp2_hit"] = True
                    sig["status"] = "TP2"
                    changed = True
                    events.append({"type": "TP2", "sig": sig})
                if not sig["tp3_hit"] and tp_price <= sig["tp3"]:
                    sig["tp3_hit"] = True
                    sig["status"] = "TP3"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "TP3", "sig": sig})
                    continue

            # 2. Check Entry Return (Breakeven after TP)
            # If TP1 hit, and we touch entry again, close as ENTRY_CLOSE
            if sig.get("tp1_hit") and sig["status"] != "ENTRY_CLOSE":
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
                if sig["tp1_hit"]:
                    # Fallback — usually ENTRY_CLOSE should hit first if checked before SL
                    sig["status"] = "CLOSED" 
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                else:
                    sig["sl_hit"] = True
                    sig["status"] = "SL"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "SL", "sig": sig})
                continue

        if changed:
            self._save()

        return events

    def get_daily_summary(self, date_str=None, since=None, until=None):
        """Get performance stats for signals in a time range.

        - since/until: datetime objects (UTC) for range filtering.
        - date_str: fallback full-day filter (YYYY-MM-DD) if since/until not provided.
        """
        if since is not None and until is not None:
            today_signals = []
            for s in self.signals:
                try:
                    logged = datetime.fromisoformat(s["logged_at"])
                    if since <= logged < until:
                        today_signals.append(s)
                except Exception:
                    continue
        else:
            target_date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_signals = [
                s for s in self.signals
                if s["logged_at"].startswith(target_date)
            ]

        total = len(today_signals)
        if total == 0:
            return None

        tp1_hits = sum(1 for s in today_signals if s["tp1_hit"])
        tp2_hits = sum(1 for s in today_signals if s["tp2_hit"])
        tp3_hits = sum(1 for s in today_signals if s["tp3_hit"])
        sl_hits  = sum(1 for s in today_signals if s["sl_hit"])
        still_open = sum(1 for s in today_signals if s["status"] == "OPEN")

        win_rate = (tp1_hits / total * 100) if total > 0 else 0

        return {
            "total": total,
            "tp1_hits": tp1_hits,
            "tp2_hits": tp2_hits,
            "tp3_hits": tp3_hits,
            "sl_hits": sl_hits,
            "still_open": still_open,
            "win_rate": win_rate,
        }

    def get_session_stats(self, session_start_hour, session_end_hour):
        """Get stats for signals fired during a specific session window."""
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")

        session_signals = []
        for s in self.signals:
            if not s["logged_at"].startswith(today):
                continue
            try:
                logged = datetime.fromisoformat(s["logged_at"])
                if session_start_hour <= logged.hour < session_end_hour:
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

    def cleanup_old(self, days=7):
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
