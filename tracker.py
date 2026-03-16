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

    def log_signal(self, side, entry, sl, tp1, tp2, tp3, tf, timestamp):
        """Log a new CONFIRMED scalp signal."""
        signal = {
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "tf": tf,
            "timestamp": timestamp,
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "status": "OPEN",       # OPEN, TP1, TP2, TP3, SL
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "sl_hit": False,
            "teaser_sent": False,
            "closed_at": None,
        }
        self.signals.append(signal)
        self._save()
        print(f"  [TRACKER] Logged {side} @ {entry:,.2f} [{tf}]")

    def check_outcomes(self, current_price):
        """Check all OPEN signals against current price."""
        changed = False
        events = []

        for sig in self.signals:
            if sig["status"] in ("SL", "TP3", "CLOSED"):
                continue  # Already fully resolved

            is_long = sig["side"] == "LONG"

            # 1. Check TPs (progressive) - Check TPs FIRST
            if is_long:
                if not sig["tp1_hit"] and current_price >= sig["tp1"]:
                    sig["tp1_hit"] = True
                    sig["status"] = "TP1"
                    changed = True
                    events.append({"type": "TP1", "sig": sig})
                if not sig["tp2_hit"] and current_price >= sig["tp2"]:
                    sig["tp2_hit"] = True
                    sig["status"] = "TP2"
                    changed = True
                    events.append({"type": "TP2", "sig": sig})
                if not sig["tp3_hit"] and current_price >= sig["tp3"]:
                    sig["tp3_hit"] = True
                    sig["status"] = "TP3"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "TP3", "sig": sig})
                    continue # Signal finished
            else:
                if not sig["tp1_hit"] and current_price <= sig["tp1"]:
                    sig["tp1_hit"] = True
                    sig["status"] = "TP1"
                    changed = True
                    events.append({"type": "TP1", "sig": sig})
                if not sig["tp2_hit"] and current_price <= sig["tp2"]:
                    sig["tp2_hit"] = True
                    sig["status"] = "TP2"
                    changed = True
                    events.append({"type": "TP2", "sig": sig})
                if not sig["tp3_hit"] and current_price <= sig["tp3"]:
                    sig["tp3_hit"] = True
                    sig["status"] = "TP3"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "TP3", "sig": sig})
                    continue # Signal finished

            # 2. Check SL
            # If TP1 was already hit, returning to SL level just closes the signal 
            # WITHOUT marking it as sl_hit=True (so it won't show as a loss in summary).
            if (is_long and current_price <= sig["sl"]) or (not is_long and current_price >= sig["sl"]):
                if sig["tp1_hit"]:
                    # Close silently as a 'Successful' trade (TP1 hit)
                    sig["status"] = "CLOSED" 
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    print(f"  [TRACKER] {sig['side']} closed at SL level AFTER targeting TP1. (Not a loss)")
                else:
                    # Pure SL hit
                    sig["sl_hit"] = True
                    sig["status"] = "SL"
                    sig["closed_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
                    events.append({"type": "SL", "sig": sig})
                continue

        if changed:
            self._save()
        
        return events

    def get_daily_summary(self, date_str=None):
        """Get performance stats for signals on a specific date (YYYY-MM-DD)."""
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
