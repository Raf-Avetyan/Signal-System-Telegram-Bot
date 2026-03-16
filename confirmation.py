# ─── Ponch Confirmation Aggregation ───────────────────────────

"""
Aggregates signals from multiple indicators within a time window.
When 2+ indicators agree → ✅ STRONG
When 3+ indicators agree → 🔥 EXTREME
"""

import time
from config import STRONG_THRESHOLD, EXTREME_THRESHOLD, CONFIRMATION_WINDOW


class ConfirmationTracker:
    """
    Tracks signals from multiple indicators and triggers
    STRONG / EXTREME when confirmations reach thresholds.
    """

    def __init__(self):
        # Active signals: {"LONG": [...], "SHORT": [...]}
        self.signals = {"LONG": [], "SHORT": []}
        # Track what we've already sent to avoid duplicates
        self.last_strong_count  = {"LONG": 0, "SHORT": 0}
        self.last_extreme_count = {"LONG": 0, "SHORT": 0}

    def add_signal(self, signal_dict):
        """
        Add a new signal from any indicator.

        signal_dict: {
            "side": "LONG"|"SHORT",
            "indicator": "Ponch_Trader",
            "signal": "ENTRY L3",
            "points": 4,
            "timestamp": time.time()
        }
        """
        side = signal_dict["side"]
        signal_dict.setdefault("timestamp", time.time())
        self.signals[side].append(signal_dict)

        # Clean old signals outside the window
        self._cleanup(side)

    def check_confirmations(self, side):
        """
        Check if confirmations reach STRONG or EXTREME threshold.

        Returns list of events:
        [{"type": "STRONG"|"EXTREME", "side": ..., "points": ...,
          "confirmations": ..., "indicators": [...]}]
        """
        self._cleanup(side)

        active = self.signals[side]
        if not active:
            return []

        events = []

        # Deduplicate by indicator name (keep highest point version)
        by_indicator = {}
        for sig in active:
            name = sig["indicator"]
            if name not in by_indicator or sig["points"] > by_indicator[name]["points"]:
                by_indicator[name] = sig

        confirmations = len(by_indicator)
        total_points = sum(s["points"] for s in by_indicator.values())

        indicators_list = [
            {
                "name":   sig["indicator"],
                "signal": f"ENTRY {sig['signal']}" if not sig["signal"].startswith("ENTRY") else sig["signal"],
                "points": sig["points"],
                "tf":     sig.get("tf", "N/A"),
            }
            for sig in by_indicator.values()
        ]

        # Check EXTREME first (3+)
        if confirmations >= EXTREME_THRESHOLD and confirmations > self.last_extreme_count[side]:
            events.append({
                "type":          "EXTREME",
                "side":          side,
                "points":        total_points,
                "confirmations": confirmations,
                "indicators":    indicators_list,
            })
            self.last_extreme_count[side] = confirmations
            # Also update strong count to suppress it
            self.last_strong_count[side] = max(self.last_strong_count[side], confirmations)

        # Check STRONG (2+)
        elif confirmations >= STRONG_THRESHOLD and confirmations > self.last_strong_count[side]:
            events.append({
                "type":          "STRONG",
                "side":          side,
                "points":        total_points,
                "confirmations": confirmations,
                "indicators":    indicators_list,
            })
            self.last_strong_count[side] = confirmations

        return events

    def _cleanup(self, side):
        """Remove signals older than the confirmation window."""
        cutoff = time.time() - CONFIRMATION_WINDOW
        self.signals[side] = [
            s for s in self.signals[side]
            if s.get("timestamp", 0) > cutoff
        ]

        # Reset counts if all signals expired
        if not self.signals[side]:
            self.last_strong_count[side] = 0
            self.last_extreme_count[side] = 0

    def reset(self, side=None):
        """Reset tracking for one or both sides."""
        if side:
            self.signals[side] = []
            self.last_strong_count[side] = 0
            self.last_extreme_count[side] = 0
        else:
            for s in ["LONG", "SHORT"]:
                self.reset(s)

    def to_dict(self):
        """Export state for persistence."""
        return {
            "signals": self.signals,
            "last_strong_count": self.last_strong_count,
            "last_extreme_count": self.last_extreme_count
        }

    def from_dict(self, data):
        """Import state for persistence."""
        if not data: return
        self.signals = data.get("signals", {"LONG": [], "SHORT": []})
        self.last_strong_count = data.get("last_strong_count", {"LONG": 0, "SHORT": 0})
        self.last_extreme_count = data.get("last_extreme_count", {"LONG": 0, "SHORT": 0})
