"""
Leader-follower relationship detection between exchanges.

Uses cross-correlation of price-change time series to determine which
exchanges move first (leaders) and which follow (laggards), plus the
typical lag in milliseconds.
"""

import logging
import time
from collections import defaultdict, deque
from decimal import Decimal
from typing import Optional

import numpy as np
from scipy import signal as scipy_signal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DEFAULT_WINDOW_S = 300
_DEFAULT_LAG_BINS_MS = [50, 100, 200, 500, 1000]
_MIN_DATA_POINTS = 30  # minimum observations before computing


class LeadLagAnalyzer:
    """Detect leader-follower dynamics across N exchanges."""

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def __init__(
        self,
        window_s: int = _DEFAULT_WINDOW_S,
        lag_bins_ms: Optional[list[int]] = None,
        min_correlation: float = 0.7,
    ):
        """
        Args:
            window_s: Rolling window in seconds for price history.
            lag_bins_ms: Lag buckets (ms) for discretised cross-correlation.
            min_correlation: Minimum peak cross-correlation to consider a
                lead-lag relationship significant.
        """
        self._window_s = window_s
        self._lag_bins_ms = lag_bins_ms or list(_DEFAULT_LAG_BINS_MS)
        self._min_correlation = min_correlation

        # exchange -> deque of (timestamp_s, price_float)
        self._price_ts: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=10_000)
        )

        # Cached results
        self._last_leaders: list[dict] = []
        self._last_matrix: dict[tuple[str, str], dict] = {}

        logger.info(
            "LeadLagAnalyzer initialised  window=%ds  lag_bins=%s  "
            "min_corr=%.2f",
            window_s,
            self._lag_bins_ms,
            min_correlation,
        )

    # ------------------------------------------------------------------
    # Price updates
    # ------------------------------------------------------------------
    def update_price(
        self, exchange: str, price: Decimal, timestamp: Optional[float] = None
    ) -> None:
        """Record a price observation for *exchange*."""
        ts = timestamp if timestamp is not None else time.time()
        pf = float(price)
        if pf <= 0:
            return
        self._price_ts[exchange].append((ts, pf))

    # ------------------------------------------------------------------
    # Internal: aligned price-change series
    # ------------------------------------------------------------------
    def _aligned_returns(
        self, ex_a: str, ex_b: str
    ) -> Optional[tuple[np.ndarray, np.ndarray, float]]:
        """
        Build aligned price-change arrays for two exchanges.

        Uses linear interpolation to a common time grid so that
        cross-correlation lags map to real time offsets.

        Returns (changes_a, changes_b, dt) where *dt* is the grid
        spacing in seconds, or None if insufficient data.
        """
        raw_a = self._price_ts.get(ex_a)
        raw_b = self._price_ts.get(ex_b)
        if not raw_a or not raw_b:
            return None

        # Filter to rolling window
        cutoff = time.time() - self._window_s
        a_filt = [(t, p) for t, p in raw_a if t >= cutoff]
        b_filt = [(t, p) for t, p in raw_b if t >= cutoff]

        if len(a_filt) < _MIN_DATA_POINTS or len(b_filt) < _MIN_DATA_POINTS:
            return None

        # Determine overlapping time range
        t_start = max(a_filt[0][0], b_filt[0][0])
        t_end = min(a_filt[-1][0], b_filt[-1][0])
        if t_end - t_start < 1.0:
            return None

        # Choose grid spacing: smallest median inter-tick, floored at 10 ms
        def median_dt(series):
            ts_arr = np.array([t for t, _ in series])
            diffs = np.diff(ts_arr)
            return float(np.median(diffs)) if len(diffs) > 0 else 1.0

        dt = max(0.01, min(median_dt(a_filt), median_dt(b_filt)))

        grid = np.arange(t_start, t_end, dt)
        if len(grid) < _MIN_DATA_POINTS:
            return None

        # Interpolate prices onto common grid
        ta = np.array([t for t, _ in a_filt])
        pa = np.array([p for _, p in a_filt])
        tb = np.array([t for t, _ in b_filt])
        pb = np.array([p for _, p in b_filt])

        interp_a = np.interp(grid, ta, pa)
        interp_b = np.interp(grid, tb, pb)

        # Price changes (first differences)
        diff_a = np.diff(interp_a)
        diff_b = np.diff(interp_b)

        # Normalise to zero-mean unit-variance for correlation
        std_a = np.std(diff_a)
        std_b = np.std(diff_b)
        if std_a < 1e-12 or std_b < 1e-12:
            return None

        diff_a = (diff_a - np.mean(diff_a)) / std_a
        diff_b = (diff_b - np.mean(diff_b)) / std_b

        return diff_a, diff_b, dt

    # ------------------------------------------------------------------
    # Pairwise lead-lag
    # ------------------------------------------------------------------
    def compute_lead_lag(self, leader: str, follower: str) -> Optional[dict]:
        """
        Cross-correlate price changes of *leader* and *follower*.

        Returns:
            {leader, follower, lag_ms, correlation, confidence}
            or None if insufficient data.

        ``lag_ms > 0`` means *leader* moves first by that many ms.
        ``lag_ms < 0`` means *follower* actually leads.
        """
        aligned = self._aligned_returns(leader, follower)
        if aligned is None:
            return None

        diff_leader, diff_follower, dt = aligned

        # Full cross-correlation using scipy (mode="full")
        corr = scipy_signal.correlate(diff_follower, diff_leader, mode="full")
        # Normalise
        corr = corr / len(diff_leader)

        # Lag axis: negative lags = leader leads
        n = len(diff_leader)
        lags = np.arange(-(n - 1), n)

        # Restrict to plausible lag range (max bin)
        max_lag_s = max(self._lag_bins_ms) / 1000.0
        max_lag_samples = int(max_lag_s / dt) + 1
        centre = n - 1
        lo = max(0, centre - max_lag_samples)
        hi = min(len(corr), centre + max_lag_samples + 1)

        corr_window = corr[lo:hi]
        lags_window = lags[lo:hi]

        if len(corr_window) == 0:
            return None

        # Peak
        peak_idx = int(np.argmax(corr_window))
        peak_corr = float(corr_window[peak_idx])
        peak_lag_samples = int(lags_window[peak_idx])
        peak_lag_ms = peak_lag_samples * dt * 1000.0

        # Confidence: ratio of peak to mean absolute correlation
        mean_abs = float(np.mean(np.abs(corr_window)))
        confidence = peak_corr / mean_abs if mean_abs > 1e-12 else 0.0
        confidence = min(confidence, 10.0)  # cap for display

        result = {
            "leader": leader,
            "follower": follower,
            "lag_ms": round(peak_lag_ms, 1),
            "correlation": round(peak_corr, 4),
            "confidence": round(confidence, 2),
        }

        # If the peak lag is negative the assumed roles are reversed
        if peak_lag_ms < 0:
            result["leader"] = follower
            result["follower"] = leader
            result["lag_ms"] = round(-peak_lag_ms, 1)

        return result

    # ------------------------------------------------------------------
    # Identify leaders
    # ------------------------------------------------------------------
    def identify_leaders(self) -> list[dict]:
        """
        Analyse all exchange pairs and rank by how often each exchange
        leads.

        Returns a sorted list (most frequent leader first):
            [{exchange, lead_count, avg_lag_ms, avg_correlation}, ...]
        """
        exchanges = sorted(self._price_ts.keys())
        if len(exchanges) < 2:
            return []

        lead_stats: dict[str, list[dict]] = defaultdict(list)

        for i, ex_a in enumerate(exchanges):
            for j in range(i + 1, len(exchanges)):
                ex_b = exchanges[j]
                result = self.compute_lead_lag(ex_a, ex_b)
                if result is None:
                    continue
                if result["correlation"] < self._min_correlation:
                    continue

                leader = result["leader"]
                lead_stats[leader].append(result)

                # Store in matrix cache
                self._last_matrix[(ex_a, ex_b)] = result

        # Aggregate
        summary: list[dict] = []
        for ex in exchanges:
            entries = lead_stats.get(ex, [])
            if not entries:
                summary.append(
                    {
                        "exchange": ex,
                        "lead_count": 0,
                        "avg_lag_ms": 0.0,
                        "avg_correlation": 0.0,
                    }
                )
                continue

            summary.append(
                {
                    "exchange": ex,
                    "lead_count": len(entries),
                    "avg_lag_ms": round(
                        float(np.mean([e["lag_ms"] for e in entries])), 1
                    ),
                    "avg_correlation": round(
                        float(np.mean([e["correlation"] for e in entries])), 4
                    ),
                }
            )

        summary.sort(key=lambda s: s["lead_count"], reverse=True)
        self._last_leaders = summary
        return summary

    # ------------------------------------------------------------------
    # Full matrix
    # ------------------------------------------------------------------
    def get_lead_lag_matrix(self) -> dict[tuple[str, str], dict]:
        """
        Return the full matrix of lead-lag relationships (computed during
        the last ``identify_leaders()`` call).

        Keys are ``(exchange_a, exchange_b)`` (alphabetical order).
        Values are the result dicts from ``compute_lead_lag()``.
        """
        if not self._last_matrix:
            self.identify_leaders()
        return dict(self._last_matrix)

    # ------------------------------------------------------------------
    # Summary / debug
    # ------------------------------------------------------------------
    def summary(self) -> dict:
        """Diagnostic snapshot."""
        exchanges = list(self._price_ts.keys())
        return {
            "exchanges_tracked": exchanges,
            "data_points": {
                ex: len(self._price_ts[ex]) for ex in exchanges
            },
            "pairs_analysed": len(self._last_matrix),
            "leaders": self._last_leaders[:3] if self._last_leaders else [],
        }

    def reset(self) -> None:
        """Clear all internal state."""
        self._price_ts.clear()
        self._last_leaders.clear()
        self._last_matrix.clear()
        logger.info("LeadLagAnalyzer state reset")
