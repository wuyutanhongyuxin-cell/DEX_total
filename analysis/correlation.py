"""
Cross-exchange price correlation analysis.

Maintains rolling price windows for N exchanges and computes pairwise
Pearson correlation to identify co-moving (and divergent) venues.
"""

import logging
import time
from collections import deque
from decimal import Decimal
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DEFAULT_WINDOW = 300  # data points


class CorrelationAnalyzer:
    """Rolling NxN price correlation across exchanges."""

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def __init__(self, window_size: int = _DEFAULT_WINDOW):
        """
        Args:
            window_size: Number of price observations to keep per exchange
                for the rolling correlation window.
        """
        if window_size < 2:
            raise ValueError("window_size must be >= 2")

        self._window_size = window_size

        # exchange -> deque of float mid-prices (most recent at right)
        self._prices: dict[str, deque[float]] = {}

        # exchange -> deque of float timestamps (parallel to _prices)
        self._timestamps: dict[str, deque[float]] = {}

        # Cached matrix from last computation
        self._last_matrix: dict[tuple[str, str], float] = {}
        self._last_computed: float = 0.0

        logger.info(
            "CorrelationAnalyzer initialised  window=%d", window_size
        )

    # ------------------------------------------------------------------
    # Price updates
    # ------------------------------------------------------------------
    def update_price(self, exchange: str, mid_price: Decimal) -> None:
        """Append *mid_price* to the rolling window for *exchange*."""
        price_f = float(mid_price)
        if price_f <= 0:
            logger.warning(
                "Ignoring non-positive price for %s: %s", exchange, mid_price
            )
            return

        if exchange not in self._prices:
            self._prices[exchange] = deque(maxlen=self._window_size)
            self._timestamps[exchange] = deque(maxlen=self._window_size)

        self._prices[exchange].append(price_f)
        self._timestamps[exchange].append(time.time())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _exchanges_with_data(self, min_points: int = 10) -> list[str]:
        """Exchanges that have at least *min_points* observations."""
        return [
            ex
            for ex, dq in self._prices.items()
            if len(dq) >= min_points
        ]

    @staticmethod
    def _truncate_to_common_length(*arrays: deque) -> list[np.ndarray]:
        """Truncate deques to the same (minimum) length and return as arrays."""
        min_len = min(len(a) for a in arrays)
        return [np.array(list(a)[-min_len:], dtype=np.float64) for a in arrays]

    # ------------------------------------------------------------------
    # Correlation matrix
    # ------------------------------------------------------------------
    def compute_correlation_matrix(self) -> dict[tuple[str, str], float]:
        """
        Compute pairwise Pearson correlation for all exchanges with
        sufficient data.

        Returns:
            Dict mapping ``(exchange_a, exchange_b)`` to correlation float
            in [-1, 1].  Only unique pairs are stored (a < b alphabetically).
        """
        exchanges = sorted(self._exchanges_with_data())
        if len(exchanges) < 2:
            logger.debug(
                "Not enough exchanges with data for correlation (%d)",
                len(exchanges),
            )
            return {}

        # Build aligned price matrix — truncate to common length
        min_len = min(len(self._prices[ex]) for ex in exchanges)
        if min_len < 2:
            return {}

        matrix = np.empty((len(exchanges), min_len), dtype=np.float64)
        for i, ex in enumerate(exchanges):
            matrix[i] = np.array(list(self._prices[ex])[-min_len:])

        # numpy corrcoef returns the full NxN correlation matrix
        corr = np.corrcoef(matrix)

        result: dict[tuple[str, str], float] = {}
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                val = float(corr[i, j])
                # Guard against NaN from constant series
                if np.isnan(val):
                    val = 0.0
                result[(exchanges[i], exchanges[j])] = val

        self._last_matrix = result
        self._last_computed = time.time()
        return result

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def get_most_correlated(
        self, exchange: str, n: int = 3
    ) -> list[tuple[str, float]]:
        """
        Top *n* exchanges most correlated with *exchange*.

        Uses the last computed matrix.  Call ``compute_correlation_matrix()``
        first if you need fresh data.
        """
        if not self._last_matrix:
            self.compute_correlation_matrix()

        pairs: list[tuple[str, float]] = []
        for (a, b), corr in self._last_matrix.items():
            if a == exchange:
                pairs.append((b, corr))
            elif b == exchange:
                pairs.append((a, corr))

        pairs.sort(key=lambda x: x[1], reverse=True)
        return pairs[:n]

    def get_least_correlated(
        self, n: int = 3
    ) -> list[tuple[tuple[str, str], float]]:
        """
        Find the *n* least-correlated exchange pairs.

        Low correlation may indicate delayed price propagation or structural
        differences — potential arbitrage candidates.
        """
        if not self._last_matrix:
            self.compute_correlation_matrix()

        sorted_pairs = sorted(self._last_matrix.items(), key=lambda x: x[1])
        return sorted_pairs[:n]

    def get_correlation(self, ex_a: str, ex_b: str) -> Optional[float]:
        """Return cached correlation between two exchanges, or None."""
        if not self._last_matrix:
            return None

        a, b = sorted([ex_a, ex_b])
        return self._last_matrix.get((a, b))

    # ------------------------------------------------------------------
    # Summary / debug
    # ------------------------------------------------------------------
    def summary(self) -> dict:
        """Diagnostic snapshot."""
        exchanges = list(self._prices.keys())
        return {
            "exchanges_tracked": exchanges,
            "data_points": {
                ex: len(self._prices[ex]) for ex in exchanges
            },
            "pairs_computed": len(self._last_matrix),
            "last_computed_ago_s": (
                round(time.time() - self._last_computed, 1)
                if self._last_computed
                else None
            ),
        }

    def reset(self) -> None:
        """Clear all internal state."""
        self._prices.clear()
        self._timestamps.clear()
        self._last_matrix.clear()
        self._last_computed = 0.0
        logger.info("CorrelationAnalyzer state reset")
