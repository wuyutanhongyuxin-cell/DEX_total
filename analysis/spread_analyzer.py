"""
NxN cross-exchange spread matrix analyzer.

Computes real-time spread opportunities across all connected exchanges,
accounting for fees, natural spreads, and persistence filtering.
"""

import logging
import time
from collections import defaultdict, deque
from decimal import Decimal, ROUND_DOWN
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_BPS = Decimal("10000")
_ZERO = Decimal("0")
_TWO = Decimal("2")
_MAX_HISTORY = 2000          # max ticks kept per deque
_NATURAL_SPREAD_MAX = 500    # max entries in natural-spread window


class SpreadAnalyzer:
    """Maintains BBO state for N exchanges and produces an NxN spread matrix."""

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def __init__(
        self,
        fee_estimate_bps: float = 5.0,
        natural_spread_window: int = 60,
        persistence_threshold_s: float = 2.0,
    ):
        """
        Args:
            fee_estimate_bps: Estimated one-way fee in basis points.
            natural_spread_window: Window (seconds) for computing rolling
                natural spread on each exchange.
            persistence_threshold_s: Minimum seconds a positive net spread
                must persist before it is considered actionable.
        """
        self._fee_estimate_bps = Decimal(str(fee_estimate_bps))
        self._natural_spread_window = natural_spread_window
        self._persistence_threshold_s = persistence_threshold_s

        # exchange -> (best_bid, best_ask, update_timestamp)
        self._bbos: dict[str, tuple[Decimal, Decimal, float]] = {}

        # exchange -> deque of (timestamp, natural_spread_bps)
        self._natural_spreads: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_NATURAL_SPREAD_MAX)
        )

        # (buy_exchange, sell_exchange) -> deque of (timestamp, net_spread_bps)
        self._spread_history: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_MAX_HISTORY)
        )

        # Cache the latest matrix so get_top_spreads works without recompute
        self._last_matrix: list[dict] = []

        logger.info(
            "SpreadAnalyzer initialised  fee=%.1f bps  natural_window=%ds  "
            "persistence=%.1fs",
            fee_estimate_bps,
            natural_spread_window,
            persistence_threshold_s,
        )

    # ------------------------------------------------------------------
    # BBO updates
    # ------------------------------------------------------------------
    def update_bbo(self, exchange: str, bid: Decimal, ask: Decimal,
                   timestamp: float = None) -> None:
        """Record latest best-bid / best-ask for *exchange*.

        Args:
            timestamp: optional override (for backtest replay).
                       Defaults to time.time() for live use.
        """
        now = timestamp if timestamp is not None else time.time()

        if bid <= _ZERO or ask <= _ZERO:
            logger.warning(
                "Ignoring invalid BBO for %s: bid=%s ask=%s", exchange, bid, ask
            )
            return

        if bid >= ask:
            logger.debug(
                "Crossed book on %s: bid=%s >= ask=%s (using as-is)",
                exchange, bid, ask,
            )

        self._bbos[exchange] = (bid, ask, now)

        # Track per-exchange natural spread
        if ask > _ZERO:
            mid = (bid + ask) / _TWO
            if mid > _ZERO:
                nat_bps = (ask - bid) / mid * _BPS
                self._natural_spreads[exchange].append((now, float(nat_bps)))

    # ------------------------------------------------------------------
    # Natural spread helpers
    # ------------------------------------------------------------------
    def _avg_natural_spread(self, exchange: str, now: float = None) -> float:
        """Average natural spread (bps) within the rolling window.

        Args:
            now: optional timestamp override (for backtest replay).
                 Defaults to time.time() for live use.
        """
        dq = self._natural_spreads.get(exchange)
        if not dq:
            return 0.0

        if now is None:
            now = time.time()
        cutoff = now - self._natural_spread_window
        values = [v for ts, v in dq if ts >= cutoff]
        if not values:
            return 0.0
        return float(np.mean(values))

    # ------------------------------------------------------------------
    # Matrix computation
    # ------------------------------------------------------------------
    def compute_matrix(self, symbol: str = "BTC-PERP", now: float = None) -> list[dict]:
        """
        Build the NxN cross-exchange spread matrix.

        For every ordered pair (buy_exchange, sell_exchange) where buy != sell:
          - raw_spread = sell_bid - buy_ask  (buy at the ask, sell at the bid)
          - spread_bps  = raw_spread / midprice * 10000
          - fee_cost_bps = 2 * fee_estimate_bps  (round trip)
          - natural_spread = max(0, average natural spread across both venues)
          - net_spread_bps = spread_bps - fee_cost_bps - natural_spread

        Returns a list of dicts sorted by net_spread_bps descending.
        """
        if now is None:
            now = time.time()
        exchanges = list(self._bbos.keys())
        results: list[dict] = []

        for buy_ex in exchanges:
            buy_bid, buy_ask, buy_ts = self._bbos[buy_ex]
            # Skip stale data (>30 s)
            if now - buy_ts > 30:
                continue

            for sell_ex in exchanges:
                if sell_ex == buy_ex:
                    continue

                sell_bid, sell_ask, sell_ts = self._bbos[sell_ex]
                if now - sell_ts > 30:
                    continue

                raw_spread = sell_bid - buy_ask
                mid = (buy_ask + sell_bid) / _TWO
                if mid <= _ZERO:
                    continue

                # 价格合理性检查: 同一资产不同交易所的价格不应偏离 50% 以上
                _ratio_limit = Decimal("1.5")
                buy_mid = (buy_bid + buy_ask) / _TWO
                sell_mid = (sell_bid + sell_ask) / _TWO
                if buy_mid > _ZERO and sell_mid > _ZERO:
                    ratio = max(buy_mid, sell_mid) / min(buy_mid, sell_mid)
                    if ratio > _ratio_limit:
                        continue

                spread_bps = raw_spread / mid * _BPS
                fee_cost_bps = _TWO * self._fee_estimate_bps
                nat_buy = max(0.0, self._avg_natural_spread(buy_ex, now=now))
                nat_sell = max(0.0, self._avg_natural_spread(sell_ex, now=now))
                natural_spread_bps = Decimal(str((nat_buy + nat_sell) / 2.0))
                net_spread_bps = spread_bps - fee_cost_bps - natural_spread_bps

                entry = {
                    "buy_exchange": buy_ex,
                    "sell_exchange": sell_ex,
                    "symbol": symbol,
                    "raw_spread": raw_spread,
                    "spread_bps": float(spread_bps),
                    "net_spread_bps": float(net_spread_bps),
                    "fee_cost_bps": float(fee_cost_bps),
                    "natural_spread_bps": float(natural_spread_bps),
                    "buy_price": buy_ask,
                    "sell_price": sell_bid,
                    "timestamp": now,
                }
                results.append(entry)

                # Persist for persistence tracking
                pair_key = (buy_ex, sell_ex)
                self._spread_history[pair_key].append(
                    (now, float(net_spread_bps))
                )

        # Sort descending by net_spread_bps
        results.sort(key=lambda r: r["net_spread_bps"], reverse=True)
        self._last_matrix = results
        return results

    # ------------------------------------------------------------------
    # Top spreads
    # ------------------------------------------------------------------
    def get_top_spreads(self, n: int = 5) -> list[dict]:
        """Return the top *n* spreads from the most recent matrix."""
        return self._last_matrix[:n]

    # ------------------------------------------------------------------
    # Persistence check
    # ------------------------------------------------------------------
    def check_persistence(
        self, buy_exchange: str, sell_exchange: str
    ) -> float:
        """
        How many continuous seconds the net spread for this pair has been > 0.

        Returns 0.0 if the spread has not persisted or there is insufficient
        history.
        """
        pair_key = (buy_exchange, sell_exchange)
        history = self._spread_history.get(pair_key)
        if not history or len(history) < 2:
            return 0.0

        # Walk backwards from most recent tick
        now = time.time()
        persistent_since: Optional[float] = None

        for ts, net_bps in reversed(history):
            if net_bps <= 0:
                break
            persistent_since = ts

        if persistent_since is None:
            return 0.0

        duration = now - persistent_since
        return duration

    def is_actionable(self, buy_exchange: str, sell_exchange: str) -> bool:
        """
        A spread is actionable when it has persisted above zero for at least
        ``persistence_threshold_s`` seconds.
        """
        return (
            self.check_persistence(buy_exchange, sell_exchange)
            >= self._persistence_threshold_s
        )

    # ------------------------------------------------------------------
    # Summary / debug
    # ------------------------------------------------------------------
    def summary(self) -> dict:
        """Quick diagnostic snapshot."""
        return {
            "exchanges_tracked": list(self._bbos.keys()),
            "matrix_entries": len(self._last_matrix),
            "positive_spreads": sum(
                1 for r in self._last_matrix if r["net_spread_bps"] > 0
            ),
            "top_spread_bps": (
                self._last_matrix[0]["net_spread_bps"]
                if self._last_matrix
                else None
            ),
        }

    def get_exchange_bbos(self) -> dict[str, dict]:
        """Return current BBO state for all exchanges (for monitoring)."""
        result = {}
        for ex, (bid, ask, ts) in self._bbos.items():
            mid = (bid + ask) / _TWO if ask > _ZERO else _ZERO
            result[ex] = {
                "bid": float(bid),
                "ask": float(ask),
                "mid": float(mid),
                "spread_bps": (
                    float((ask - bid) / mid * _BPS) if mid > _ZERO else 0.0
                ),
                "age_s": round(time.time() - ts, 2),
            }
        return result

    def reset(self) -> None:
        """Clear all internal state."""
        self._bbos.clear()
        self._natural_spreads.clear()
        self._spread_history.clear()
        self._last_matrix.clear()
        logger.info("SpreadAnalyzer state reset")
