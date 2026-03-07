"""
SignalGenerator -- Multi-dimensional confidence scoring for cross-exchange arbitrage.
Generates signals with confidence scores (0-1). Does NOT execute trades.
"""

import logging
import time
from collections import deque
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


class SignalGenerator:
    """Generate arbitrage signals with confidence scoring.

    Uses weighted multi-factor scoring instead of hard gates
    to avoid the "0 trades" problem of binary filtering.
    """

    def __init__(
        self,
        min_confidence: float = 0.3,
        cooldown_s: float = 5.0,
        warmup_ticks: int = 30,
        weights: Optional[dict] = None,
    ):
        self.min_confidence = min_confidence
        self.cooldown_s = cooldown_s
        self.warmup_ticks = warmup_ticks
        self.weights = weights or {
            "spread_magnitude": 0.30,
            "spread_persistence": 0.20,
            "volume_confirmation": 0.15,
            "funding_alignment": 0.10,
            "lead_lag_signal": 0.15,
            "oi_divergence": 0.10,
        }

        self._tick_count: int = 0
        self._last_signal_time: float = 0.0
        self._signal_count: int = 0
        self._spread_history: dict[str, deque] = {}
        self._funding_cache: dict[str, Decimal] = {}
        self._oi_cache: dict[str, Decimal] = {}
        self._leader_exchange: Optional[str] = None

    def update_funding(self, exchange: str, rate: Decimal) -> None:
        self._funding_cache[exchange] = rate

    def update_oi(self, exchange: str, oi: Decimal) -> None:
        self._oi_cache[exchange] = oi

    def set_leader(self, exchange: str) -> None:
        self._leader_exchange = exchange

    def evaluate(self, spreads: list[dict], now: float = None) -> list[dict]:
        """Evaluate spread matrix and generate signals.

        Args:
            spreads: list of spread dicts from SpreadAnalyzer.compute_matrix()
            now: optional timestamp override (for backtest replay).
                 Defaults to time.time() for live use.

        Returns:
            list of signal dicts sorted by confidence desc
        """
        self._tick_count += 1
        if now is None:
            now = time.time()

        if self._tick_count < self.warmup_ticks:
            return []

        if (now - self._last_signal_time) < self.cooldown_s:
            return []

        signals = []
        for spread in spreads:
            net_bps = float(spread.get("net_spread_bps", 0))
            if net_bps <= 0:
                continue

            pair_key = f"{spread['buy_exchange']}->{spread['sell_exchange']}"
            confidence, components = self._score(spread, pair_key, now)

            if confidence >= self.min_confidence:
                signal = {
                    "timestamp": now,
                    "buy_exchange": spread["buy_exchange"],
                    "sell_exchange": spread["sell_exchange"],
                    "symbol": spread.get("symbol", ""),
                    "direction": f"buy@{spread['buy_exchange']}_sell@{spread['sell_exchange']}",
                    "spread_bps": net_bps,
                    "buy_price": str(spread.get("buy_price", "")),
                    "sell_price": str(spread.get("sell_price", "")),
                    "confidence": round(confidence, 4),
                    "components": components,
                }
                signals.append(signal)

        signals.sort(key=lambda s: s["confidence"], reverse=True)

        if signals:
            self._last_signal_time = now
            self._signal_count += len(signals)

        return signals

    def _score(self, spread: dict, pair_key: str, now: float) -> tuple[float, dict]:
        """Compute weighted confidence score."""
        components = {}

        # 1. Spread magnitude: 0 at 0bps, 1.0 at 20+ bps
        net_bps = float(spread.get("net_spread_bps", 0))
        components["spread_magnitude"] = round(min(net_bps / 20.0, 1.0), 3)

        # 2. Spread persistence
        components["spread_persistence"] = round(
            self._score_persistence(pair_key, net_bps, now), 3
        )

        # 3. Volume confirmation (placeholder)
        components["volume_confirmation"] = 0.5

        # 4. Funding alignment
        components["funding_alignment"] = round(self._score_funding(spread), 3)

        # 5. Lead-lag signal
        components["lead_lag_signal"] = round(self._score_lead_lag(spread), 3)

        # 6. OI divergence
        components["oi_divergence"] = round(self._score_oi(spread), 3)

        confidence = sum(self.weights.get(k, 0) * v for k, v in components.items())
        return confidence, components

    def _score_persistence(self, pair_key: str, net_bps: float, now: float) -> float:
        if pair_key not in self._spread_history:
            self._spread_history[pair_key] = deque(maxlen=300)

        history = self._spread_history[pair_key]
        history.append((now, net_bps))

        persist_s = 0.0
        for ts, bps in reversed(history):
            if bps <= 0:
                break
            persist_s = now - ts

        return min(persist_s / 10.0, 1.0)

    def _score_funding(self, spread: dict) -> float:
        buy_f = self._funding_cache.get(spread.get("buy_exchange", ""))
        sell_f = self._funding_cache.get(spread.get("sell_exchange", ""))
        if buy_f is None or sell_f is None:
            return 0.5
        diff = float(sell_f - buy_f)
        return min(max(diff * 10000 + 0.5, 0.0), 1.0)

    def _score_lead_lag(self, spread: dict) -> float:
        if not self._leader_exchange:
            return 0.5
        if spread.get("buy_exchange") == self._leader_exchange:
            return 0.8
        if spread.get("sell_exchange") == self._leader_exchange:
            return 0.8
        return 0.5

    def _score_oi(self, spread: dict) -> float:
        buy_oi = self._oi_cache.get(spread.get("buy_exchange", ""))
        sell_oi = self._oi_cache.get(spread.get("sell_exchange", ""))
        if buy_oi is None or sell_oi is None:
            return 0.5
        total = float(buy_oi + sell_oi)
        if total == 0:
            return 0.5
        ratio = abs(float(buy_oi - sell_oi)) / total
        return min(0.5 + ratio, 1.0)

    @property
    def stats(self) -> dict:
        return {
            "tick_count": self._tick_count,
            "signal_count": self._signal_count,
            "warmed_up": self._tick_count >= self.warmup_ticks,
        }
