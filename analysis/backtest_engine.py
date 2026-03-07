"""
BacktestEngine -- Historical data replay and strategy backtesting.
Reads BBO CSVs, replays through a strategy function, computes stats.

P0 fixes applied:
- replay_with_pipeline() replicates live signal path exactly
- Timestamps injected into SpreadAnalyzer/SignalGenerator (no time collapse)
- Ticks batched by interval to prevent look-ahead bias
- compute_stats() handles both float and ISO string timestamps
"""

import csv
import logging
import os
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from analysis.spread_analyzer import SpreadAnalyzer
    from strategy.signal_generator import SignalGenerator

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


class BacktestEngine:
    """Replay historical BBO data through a strategy function."""

    def __init__(self, data_dir: str = "data", fee_bps: float = 5.0):
        self.data_dir = Path(data_dir)
        self.fee_bps = Decimal(str(fee_bps))

    def load_bbo_data(self, date: str, exchanges: list[str]) -> list[dict]:
        """Load BBO CSVs for a date, merge and sort by timestamp.

        Args:
            date: YYYYMMDD format
            exchanges: list of exchange names to load

        Returns:
            Sorted list of BBO tick dicts.
        """
        day_dir = self.data_dir / date
        if not day_dir.exists():
            raise FileNotFoundError(f"No data directory for date: {date}")

        all_rows = []
        for exchange in exchanges:
            filepath = day_dir / f"bbo_{exchange}.csv"
            if not filepath.exists():
                logger.warning(f"No BBO data for {exchange} on {date}")
                continue

            with open(filepath, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        all_rows.append({
                            "timestamp": row["timestamp"],
                            "exchange": row["exchange"],
                            "symbol": row.get("symbol", ""),
                            "bid": Decimal(row["bid"]) if row.get("bid") else ZERO,
                            "ask": Decimal(row["ask"]) if row.get("ask") else ZERO,
                            "bid_size": Decimal(row["bid_size"]) if row.get("bid_size") else ZERO,
                            "ask_size": Decimal(row["ask_size"]) if row.get("ask_size") else ZERO,
                        })
                    except Exception as e:
                        logger.debug(f"Skipping malformed row in {filepath}: {e}")

        all_rows.sort(key=lambda r: r["timestamp"])
        logger.info(f"Loaded {len(all_rows)} BBO ticks for {date} from {len(exchanges)} exchanges")
        return all_rows

    def load_spread_data(self, date: str) -> list[dict]:
        """Load spread matrix CSVs for a date."""
        day_dir = self.data_dir / date
        filepath = day_dir / "spreads_matrix.csv"
        if not filepath.exists():
            raise FileNotFoundError(f"No spread data for date: {date}")

        rows = []
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rows.append({
                        "timestamp": row["timestamp"],
                        "buy_exchange": row["buy_exchange"],
                        "sell_exchange": row["sell_exchange"],
                        "symbol": row.get("symbol", ""),
                        "spread_bps": Decimal(row["spread_bps"]),
                        "net_spread_bps": Decimal(row["net_spread_bps"]),
                        "buy_price": Decimal(row["buy_price"]),
                        "sell_price": Decimal(row["sell_price"]),
                    })
                except Exception as e:
                    logger.debug(f"Skipping malformed spread row: {e}")

        rows.sort(key=lambda r: r["timestamp"])
        logger.info(f"Loaded {len(rows)} spread ticks for {date}")
        return rows

    def replay(
        self,
        data: list[dict],
        strategy_fn: Callable[[dict, dict], Optional[dict]],
    ) -> dict:
        """Replay data through a custom strategy function.

        For replaying through the real SpreadAnalyzer + SignalGenerator pipeline,
        use replay_with_pipeline() instead.

        Args:
            data: sorted list of tick dicts
            strategy_fn: callable(tick, state) -> Optional[signal_dict]

        Returns:
            {signals: list[dict], stats: dict}
        """
        signals = []
        state = {"bbo_cache": {}, "tick_count": 0}

        for tick in data:
            state["tick_count"] += 1

            if "exchange" in tick and "bid" in tick:
                state["bbo_cache"][tick["exchange"]] = {
                    "bid": tick["bid"],
                    "ask": tick["ask"],
                    "timestamp": tick["timestamp"],
                }

            try:
                signal = strategy_fn(tick, state)
                if signal is not None:
                    signals.append(signal)
            except Exception as e:
                logger.warning(f"Strategy error at tick {state['tick_count']}: {e}")

        stats = self.compute_stats(signals)
        return {"signals": signals, "stats": stats}

    def replay_with_pipeline(
        self,
        bbo_data: list[dict],
        spread_analyzer: "SpreadAnalyzer",
        signal_gen: "SignalGenerator",
        symbol: str = "BTC-PERP",
        batch_interval_s: float = 1.0,
    ) -> dict:
        """Replay BBO data through the real live pipeline.

        Fixes P0 issues vs raw replay():
        - Routes through SpreadAnalyzer -> SignalGenerator (same as live)
        - Injects historical timestamps (no time.time() collapse)
        - Batches ticks by interval (prevents look-ahead bias)

        Args:
            bbo_data: sorted list of BBO tick dicts from load_bbo_data()
            spread_analyzer: SpreadAnalyzer instance (will be mutated)
            signal_gen: SignalGenerator instance (will be mutated)
            symbol: normalized symbol for spread matrix
            batch_interval_s: batch window matching live BBO loop interval

        Returns:
            {signals: list[dict], stats: dict}
        """
        signals = []
        if not bbo_data:
            return {"signals": [], "stats": self.compute_stats([])}

        # Group ticks into time-aligned batches
        batches = self._group_by_interval(bbo_data, batch_interval_s)

        for batch_ts, ticks in batches:
            # Parse batch timestamp to epoch seconds
            batch_epoch = self._parse_ts_to_epoch(batch_ts)

            # Feed all ticks in this batch to SpreadAnalyzer
            for tick in ticks:
                exchange = tick.get("exchange", "")
                bid = tick.get("bid", ZERO)
                ask = tick.get("ask", ZERO)
                if exchange and bid > ZERO and ask > ZERO:
                    tick_epoch = self._parse_ts_to_epoch(tick["timestamp"])
                    spread_analyzer.update_bbo(exchange, bid, ask, timestamp=tick_epoch)

            # Compute matrix AFTER all exchanges in batch are updated
            matrix = spread_analyzer.compute_matrix(symbol, now=batch_epoch)

            # Evaluate signals with historical timestamp
            batch_signals = signal_gen.evaluate(matrix, now=batch_epoch)
            signals.extend(batch_signals)

        stats = self.compute_stats(signals)
        return {"signals": signals, "stats": stats}

    def _group_by_interval(
        self, data: list[dict], interval_s: float
    ) -> list[tuple[str, list[dict]]]:
        """Group sorted ticks into time-aligned batches."""
        if not data:
            return []

        batches = []
        current_ts = data[0]["timestamp"]
        current_epoch = self._parse_ts_to_epoch(current_ts)
        current_batch = []

        for tick in data:
            tick_epoch = self._parse_ts_to_epoch(tick["timestamp"])
            if tick_epoch - current_epoch >= interval_s:
                if current_batch:
                    batches.append((current_ts, current_batch))
                current_ts = tick["timestamp"]
                current_epoch = tick_epoch
                current_batch = [tick]
            else:
                current_batch.append(tick)

        if current_batch:
            batches.append((current_ts, current_batch))

        return batches

    @staticmethod
    def _parse_ts_to_epoch(ts) -> float:
        """Convert timestamp (ISO string or float) to epoch seconds."""
        if isinstance(ts, (int, float)):
            return float(ts)
        try:
            return datetime.fromisoformat(str(ts)).timestamp()
        except (ValueError, TypeError):
            return 0.0

    def compute_stats(self, signals: list[dict]) -> dict:
        """Compute summary statistics from generated signals."""
        if not signals:
            return {
                "total_signals": 0,
                "signals_per_hour": 0.0,
                "avg_confidence": 0.0,
                "avg_spread_bps": 0.0,
                "max_spread_bps": 0.0,
                "min_spread_bps": 0.0,
                "hit_rate": 0.0,
                "theoretical_pnl_bps": 0.0,
            }

        confidences = [float(s.get("confidence", 0)) for s in signals]
        spreads = [float(s.get("spread_bps", 0)) for s in signals]
        fee = float(self.fee_bps) * 2

        timestamps = [s.get("timestamp") for s in signals if s.get("timestamp")]
        if len(timestamps) >= 2:
            try:
                t0_epoch = self._parse_ts_to_epoch(timestamps[0])
                t1_epoch = self._parse_ts_to_epoch(timestamps[-1])
                hours = max((t1_epoch - t0_epoch) / 3600, 0.001)
            except (ValueError, TypeError):
                hours = 1.0
        else:
            hours = 1.0

        hits = [s for s in spreads if s > fee]

        return {
            "total_signals": len(signals),
            "signals_per_hour": round(len(signals) / hours, 2),
            "avg_confidence": round(sum(confidences) / len(confidences), 4),
            "avg_spread_bps": round(sum(spreads) / len(spreads), 2),
            "max_spread_bps": round(max(spreads), 2),
            "min_spread_bps": round(min(spreads), 2),
            "hit_rate": round(len(hits) / len(signals) * 100, 1),
            "theoretical_pnl_bps": round(sum(s - fee for s in spreads if s > fee), 2),
        }

    def generate_report(self, stats: dict) -> str:
        """Generate human-readable summary."""
        lines = [
            "=" * 50,
            "  BACKTEST REPORT",
            "=" * 50,
            f"  Total Signals:      {stats.get('total_signals', 0)}",
            f"  Signals/Hour:       {stats.get('signals_per_hour', 0)}",
            f"  Avg Confidence:     {stats.get('avg_confidence', 0):.4f}",
            f"  Avg Spread (bps):   {stats.get('avg_spread_bps', 0):.2f}",
            f"  Max Spread (bps):   {stats.get('max_spread_bps', 0):.2f}",
            f"  Min Spread (bps):   {stats.get('min_spread_bps', 0):.2f}",
            f"  Hit Rate:           {stats.get('hit_rate', 0):.1f}%",
            f"  Fee (round trip):   {float(self.fee_bps) * 2:.1f} bps",
            f"  Theo PnL (bps):     {stats.get('theoretical_pnl_bps', 0):.2f}",
            "=" * 50,
        ]
        return "\n".join(lines)
