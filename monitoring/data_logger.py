"""
CSV DataLogger — writes BBO, trades, and analysis data to daily CSV files.
Ported from grvt_lighter/helpers/logger.py.

Key patterns:
- Timestamp in filename for traceability
- flush() after every write (crash-safe)
- All values converted to str() before writing
- Daily rotation
"""

import csv
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DataLogger:
    """Logs market data to daily CSV files organized by exchange and data type."""

    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self._files: dict[str, tuple] = {}  # key -> (file, writer)

    def _get_writer(self, exchange: str, data_type: str, headers: list[str]):
        """Get or create a CSV writer for the given exchange/data_type."""
        today = datetime.utcnow().strftime("%Y%m%d")
        key = f"{today}_{exchange}_{data_type}"

        if key in self._files:
            return self._files[key][1], self._files[key][0]

        # P0 FIX: Close stale file handles from previous days to prevent FD leak
        stale_keys = [k for k in self._files if not k.startswith(today + "_")]
        for sk in stale_keys:
            old_f, _ = self._files.pop(sk)
            if not old_f.closed:
                old_f.close()
            logger.debug("Closed rotated file handle: %s", sk)

        # Create directory
        day_dir = self.base_dir / today
        day_dir.mkdir(parents=True, exist_ok=True)

        filepath = day_dir / f"{data_type}_{exchange}.csv"
        file_exists = filepath.exists()

        f = open(filepath, "a", newline="", encoding="utf-8")
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(headers)
            f.flush()

        self._files[key] = (f, writer)
        return writer, f

    # ─── BBO Logging ─────────────────────────────────────────

    BBO_HEADERS = [
        "timestamp", "exchange", "symbol", "bid", "ask", "bid_size", "ask_size",
    ]

    def log_bbo(self, exchange: str, symbol: str, bid, ask,
                bid_size=None, ask_size=None) -> None:
        writer, f = self._get_writer(exchange, "bbo", self.BBO_HEADERS)
        writer.writerow([
            datetime.utcnow().isoformat(),
            str(exchange),
            str(symbol),
            str(bid),
            str(ask),
            str(bid_size or ""),
            str(ask_size or ""),
        ])
        f.flush()

    # ─── Trades Logging ──────────────────────────────────────

    TRADES_HEADERS = [
        "timestamp", "exchange", "symbol", "side", "price", "size", "trade_id",
    ]

    def log_trade(self, exchange: str, symbol: str, side: str,
                  price, size, trade_id: str = "") -> None:
        writer, f = self._get_writer(exchange, "trades", self.TRADES_HEADERS)
        writer.writerow([
            datetime.utcnow().isoformat(),
            str(exchange),
            str(symbol),
            str(side),
            str(price),
            str(size),
            str(trade_id),
        ])
        f.flush()

    # ─── Funding Rate Logging ────────────────────────────────

    FUNDING_HEADERS = [
        "timestamp", "exchange", "symbol", "rate", "next_funding_time",
    ]

    def log_funding(self, exchange: str, symbol: str, rate,
                    next_funding_time: str = "") -> None:
        writer, f = self._get_writer(exchange, "funding", self.FUNDING_HEADERS)
        writer.writerow([
            datetime.utcnow().isoformat(),
            str(exchange),
            str(symbol),
            str(rate),
            str(next_funding_time),
        ])
        f.flush()

    # ─── Open Interest Logging ───────────────────────────────

    OI_HEADERS = [
        "timestamp", "exchange", "symbol", "open_interest", "oi_value",
    ]

    def log_oi(self, exchange: str, symbol: str, oi, oi_value="") -> None:
        writer, f = self._get_writer(exchange, "oi", self.OI_HEADERS)
        writer.writerow([
            datetime.utcnow().isoformat(),
            str(exchange),
            str(symbol),
            str(oi),
            str(oi_value),
        ])
        f.flush()

    # ─── Spread Matrix Logging ───────────────────────────────

    SPREAD_HEADERS = [
        "timestamp", "buy_exchange", "sell_exchange", "symbol",
        "spread_bps", "net_spread_bps", "buy_price", "sell_price",
    ]

    def log_spread(self, buy_exchange: str, sell_exchange: str, symbol: str,
                   spread_bps, net_spread_bps, buy_price, sell_price) -> None:
        writer, f = self._get_writer("matrix", "spreads", self.SPREAD_HEADERS)
        writer.writerow([
            datetime.utcnow().isoformat(),
            str(buy_exchange),
            str(sell_exchange),
            str(symbol),
            str(spread_bps),
            str(net_spread_bps),
            str(buy_price),
            str(sell_price),
        ])
        f.flush()

    # ─── Signal Logging ──────────────────────────────────────

    SIGNAL_HEADERS = [
        "timestamp", "buy_exchange", "sell_exchange", "symbol",
        "direction", "confidence", "spread_bps", "components",
    ]

    def log_signal(self, signal: dict) -> None:
        writer, f = self._get_writer("signals", "signals", self.SIGNAL_HEADERS)
        writer.writerow([
            datetime.utcnow().isoformat(),
            str(signal.get("buy_exchange", "")),
            str(signal.get("sell_exchange", "")),
            str(signal.get("symbol", "")),
            str(signal.get("direction", "")),
            str(signal.get("confidence", "")),
            str(signal.get("spread_bps", "")),
            str(signal.get("components", "")),
        ])
        f.flush()

    # ─── Cleanup ─────────────────────────────────────────────

    def close(self) -> None:
        for key, (f, writer) in self._files.items():
            if not f.closed:
                f.close()
        self._files.clear()
