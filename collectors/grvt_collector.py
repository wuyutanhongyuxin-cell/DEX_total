"""
GRVTCollector — Tier 2 DEX data collector for GRVT exchange.

Uses REST API only (no WS for data collection).
IMPORTANT: GRVT uses POST for ALL endpoints, not GET.

Design rules:
- All prices/sizes -> Decimal(str(value)), NEVER float
- API failures raise, never return defaults
- try/except every HTTP call: record_error() + re-raise
- On success: clear_errors()
- Prices must be aligned to tick_size (from golden rules)
"""

import asyncio
import logging
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Optional

import aiohttp

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)

# Default tick size for GRVT price alignment
DEFAULT_TICK_SIZE = Decimal("0.1")


class GRVTCollector(BaseCollector):
    """GRVT DEX data collector — REST only (all POST endpoints)."""

    def __init__(
        self,
        name: str = "grvt",
        tier: int = 2,
        rest_url: str = "https://trades.grvt.io",
    ):
        super().__init__(name=name, tier=tier)
        self._rest_url = rest_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._tick_sizes: dict[str, Decimal] = {}

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session and verify connectivity with a test call."""
        if self._session is not None and not self._session.closed:
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"Content-Type": "application/json"},
        )
        # Verify connectivity — POST with a known instrument
        try:
            async with self._session.post(
                f"{self._rest_url}/full/v1/mini",
                json={"instrument": "BTC_USDT_Perp"},
            ) as resp:
                resp.raise_for_status()
                await resp.json()
        except Exception as e:
            await self._session.close()
            self._session = None
            self.record_error(f"connect failed: {e}")
            raise ConnectionError(f"GRVT connect failed: {e}") from e

        self._connected = True
        self.clear_errors()
        logger.info("[grvt] Connected to REST API")

    async def disconnect(self) -> None:
        """Close aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._connected = False
        logger.info("[grvt] Disconnected")

    # ─── Price Alignment ─────────────────────────────────────

    @staticmethod
    def _align_price(price: Decimal, tick_size: Decimal, direction: str = "down") -> Decimal:
        """Align price to tick_size boundary.

        direction='down' for sell (ROUND_DOWN), 'up' for buy (ROUND_UP).
        From golden rules: GRVT silently rejects non-tick-aligned prices.
        """
        if tick_size <= 0:
            return price
        rounding = ROUND_DOWN if direction == "down" else ROUND_UP
        return (price / tick_size).quantize(Decimal("1"), rounding=rounding) * tick_size

    def _get_tick_size(self, symbol: str) -> Decimal:
        """Get tick size for symbol, defaulting to 0.1."""
        return self._tick_sizes.get(symbol, DEFAULT_TICK_SIZE)

    # ─── REST: Orderbook ─────────────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """POST /full/v1/book with body {"instrument": symbol}"""
        try:
            async with self._session.post(
                f"{self._rest_url}/full/v1/book",
                json={"instrument": symbol, "depth": depth},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            result = data.get("result", data)

            bids = [
                [Decimal(str(b["price"])), Decimal(str(b["size"]))]
                for b in result.get("bids", [])
            ]
            asks = [
                [Decimal(str(a["price"])), Decimal(str(a["size"]))]
                for a in result.get("asks", [])
            ]

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bids": bids,
                "asks": asks,
            }
        except Exception as e:
            self.record_error(f"get_orderbook({symbol}): {e}")
            raise

    # ─── REST: Ticker ────────────────────────────────────────

    async def get_ticker(self, symbol: str) -> dict:
        """POST /full/v1/mini with body {"instrument": symbol}"""
        try:
            async with self._session.post(
                f"{self._rest_url}/full/v1/mini",
                json={"instrument": symbol},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            result = data.get("result", data)

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": Decimal(str(result.get("best_bid", result.get("bid", "0")))),
                "ask": Decimal(str(result.get("best_ask", result.get("ask", "0")))),
                "last": Decimal(str(result.get("last_price", result.get("last", "0")))),
                "volume_24h": Decimal(
                    str(result.get("volume_24h", result.get("volume", "0")))
                ),
            }
        except Exception as e:
            self.record_error(f"get_ticker({symbol}): {e}")
            raise

    # ─── REST: Recent Trades ─────────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """POST /full/v1/trade_history with body."""
        try:
            async with self._session.post(
                f"{self._rest_url}/full/v1/trade_history",
                json={"instrument": symbol, "limit": limit},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            result = data.get("result", data)
            raw_trades = result.get("trades", result if isinstance(result, list) else [])

            trades = []
            for t in raw_trades:
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("timestamp", t.get("created_at", 0))),
                    "price": Decimal(str(t["price"])),
                    "size": Decimal(str(t.get("size", t.get("quantity", "0")))),
                    "side": t.get("side", t.get("taker_side", "unknown")),
                })

            self.clear_errors()
            return trades
        except Exception as e:
            self.record_error(f"get_recent_trades({symbol}): {e}")
            raise

    # ─── REST: Funding Rate ──────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """POST /full/v1/funding with body {"instrument": symbol}"""
        try:
            async with self._session.post(
                f"{self._rest_url}/full/v1/funding",
                json={"instrument": symbol},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            result = data.get("result", data)

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "rate": Decimal(
                    str(result.get("funding_rate", result.get("rate", "0")))
                ),
                "next_funding_time": float(
                    result.get("next_funding_time", result.get("next_funding_ts", 0))
                ),
            }
        except Exception as e:
            self.record_error(f"get_funding_rate({symbol}): {e}")
            raise
