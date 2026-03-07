"""
ParadexCollector — Tier 3 REST-only collector for Paradex DEX.

Paradex is a StarkNet-based perpetual DEX. This collector uses their
public REST API for market data (no authentication required for reads).

Design principles:
- All prices/sizes use Decimal(str(value)), never raw floats
- API failures raise exceptions, never return default values
- Tier 3 graceful degradation: 404s raise NotImplementedError with helpful message
- Proper aiohttp session lifecycle (create on connect, close on disconnect)
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional

import aiohttp

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)


class ParadexCollector(BaseCollector):
    """REST-only data collector for Paradex DEX (Tier 3)."""

    def __init__(self, rest_url: str = "https://api.paradex.trade/v1"):
        super().__init__(name="paradex", tier=3)
        self._rest_url = rest_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session for REST calls."""
        if self._session is not None and not self._session.closed:
            logger.warning("[paradex] connect() called but session already open")
            return
        self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
        self._connected = True
        self.clear_errors()
        logger.info("[paradex] Connected (REST only)")

    async def disconnect(self) -> None:
        """Close aiohttp session cleanly."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connected = False
        logger.info("[paradex] Disconnected")

    # ─── Internal Helpers ────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Execute GET request with error handling and Tier 3 graceful degradation."""
        if self._session is None or self._session.closed:
            raise RuntimeError("ParadexCollector: session not connected. Call connect() first.")

        url = f"{self._rest_url}{path}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 404:
                    raise NotImplementedError(
                        f"Paradex endpoint {path} returned 404 — "
                        f"this endpoint may not be available yet. "
                        f"Check https://docs.paradex.trade for current API status."
                    )
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"Paradex API error: {resp.status} {resp.reason} — {body[:500]}"
                    )
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.record_error(str(e))
            raise RuntimeError(f"Paradex request failed ({path}): {e}") from e

    # ─── Required Data Methods ───────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot from Paradex.

        GET /orderbook?market={symbol}&depth={depth}
        Response: {"bids": [[price, size], ...], "asks": [[price, size], ...]}
        """
        try:
            raw = await self._get("/orderbook", params={"market": symbol, "depth": depth})
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bids": [
                    [Decimal(str(entry[0])), Decimal(str(entry[1]))]
                    for entry in raw.get("bids", [])
                ],
                "asks": [
                    [Decimal(str(entry[0])), Decimal(str(entry[1]))]
                    for entry in raw.get("asks", [])
                ],
            }
            self.clear_errors()
            return result
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_ticker(self, symbol: str) -> dict:
        """Fetch ticker/BBO from Paradex.

        GET /markets/summary?market={symbol}
        Response: {"bid": ..., "ask": ..., "last_price": ..., "volume_24h": ...}
        """
        try:
            raw = await self._get("/markets/summary", params={"market": symbol})
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": Decimal(str(raw["bid"])) if raw.get("bid") is not None else Decimal("0"),
                "ask": Decimal(str(raw["ask"])) if raw.get("ask") is not None else Decimal("0"),
                "last": Decimal(str(raw["last_price"])) if raw.get("last_price") is not None else Decimal("0"),
                "volume_24h": Decimal(str(raw.get("volume_24h", 0))),
            }
            self.clear_errors()
            return result
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Optional Data Methods ───────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """Fetch recent trades from Paradex.

        GET /trades?market={symbol}&limit={limit}
        """
        try:
            raw = await self._get("/trades", params={"market": symbol, "limit": limit})
            trades = []
            for t in raw.get("results", raw if isinstance(raw, list) else []):
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("created_at", t.get("timestamp", 0))),
                    "side": t.get("side", ""),
                    "price": Decimal(str(t["price"])),
                    "size": Decimal(str(t.get("size", t.get("amount", 0)))),
                    "id": str(t.get("id", "")),
                })
            self.clear_errors()
            return trades
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch current funding rate from Paradex.

        GET /funding?market={symbol}
        """
        try:
            raw = await self._get("/funding", params={"market": symbol})
            # Paradex may return a list or dict — handle both
            entry = raw[0] if isinstance(raw, list) and raw else raw
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "rate": Decimal(str(entry.get("funding_rate", entry.get("rate", 0)))),
                "next_funding_time": float(entry.get("next_funding_time", 0)),
            }
            self.clear_errors()
            return result
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise
