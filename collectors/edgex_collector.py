"""
EdgeXCollector — Tier 3 REST-only collector for EdgeX DEX (beta).

EdgeX is a newer decentralized exchange with a beta/changing API.
This is a minimal implementation covering core data endpoints only.

Design principles:
- All prices/sizes use Decimal(str(value)), never raw floats
- API failures raise exceptions, never return default values
- Minimal implementation — API is beta and subject to change
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


class EdgeXCollector(BaseCollector):
    """REST-only data collector for EdgeX DEX (Tier 3, beta)."""

    def __init__(self, rest_url: str = "https://api.edgex.exchange"):
        super().__init__(name="edgex", tier=3)
        self._rest_url = rest_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session for REST calls."""
        if self._session is not None and not self._session.closed:
            logger.warning("[edgex] connect() called but session already open")
            return
        self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
        self._connected = True
        self.clear_errors()
        logger.info("[edgex] Connected (REST only, beta API)")

    async def disconnect(self) -> None:
        """Close aiohttp session cleanly."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connected = False
        logger.info("[edgex] Disconnected")

    # ─── Internal Helpers ────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Execute GET request with error handling."""
        if self._session is None or self._session.closed:
            raise RuntimeError("EdgeXCollector: session not connected. Call connect() first.")

        url = f"{self._rest_url}{path}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 404:
                    raise NotImplementedError(
                        f"EdgeX endpoint {path} returned 404 — "
                        f"beta API endpoint may have changed. "
                        f"Check EdgeX documentation for current API paths."
                    )
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"EdgeX API error: {resp.status} {resp.reason} — {body[:500]}"
                    )
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.record_error(str(e))
            raise RuntimeError(f"EdgeX request failed ({path}): {e}") from e

    # ─── Required Data Methods ───────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot from EdgeX.

        GET /api/v1/depth?symbol={symbol}&limit={depth}
        """
        try:
            raw = await self._get("/api/v1/depth", params={"symbol": symbol, "limit": depth})
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
        """Fetch ticker/BBO from EdgeX.

        GET /api/v1/ticker?symbol={symbol}
        """
        try:
            raw = await self._get("/api/v1/ticker", params={"symbol": symbol})
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": Decimal(str(raw["bid"])) if raw.get("bid") is not None else Decimal("0"),
                "ask": Decimal(str(raw["ask"])) if raw.get("ask") is not None else Decimal("0"),
                "last": Decimal(str(raw.get("last", raw.get("last_price", 0)))),
                "volume_24h": Decimal(str(raw.get("volume_24h", raw.get("volume", 0)))),
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
        """Fetch recent trades from EdgeX.

        GET /api/v1/trades?symbol={symbol}&limit={limit}
        """
        try:
            raw = await self._get("/api/v1/trades", params={"symbol": symbol, "limit": limit})
            trades_list = raw if isinstance(raw, list) else raw.get("trades", raw.get("data", []))
            trades = []
            for t in trades_list:
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("timestamp", t.get("time", 0))),
                    "side": t.get("side", ""),
                    "price": Decimal(str(t["price"])),
                    "size": Decimal(str(t.get("size", t.get("amount", t.get("qty", 0))))),
                    "id": str(t.get("id", "")),
                })
            self.clear_errors()
            return trades
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise
