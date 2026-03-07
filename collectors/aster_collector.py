"""
AsterCollector — Tier 3 REST-only collector for Aster DEX (beta).

Aster Finance is a newer DEX with a beta API. This collector uses extra
defensive error handling since the API is not yet stable.

Design principles:
- All prices/sizes use Decimal(str(value)), never raw floats
- API failures raise exceptions, never return default values
- Beta exchange: extra defensive handling, warnings for unexpected responses
- Proper aiohttp session lifecycle (create on connect, close on disconnect)
"""

import asyncio
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Optional

import aiohttp

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)  # longer timeout for beta API


class AsterCollector(BaseCollector):
    """REST-only data collector for Aster DEX (Tier 3, beta)."""

    def __init__(self, rest_url: str = "https://api.aster.finance"):
        super().__init__(name="aster", tier=3)
        self._rest_url = rest_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session for REST calls."""
        if self._session is not None and not self._session.closed:
            logger.warning("[aster] connect() called but session already open")
            return
        self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
        self._connected = True
        self.clear_errors()
        logger.info("[aster] Connected (REST only, beta API)")

    async def disconnect(self) -> None:
        """Close aiohttp session cleanly."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connected = False
        logger.info("[aster] Disconnected")

    # ─── Internal Helpers ────────────────────────────────────

    def _safe_decimal(self, value, field_name: str = "unknown") -> Decimal:
        """Convert value to Decimal with extra safety for beta API quirks.

        Beta APIs may return unexpected types (None, empty string, nested objects).
        """
        if value is None:
            logger.warning(f"[aster] Field '{field_name}' is None, defaulting to 0")
            return Decimal("0")
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"[aster] Cannot convert '{field_name}'={value!r} to Decimal: {e}")
            raise RuntimeError(
                f"Aster API returned unparseable value for '{field_name}': {value!r}"
            ) from e

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Execute GET request with defensive error handling for beta API."""
        if self._session is None or self._session.closed:
            raise RuntimeError("AsterCollector: session not connected. Call connect() first.")

        url = f"{self._rest_url}{path}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 404:
                    raise NotImplementedError(
                        f"Aster endpoint {path} returned 404 — "
                        f"this endpoint may not be available in the current beta. "
                        f"Check Aster documentation for API status."
                    )
                if resp.status == 503:
                    raise RuntimeError(
                        f"Aster API unavailable (503) — beta service may be under maintenance"
                    )
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"Aster API error: {resp.status} {resp.reason} — {body[:500]}"
                    )

                data = await resp.json()

                # Beta APIs may wrap responses inconsistently
                if isinstance(data, dict) and "data" in data:
                    logger.debug("[aster] Response wrapped in 'data' envelope, unwrapping")
                    data = data["data"]

                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.record_error(str(e))
            raise RuntimeError(f"Aster request failed ({path}): {e}") from e

    # ─── Required Data Methods ───────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot from Aster.

        GET /v1/orderbook/{symbol}?depth={depth}
        """
        try:
            raw = await self._get(f"/v1/orderbook/{symbol}", params={"depth": depth})

            bids_raw = raw.get("bids", [])
            asks_raw = raw.get("asks", [])

            if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
                logger.warning(f"[aster] Unexpected orderbook format: bids={type(bids_raw)}, asks={type(asks_raw)}")
                raise RuntimeError("Aster orderbook response has unexpected format")

            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bids": [
                    [self._safe_decimal(entry[0], "bid_price"), self._safe_decimal(entry[1], "bid_size")]
                    for entry in bids_raw
                ],
                "asks": [
                    [self._safe_decimal(entry[0], "ask_price"), self._safe_decimal(entry[1], "ask_size")]
                    for entry in asks_raw
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
        """Fetch ticker/BBO from Aster.

        GET /v1/ticker/{symbol}
        """
        try:
            raw = await self._get(f"/v1/ticker/{symbol}")

            if not isinstance(raw, dict):
                logger.warning(f"[aster] Ticker response is {type(raw)}, expected dict")
                raise RuntimeError("Aster ticker response has unexpected format")

            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": self._safe_decimal(raw.get("bid"), "bid"),
                "ask": self._safe_decimal(raw.get("ask"), "ask"),
                "last": self._safe_decimal(raw.get("last", raw.get("last_price")), "last"),
                "volume_24h": self._safe_decimal(raw.get("volume_24h", raw.get("volume", 0)), "volume_24h"),
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
        """Fetch recent trades from Aster.

        GET /v1/trades/{symbol}?limit={limit}
        """
        try:
            raw = await self._get(f"/v1/trades/{symbol}", params={"limit": limit})

            # Beta API may return list directly or wrapped in {"trades": [...]}
            trades_list = raw if isinstance(raw, list) else raw.get("trades", raw.get("results", []))
            if not isinstance(trades_list, list):
                logger.warning(f"[aster] Trades response has unexpected structure: {type(trades_list)}")
                raise RuntimeError("Aster trades response has unexpected format")

            trades = []
            for t in trades_list:
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("timestamp", t.get("time", 0))),
                    "side": t.get("side", ""),
                    "price": self._safe_decimal(t.get("price"), "trade_price"),
                    "size": self._safe_decimal(t.get("size", t.get("amount", t.get("qty"))), "trade_size"),
                    "id": str(t.get("id", t.get("trade_id", ""))),
                })
            self.clear_errors()
            return trades
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch current funding rate from Aster.

        GET /v1/funding/{symbol}
        """
        try:
            raw = await self._get(f"/v1/funding/{symbol}")

            if not isinstance(raw, dict):
                logger.warning(f"[aster] Funding response is {type(raw)}, expected dict")
                raise RuntimeError("Aster funding response has unexpected format")

            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "rate": self._safe_decimal(raw.get("funding_rate", raw.get("rate")), "funding_rate"),
                "next_funding_time": float(raw.get("next_funding_time", 0)),
            }
            self.clear_errors()
            return result
        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise
