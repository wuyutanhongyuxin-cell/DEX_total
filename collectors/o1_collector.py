"""
O1Collector — Tier 3 REST-only collector for 01.xyz DEX.

01.xyz is a perpetual DEX running on the ZO mainnet.
This collector uses their public REST API (no authentication required).

API base: https://zo-mainnet.n1.xyz
- GET /info          — market metadata (symbol → marketId mapping)
- GET /market/{id}/orderbook — bids/asks as [[price, size], ...] or {"price":..,"size":..}

No WebSocket, no funding rate, no OI, no trades public endpoints known.

Design principles:
- All prices/sizes use Decimal(str(value)), never raw floats
- API failures raise exceptions, never return default values
- symbol→marketId mapping built once on connect(), reused thereafter
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

# Candidate field names that 01.xyz /info might use for the market symbol.
# Checked in order; first non-empty match wins.
_SYMBOL_FIELDS = ("symbol", "ticker", "name", "market")


def _parse_entry(entry) -> tuple[Decimal, Decimal]:
    """Parse a single orderbook entry into (price, size).

    Handles both array form [price, size] and dict form {"price": .., "size": ..}.
    Raises ValueError if the entry cannot be parsed.
    """
    if isinstance(entry, (list, tuple)):
        if len(entry) < 2:
            raise ValueError(f"Orderbook entry too short: {entry!r}")
        return Decimal(str(entry[0])), Decimal(str(entry[1]))
    if isinstance(entry, dict):
        price_raw = entry.get("price", entry.get("p"))
        size_raw = entry.get("size", entry.get("s", entry.get("qty", entry.get("amount"))))
        if price_raw is None or size_raw is None:
            raise ValueError(f"Orderbook dict entry missing price/size: {entry!r}")
        return Decimal(str(price_raw)), Decimal(str(size_raw))
    raise ValueError(f"Unknown orderbook entry format: {entry!r}")


class O1Collector(BaseCollector):
    """REST-only data collector for 01.xyz DEX (Tier 3).

    Builds a symbol→marketId map on connect() via GET /info, then uses
    market IDs to fetch live orderbooks.
    """

    def __init__(self, rest_url: str = "https://zo-mainnet.n1.xyz"):
        super().__init__(name="o1xyz", tier=3)
        self._rest_url = rest_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        # Populated in connect(): canonical_symbol (upper) → int marketId
        self._market_ids: dict[str, int] = {}
        # Raw market metadata keyed by marketId, for debugging
        self._market_meta: dict[int, dict] = {}

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session and load market metadata from GET /info."""
        if self._session is not None and not self._session.closed:
            logger.warning("[o1] connect() called but session already open")
            return

        self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)

        # Load market metadata — this is required; raise on failure so the
        # orchestrator knows 01.xyz is unavailable rather than silently failing.
        try:
            info = await self._get("/info")
        except Exception as e:
            await self._session.close()
            self._session = None
            raise RuntimeError(f"[o1] Failed to load /info on connect: {e}") from e

        markets = info.get("markets", [])
        if not markets:
            await self._session.close()
            self._session = None
            raise RuntimeError(
                "[o1] /info returned empty markets list — exchange may be down"
            )

        for m in markets:
            market_id = m.get("marketId", m.get("market_id", m.get("id")))
            if market_id is None:
                logger.debug(f"[o1] /info entry missing marketId, skipping: {m}")
                continue

            # Try multiple field names for the symbol
            raw_symbol = ""
            for field in _SYMBOL_FIELDS:
                raw_symbol = m.get(field, "")
                if raw_symbol:
                    break

            if not raw_symbol:
                logger.debug(f"[o1] /info entry missing symbol field, skipping: {m}")
                continue

            canonical = raw_symbol.upper()
            self._market_ids[canonical] = int(market_id)
            self._market_meta[int(market_id)] = m

        if not self._market_ids:
            await self._session.close()
            self._session = None
            raise RuntimeError(
                "[o1] No valid markets parsed from /info — check API response"
            )

        self._connected = True
        self.clear_errors()
        logger.info(
            f"[o1] Connected. {len(self._market_ids)} markets loaded: "
            f"{sorted(self._market_ids.keys())}"
        )

    async def disconnect(self) -> None:
        """Close aiohttp session cleanly."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connected = False
        self._market_ids.clear()
        self._market_meta.clear()
        logger.info("[o1] Disconnected")

    # ─── Internal Helpers ────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Execute GET request with error handling."""
        if self._session is None or self._session.closed:
            raise RuntimeError("[o1] Session not connected. Call connect() first.")

        url = f"{self._rest_url}{path}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 404:
                    raise NotImplementedError(
                        f"[o1] Endpoint {path} returned 404 — "
                        f"this endpoint may not exist on 01.xyz. "
                        f"Check https://zo-mainnet.n1.xyz/docs for current API."
                    )
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"[o1] API error: {resp.status} {resp.reason} — {body[:500]}"
                    )
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.record_error(str(e))
            raise RuntimeError(f"[o1] Request failed ({path}): {e}") from e

    def _resolve_market_id(self, symbol: str) -> int:
        """Resolve a symbol string to a 01.xyz marketId.

        Tries several canonical forms in order:
          1. Exact uppercase match (e.g. "BTCUSD")
          2. Strip "-PERP" suffix  (e.g. "BTC-PERP" → "BTC" → match "BTCUSD" prefix)
          3. Prefix search         (e.g. "BTC" matches "BTCUSD", "BTCUSDT", ...)

        Raises KeyError if no match found.
        """
        upper = symbol.upper()

        # 1. Exact match
        if upper in self._market_ids:
            return self._market_ids[upper]

        # 2. Strip known suffixes and try exact
        stripped = upper.replace("-PERP", "").replace("_PERP", "").replace("-USD", "")
        if stripped in self._market_ids:
            return self._market_ids[stripped]

        # 3. Prefix search: look for any key that starts with the stripped base
        candidates = [k for k in self._market_ids if k.startswith(stripped)]
        if len(candidates) == 1:
            logger.debug(
                f"[o1] Symbol '{symbol}' resolved via prefix to '{candidates[0]}'"
            )
            return self._market_ids[candidates[0]]
        if len(candidates) > 1:
            # Prefer the shortest key (most specific match)
            best = min(candidates, key=len)
            logger.warning(
                f"[o1xyz] Symbol '{symbol}' resolved ambiguously via prefix to '{best}' "
                f"(candidates: {candidates}) — consider adding explicit symbol map"
            )
            return self._market_ids[best]

        available = sorted(self._market_ids.keys())
        raise KeyError(
            f"[o1] Symbol '{symbol}' not found in market list. "
            f"Available: {available}"
        )

    # ─── Required Data Methods ───────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot from 01.xyz.

        GET /market/{market_id}/orderbook
        Response: {"bids": [[price, size], ...], "asks": [[price, size], ...]}
        Each entry may also be a dict {"price": ..., "size": ...}.

        Raises on API failure or unknown symbol — never returns empty/default.
        """
        try:
            market_id = self._resolve_market_id(symbol)
            raw = await self._get(f"/market/{market_id}/orderbook")

            bids_raw = raw.get("bids", [])
            asks_raw = raw.get("asks", [])

            bids = []
            for entry in bids_raw[:depth]:
                price, size = _parse_entry(entry)
                bids.append([price, size])

            asks = []
            for entry in asks_raw[:depth]:
                price, size = _parse_entry(entry)
                asks.append([price, size])

            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bids": bids,
                "asks": asks,
            }
            self.clear_errors()
            return result

        except NotImplementedError:
            raise
        except KeyError as e:
            self.record_error(str(e))
            raise RuntimeError(str(e)) from e
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_ticker(self, symbol: str) -> dict:
        """Derive BBO ticker from the live orderbook.

        01.xyz has no dedicated ticker endpoint, so we fetch the orderbook
        and extract best bid / best ask. last price is set to mid-price
        as no last-trade endpoint is publicly available.

        Raises on API failure — never returns default values.
        """
        try:
            ob = await self.get_orderbook(symbol, depth=1)

            bids = ob["bids"]
            asks = ob["asks"]

            if not bids or not asks:
                raise RuntimeError(
                    f"[o1] Orderbook for '{symbol}' returned empty bids or asks — "
                    f"market may be inactive"
                )

            best_bid: Decimal = bids[0][0]
            best_ask: Decimal = asks[0][0]
            mid: Decimal = (best_bid + best_ask) / Decimal("2")

            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": ob["timestamp"],
                "bid": best_bid,
                "ask": best_ask,
                "last": mid,          # no last-trade endpoint; use mid as proxy
                "volume_24h": Decimal("0"),  # not available via public API
            }
            # clear_errors already called inside get_orderbook on success
            return result

        except NotImplementedError:
            raise
        except Exception:
            # Don't call record_error here — get_orderbook already does it.
            # Double record_error would halve the health tolerance window.
            raise

    # ─── Optional Data Methods (not supported) ───────────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """Not available: 01.xyz has no public funding rate endpoint."""
        raise NotImplementedError("[o1]: get_funding_rate not supported — no public endpoint")

    async def get_open_interest(self, symbol: str) -> dict:
        """Not available: 01.xyz has no public open interest endpoint."""
        raise NotImplementedError("[o1]: get_open_interest not supported — no public endpoint")

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """Not available: 01.xyz has no public trades endpoint."""
        raise NotImplementedError("[o1]: get_recent_trades not supported — no public endpoint")
