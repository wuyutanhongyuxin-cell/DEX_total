"""
VariationalCollector — Tier 3 REST-only collector for Variational DEX.

Variational exposes a public REST API at omni-client-api.prod.ap-northeast-1.variational.io
that is accessible without authentication and bypasses Cloudflare. This collector
uses that endpoint for market data.

Design principles:
- All prices/sizes use Decimal(str(value)), never raw floats
- API failures raise exceptions, never return default values
- Tier 3 graceful degradation: missing fields raise, auth-gated endpoints raise NotImplementedError
- Proper aiohttp session lifecycle (create on connect, close on disconnect)

Symbol mapping:
  User-facing canonical symbol   → Variational stats ticker field
  "BTC-PERP"                     → "BTC"
  "ETH-PERP"                     → "ETH"
  Generic rule: strip "-PERP" suffix (and any other dash-suffix).

Public endpoints (no auth required):
  GET /metadata/stats
  Response shape:
    {
      "listings": [
        {
          "ticker": "BTC",
          "mark_price": "69000.0",
          "quotes": {
            "base": {
              "bid": "68995.0",
              "ask": "69005.0"
            }
          }
        },
        ...
      ]
    }

Private endpoints (Cookie + curl_cffi required — NOT implemented here):
  GET /api/funding              — funding rate
  GET /api/metadata/open_interest — open interest
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

# Base URL of the public Variational backend (no Cloudflare, no auth required).
_DEFAULT_REST_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io"


def _canonical_to_ticker(symbol: str) -> str:
    """Convert user-facing canonical symbol to Variational ticker field.

    Examples:
        "BTC-PERP" -> "BTC"
        "ETH-PERP" -> "ETH"
        "BTC"      -> "BTC"   (already a bare ticker, pass-through)
    """
    # Strip any dash-separated suffix (e.g. "-PERP", "-USD", "-USDT")
    if "-" in symbol:
        return symbol.split("-")[0].upper()
    return symbol.upper()


class VariationalCollector(BaseCollector):
    """REST-only data collector for Variational DEX (Tier 3).

    Uses the public omni-client-api backend which requires no authentication
    and is not behind Cloudflare. The /metadata/stats endpoint has a 2-second
    server-side cache which is acceptable for market data collection.

    Funding rate and open interest require Cookie-based authentication against
    the private omni.variational.io backend — these are not implemented and
    raise NotImplementedError.
    """

    def __init__(self, rest_url: str = _DEFAULT_REST_URL):
        super().__init__(name="variational", tier=3)
        self._rest_url = rest_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session and validate connectivity via /metadata/stats."""
        if self._session is not None and not self._session.closed:
            logger.warning("[variational] connect() called but session already open")
            return

        self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)

        # Connectivity check — raises on failure so orchestrator knows to skip this exchange.
        try:
            raw = await self._get("/metadata/stats")
        except Exception as e:
            await self._session.close()
            self._session = None
            raise RuntimeError(
                f"[variational] Connectivity check failed ({self._rest_url}/metadata/stats): {e}"
            ) from e

        listings = raw.get("listings", [])
        if not isinstance(listings, list) or not listings:
            await self._session.close()
            self._session = None
            raise RuntimeError(
                f"[variational] Unexpected /metadata/stats shape — "
                f"'listings' is {'empty' if isinstance(listings, list) else 'not a list'}. "
                f"Got: {type(listings)} len={len(listings) if isinstance(listings, list) else 'N/A'}"
            )

        self._connected = True
        self.clear_errors()
        logger.info(
            f"[variational] Connected (REST only) — "
            f"{len(listings)} listing(s) available at {self._rest_url}"
        )

    async def disconnect(self) -> None:
        """Close aiohttp session cleanly."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connected = False
        logger.info("[variational] Disconnected")

    # ─── Internal Helpers ────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Execute GET request with error handling.

        Raises RuntimeError on any non-200 response or network error.
        Raises NotImplementedError on 404 (endpoint may not exist publicly).
        """
        if self._session is None or self._session.closed:
            raise RuntimeError(
                "VariationalCollector: session not connected. Call connect() first."
            )

        url = f"{self._rest_url}{path}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 404:
                    raise NotImplementedError(
                        f"Variational endpoint {path} returned 404 — "
                        f"this endpoint may not be publicly accessible. "
                        f"Private endpoints require Cookie authentication."
                    )
                if resp.status == 403:
                    raise NotImplementedError(
                        f"Variational endpoint {path} returned 403 Forbidden — "
                        f"this endpoint requires authentication (Cookie + Cloudflare bypass)."
                    )
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"Variational API error: {resp.status} {resp.reason} — {body[:500]}"
                    )
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.record_error(str(e))
            raise RuntimeError(f"Variational request failed ({path}): {e}") from e

    def _find_listing(self, listings: list, ticker: str) -> dict:
        """Find a listing entry by ticker field (case-insensitive).

        Raises RuntimeError if ticker is not found — API failure must not
        silently return default values.
        """
        ticker_upper = ticker.upper()
        for entry in listings:
            if str(entry.get("ticker", "")).upper() == ticker_upper:
                return entry
        available = [str(e.get("ticker", "")) for e in listings]
        raise RuntimeError(
            f"Variational: ticker '{ticker}' not found in /metadata/stats listings. "
            f"Available tickers: {available}"
        )

    # ─── Required Data Methods ───────────────────────────────

    async def get_ticker(self, symbol: str) -> dict:
        """Fetch current ticker/BBO from Variational.

        Calls GET /metadata/stats, finds the listing matching the symbol,
        and extracts bid/ask from quotes.base and mark_price as last.

        Args:
            symbol: Canonical symbol e.g. "BTC-PERP" or bare ticker "BTC".

        Returns:
            {
                "exchange": "variational",
                "symbol": str,
                "timestamp": float (epoch ms),
                "bid": Decimal,
                "ask": Decimal,
                "last": Decimal,   # mark_price
                "volume_24h": Decimal,  # not available, returns Decimal("0")
            }

        Raises:
            RuntimeError: On API failure or symbol not found.
        """
        try:
            raw = await self._get("/metadata/stats")
            listings = raw.get("listings", [])
            if not isinstance(listings, list):
                raise RuntimeError(
                    f"Variational /metadata/stats: 'listings' is not a list — got {type(listings)}"
                )

            ticker = _canonical_to_ticker(symbol)
            entry = self._find_listing(listings, ticker)

            # Extract bid/ask from quotes.base
            quotes = entry.get("quotes", {})
            base_quotes = quotes.get("base", {})

            bid_raw = base_quotes.get("bid")
            ask_raw = base_quotes.get("ask")
            mark_raw = entry.get("mark_price")

            if bid_raw is None:
                raise RuntimeError(
                    f"Variational ticker '{ticker}': 'quotes.base.bid' field missing in response"
                )
            if ask_raw is None:
                raise RuntimeError(
                    f"Variational ticker '{ticker}': 'quotes.base.ask' field missing in response"
                )
            if mark_raw is None:
                raise RuntimeError(
                    f"Variational ticker '{ticker}': 'mark_price' field missing in response"
                )

            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": Decimal(str(bid_raw)),
                "ask": Decimal(str(ask_raw)),
                "last": Decimal(str(mark_raw)),
                "volume_24h": Decimal("0"),  # not exposed on public endpoint
            }
            self.clear_errors()
            return result

        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot from Variational.

        Variational does not expose a public depth orderbook endpoint.
        Only the top-of-book (BBO) is available via /metadata/stats.
        This method returns a synthetic single-level orderbook using BBO data.

        Args:
            symbol: Canonical symbol e.g. "BTC-PERP".
            depth: Ignored — only BBO is available.

        Returns:
            {
                "exchange": "variational",
                "symbol": str,
                "timestamp": float (epoch ms),
                "bids": [[Decimal price, Decimal size]],  # single level, size=0 (unknown)
                "asks": [[Decimal price, Decimal size]],  # single level, size=0 (unknown)
            }

        Raises:
            RuntimeError: On API failure or symbol not found.
        """
        try:
            raw = await self._get("/metadata/stats")
            listings = raw.get("listings", [])
            if not isinstance(listings, list):
                raise RuntimeError(
                    f"Variational /metadata/stats: 'listings' is not a list — got {type(listings)}"
                )

            ticker = _canonical_to_ticker(symbol)
            entry = self._find_listing(listings, ticker)

            quotes = entry.get("quotes", {})
            base_quotes = quotes.get("base", {})

            bid_raw = base_quotes.get("bid")
            ask_raw = base_quotes.get("ask")

            if bid_raw is None:
                raise RuntimeError(
                    f"Variational ticker '{ticker}': 'quotes.base.bid' field missing in response"
                )
            if ask_raw is None:
                raise RuntimeError(
                    f"Variational ticker '{ticker}': 'quotes.base.ask' field missing in response"
                )

            # Size is unknown — public endpoint does not expose depth/size.
            # Use None to clearly signal "no size data" — consumers MUST handle this.
            # Returning Decimal("0") would violate golden rule #1 (no silent defaults).
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bids": [[Decimal(str(bid_raw)), None]],
                "asks": [[Decimal(str(ask_raw)), None]],
            }
            self.clear_errors()
            return result

        except NotImplementedError:
            raise
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Optional Data Methods (auth-gated, not implemented) ─

    async def get_funding_rate(self, symbol: str) -> dict:
        """Not implemented — requires Cookie authentication against private backend.

        The private endpoint GET /api/funding on omni.variational.io requires
        Cookie-based auth and Cloudflare bypass (curl_cffi). Not supported in
        this collector.
        """
        raise NotImplementedError(
            "variational: get_funding_rate requires Cookie authentication "
            "(private endpoint GET /api/funding on omni.variational.io). "
            "Use curl_cffi with a valid session cookie to access this data."
        )

    async def get_open_interest(self, symbol: str) -> dict:
        """Not implemented — requires Cookie authentication against private backend.

        The private endpoint GET /api/metadata/open_interest on omni.variational.io
        requires Cookie-based auth and Cloudflare bypass (curl_cffi). Not supported
        in this collector.
        """
        raise NotImplementedError(
            "variational: get_open_interest requires Cookie authentication "
            "(private endpoint GET /api/metadata/open_interest on omni.variational.io). "
            "Use curl_cffi with a valid session cookie to access this data."
        )

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """Not implemented — no public trades endpoint available."""
        raise NotImplementedError(
            "variational: get_recent_trades has no public endpoint available."
        )
