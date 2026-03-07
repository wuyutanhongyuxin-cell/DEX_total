"""
LighterCollector — Tier 2 DEX data collector for Lighter exchange.

Uses REST API (no SDK dependency) + WebSocket for real-time BBO.
REST: GET endpoints at mainnet.lighter.xyz
WS:   wss://mainnet.lighter.xyz/ws for orderbook streaming

Design rules:
- All prices/sizes -> Decimal(str(value)), NEVER float
- API failures raise, never return defaults
- try/except every HTTP call: record_error() + re-raise
- On success: clear_errors()
- Exponential backoff reconnect on WS (base=1s, max=64s)
"""

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Optional

import aiohttp
import websockets

from collectors.base_collector import BaseCollector
from collectors.ws_parsers import parse_lighter_orderbook

logger = logging.getLogger(__name__)


class LighterCollector(BaseCollector):
    """Lighter DEX data collector — REST + WS."""

    def __init__(
        self,
        name: str = "lighter",
        tier: int = 2,
        rest_url: str = "https://mainnet.lighter.xyz",
        ws_url: str = "wss://mainnet.lighter.xyz/ws",
    ):
        super().__init__(name=name, tier=tier)
        self._rest_url = rest_url.rstrip("/")
        self._ws_url = ws_url
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_symbol: Optional[str] = None
        self._ws_reconnect_base: float = 1.0
        self._ws_reconnect_max: float = 64.0
        self._ws_shutdown: bool = False

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session and verify connectivity."""
        if self._session is not None and not self._session.closed:
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
        )
        # Verify connectivity with a lightweight call
        try:
            async with self._session.get(
                f"{self._rest_url}/api/v1/ticker", params={"market": "BTCUSDC"}
            ) as resp:
                resp.raise_for_status()
        except Exception as e:
            await self._session.close()
            self._session = None
            self.record_error(f"connect failed: {e}")
            raise ConnectionError(f"Lighter connect failed: {e}") from e

        self._connected = True
        self.clear_errors()
        logger.info("[lighter] Connected to REST API")

    async def disconnect(self) -> None:
        """Close WS + aiohttp session. Clean shutdown."""
        self._ws_shutdown = True

        # Cancel WS background task
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None

        # Close WS connection
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        # Close HTTP session
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

        self._connected = False
        self._ws_connected = False
        logger.info("[lighter] Disconnected")

    # ─── REST: Orderbook ─────────────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """GET /api/v1/orderbook?market={symbol}&limit={depth}"""
        try:
            async with self._session.get(
                f"{self._rest_url}/api/v1/orderbook",
                params={"market": symbol, "limit": depth},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            bids = [
                [Decimal(str(b["price"])), Decimal(str(b["size"]))]
                for b in data.get("bids", [])
            ]
            asks = [
                [Decimal(str(a["price"])), Decimal(str(a["size"]))]
                for a in data.get("asks", [])
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
        """GET /api/v1/ticker?market={symbol}"""
        try:
            async with self._session.get(
                f"{self._rest_url}/api/v1/ticker",
                params={"market": symbol},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": Decimal(str(data.get("bestBid", data.get("bid", "0")))),
                "ask": Decimal(str(data.get("bestAsk", data.get("ask", "0")))),
                "last": Decimal(str(data.get("lastPrice", data.get("last", "0")))),
                "volume_24h": Decimal(str(data.get("volume24h", data.get("volume", "0")))),
            }
        except Exception as e:
            self.record_error(f"get_ticker({symbol}): {e}")
            raise

    # ─── REST: Recent Trades ─────────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """GET /api/v1/trades?market={symbol}&limit={limit}"""
        try:
            async with self._session.get(
                f"{self._rest_url}/api/v1/trades",
                params={"market": symbol, "limit": limit},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            trades = []
            for t in data.get("trades", data if isinstance(data, list) else []):
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("timestamp", 0)),
                    "price": Decimal(str(t["price"])),
                    "size": Decimal(str(t.get("size", t.get("amount", "0")))),
                    "side": t.get("side", "unknown"),
                })

            self.clear_errors()
            return trades
        except Exception as e:
            self.record_error(f"get_recent_trades({symbol}): {e}")
            raise

    # ─── REST: Funding Rate ──────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """GET /api/v1/funding_rate?market={symbol}"""
        try:
            async with self._session.get(
                f"{self._rest_url}/api/v1/funding_rate",
                params={"market": symbol},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "rate": Decimal(str(data.get("fundingRate", data.get("rate", "0")))),
                "next_funding_time": float(
                    data.get("nextFundingTime", data.get("next_funding_time", 0))
                ),
            }
        except Exception as e:
            self.record_error(f"get_funding_rate({symbol}): {e}")
            raise

    # ─── WebSocket: Orderbook Stream ─────────────────────────

    async def start_orderbook_stream(self, symbol: str) -> None:
        """Start WS orderbook stream with exponential backoff reconnect."""
        self._ws_shutdown = False
        self._ws_symbol = symbol

        # Cancel existing task if any
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        self._ws_task = asyncio.create_task(self._ws_run_loop(symbol))
        logger.info(f"[lighter] WS orderbook stream started for {symbol}")

    async def _ws_run_loop(self, symbol: str) -> None:
        """Main WS loop with exponential backoff reconnect."""
        backoff = self._ws_reconnect_base

        while not self._ws_shutdown:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._ws_connected = True
                    backoff = self._ws_reconnect_base  # Reset on successful connect

                    # Subscribe to orderbook channel
                    subscribe_msg = json.dumps({
                        "method": "subscribe",
                        "params": {
                            "channel": "orderbook",
                            "market": symbol,
                        },
                    })
                    await ws.send(subscribe_msg)
                    logger.info(f"[lighter] WS subscribed to orderbook:{symbol}")

                    async for raw_msg in ws:
                        if self._ws_shutdown:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            self._handle_ws_message(msg)
                        except json.JSONDecodeError:
                            logger.warning("[lighter] WS received non-JSON message")
                        except Exception as e:
                            logger.warning(f"[lighter] WS message handler error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws_connected = False
                if self._ws_shutdown:
                    break
                logger.warning(
                    f"[lighter] WS disconnected: {e}. "
                    f"Reconnecting in {backoff:.1f}s..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._ws_reconnect_max)

        self._ws_connected = False
        self._ws = None

    def _handle_ws_message(self, msg: dict) -> None:
        """Parse WS orderbook message and update BBO cache.

        Delegates to free function parse_lighter_orderbook() for Rust migration.
        """
        result = parse_lighter_orderbook(msg)
        if result is not None:
            best_bid, best_ask = result
            self._update_bbo_cache(best_bid, best_ask)

    async def stop_orderbook_stream(self) -> None:
        """Stop WS orderbook stream."""
        self._ws_shutdown = True
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None
        self._ws_connected = False
        logger.info("[lighter] WS orderbook stream stopped")
