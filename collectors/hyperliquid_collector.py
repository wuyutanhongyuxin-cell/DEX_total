"""
HyperliquidCollector — Tier 2 DEX data collector for Hyperliquid exchange.

Uses REST + WebSocket APIs.
REST: All POST to https://api.hyperliquid.xyz/info (single endpoint, JSON body)
WS:   wss://api.hyperliquid.xyz/ws for real-time orderbook

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
from collectors.ws_parsers import parse_hyperliquid_l2book

logger = logging.getLogger(__name__)


class HyperliquidCollector(BaseCollector):
    """Hyperliquid DEX data collector — REST + WS."""

    def __init__(
        self,
        name: str = "hyperliquid",
        tier: int = 2,
        rest_url: str = "https://api.hyperliquid.xyz/info",
        ws_url: str = "wss://api.hyperliquid.xyz/ws",
    ):
        super().__init__(name=name, tier=tier)
        self._rest_url = rest_url
        self._ws_url = ws_url
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_symbol: Optional[str] = None
        self._ws_reconnect_base: float = 1.0
        self._ws_reconnect_max: float = 64.0
        self._ws_shutdown: bool = False

        # Cache for metaAndAssetCtxs (funding + OI) to avoid redundant calls
        self._meta_cache: Optional[dict] = None
        self._meta_cache_ts: float = 0.0
        self._meta_cache_ttl: float = 5.0  # seconds

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session and verify connectivity."""
        if self._session is not None and not self._session.closed:
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"Content-Type": "application/json"},
        )
        # Verify connectivity with allMids
        try:
            async with self._session.post(
                self._rest_url,
                json={"type": "allMids"},
            ) as resp:
                resp.raise_for_status()
                await resp.json()
        except Exception as e:
            await self._session.close()
            self._session = None
            self.record_error(f"connect failed: {e}")
            raise ConnectionError(f"Hyperliquid connect failed: {e}") from e

        self._connected = True
        self.clear_errors()
        logger.info("[hyperliquid] Connected to REST API")

    async def disconnect(self) -> None:
        """Close WS + aiohttp session."""
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
        self._meta_cache = None
        logger.info("[hyperliquid] Disconnected")

    # ─── REST: Orderbook ─────────────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """POST {"type": "l2Book", "coin": symbol} to /info"""
        try:
            async with self._session.post(
                self._rest_url,
                json={"type": "l2Book", "coin": symbol},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            levels = data.get("levels", [[], []])
            # levels[0] = bids, levels[1] = asks
            raw_bids = levels[0] if len(levels) > 0 else []
            raw_asks = levels[1] if len(levels) > 1 else []

            bids = [
                [Decimal(str(b["px"])), Decimal(str(b["sz"]))]
                for b in raw_bids[:depth]
            ]
            asks = [
                [Decimal(str(a["px"])), Decimal(str(a["sz"]))]
                for a in raw_asks[:depth]
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
        """POST {"type": "allMids"} → find symbol in response dict."""
        try:
            async with self._session.post(
                self._rest_url,
                json={"type": "allMids"},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            mid_price_str = data.get(symbol)
            if mid_price_str is None:
                raise KeyError(f"Symbol '{symbol}' not found in allMids response")

            mid_price = Decimal(str(mid_price_str))

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": mid_price,  # allMids only gives mid; use orderbook for true BBO
                "ask": mid_price,
                "last": mid_price,
                "volume_24h": Decimal("0"),  # Not available from allMids
            }
        except Exception as e:
            self.record_error(f"get_ticker({symbol}): {e}")
            raise

    # ─── REST: Recent Trades ─────────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """POST {"type": "recentTrades", "coin": symbol, "limit": limit}"""
        try:
            async with self._session.post(
                self._rest_url,
                json={"type": "recentTrades", "coin": symbol, "limit": limit},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            raw_trades = data if isinstance(data, list) else data.get("trades", [])

            trades = []
            for t in raw_trades:
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("time", t.get("timestamp", 0))),
                    "price": Decimal(str(t.get("px", t.get("price", "0")))),
                    "size": Decimal(str(t.get("sz", t.get("size", "0")))),
                    "side": t.get("side", "unknown"),
                })

            self.clear_errors()
            return trades
        except Exception as e:
            self.record_error(f"get_recent_trades({symbol}): {e}")
            raise

    # ─── REST: Meta + Asset Contexts (shared) ────────────────

    async def _get_meta_and_asset_ctxs(self) -> dict:
        """POST {"type": "metaAndAssetCtxs"} — cached for _meta_cache_ttl seconds."""
        now = time.time()
        if self._meta_cache and (now - self._meta_cache_ts) < self._meta_cache_ttl:
            return self._meta_cache

        try:
            async with self._session.post(
                self._rest_url,
                json={"type": "metaAndAssetCtxs"},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            self._meta_cache = data
            self._meta_cache_ts = now
            self.clear_errors()
            return data
        except Exception as e:
            self.record_error(f"_get_meta_and_asset_ctxs: {e}")
            raise

    def _find_asset_ctx(self, meta_data: dict, symbol: str) -> Optional[dict]:
        """Find asset context for symbol in metaAndAssetCtxs response.

        Response format: [meta_info, [asset_ctx_0, asset_ctx_1, ...]]
        meta_info.universe[i].name corresponds to asset_ctx[i]
        """
        if not isinstance(meta_data, list) or len(meta_data) < 2:
            return None

        meta_info = meta_data[0]
        asset_ctxs = meta_data[1]
        universe = meta_info.get("universe", [])

        for i, asset in enumerate(universe):
            if asset.get("name") == symbol and i < len(asset_ctxs):
                return asset_ctxs[i]

        return None

    # ─── REST: Funding Rate ──────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """Extract funding rate from metaAndAssetCtxs response."""
        try:
            meta_data = await self._get_meta_and_asset_ctxs()
            ctx = self._find_asset_ctx(meta_data, symbol)

            if ctx is None:
                raise KeyError(f"Symbol '{symbol}' not found in metaAndAssetCtxs")

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "rate": Decimal(str(ctx.get("funding", "0"))),
                "next_funding_time": 0.0,  # Hyperliquid uses continuous funding
            }
        except Exception as e:
            self.record_error(f"get_funding_rate({symbol}): {e}")
            raise

    # ─── REST: Open Interest ─────────────────────────────────

    async def get_open_interest(self, symbol: str) -> dict:
        """Extract open interest from metaAndAssetCtxs response."""
        try:
            meta_data = await self._get_meta_and_asset_ctxs()
            ctx = self._find_asset_ctx(meta_data, symbol)

            if ctx is None:
                raise KeyError(f"Symbol '{symbol}' not found in metaAndAssetCtxs")

            oi = Decimal(str(ctx.get("openInterest", "0")))
            mark_price = Decimal(str(ctx.get("markPx", "0")))
            oi_value = oi * mark_price if mark_price > 0 else Decimal("0")

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "open_interest": oi,
                "open_interest_value": oi_value,
            }
        except Exception as e:
            self.record_error(f"get_open_interest({symbol}): {e}")
            raise

    # ─── WebSocket: Orderbook Stream ─────────────────────────

    async def start_orderbook_stream(self, symbol: str) -> None:
        """Start WS l2Book stream with exponential backoff reconnect."""
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
        logger.info(f"[hyperliquid] WS orderbook stream started for {symbol}")

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

                    # Subscribe to l2Book channel
                    subscribe_msg = json.dumps({
                        "method": "subscribe",
                        "subscription": {
                            "type": "l2Book",
                            "coin": symbol,
                        },
                    })
                    await ws.send(subscribe_msg)
                    logger.info(f"[hyperliquid] WS subscribed to l2Book:{symbol}")

                    async for raw_msg in ws:
                        if self._ws_shutdown:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            self._handle_ws_message(msg)
                        except json.JSONDecodeError:
                            logger.warning("[hyperliquid] WS received non-JSON message")
                        except Exception as e:
                            logger.warning(f"[hyperliquid] WS message handler error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws_connected = False
                if self._ws_shutdown:
                    break
                logger.warning(
                    f"[hyperliquid] WS disconnected: {e}. "
                    f"Reconnecting in {backoff:.1f}s..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._ws_reconnect_max)

        self._ws_connected = False
        self._ws = None

    def _handle_ws_message(self, msg: dict) -> None:
        """Parse WS l2Book message and update BBO cache.

        Delegates to free function parse_hyperliquid_l2book() for Rust migration.
        """
        result = parse_hyperliquid_l2book(msg)
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
        logger.info("[hyperliquid] WS orderbook stream stopped")
