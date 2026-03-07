"""
NadoCollector — Tier 2 DEX data collector for Nado exchange.

Uses REST API (no SDK dependency) + WebSocket for real-time BBO.

Endpoints:
  Gateway REST V2 : https://gateway.prod.nado.xyz/v2  (float, human-readable)
  Gateway REST V1 : https://gateway.prod.nado.xyz/v1  (1e18 fixed-point)
  Archive REST V2 : https://archive.prod.nado.xyz/v2  (float, human-readable)
  Archive REST V1 : https://archive.prod.nado.xyz/v1  (POST, 1e18 fixed-point)
  WS Subscribe    : wss://gateway.prod.nado.xyz/v1/subscribe

No authentication required — all endpoints are public.

Design rules (from 21 golden rules):
- All prices/sizes -> Decimal(str(value)), NEVER float contamination
- API failures raise, never return defaults
- try/except every HTTP call: record_error() + re-raise
- On success: clear_errors()
- Exponential backoff reconnect on WS (base=1s, max=64s)
- disconnect() closes ALL aiohttp sessions and WS connections
- Never use abs() on position sizes

1e18 handling:
  V1 endpoints return _x18 fields — divide by Decimal("1e18") to get real value.
  V2 endpoints return float directly — safe to use after Decimal(str(...)).
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
from collectors.ws_parsers import parse_nado_bbo

logger = logging.getLogger(__name__)

# Nado V1 scale factor
_1E18 = Decimal("1000000000000000000")


class NadoCollector(BaseCollector):
    """Nado DEX data collector — REST (V1+V2) + WebSocket BBO stream."""

    def __init__(
        self,
        name: str = "nado",
        tier: int = 2,
        rest_url: str = "https://gateway.prod.nado.xyz",
        archive_url: str = "https://archive.prod.nado.xyz",
        ws_url: str = "wss://gateway.prod.nado.xyz/v1/subscribe",
    ):
        super().__init__(name=name, tier=tier)
        self._rest_url = rest_url.rstrip("/")
        self._archive_url = archive_url.rstrip("/")
        self._ws_url = ws_url

        self._session: Optional[aiohttp.ClientSession] = None

        # symbol → product_id mapping loaded on connect()
        # e.g. {"BTC-PERP": 2, "ETH-PERP": 3}
        self._symbol_to_product_id: dict[str, int] = {}

        # WS state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_ping_task: Optional[asyncio.Task] = None
        self._ws_symbol: Optional[str] = None
        self._ws_reconnect_base: float = 1.0
        self._ws_reconnect_max: float = 64.0
        self._ws_shutdown: bool = False

    # ─── Helpers ─────────────────────────────────────────────

    def _ticker_id(self, symbol: str) -> str:
        """Convert canonical symbol to Nado V2 ticker_id.

        e.g. "BTC-PERP" → "BTC-PERP_USDT0"
        The convention for all perp markets on Nado is {symbol}_USDT0.
        """
        return f"{symbol}_USDT0"

    def _product_id(self, symbol: str) -> int:
        """Return product_id for symbol. Raises KeyError if symbol unknown."""
        pid = self._symbol_to_product_id.get(symbol)
        if pid is None:
            raise KeyError(
                f"[nado] Unknown symbol '{symbol}'. "
                f"Known: {list(self._symbol_to_product_id.keys())}. "
                "Call connect() first to load symbol map."
            )
        return pid

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Create aiohttp session and load symbol→product_id map from API."""
        if self._session is not None and not self._session.closed:
            return

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
        )

        try:
            await self._load_symbols()
        except Exception as e:
            await self._session.close()
            self._session = None
            self.record_error(f"connect failed: {e}")
            raise ConnectionError(f"Nado connect failed: {e}") from e

        self._connected = True
        self.clear_errors()
        logger.info(
            f"[nado] Connected. Loaded {len(self._symbol_to_product_id)} symbols: "
            f"{list(self._symbol_to_product_id.keys())}"
        )

    async def _load_symbols(self) -> None:
        """Load all perp symbols and their product_ids from gateway/v1/query.

        Endpoint: GET /v1/query?type=symbols&product_type=perp
        Response: {
            "data": {
                "symbols": {
                    "BTC-PERP": {"product_id": 2, "symbol": "BTC-PERP", ...},
                    ...
                }
            }
        }
        """
        url = f"{self._rest_url}/v1/query"
        params = {"type": "symbols", "product_type": "perp"}

        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        symbols_map = data.get("data", {}).get("symbols", {})
        if not symbols_map:
            raise RuntimeError(
                f"Nado symbols endpoint returned empty map. Response: {data}"
            )

        for sym, info in symbols_map.items():
            pid = info.get("product_id")
            if pid is not None:
                self._symbol_to_product_id[sym] = int(pid)

        if not self._symbol_to_product_id:
            raise RuntimeError("Nado: no product_ids found in symbols response")

    async def disconnect(self) -> None:
        """Close WS stream, ping task, and aiohttp session. Full cleanup."""
        self._ws_shutdown = True

        # Cancel WS background tasks
        for task_attr in ("_ws_task", "_ws_ping_task"):
            task = getattr(self, task_attr)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            setattr(self, task_attr, None)

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
        logger.info("[nado] Disconnected")

    # ─── REST: Orderbook (V2, float) ─────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """GET /v2/orderbook?ticker_id={ticker_id}&depth={depth}

        Response: {"bids": [[price, size], ...], "asks": [[price, size], ...]}
        All values are floats in V2 — safe after Decimal(str(...)).
        """
        ticker_id = self._ticker_id(symbol)
        try:
            async with self._session.get(
                f"{self._rest_url}/v2/orderbook",
                params={"ticker_id": ticker_id, "depth": depth},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            raw_bids = data.get("bids", [])
            raw_asks = data.get("asks", [])

            bids = [
                [Decimal(str(entry[0])), Decimal(str(entry[1]))]
                for entry in raw_bids
            ]
            asks = [
                [Decimal(str(entry[0])), Decimal(str(entry[1]))]
                for entry in raw_asks
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

    # ─── REST: Ticker (V2 archive tickers) ───────────────────

    async def get_ticker(self, symbol: str) -> dict:
        """GET archive/v2/tickers?market=perp → find matching ticker_id.

        Response is a list or dict of ticker objects. Each ticker contains
        24h market data including best bid/ask and last price.

        ticker_id format: "BTC-PERP_USDT0"
        """
        ticker_id = self._ticker_id(symbol)
        try:
            async with self._session.get(
                f"{self._archive_url}/v2/tickers",
                params={"market": "perp"},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            # Response may be a list or dict keyed by ticker_id
            ticker = None
            if isinstance(data, list):
                for entry in data:
                    if entry.get("ticker_id") == ticker_id:
                        ticker = entry
                        break
            elif isinstance(data, dict):
                ticker = data.get(ticker_id) or data.get(symbol)

            if ticker is None:
                raise RuntimeError(
                    f"[nado] Ticker '{ticker_id}' not found in tickers response. "
                    f"Available: {[t.get('ticker_id', t) for t in (data if isinstance(data, list) else [])]}"
                )

            # Field names follow CoinGecko DEX perp format
            bid_raw = ticker.get("bid", ticker.get("best_bid"))
            ask_raw = ticker.get("ask", ticker.get("best_ask"))
            if bid_raw is None or ask_raw is None:
                raise RuntimeError(
                    f"[nado] Ticker '{ticker_id}' missing bid/ask fields. "
                    f"Keys: {list(ticker.keys())}"
                )

            bid = Decimal(str(bid_raw))
            ask = Decimal(str(ask_raw))

            last_raw = ticker.get("last_price", ticker.get("last"))
            last = Decimal(str(last_raw)) if last_raw is not None else (bid + ask) / 2

            volume_raw = ticker.get("base_volume", ticker.get("volume"))
            volume = Decimal(str(volume_raw)) if volume_raw is not None else Decimal("0")

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "bid": bid,
                "ask": ask,
                "last": last,
                "volume_24h": volume,
            }
        except Exception as e:
            self.record_error(f"get_ticker({symbol}): {e}")
            raise

    # ─── REST: Recent Trades (V2 archive) ────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """GET archive/v2/trades?ticker_id={ticker_id}&limit={limit}

        Response: [{"price": float, "base_filled": float,
                    "trade_type": "buy"|"sell", "timestamp": int, ...}, ...]
        """
        ticker_id = self._ticker_id(symbol)
        try:
            async with self._session.get(
                f"{self._archive_url}/v2/trades",
                params={"ticker_id": ticker_id, "limit": limit},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            # Accept list directly or wrapped in a key
            raw_trades = data if isinstance(data, list) else data.get("trades", [])

            trades = []
            for t in raw_trades:
                # trade_type: "buy" means taker bought (price moved up)
                # base_filled or size — handle both field names
                size_raw = t.get("base_filled", t.get("size", t.get("amount")))
                price_raw = t.get("price")
                if price_raw is None or size_raw is None:
                    logger.warning(f"[nado] Trade entry missing price/size, skipping: {t}")
                    continue
                ts_raw = t.get("timestamp", t.get("time", 0))

                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(ts_raw),
                    "price": Decimal(str(price_raw)),
                    "size": Decimal(str(size_raw)),
                    "side": str(t.get("trade_type", t.get("side", "unknown"))),
                })

            self.clear_errors()
            return trades
        except Exception as e:
            self.record_error(f"get_recent_trades({symbol}): {e}")
            raise

    # ─── REST: Funding Rate (V1 archive POST, 1e18) ──────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """POST archive/v1 {"funding_rate": {"product_id": N}}

        Response: {"product_id": N, "funding_rate_x18": str, "update_time": int}
        funding_rate_x18 / 1e18 = 24h funding rate (divide by 8 for hourly if needed).
        """
        product_id = self._product_id(symbol)
        payload = {"funding_rate": {"product_id": product_id}}
        try:
            async with self._session.post(
                f"{self._archive_url}/v1",
                json=payload,
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            rate_x18_raw = data.get("funding_rate_x18")
            if rate_x18_raw is None:
                raise RuntimeError(
                    f"[nado] funding_rate response missing 'funding_rate_x18'. "
                    f"Keys: {list(data.keys())}"
                )
            # Always convert via str to avoid float contamination
            rate = Decimal(str(rate_x18_raw)) / _1E18

            update_time = float(data.get("update_time", time.time() * 1000))

            self.clear_errors()
            return {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": time.time() * 1000,
                "rate": rate,
                # Nado does not expose next_funding_time directly;
                # use 0 as sentinel — callers must handle missing field.
                "next_funding_time": update_time,
            }
        except Exception as e:
            self.record_error(f"get_funding_rate({symbol}): {e}")
            raise

    # ─── REST: Open Interest (V1 gateway, all_products) ──────

    async def get_open_interest(self, symbol: str) -> dict:
        """GET gateway/v1/query?type=all_products → extract perp open_interest.

        Response: {
            "data": {
                "perp_products": [
                    {
                        "product_id": 2,
                        "symbol": "BTC-PERP",
                        "state": {"open_interest": "12345678901234567890", ...},
                        ...
                    }, ...
                ]
            }
        }

        open_interest field is a 1e18-scaled integer string.
        """
        product_id = self._product_id(symbol)
        try:
            async with self._session.get(
                f"{self._rest_url}/v1/query",
                params={"type": "all_products"},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            perp_products = data.get("data", {}).get("perp_products", [])
            if not perp_products:
                raise RuntimeError(
                    f"[nado] all_products response has no perp_products. "
                    f"Response keys: {list(data.get('data', {}).keys())}"
                )

            # Find the product matching our product_id
            product = None
            for p in perp_products:
                if p.get("product_id") == product_id:
                    product = p
                    break

            if product is None:
                raise RuntimeError(
                    f"[nado] product_id={product_id} ({symbol}) not found "
                    f"in all_products. Available ids: "
                    f"{[p.get('product_id') for p in perp_products]}"
                )

            state = product.get("state", {})
            oi_raw = state.get("open_interest")
            if oi_raw is None:
                raise RuntimeError(
                    f"[nado] product_id={product_id} state missing 'open_interest'. "
                    f"State keys: {list(state.keys())}"
                )
            # open_interest is 1e18-scaled — divide for real BTC/ETH units
            oi = Decimal(str(oi_raw)) / _1E18

            # open_interest_value in USD: oi * mark_price
            mark_raw = state.get("mark_price", product.get("mark_price"))
            if mark_raw is not None and Decimal(str(mark_raw)) != 0:
                mark = Decimal(str(mark_raw)) / _1E18
            else:
                mark = Decimal("0")
            oi_value = oi * mark if mark > 0 else Decimal("0")

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

    # ─── WebSocket: BBO Stream ────────────────────────────────

    async def start_orderbook_stream(self, symbol: str) -> None:
        """Start WS best_bid_offer subscription with exponential backoff reconnect.

        Sends every 30 seconds ping to keep connection alive.
        Uses permessage-deflate compression (websockets default on supported servers).
        """
        self._ws_shutdown = False
        self._ws_symbol = symbol

        # Cancel any existing tasks
        for task_attr in ("_ws_task", "_ws_ping_task"):
            task = getattr(self, task_attr)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._ws_task = asyncio.create_task(self._ws_run_loop(symbol))
        logger.info(f"[nado] WS BBO stream started for {symbol}")

    async def _ws_run_loop(self, symbol: str) -> None:
        """Main WS reconnect loop with exponential backoff."""
        backoff = self._ws_reconnect_base
        product_id = self._product_id(symbol)

        while not self._ws_shutdown:
            try:
                # permessage-deflate: websockets enables by default when server offers it.
                # explicit compression=None disables, omit to let websockets negotiate.
                async with websockets.connect(
                    self._ws_url,
                    # Do NOT pass ping_interval here — we manage pings manually
                    # because Nado requires a specific 30s ping cadence.
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    # permessage-deflate is negotiated automatically by the library
                ) as ws:
                    self._ws = ws
                    self._ws_connected = True
                    backoff = self._ws_reconnect_base  # Reset on successful connect

                    # Subscribe to best_bid_offer channel
                    subscribe_msg = json.dumps({
                        "method": "subscribe",
                        "stream": {
                            "type": "best_bid_offer",
                            "product_id": product_id,
                        },
                        "id": 1,
                    })
                    await ws.send(subscribe_msg)
                    logger.info(
                        f"[nado] WS subscribed to best_bid_offer "
                        f"product_id={product_id} ({symbol})"
                    )

                    # Start ping task alongside message loop
                    ping_task = asyncio.create_task(self._ws_ping_loop(ws))
                    self._ws_ping_task = ping_task

                    try:
                        async for raw_msg in ws:
                            if self._ws_shutdown:
                                break
                            try:
                                msg = json.loads(raw_msg)
                                self._handle_ws_message(msg)
                            except json.JSONDecodeError:
                                logger.warning("[nado] WS received non-JSON message")
                            except Exception as e:
                                logger.warning(f"[nado] WS message handler error: {e}")
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
                        self._ws_ping_task = None

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws_connected = False
                self._ws = None
                if self._ws_shutdown:
                    break
                logger.warning(
                    f"[nado] WS disconnected: {e}. "
                    f"Reconnecting in {backoff:.1f}s..."
                )
                # Interruptible sleep — check shutdown every 0.5s
                elapsed = 0.0
                while elapsed < backoff and not self._ws_shutdown:
                    await asyncio.sleep(min(0.5, backoff - elapsed))
                    elapsed += 0.5
                if self._ws_shutdown:
                    break
                backoff = min(backoff * 2, self._ws_reconnect_max)

        self._ws_connected = False
        self._ws = None

    async def _ws_ping_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Send a WebSocket ping every 30 seconds to keep connection alive.

        Nado gateway requires pings at this cadence. Logs but does not crash
        if ping fails — the outer message loop will detect the dead connection.
        """
        while True:
            await asyncio.sleep(30)
            try:
                await ws.ping()
            except Exception as e:
                logger.debug(f"[nado] WS ping failed: {e}")
                break

    def _handle_ws_message(self, msg: dict) -> None:
        """Parse WS BBO message and update BBO cache.

        Delegates to free function parse_nado_bbo() for Rust migration readiness.
        """
        result = parse_nado_bbo(msg)
        if result is not None:
            best_bid, best_ask = result
            self._update_bbo_cache(best_bid, best_ask)

    async def stop_orderbook_stream(self) -> None:
        """Stop WS BBO stream and clean up tasks."""
        self._ws_shutdown = True

        for task_attr in ("_ws_task", "_ws_ping_task"):
            task = getattr(self, task_attr)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            setattr(self, task_attr, None)

        self._ws_connected = False
        logger.info("[nado] WS BBO stream stopped")
