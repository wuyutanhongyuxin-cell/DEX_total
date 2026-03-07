"""
CcxtCollector — Unified ccxt-based collector serving as base for Binance/OKX/Bitget.

Uses ccxt.async_support for all REST calls. Each API call is wrapped in
try/except with record_error()/clear_errors() for health tracking.

Design principles:
- All prices/sizes use Decimal(str(value)), never raw floats
- API failures raise exceptions, never return default values
- On success, clear_errors() resets the health counter
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional

import ccxt.async_support as ccxt

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class CcxtCollector(BaseCollector):
    """Unified ccxt-based data collector.

    Subclasses (Binance, OKX, Bitget) override ccxt_id and can customize
    individual methods for exchange-specific endpoints.
    """

    def __init__(
        self,
        name: str,
        tier: int,
        ccxt_id: str,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        **options,
    ):
        super().__init__(name=name, tier=tier)
        self.ccxt_id = ccxt_id

        # Build ccxt config
        config: dict = {
            "enableRateLimit": True,
        }
        if api_key:
            config["apiKey"] = api_key
        if api_secret:
            config["secret"] = api_secret
        if passphrase:
            config["password"] = passphrase

        # Merge any extra options (e.g. defaultType, instType)
        config.update(options)

        # Instantiate the ccxt exchange object
        exchange_class = getattr(ccxt, ccxt_id, None)
        if exchange_class is None:
            raise ValueError(f"Unknown ccxt exchange id: {ccxt_id}")
        self.exchange: ccxt.Exchange = exchange_class(config)

        logger.info(f"[{self.name}] CcxtCollector created (ccxt_id={ccxt_id})")

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Load markets from the exchange. Raises on failure."""
        try:
            await self.exchange.load_markets()
            self._connected = True
            self.clear_errors()
            logger.info(f"[{self.name}] Connected — {len(self.exchange.markets)} markets loaded")
        except Exception as e:
            self.record_error(str(e))
            raise

    async def disconnect(self) -> None:
        """Close the ccxt exchange session cleanly."""
        try:
            await self.exchange.close()
            self._connected = False
            logger.info(f"[{self.name}] Disconnected")
        except Exception as e:
            logger.warning(f"[{self.name}] Error during disconnect: {e}")
            self._connected = False

    # ─── Required Data Methods ───────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot via ccxt.

        Returns normalized dict with Decimal prices/sizes matching
        BaseCollector format.
        """
        try:
            raw = await self.exchange.fetch_order_book(symbol, limit=depth)
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("timestamp") or time.time() * 1000),
                "bids": [
                    [Decimal(str(price)), Decimal(str(size))]
                    for price, size in raw.get("bids", [])
                ],
                "asks": [
                    [Decimal(str(price)), Decimal(str(size))]
                    for price, size in raw.get("asks", [])
                ],
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_ticker(self, symbol: str) -> dict:
        """Fetch current ticker/BBO via ccxt.

        Returns normalized dict with Decimal values.
        """
        try:
            raw = await self.exchange.fetch_ticker(symbol)
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("timestamp") or time.time() * 1000),
                "bid": Decimal(str(raw["bid"])) if raw.get("bid") is not None else Decimal("0"),
                "ask": Decimal(str(raw["ask"])) if raw.get("ask") is not None else Decimal("0"),
                "last": Decimal(str(raw["last"])) if raw.get("last") is not None else Decimal("0"),
                "volume_24h": Decimal(str(raw.get("quoteVolume") or raw.get("baseVolume") or 0)),
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Optional Data Methods ───────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """Fetch recent trades via ccxt.

        Returns list of normalized trade dicts with Decimal price/amount.
        """
        try:
            raw_trades = await self.exchange.fetch_trades(symbol, limit=limit)
            trades = []
            for t in raw_trades:
                trades.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(t.get("timestamp", 0)),
                    "side": t.get("side", ""),
                    "price": Decimal(str(t["price"])),
                    "amount": Decimal(str(t["amount"])),
                    "id": t.get("id", ""),
                })
            self.clear_errors()
            return trades
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list:
        """Fetch OHLCV kline data via ccxt.

        Returns list of kline dicts with Decimal OHLCV values.
        Each ccxt OHLCV entry is [timestamp, open, high, low, close, volume].
        """
        try:
            raw_klines = await self.exchange.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
            klines = []
            for k in raw_klines:
                klines.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(k[0]),
                    "open": Decimal(str(k[1])),
                    "high": Decimal(str(k[2])),
                    "low": Decimal(str(k[3])),
                    "close": Decimal(str(k[4])),
                    "volume": Decimal(str(k[5])),
                })
            self.clear_errors()
            return klines
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch current funding rate via ccxt.

        Returns normalized dict matching BaseCollector format.
        """
        try:
            raw = await self.exchange.fetch_funding_rate(symbol)
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("timestamp") or time.time() * 1000),
                "rate": Decimal(str(raw.get("fundingRate", 0))),
                "next_funding_time": float(raw.get("fundingDatetime") or raw.get("nextFundingDatetime") or 0),
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_open_interest(self, symbol: str) -> dict:
        """Fetch open interest via ccxt.

        Returns normalized dict matching BaseCollector format.
        """
        try:
            raw = await self.exchange.fetch_open_interest(symbol)
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("timestamp") or time.time() * 1000),
                "open_interest": Decimal(str(raw.get("openInterestAmount", 0))),
                "open_interest_value": Decimal(str(raw.get("openInterestValue", 0))),
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Utilities ───────────────────────────────────────────

    def _normalize_symbol(self, symbol: str) -> str:
        """Convert a unified symbol to exchange-specific format if needed.

        Override in subclasses for exchange-specific symbol mapping.
        """
        return symbol

    def __repr__(self) -> str:
        status = "OK" if self.is_healthy() else "UNHEALTHY"
        ws = "WS:live" if (self._ws_connected and not self.is_ws_stale()) else "WS:off"
        return f"<{self.name} ccxt={self.ccxt_id} tier={self.tier} {status} {ws}>"
