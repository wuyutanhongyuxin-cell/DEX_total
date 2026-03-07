"""
BaseCollector — Abstract base class for all exchange data collectors.

Design principles (from 21 golden rules):
- API failures raise exceptions, never return default values
- WS has stale detection (configurable threshold)
- All prices/quantities use Decimal throughout
- Auth failures must fail-fast (raise, not log)
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for all exchange data collectors.

    Subclasses must implement at minimum:
    - connect() / disconnect()
    - get_orderbook()
    - get_ticker()

    Optional methods have default NotImplementedError for graceful degradation.
    """

    def __init__(self, name: str, tier: int):
        self.name = name
        self.tier = tier
        self._connected = False
        self._ws_connected = False
        self._last_ws_update: float = 0.0
        self._ws_stale_threshold: float = 30.0
        self._cached_bid: Optional[Decimal] = None
        self._cached_ask: Optional[Decimal] = None
        self._error_count: int = 0
        self._last_error: Optional[str] = None

    # ─── Lifecycle ───────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None:
        """Connect to exchange. Must raise on auth failure."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from exchange. Clean up all sessions/sockets."""
        ...

    # ─── Required Data Methods ───────────────────────────────

    @abstractmethod
    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """Fetch orderbook snapshot.

        Returns:
            {
                "exchange": str,
                "symbol": str,
                "timestamp": float (epoch ms),
                "bids": [[price: Decimal, size: Decimal], ...],
                "asks": [[price: Decimal, size: Decimal], ...],
            }

        Raises on API failure — never returns empty/default.
        """
        ...

    @abstractmethod
    async def get_ticker(self, symbol: str) -> dict:
        """Fetch current ticker/BBO.

        Returns:
            {
                "exchange": str,
                "symbol": str,
                "timestamp": float,
                "bid": Decimal,
                "ask": Decimal,
                "last": Decimal,
                "volume_24h": Decimal,
            }

        Raises on API failure.
        """
        ...

    # ─── Optional Data Methods ───────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """Fetch recent trades. Returns list of trade dicts."""
        raise NotImplementedError(f"{self.name}: get_recent_trades not supported")

    async def get_klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list:
        """Fetch kline/candlestick data."""
        raise NotImplementedError(f"{self.name}: get_klines not supported")

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch current funding rate.

        Returns:
            {
                "exchange": str,
                "symbol": str,
                "timestamp": float,
                "rate": Decimal,
                "next_funding_time": float,
            }
        """
        raise NotImplementedError(f"{self.name}: get_funding_rate not supported")

    async def get_open_interest(self, symbol: str) -> dict:
        """Fetch open interest.

        Returns:
            {
                "exchange": str,
                "symbol": str,
                "timestamp": float,
                "open_interest": Decimal,
                "open_interest_value": Decimal,
            }
        """
        raise NotImplementedError(f"{self.name}: get_open_interest not supported")

    async def get_liquidations(self, symbol: str) -> list:
        """Fetch recent liquidations."""
        raise NotImplementedError(f"{self.name}: get_liquidations not supported")

    # ─── WebSocket Methods ───────────────────────────────────

    async def start_orderbook_stream(self, symbol: str) -> None:
        """Start WS orderbook/BBO stream. Override for WS-capable exchanges."""
        raise NotImplementedError(f"{self.name}: WS orderbook stream not supported")

    async def stop_orderbook_stream(self) -> None:
        """Stop WS orderbook stream."""
        self._ws_connected = False

    def get_cached_bbo(self) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """Get cached best bid/offer from WS stream.

        Returns (bid, ask) — both None if WS not active or stale.
        """
        if self.is_ws_stale():
            return None, None
        return self._cached_bid, self._cached_ask

    def _update_bbo_cache(self, bid: Decimal, ask: Decimal) -> None:
        """Update BBO cache. Call from WS message handler."""
        self._cached_bid = bid
        self._cached_ask = ask
        self._last_ws_update = time.time()

    # ─── Health Checks ───────────────────────────────────────

    def is_healthy(self) -> bool:
        """Overall health: connected and no recent errors."""
        return self._connected and self._error_count < 5

    def is_ws_stale(self) -> bool:
        """Check if WS data is stale (no update within threshold)."""
        if not self._ws_connected:
            return True
        return (time.time() - self._last_ws_update) > self._ws_stale_threshold

    def supported_data_types(self) -> list[str]:
        """Return list of supported data types for this collector."""
        supported = ["orderbook", "ticker"]  # always required
        for method_name in ["get_recent_trades", "get_klines", "get_funding_rate",
                            "get_open_interest", "get_liquidations"]:
            method = getattr(self, method_name)
            # Check if method is overridden from base
            if type(self).__dict__.get(method_name) is not None:
                data_type = method_name.replace("get_", "")
                supported.append(data_type)
        return supported

    # ─── Error Tracking ──────────────────────────────────────

    def record_error(self, error: str) -> None:
        """Record an error for health tracking."""
        self._error_count += 1
        self._last_error = error
        logger.warning(f"[{self.name}] Error #{self._error_count}: {error}")

    def clear_errors(self) -> None:
        """Clear error count (call after successful operation)."""
        self._error_count = 0
        self._last_error = None

    # ─── Utilities ───────────────────────────────────────────

    @staticmethod
    def to_decimal(value) -> Decimal:
        """Safe conversion to Decimal. Always via str() to avoid float contamination."""
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def __repr__(self) -> str:
        status = "OK" if self.is_healthy() else "UNHEALTHY"
        ws = "WS:live" if (self._ws_connected and not self.is_ws_stale()) else "WS:off"
        return f"<{self.name} tier={self.tier} {status} {ws}>"
