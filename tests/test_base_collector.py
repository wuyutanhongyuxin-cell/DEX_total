"""Tests for BaseCollector."""

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

# Add parent to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.base_collector import BaseCollector


class MockCollector(BaseCollector):
    """Concrete implementation for testing."""

    def __init__(self):
        super().__init__(name="mock", tier=1)

    async def connect(self):
        self._connected = True

    async def disconnect(self):
        self._connected = False

    async def get_orderbook(self, symbol, depth=20):
        return {
            "exchange": self.name,
            "symbol": symbol,
            "timestamp": time.time(),
            "bids": [[Decimal("50000.0"), Decimal("1.0")]],
            "asks": [[Decimal("50001.0"), Decimal("1.0")]],
        }

    async def get_ticker(self, symbol):
        return {
            "exchange": self.name,
            "symbol": symbol,
            "timestamp": time.time(),
            "bid": Decimal("50000.0"),
            "ask": Decimal("50001.0"),
            "last": Decimal("50000.5"),
            "volume_24h": Decimal("1000"),
        }


def test_collector_init():
    c = MockCollector()
    assert c.name == "mock"
    assert c.tier == 1
    assert not c._connected
    assert not c._ws_connected


def test_health_check():
    c = MockCollector()
    assert not c.is_healthy()  # not connected

    c._connected = True
    assert c.is_healthy()

    # Errors degrade health
    for i in range(5):
        c.record_error(f"error {i}")
    assert not c.is_healthy()

    c.clear_errors()
    assert c.is_healthy()


def test_ws_stale():
    c = MockCollector()
    assert c.is_ws_stale()  # WS not connected

    c._ws_connected = True
    assert c.is_ws_stale()  # No updates yet

    c._update_bbo_cache(Decimal("50000"), Decimal("50001"))
    assert not c.is_ws_stale()  # Just updated

    # Simulate stale
    c._last_ws_update = time.time() - 60
    assert c.is_ws_stale()


def test_bbo_cache():
    c = MockCollector()
    bid, ask = c.get_cached_bbo()
    assert bid is None
    assert ask is None

    c._ws_connected = True
    c._update_bbo_cache(Decimal("50000"), Decimal("50001"))
    bid, ask = c.get_cached_bbo()
    assert bid == Decimal("50000")
    assert ask == Decimal("50001")


def test_to_decimal():
    assert BaseCollector.to_decimal(1.5) == Decimal("1.5")
    assert BaseCollector.to_decimal("2.5") == Decimal("2.5")
    assert BaseCollector.to_decimal(Decimal("3.5")) == Decimal("3.5")
    assert BaseCollector.to_decimal(0) == Decimal("0")


def test_repr():
    c = MockCollector()
    r = repr(c)
    assert "mock" in r
    assert "tier=1" in r


def test_supported_data_types():
    c = MockCollector()
    types = c.supported_data_types()
    assert "orderbook" in types
    assert "ticker" in types


@pytest.mark.asyncio
async def test_connect_disconnect():
    c = MockCollector()
    await c.connect()
    assert c._connected
    await c.disconnect()
    assert not c._connected


@pytest.mark.asyncio
async def test_get_orderbook():
    c = MockCollector()
    ob = await c.get_orderbook("BTC-PERP")
    assert ob["exchange"] == "mock"
    assert len(ob["bids"]) > 0
    assert isinstance(ob["bids"][0][0], Decimal)


@pytest.mark.asyncio
async def test_optional_methods_raise():
    c = MockCollector()
    with pytest.raises(NotImplementedError):
        await c.get_recent_trades("BTC")
    with pytest.raises(NotImplementedError):
        await c.get_klines("BTC")
    with pytest.raises(NotImplementedError):
        await c.get_funding_rate("BTC")
    with pytest.raises(NotImplementedError):
        await c.get_open_interest("BTC")
    with pytest.raises(NotImplementedError):
        await c.get_liquidations("BTC")
    with pytest.raises(NotImplementedError):
        await c.start_orderbook_stream("BTC")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
