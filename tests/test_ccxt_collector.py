"""Tests for CcxtCollector (mocked, no real API calls)."""

import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


def test_ccxt_collector_import():
    """Verify CcxtCollector can be imported."""
    from collectors.ccxt_collector import CcxtCollector
    assert CcxtCollector is not None


def test_binance_collector_import():
    from collectors.binance_collector import BinanceCollector
    assert BinanceCollector is not None


def test_okx_collector_import():
    from collectors.okx_collector import OkxCollector
    assert OkxCollector is not None


def test_bitget_collector_import():
    from collectors.bitget_collector import BitgetCollector
    assert BitgetCollector is not None


def test_all_dex_collectors_import():
    from collectors.lighter_collector import LighterCollector
    from collectors.grvt_collector import GRVTCollector
    from collectors.hyperliquid_collector import HyperliquidCollector
    from collectors.paradex_collector import ParadexCollector
    from collectors.aster_collector import AsterCollector
    from collectors.edgex_collector import EdgeXCollector
    assert all([
        LighterCollector, GRVTCollector, HyperliquidCollector,
        ParadexCollector, AsterCollector, EdgeXCollector,
    ])


def test_stubs_import():
    from collectors.stubs.variational_stub import VariationalStub
    from collectors.stubs.nado_stub import NadoStub
    from collectors.stubs.o1_stub import O1Stub
    assert all([VariationalStub, NadoStub, O1Stub])


def test_stub_not_healthy():
    from collectors.stubs.variational_stub import VariationalStub
    stub = VariationalStub()
    assert not stub.is_healthy()


@pytest.mark.asyncio
async def test_stub_raises():
    from collectors.stubs.variational_stub import VariationalStub
    stub = VariationalStub()
    with pytest.raises(NotImplementedError):
        await stub.get_orderbook("BTC")
    with pytest.raises(NotImplementedError):
        await stub.get_ticker("BTC")


def test_analysis_imports():
    from analysis.spread_analyzer import SpreadAnalyzer
    from analysis.correlation import CorrelationAnalyzer
    from analysis.lead_lag import LeadLagAnalyzer
    from analysis.backtest_engine import BacktestEngine
    assert all([SpreadAnalyzer, CorrelationAnalyzer, LeadLagAnalyzer, BacktestEngine])


def test_strategy_imports():
    from strategy.signal_generator import SignalGenerator
    from strategy.execution_engine import ExecutionEngine
    assert all([SignalGenerator, ExecutionEngine])


def test_monitoring_imports():
    from monitoring.alerts import TelegramNotifier
    from monitoring.dashboard import Dashboard
    from monitoring.data_logger import DataLogger
    assert all([TelegramNotifier, Dashboard, DataLogger])


def test_signal_generator_warmup():
    from strategy.signal_generator import SignalGenerator
    sg = SignalGenerator(warmup_ticks=5)
    # Should return empty during warmup
    for _ in range(4):
        result = sg.evaluate([{"net_spread_bps": 10, "buy_exchange": "a",
                               "sell_exchange": "b", "symbol": "BTC"}])
        assert result == []

    # Tick 5 should allow evaluation
    result = sg.evaluate([{"net_spread_bps": 10, "buy_exchange": "a",
                           "sell_exchange": "b", "symbol": "BTC"}])
    # May or may not generate signal depending on confidence


def test_execution_stub():
    from strategy.execution_engine import ExecutionEngine
    ee = ExecutionEngine()
    assert ee.stats["executed"] == 0
    assert ee.stats["received"] == 0


def test_data_logger_headers():
    from monitoring.data_logger import DataLogger
    dl = DataLogger()
    assert len(dl.BBO_HEADERS) > 0
    assert "timestamp" in dl.BBO_HEADERS
    assert "exchange" in dl.BBO_HEADERS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
