"""Tests for SpreadAnalyzer."""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.spread_analyzer import SpreadAnalyzer


def test_init():
    sa = SpreadAnalyzer(fee_estimate_bps=5.0)
    assert sa is not None


def test_update_bbo():
    sa = SpreadAnalyzer()
    sa.update_bbo("binance", Decimal("50000"), Decimal("50001"))
    sa.update_bbo("okx", Decimal("50002"), Decimal("50003"))
    assert "binance" in sa._bbos
    assert "okx" in sa._bbos


def test_compute_matrix_basic():
    sa = SpreadAnalyzer(fee_estimate_bps=5.0)
    sa.update_bbo("binance", Decimal("50000"), Decimal("50001"))
    sa.update_bbo("okx", Decimal("50002"), Decimal("50003"))

    matrix = sa.compute_matrix("BTC-PERP")
    assert len(matrix) > 0

    # Each entry should have required fields
    for entry in matrix:
        assert "buy_exchange" in entry
        assert "sell_exchange" in entry
        assert "spread_bps" in entry
        assert "net_spread_bps" in entry


def test_compute_matrix_no_self_pair():
    sa = SpreadAnalyzer()
    sa.update_bbo("binance", Decimal("50000"), Decimal("50001"))
    sa.update_bbo("okx", Decimal("50002"), Decimal("50003"))

    matrix = sa.compute_matrix("BTC-PERP")
    for entry in matrix:
        assert entry["buy_exchange"] != entry["sell_exchange"]


def test_get_top_spreads():
    sa = SpreadAnalyzer(fee_estimate_bps=0.1)  # Low fee for testing
    sa.update_bbo("a", Decimal("50000"), Decimal("50001"))
    sa.update_bbo("b", Decimal("50010"), Decimal("50011"))
    sa.update_bbo("c", Decimal("50005"), Decimal("50006"))

    sa.compute_matrix("BTC")
    top = sa.get_top_spreads(2)
    assert len(top) <= 2
    # Should be sorted by net_spread_bps descending
    if len(top) >= 2:
        assert float(top[0]["net_spread_bps"]) >= float(top[1]["net_spread_bps"])


def test_empty_matrix():
    sa = SpreadAnalyzer()
    matrix = sa.compute_matrix("BTC")
    assert matrix == []


def test_single_exchange():
    sa = SpreadAnalyzer()
    sa.update_bbo("binance", Decimal("50000"), Decimal("50001"))
    matrix = sa.compute_matrix("BTC")
    assert matrix == []  # Need at least 2 exchanges


def test_spread_direction():
    """Verify spread calculation: buy at ask, sell at bid."""
    sa = SpreadAnalyzer(fee_estimate_bps=0)
    sa.update_bbo("cheap", Decimal("49990"), Decimal("50000"))  # ask=50000
    sa.update_bbo("expensive", Decimal("50010"), Decimal("50020"))  # bid=50010

    matrix = sa.compute_matrix("BTC")

    # Find buy@cheap sell@expensive
    for entry in matrix:
        if entry["buy_exchange"] == "cheap" and entry["sell_exchange"] == "expensive":
            # spread = sell_bid - buy_ask = 50010 - 50000 = 10
            assert float(entry["spread_bps"]) > 0
            break


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
