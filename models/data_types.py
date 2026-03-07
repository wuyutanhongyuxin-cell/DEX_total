"""
Rust-migration-ready data types.

All hot-path data structures use @dataclass for:
1. Flat, FFI-friendly layout (no nested dicts)
2. Clear field names and types for Rust struct mapping
3. Serializable via dataclasses.asdict() for IPC

When migrating to Rust:
- Each dataclass maps to a Rust struct
- Decimal fields -> rust_decimal::Decimal or f64 (depending on precision needs)
- Optional fields -> Option<T>
- Timestamps are always epoch seconds (f64)
"""

from dataclasses import dataclass, field, asdict
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class BBOState:
    """Best-bid/offer snapshot for one exchange.

    Rust equivalent: struct BBOState { exchange: String, bid: f64, ... }
    """
    exchange: str
    bid: Decimal
    ask: Decimal
    bid_size: Decimal = Decimal("0")
    ask_size: Decimal = Decimal("0")
    timestamp: float = 0.0

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2

    @property
    def spread_bps(self) -> float:
        mid = self.mid
        if mid <= 0:
            return 0.0
        return float((self.ask - self.bid) / mid * 10000)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SpreadEntry:
    """One row of the NxN spread matrix.

    Rust equivalent: struct SpreadEntry { buy_exchange: String, ... }
    """
    buy_exchange: str
    sell_exchange: str
    symbol: str
    raw_spread: Decimal
    spread_bps: float
    net_spread_bps: float
    fee_cost_bps: float
    natural_spread_bps: float
    buy_price: Decimal
    sell_price: Decimal
    timestamp: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SignalOutput:
    """Generated trading signal.

    Rust equivalent: struct SignalOutput { timestamp: f64, ... }
    """
    timestamp: float
    buy_exchange: str
    sell_exchange: str
    symbol: str
    direction: str
    spread_bps: float
    buy_price: str
    sell_price: str
    confidence: float
    components: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class TradeEntry:
    """Normalized trade record across all exchanges.

    Rust equivalent: struct TradeEntry { exchange: String, ... }
    """
    exchange: str
    symbol: str
    timestamp: float
    side: str
    price: Decimal
    size: Decimal
    trade_id: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class FundingSnapshot:
    """Funding rate snapshot.

    Rust equivalent: struct FundingSnapshot { exchange: String, ... }
    """
    exchange: str
    symbol: str
    timestamp: float
    rate: Decimal
    next_funding_time: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class OISnapshot:
    """Open interest snapshot.

    Rust equivalent: struct OISnapshot { exchange: String, ... }
    """
    exchange: str
    symbol: str
    timestamp: float
    open_interest: Decimal
    open_interest_value: Decimal = Decimal("0")

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class LeadLagResult:
    """Leader-follower detection result.

    Rust equivalent: struct LeadLagResult { leader: String, ... }
    """
    leader: str
    follower: str
    lag_ms: float
    correlation: float
    confidence: float

    def to_dict(self) -> dict:
        return asdict(self)
