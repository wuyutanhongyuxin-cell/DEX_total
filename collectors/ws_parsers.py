"""
Free-standing WS message parsers — independent of collector instance state.

Rust migration: Each parse_* function maps to a Rust function that takes
raw bytes/JSON and returns a typed struct. No self/instance dependencies.

These functions are pure: input → output, no side effects.
"""

from decimal import Decimal
from typing import Optional


def parse_lighter_orderbook(msg: dict) -> Optional[tuple[Decimal, Decimal]]:
    """Parse Lighter WS orderbook message → (best_bid, best_ask) or None.

    Lighter WS format:
    {"data": {"bids": [[price, size], ...], "asks": [[price, size], ...]}}
    or:
    {"data": {"bids": [{"price": ..., "size": ...}, ...], "asks": [...]}}

    Returns None if message doesn't contain valid BBO data.
    """
    data = msg.get("data", msg)
    bids = data.get("bids", [])
    asks = data.get("asks", [])

    if not bids and not asks:
        return None

    best_bid = None
    best_ask = None

    if bids:
        top_bid = bids[0]
        if isinstance(top_bid, list):
            best_bid = Decimal(str(top_bid[0]))
        elif isinstance(top_bid, dict):
            best_bid = Decimal(str(top_bid.get("price", "0")))

    if asks:
        top_ask = asks[0]
        if isinstance(top_ask, list):
            best_ask = Decimal(str(top_ask[0]))
        elif isinstance(top_ask, dict):
            best_ask = Decimal(str(top_ask.get("price", "0")))

    if best_bid is not None and best_ask is not None:
        return best_bid, best_ask
    return None


def parse_hyperliquid_l2book(msg: dict) -> Optional[tuple[Decimal, Decimal]]:
    """Parse Hyperliquid WS l2Book message → (best_bid, best_ask) or None.

    Hyperliquid WS format:
    {
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "levels": [
                [{"px": "95000.0", "sz": "1.5", "n": 3}, ...],  # bids
                [{"px": "95001.0", "sz": "0.8", "n": 2}, ...]   # asks
            ],
            "time": 1700000000000
        }
    }

    Returns None if not l2Book or missing data.
    """
    channel = msg.get("channel", "")
    if channel != "l2Book":
        return None

    data = msg.get("data", {})
    levels = data.get("levels", [[], []])

    if len(levels) < 2:
        return None

    bids = levels[0]
    asks = levels[1]

    if not bids or not asks:
        return None

    best_bid = Decimal(str(bids[0]["px"]))
    best_ask = Decimal(str(asks[0]["px"]))

    return best_bid, best_ask


def parse_generic_orderbook(msg: dict) -> Optional[tuple[Decimal, Decimal]]:
    """Generic WS orderbook parser for exchanges with standard format.

    Handles formats:
    - {"bids": [[price, size], ...], "asks": [[price, size], ...]}
    - {"data": {"bids": [...], "asks": [...]}}
    - {"b": [[price, size], ...], "a": [[price, size], ...]}

    Returns (best_bid, best_ask) or None.
    """
    data = msg.get("data", msg)

    bids = data.get("bids", data.get("b", []))
    asks = data.get("asks", data.get("a", []))

    if not bids or not asks:
        return None

    try:
        bid_entry = bids[0]
        ask_entry = asks[0]

        if isinstance(bid_entry, (list, tuple)):
            best_bid = Decimal(str(bid_entry[0]))
        elif isinstance(bid_entry, dict):
            best_bid = Decimal(str(bid_entry.get("price", bid_entry.get("px", "0"))))
        else:
            return None

        if isinstance(ask_entry, (list, tuple)):
            best_ask = Decimal(str(ask_entry[0]))
        elif isinstance(ask_entry, dict):
            best_ask = Decimal(str(ask_entry.get("price", ask_entry.get("px", "0"))))
        else:
            return None

        return best_bid, best_ask
    except (IndexError, KeyError, ValueError):
        return None


def parse_nado_bbo(msg: dict) -> Optional[tuple[Decimal, Decimal]]:
    """Parse Nado WS best_bid_offer message → (best_bid, best_ask) or None.

    Nado WS BBO format (gateway v1/subscribe):
    {
        "type": "best_bid_offer",
        "product_id": 2,
        "bid_price": "95000.50",
        "bid_size": "1.5",
        "ask_price": "95001.00",
        "ask_size": "0.8",
        "timestamp": 1700000000000
    }

    May also appear nested under "data":
    {"data": {"bid_price": ..., "ask_price": ..., ...}}

    Returns None if message type is not best_bid_offer or missing bid/ask.
    """
    # Accept top-level or data-wrapped message
    payload = msg if "bid_price" in msg or "ask_price" in msg else msg.get("data", msg)

    # Filter by message type if present — skip subscription acks
    msg_type = msg.get("type", payload.get("type", ""))
    if msg_type and msg_type not in ("best_bid_offer", "bbo", ""):
        return None

    bid_raw = payload.get("bid_price", payload.get("bid"))
    ask_raw = payload.get("ask_price", payload.get("ask"))

    if bid_raw is None or ask_raw is None:
        return None

    try:
        best_bid = Decimal(str(bid_raw))
        best_ask = Decimal(str(ask_raw))
    except Exception:
        return None

    if best_bid <= 0 or best_ask <= 0:
        return None

    # Reject crossed market (bid >= ask) — indicates data error
    if best_bid >= best_ask:
        return None

    return best_bid, best_ask
