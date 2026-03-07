"""
VariationalStub — Stub collector for Variational DEX.

Variational does not have a public trading API available.
This stub exists so the system can register Variational as a known exchange
without crashing. All data methods raise NotImplementedError.
"""

import logging

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class VariationalStub(BaseCollector):
    """Stub collector for Variational (no trading API available)."""

    def __init__(self):
        super().__init__(name="variational", tier=4)

    async def connect(self) -> None:
        logger.info("[variational] Variational: no trading API available")
        self._connected = False

    async def disconnect(self) -> None:
        self._connected = False

    def is_healthy(self) -> bool:
        return False

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        raise NotImplementedError("Variational: no trading API available — orderbook not accessible")

    async def get_ticker(self, symbol: str) -> dict:
        raise NotImplementedError("Variational: no trading API available — ticker not accessible")
