"""
NadoStub — Stub collector for Nado DEX.

Nado does not have a Python SDK available.
This stub exists so the system can register Nado as a known exchange
without crashing. All data methods raise NotImplementedError.
"""

import logging

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class NadoStub(BaseCollector):
    """Stub collector for Nado (no Python SDK available)."""

    def __init__(self):
        super().__init__(name="nado", tier=4)

    async def connect(self) -> None:
        logger.info("[nado] Nado: no Python SDK available")
        self._connected = False

    async def disconnect(self) -> None:
        self._connected = False

    def is_healthy(self) -> bool:
        return False

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        raise NotImplementedError("Nado: no Python SDK available — orderbook not accessible")

    async def get_ticker(self, symbol: str) -> dict:
        raise NotImplementedError("Nado: no Python SDK available — ticker not accessible")
