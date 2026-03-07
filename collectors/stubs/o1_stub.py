"""
O1Stub — Stub collector for 01.xyz DEX.

01.xyz is undergoing a chain migration and its API is not currently stable.
This stub exists so the system can register 01 as a known exchange
without crashing. All data methods raise NotImplementedError.
"""

import logging

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class O1Stub(BaseCollector):
    """Stub collector for 01.xyz (chain migration in progress)."""

    def __init__(self):
        super().__init__(name="o1", tier=4)

    async def connect(self) -> None:
        logger.info("[o1] 01.xyz: chain migration in progress")
        self._connected = False

    async def disconnect(self) -> None:
        self._connected = False

    def is_healthy(self) -> bool:
        return False

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        raise NotImplementedError("01.xyz: chain migration in progress — orderbook not accessible")

    async def get_ticker(self, symbol: str) -> dict:
        raise NotImplementedError("01.xyz: chain migration in progress — ticker not accessible")
