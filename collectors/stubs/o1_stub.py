"""
O1Stub — DEPRECATED stub collector for 01.xyz DEX.

THIS FILE IS SUPERSEDED by collectors/o1_collector.py (Tier 3).

The full O1Collector implementation lives at:
    collectors/o1_collector.py

main.py COLLECTOR_MAP now uses O1Collector directly.
This stub is retained only for import compatibility — do not use it
for new code. It will be removed in a future cleanup pass.

Original context: 01.xyz was undergoing a chain migration; the stub was
created as a placeholder. The migration is complete and the public REST
API at https://zo-mainnet.n1.xyz is now accessible.
"""

import logging

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class O1Stub(BaseCollector):
    """DEPRECATED — use collectors.o1_collector.O1Collector instead."""

    def __init__(self):
        super().__init__(name="o1_stub_deprecated", tier=4)
        logger.warning(
            "[o1_stub] O1Stub is deprecated. "
            "Use collectors.o1_collector.O1Collector (Tier 3) instead."
        )

    async def connect(self) -> None:
        logger.warning("[o1_stub] O1Stub.connect() — no-op; use O1Collector")
        self._connected = False

    async def disconnect(self) -> None:
        self._connected = False

    def is_healthy(self) -> bool:
        return False

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        raise NotImplementedError(
            "O1Stub is deprecated — use collectors.o1_collector.O1Collector"
        )

    async def get_ticker(self, symbol: str) -> dict:
        raise NotImplementedError(
            "O1Stub is deprecated — use collectors.o1_collector.O1Collector"
        )
