"""
VariationalStub — Stub collector for Variational DEX.

DEPRECATED: This stub has been superseded by VariationalCollector (Tier 3).

  New implementation: collectors/variational_collector.py
  Replaced on: 2026-03-08
  Reason: Variational exposes a public REST API at
    https://omni-client-api.prod.ap-northeast-1.variational.io
    which does not require authentication and bypasses Cloudflare.
    The real collector uses GET /metadata/stats for BBO data.

This file is retained for reference only. main.py now imports and uses
VariationalCollector directly. Do not use VariationalStub in production.
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
