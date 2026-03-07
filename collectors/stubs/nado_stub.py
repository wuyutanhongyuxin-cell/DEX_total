"""
NadoStub — SUPERSEDED. Use NadoCollector instead.

This stub has been replaced by the full implementation:
  collectors/nado_collector.py  (Tier 2, REST V1/V2 + WebSocket BBO)

This file is kept for reference only and is no longer used by main.py.
The COLLECTOR_MAP in main.py now uses NadoCollector directly.

Original stub reason: "Nado does not have a Python SDK available."
Resolution: Nado exposes a public REST API (no auth required) at
  https://gateway.prod.nado.xyz and https://archive.prod.nado.xyz,
  plus a WebSocket stream at wss://gateway.prod.nado.xyz/v1/subscribe.
  No SDK is needed — aiohttp + websockets suffice.
"""

import logging

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class NadoStub(BaseCollector):
    """DEPRECATED stub for Nado. Use NadoCollector from collectors/nado_collector.py."""

    def __init__(self):
        super().__init__(name="nado_stub", tier=4)
        logger.warning(
            "[nado_stub] NadoStub is deprecated. "
            "Use NadoCollector (collectors/nado_collector.py) instead."
        )

    async def connect(self) -> None:
        logger.warning("[nado_stub] Stub — not connected. Use NadoCollector.")
        self._connected = False

    async def disconnect(self) -> None:
        self._connected = False

    def is_healthy(self) -> bool:
        return False

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        raise NotImplementedError(
            "NadoStub is deprecated. Use NadoCollector from collectors/nado_collector.py"
        )

    async def get_ticker(self, symbol: str) -> dict:
        raise NotImplementedError(
            "NadoStub is deprecated. Use NadoCollector from collectors/nado_collector.py"
        )
