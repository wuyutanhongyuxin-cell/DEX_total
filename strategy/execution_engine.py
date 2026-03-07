"""
ExecutionEngine -- STUB interface for trade execution.

This is intentionally a stub. Actual execution is handled by
exchange-specific bots (e.g., grvt_lighter) with proper safety rules.
DEX_total generates signals only; execution is delegated.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """Stub execution engine. Logs signals but does not trade.

    To implement actual execution, subclass this and override execute().
    See: /tmp/grvt_lighter_dev_experience/06-golden-rules.md
    """

    def __init__(self):
        self._executed_count = 0
        self._total_received = 0

    async def execute(self, signal: dict) -> Optional[dict]:
        """Process a signal. Stub: logs and returns None."""
        self._total_received += 1
        logger.info(
            f"[STUB] Signal: {signal.get('direction', '?')} "
            f"spread={signal.get('spread_bps', 0):.1f}bps "
            f"conf={signal.get('confidence', 0):.3f}"
        )
        return None

    @property
    def stats(self) -> dict:
        return {
            "executed": self._executed_count,
            "received": self._total_received,
        }
