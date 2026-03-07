"""
BitgetCollector — Bitget Futures data collector.

Extends CcxtCollector with Bitget-specific behavior:
- Passphrase handling for Bitget API authentication
- productType configuration for USDT-M futures
- Funding rate normalization for Bitget format

All prices/sizes use Decimal(str(value)), never raw floats.
API failures raise exceptions, never return default values.
"""

import logging
import time
from decimal import Decimal
from typing import Optional

from collectors.ccxt_collector import CcxtCollector

logger = logging.getLogger(__name__)


class BitgetCollector(CcxtCollector):
    """Bitget USDT-M futures data collector.

    Uses ccxt_id="bitget". Requires passphrase for authenticated endpoints.
    """

    def __init__(
        self,
        tier: int = 2,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        **options,
    ):
        # Bitget defaults to swap (linear perpetual)
        options.setdefault("defaultType", "swap")
        super().__init__(
            name="bitget",
            tier=tier,
            ccxt_id="bitget",
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            **options,
        )

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to Bitget. Configures productType for USDT-M futures."""
        # Set productType option for Bitget USDT-M before loading markets
        self.exchange.options["defaultProductType"] = "USDT-FUTURES"
        await super().connect()
        logger.info(f"[{self.name}] Bitget USDT-M futures connected")

    # ─── Override: Funding Rate ──────────────────────────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch funding rate from Bitget.

        Bitget may return the funding rate in a slightly different structure.
        We normalize to the BaseCollector schema, ensuring Decimal values.
        """
        try:
            raw = await self.exchange.fetch_funding_rate(symbol)
            # Bitget provides fundingRate and may use different keys
            # for next funding time
            next_funding = (
                raw.get("nextFundingTimestamp")
                or raw.get("nextFundingDatetime")
                or 0
            )
            if isinstance(next_funding, str):
                next_funding = float(raw.get("nextFundingTimestamp", 0))
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("timestamp") or time.time() * 1000),
                "rate": Decimal(str(raw.get("fundingRate", 0))),
                "next_funding_time": float(next_funding),
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Utilities ───────────────────────────────────────────

    @staticmethod
    def _to_bitget_symbol(symbol: str) -> str:
        """Convert unified symbol to Bitget format if needed.

        Examples:
        - 'BTC/USDT:USDT' -> 'BTCUSDT'
        - 'BTC/USDT' -> 'BTCUSDT'

        Note: ccxt usually handles this internally. This is useful
        for direct API calls bypassing ccxt.
        """
        return symbol.replace("/", "").replace(":USDT", "")
