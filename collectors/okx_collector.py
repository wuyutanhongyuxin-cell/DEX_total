"""
OKXCollector — OKX Futures/Swap data collector.

Extends CcxtCollector with OKX-specific behavior:
- Passphrase handling for OKX API authentication
- instType / instId symbol mapping for OKX endpoints
- Funding rate normalization for OKX format
- Open interest with OKX-specific instId mapping

All prices/sizes use Decimal(str(value)), never raw floats.
API failures raise exceptions, never return default values.
"""

import logging
import time
from decimal import Decimal
from typing import Optional

from collectors.ccxt_collector import CcxtCollector

logger = logging.getLogger(__name__)


class OkxCollector(CcxtCollector):
    """OKX perpetual swap data collector.

    Uses ccxt_id="okx". Requires passphrase for authenticated endpoints.
    """

    def __init__(
        self,
        tier: int = 1,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        **options,
    ):
        # OKX defaults to swap (linear perpetual)
        options.setdefault("defaultType", "swap")
        super().__init__(
            name="okx",
            tier=tier,
            ccxt_id="okx",
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            **options,
        )

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to OKX. Sets instType for swap markets."""
        await super().connect()
        logger.info(f"[{self.name}] OKX swap connected")

    # ─── Override: Funding Rate ──────────────────────────────

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch funding rate from OKX.

        OKX returns fundingRate and nextFundingTime in its own format.
        We normalize to the BaseCollector schema, extracting
        nextFundingDatetime as epoch ms.
        """
        try:
            raw = await self.exchange.fetch_funding_rate(symbol)
            # OKX provides nextFundingDatetime as ISO string or timestamp
            next_funding = raw.get("nextFundingTimestamp") or raw.get("nextFundingDatetime") or 0
            if isinstance(next_funding, str):
                # ccxt may provide ISO datetime string; prefer numeric timestamp
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

    # ─── Override: Open Interest ─────────────────────────────

    async def get_open_interest(self, symbol: str) -> dict:
        """Fetch open interest from OKX.

        OKX uses instId format (e.g. 'BTC-USDT-SWAP') internally.
        ccxt handles the mapping, but we normalize the output to ensure
        consistent Decimal values.
        """
        try:
            raw = await self.exchange.fetch_open_interest(symbol)
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("timestamp") or time.time() * 1000),
                "open_interest": Decimal(str(raw.get("openInterestAmount", 0))),
                "open_interest_value": Decimal(str(raw.get("openInterestValue", 0))),
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Utilities ───────────────────────────────────────────

    @staticmethod
    def _to_okx_inst_id(symbol: str) -> str:
        """Convert unified symbol to OKX instId format.

        Examples:
        - 'BTC/USDT:USDT' -> 'BTC-USDT-SWAP'
        - 'ETH/USDT:USDT' -> 'ETH-USDT-SWAP'

        Note: ccxt usually handles this internally, but this is useful
        for direct API calls bypassing ccxt.
        """
        # Strip the settlement part (':USDT')
        base_symbol = symbol.split(":")[0]  # 'BTC/USDT'
        parts = base_symbol.split("/")      # ['BTC', 'USDT']
        if len(parts) == 2:
            return f"{parts[0]}-{parts[1]}-SWAP"
        return symbol
