"""
BinanceCollector — Binance USDM Futures data collector.

Extends CcxtCollector with Binance-specific endpoints:
- Forced liquidation orders (fapiPublic)
- Mark price / premium index (fapiPublic)
- Proper symbol normalization for Binance futures

All prices/sizes use Decimal(str(value)), never raw floats.
API failures raise exceptions, never return default values.
"""

import logging
import time
from decimal import Decimal
from typing import Optional

from collectors.ccxt_collector import CcxtCollector

logger = logging.getLogger(__name__)


class BinanceCollector(CcxtCollector):
    """Binance USDM Futures collector.

    Uses ccxt_id="binanceusdm" for linear perpetual futures.
    Adds Binance-specific methods for liquidations and mark price.
    """

    def __init__(
        self,
        tier: int = 1,
        api_key: str = "",
        api_secret: str = "",
        **options,
    ):
        # Force defaultType to future for USDM
        options.setdefault("defaultType", "future")
        super().__init__(
            name="binance",
            tier=tier,
            ccxt_id="binanceusdm",
            api_key=api_key,
            api_secret=api_secret,
            **options,
        )

    # ─── Lifecycle ───────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to Binance USDM Futures."""
        await super().connect()
        logger.info(f"[{self.name}] Binance USDM connected")

    # ─── Binance-Specific Methods ────────────────────────────

    async def get_liquidations(self, symbol: str) -> list:
        """Fetch recent forced liquidation orders from Binance.

        Uses the fapiPublic GET /fapi/v1/allForceOrders endpoint.

        Returns list of normalized liquidation dicts with Decimal values.
        """
        try:
            # Binance fapi expects raw symbol like "BTCUSDT", not "BTC/USDT:USDT"
            binance_symbol = self._to_binance_symbol(symbol)
            raw = await self.exchange.fapiPublicGetAllForceOrders({
                "symbol": binance_symbol,
            })
            liquidations = []
            for liq in raw:
                liquidations.append({
                    "exchange": self.name,
                    "symbol": symbol,
                    "timestamp": float(liq.get("time", 0)),
                    "side": liq.get("side", "").lower(),
                    "price": Decimal(str(liq.get("price", 0))),
                    "original_qty": Decimal(str(liq.get("origQty", 0))),
                    "executed_qty": Decimal(str(liq.get("executedQty", 0))),
                    "average_price": Decimal(str(liq.get("averagePrice", 0))),
                    "status": liq.get("status", ""),
                    "type": liq.get("type", ""),
                })
            self.clear_errors()
            return liquidations
        except Exception as e:
            self.record_error(str(e))
            raise

    async def get_mark_price(self, symbol: str) -> dict:
        """Fetch mark price and premium index from Binance.

        Uses the fapiPublic GET /fapi/v1/premiumIndex endpoint.

        Returns dict with Decimal values for mark price, index price,
        and funding rate.
        """
        try:
            binance_symbol = self._to_binance_symbol(symbol)
            raw = await self.exchange.fapiPublicGetPremiumIndex({
                "symbol": binance_symbol,
            })
            # fapiPublicGetPremiumIndex returns a single dict for one symbol
            if isinstance(raw, list):
                raw = raw[0] if raw else {}
            result = {
                "exchange": self.name,
                "symbol": symbol,
                "timestamp": float(raw.get("time", time.time() * 1000)),
                "mark_price": Decimal(str(raw.get("markPrice", 0))),
                "index_price": Decimal(str(raw.get("indexPrice", 0))),
                "estimated_settle_price": Decimal(str(raw.get("estimatedSettlePrice", 0))),
                "last_funding_rate": Decimal(str(raw.get("lastFundingRate", 0))),
                "next_funding_time": float(raw.get("nextFundingTime", 0)),
                "interest_rate": Decimal(str(raw.get("interestRate", 0))),
            }
            self.clear_errors()
            return result
        except Exception as e:
            self.record_error(str(e))
            raise

    # ─── Utilities ───────────────────────────────────────────

    @staticmethod
    def _to_binance_symbol(symbol: str) -> str:
        """Convert unified symbol (e.g. 'BTC/USDT:USDT') to Binance format ('BTCUSDT').

        Handles common formats:
        - 'BTC/USDT:USDT' -> 'BTCUSDT'
        - 'BTC/USDT' -> 'BTCUSDT'
        - 'BTCUSDT' -> 'BTCUSDT' (pass-through)
        """
        return symbol.replace("/", "").replace(":USDT", "")
