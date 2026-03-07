"""
DEX_total -- Main orchestrator.
Manages collector lifecycle, data collection loops, analysis, and signal generation.
"""

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.live import Live

from config.loader import (
    get_collection_config,
    get_exchanges_config,
    get_strategy_config,
    get_symbol_for_exchange,
)
from analysis.spread_analyzer import SpreadAnalyzer
from analysis.correlation import CorrelationAnalyzer
from analysis.lead_lag import LeadLagAnalyzer
from strategy.signal_generator import SignalGenerator
from strategy.execution_engine import ExecutionEngine
from monitoring.alerts import TelegramNotifier
from monitoring.dashboard import Dashboard
from monitoring.data_logger import DataLogger

# Collectors
from collectors.ccxt_collector import CcxtCollector
from collectors.binance_collector import BinanceCollector
from collectors.okx_collector import OkxCollector
from collectors.bitget_collector import BitgetCollector
from collectors.lighter_collector import LighterCollector
from collectors.grvt_collector import GRVTCollector
from collectors.hyperliquid_collector import HyperliquidCollector
from collectors.paradex_collector import ParadexCollector
from collectors.aster_collector import AsterCollector
from collectors.edgex_collector import EdgeXCollector
from collectors.stubs.variational_stub import VariationalStub
from collectors.stubs.nado_stub import NadoStub
from collectors.stubs.o1_stub import O1Stub

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"dex_total_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("dex_total")


COLLECTOR_MAP = {
    "binance": lambda cfg: BinanceCollector(
        api_key=cfg.get("api_key", ""),
        api_secret=cfg.get("api_secret", ""),
    ),
    "okx": lambda cfg: OkxCollector(
        api_key=cfg.get("api_key", ""),
        api_secret=cfg.get("api_secret", ""),
        passphrase=cfg.get("passphrase", ""),
    ),
    "bitget": lambda cfg: BitgetCollector(
        api_key=cfg.get("api_key", ""),
        api_secret=cfg.get("api_secret", ""),
        passphrase=cfg.get("passphrase", ""),
    ),
    "lighter": lambda cfg: LighterCollector(
        rest_url=cfg.get("rest_url", "https://mainnet.lighter.xyz"),
        ws_url=cfg.get("ws_url", "wss://mainnet.lighter.xyz/ws"),
    ),
    "grvt": lambda cfg: GRVTCollector(
        rest_url=cfg.get("rest_url", "https://edge.grvt.io"),
    ),
    "hyperliquid": lambda cfg: HyperliquidCollector(
        rest_url=cfg.get("rest_url", "https://api.hyperliquid.xyz"),
        ws_url=cfg.get("ws_url", "wss://api.hyperliquid.xyz/ws"),
    ),
    "paradex": lambda cfg: ParadexCollector(
        rest_url=cfg.get("rest_url", "https://api.paradex.trade/v1"),
    ),
    "aster": lambda cfg: AsterCollector(
        rest_url=cfg.get("rest_url", "https://api.aster.finance"),
    ),
    "edgex": lambda cfg: EdgeXCollector(
        rest_url=cfg.get("rest_url", "https://api.edgex.exchange"),
    ),
    "variational": lambda cfg: VariationalStub(),
    "nado": lambda cfg: NadoStub(),
    "o1xyz": lambda cfg: O1Stub(),
}


class Orchestrator:
    """Main orchestrator for DEX_total data collection and analysis."""

    def __init__(self):
        self.collectors: dict = {}
        self.spread_analyzer: Optional[SpreadAnalyzer] = None
        self.correlation: Optional[CorrelationAnalyzer] = None
        self.lead_lag: Optional[LeadLagAnalyzer] = None
        self.signal_gen: Optional[SignalGenerator] = None
        self.execution: Optional[ExecutionEngine] = None
        self.telegram: Optional[TelegramNotifier] = None
        self.dashboard: Optional[Dashboard] = None
        self.data_logger: Optional[DataLogger] = None

        self._running = False
        self._start_time = 0.0
        self._symbols: list[str] = []
        self._heartbeat_interval = 300  # 5 min

    async def start(self) -> None:
        """Initialize all components and start collection loops."""
        self._start_time = time.time()
        self._running = True

        # Load configs
        exchanges_cfg = get_exchanges_config()
        collection_cfg = get_collection_config()
        strategy_cfg = get_strategy_config()

        self._symbols = collection_cfg.get("symbols", ["BTC-PERP"])
        intervals = collection_cfg.get("intervals", {})

        # Initialize components
        self.telegram = TelegramNotifier()
        self.data_logger = DataLogger(collection_cfg.get("csv", {}).get("base_dir", "data"))
        self.dashboard = Dashboard(
            refresh_ms=strategy_cfg.get("monitoring", {}).get("dashboard_refresh_ms", 500)
        )

        # Analysis
        spread_cfg = strategy_cfg.get("spread", {})
        self.spread_analyzer = SpreadAnalyzer(
            fee_estimate_bps=spread_cfg.get("fee_estimate_bps", 5.0),
            natural_spread_window=spread_cfg.get("natural_spread_window", 60),
            persistence_threshold_s=spread_cfg.get("persistence_threshold_s", 2.0),
        )
        self.correlation = CorrelationAnalyzer(
            window_size=strategy_cfg.get("lead_lag", {}).get("window_s", 300)
        )
        self.lead_lag = LeadLagAnalyzer(
            window_s=strategy_cfg.get("lead_lag", {}).get("window_s", 300),
        )

        # Signal generation
        sig_cfg = strategy_cfg.get("signal", {})
        self.signal_gen = SignalGenerator(
            min_confidence=sig_cfg.get("min_confidence", 0.3),
            cooldown_s=sig_cfg.get("cooldown_s", 5.0),
            warmup_ticks=sig_cfg.get("warmup_ticks", 30),
            weights=strategy_cfg.get("weights"),
        )
        self.execution = ExecutionEngine()

        # Create collectors
        active_names = []
        for name, cfg in exchanges_cfg.items():
            if not cfg.get("enabled", False):
                continue
            factory = COLLECTOR_MAP.get(name)
            if not factory:
                logger.warning(f"No collector factory for {name}, skipping")
                continue
            try:
                collector = factory(cfg)
                self.collectors[name] = collector
                active_names.append(name)
            except Exception as e:
                logger.error(f"Failed to create collector {name}: {e}")

        logger.info(f"Created {len(self.collectors)} collectors: {active_names}")

        # Connect all collectors
        for name, collector in list(self.collectors.items()):
            try:
                await collector.connect()
                logger.info(f"Connected: {name}")
                self.dashboard.update_exchange_status(
                    name, True, collector._ws_connected, collector.tier
                )
            except Exception as e:
                logger.error(f"Failed to connect {name}: {e}")
                self.dashboard.update_exchange_status(name, False, False, collector.tier)
                await self.telegram.notify_error(name, f"Connect failed: {e}")

        await self.telegram.notify_start(active_names, self._symbols)
        await self.dashboard.start()

        # Start collection tasks
        tasks = []
        bbo_interval = intervals.get("orderbook", 1.0)
        trades_interval = intervals.get("trades", 5.0)
        funding_interval = intervals.get("funding", 60.0)
        oi_interval = intervals.get("open_interest", 30.0)
        analysis_interval = strategy_cfg.get("lead_lag", {}).get("compute_interval_s", 120.0)

        tasks.append(asyncio.create_task(self._bbo_loop(bbo_interval)))
        tasks.append(asyncio.create_task(self._trades_loop(trades_interval)))
        tasks.append(asyncio.create_task(self._funding_loop(funding_interval)))
        tasks.append(asyncio.create_task(self._oi_loop(oi_interval)))
        tasks.append(asyncio.create_task(self._analysis_loop(analysis_interval)))
        tasks.append(asyncio.create_task(self._heartbeat_loop()))
        tasks.append(asyncio.create_task(self._dashboard_loop()))

        # Start WS streams for Tier 2 exchanges
        for name, collector in self.collectors.items():
            if collector.tier <= 2:
                for symbol in self._symbols:
                    ex_symbol = get_symbol_for_exchange(name, symbol)
                    try:
                        await collector.start_orderbook_stream(ex_symbol)
                        logger.info(f"WS stream started: {name} {ex_symbol}")
                    except NotImplementedError:
                        pass
                    except Exception as e:
                        logger.warning(f"WS stream failed for {name}: {e}")

        logger.info("DEX_total orchestrator running")

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Tasks cancelled, shutting down")

    async def stop(self, reason: str = "manual") -> None:
        """Graceful shutdown."""
        self._running = False
        runtime_h = (time.time() - self._start_time) / 3600

        logger.info(f"Stopping DEX_total: {reason}")

        for name, collector in self.collectors.items():
            try:
                await collector.disconnect()
                logger.info(f"Disconnected: {name}")
            except Exception as e:
                logger.error(f"Disconnect error {name}: {e}")

        if self.dashboard:
            await self.dashboard.stop()
        if self.data_logger:
            self.data_logger.close()
        if self.telegram:
            await self.telegram.notify_stop(reason, runtime_h)
            await self.telegram.close()

        logger.info(f"DEX_total stopped after {runtime_h:.1f}h")

    # --- Collection Loops ---

    async def _bbo_loop(self, interval: float) -> None:
        """Main BBO collection + analysis loop."""
        while self._running:
            for symbol in self._symbols:
                for name, collector in self.collectors.items():
                    if not collector.is_healthy():
                        continue
                    ex_symbol = get_symbol_for_exchange(name, symbol)
                    try:
                        # Prefer WS cache, fall back to REST
                        bid, ask = collector.get_cached_bbo()
                        if bid is None or ask is None:
                            ticker = await collector.get_ticker(ex_symbol)
                            bid = ticker["bid"]
                            ask = ticker["ask"]

                        # Update analysis
                        self.spread_analyzer.update_bbo(name, bid, ask)
                        mid = (bid + ask) / 2
                        self.correlation.update_price(name, mid)
                        self.lead_lag.update_price(name, mid, time.time())

                        # Log to CSV
                        self.data_logger.log_bbo(name, symbol, bid, ask)

                        # Update dashboard
                        self.dashboard.update_bbo(name, symbol, bid, ask)
                        self.dashboard.update_exchange_status(
                            name, True, collector._ws_connected,
                            collector.tier, collector._error_count,
                        )

                    except NotImplementedError:
                        pass
                    except Exception as e:
                        logger.debug(f"BBO fetch error {name}: {e}")

                # Compute spreads + signals after all exchanges updated
                try:
                    spreads = self.spread_analyzer.compute_matrix(symbol)
                    self.dashboard.update_spreads(spreads)

                    # Log top spreads
                    for s in spreads[:3]:
                        self.data_logger.log_spread(
                            s["buy_exchange"], s["sell_exchange"], symbol,
                            s["spread_bps"], s["net_spread_bps"],
                            s["buy_price"], s["sell_price"],
                        )

                    # Generate signals
                    signals = self.signal_gen.evaluate(spreads)
                    if signals:
                        self.dashboard.update_signals(signals)
                        for sig in signals:
                            self.data_logger.log_signal(sig)
                            await self.telegram.notify_signal(sig)
                            await self.execution.execute(sig)

                    self.dashboard.update_stats(self.signal_gen.stats)
                except Exception as e:
                    logger.error(f"Analysis error: {e}")

            await asyncio.sleep(interval)

    async def _funding_loop(self, interval: float) -> None:
        """Funding rate collection loop."""
        while self._running:
            for symbol in self._symbols:
                for name, collector in self.collectors.items():
                    if not collector.is_healthy():
                        continue
                    ex_symbol = get_symbol_for_exchange(name, symbol)
                    try:
                        funding = await collector.get_funding_rate(ex_symbol)
                        rate = funding["rate"]
                        self.signal_gen.update_funding(name, rate)
                        self.data_logger.log_funding(
                            name, symbol, rate,
                            str(funding.get("next_funding_time", "")),
                        )
                    except NotImplementedError:
                        pass
                    except Exception as e:
                        logger.debug(f"Funding fetch error {name}: {e}")

            await asyncio.sleep(interval)

    async def _oi_loop(self, interval: float) -> None:
        """Open interest collection loop."""
        while self._running:
            for symbol in self._symbols:
                for name, collector in self.collectors.items():
                    if not collector.is_healthy():
                        continue
                    ex_symbol = get_symbol_for_exchange(name, symbol)
                    try:
                        oi_data = await collector.get_open_interest(ex_symbol)
                        oi = oi_data["open_interest"]
                        self.signal_gen.update_oi(name, oi)
                        self.data_logger.log_oi(
                            name, symbol, oi,
                            str(oi_data.get("open_interest_value", "")),
                        )
                    except NotImplementedError:
                        pass
                    except Exception as e:
                        logger.debug(f"OI fetch error {name}: {e}")

            await asyncio.sleep(interval)

    async def _trades_loop(self, interval: float) -> None:
        """Recent trades collection loop."""
        while self._running:
            for symbol in self._symbols:
                for name, collector in self.collectors.items():
                    if not collector.is_healthy():
                        continue
                    ex_symbol = get_symbol_for_exchange(name, symbol)
                    try:
                        trades = await collector.get_recent_trades(ex_symbol)
                        for t in trades:
                            self.data_logger.log_trade(
                                name, symbol, t.get("side", ""),
                                t.get("price", ""), t.get("size", ""),
                                str(t.get("trade_id", t.get("id", ""))),
                            )
                    except NotImplementedError:
                        pass
                    except Exception as e:
                        logger.debug(f"Trades fetch error {name}: {e}")

            await asyncio.sleep(interval)

    async def _analysis_loop(self, interval: float) -> None:
        """Periodic lead-lag and correlation computation.

        Runs less frequently than BBO loop. Results feed into signal generator.
        """
        while self._running:
            await asyncio.sleep(interval)

            try:
                # Compute correlation matrix
                corr_matrix = self.correlation.compute_correlation_matrix()
                if corr_matrix:
                    logger.info(
                        f"Correlation matrix: {len(corr_matrix)} pairs computed"
                    )

                # Identify leaders
                leaders = self.lead_lag.identify_leaders()
                if leaders:
                    top_leader = leaders[0]
                    self.signal_gen.set_leader(top_leader["exchange"])
                    logger.info(
                        f"Leader: {top_leader['exchange']} "
                        f"(leads {top_leader['lead_count']} pairs, "
                        f"avg lag {top_leader['avg_lag_ms']:.0f}ms)"
                    )
            except Exception as e:
                logger.error(f"Analysis loop error: {e}")

    async def _heartbeat_loop(self) -> None:
        """Periodic heartbeat to Telegram."""
        while self._running:
            await asyncio.sleep(self._heartbeat_interval)
            runtime_h = (time.time() - self._start_time) / 3600
            active = sum(1 for c in self.collectors.values() if c.is_healthy())
            top = self.spread_analyzer.get_top_spreads(1)
            top_spread = None
            if top:
                t = top[0]
                top_spread = {
                    "pair": f"{t['buy_exchange']}->{t['sell_exchange']}",
                    "spread_bps": float(t["net_spread_bps"]),
                }
            await self.telegram.notify_heartbeat(
                runtime_h, active, len(self.collectors),
                self.signal_gen.stats.get("signal_count", 0),
                top_spread,
            )

    async def _dashboard_loop(self) -> None:
        """Render dashboard at configured interval."""
        with Live(self.dashboard.render_once(), refresh_per_second=2, console=self.dashboard.console) as live:
            while self._running:
                live.update(self.dashboard.render_once())
                await asyncio.sleep(self.dashboard.refresh_ms / 1000.0)


async def main():
    orchestrator = Orchestrator()

    loop = asyncio.get_event_loop()

    def shutdown_handler():
        logger.info("Shutdown signal received")
        asyncio.create_task(orchestrator.stop("signal"))

    # Register signal handlers (Unix only)
    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, shutdown_handler)
        loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        await orchestrator.stop("keyboard_interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        await orchestrator.stop(f"error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
