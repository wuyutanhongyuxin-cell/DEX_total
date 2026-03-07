"""
Rich terminal dashboard for DEX_total.
Displays real-time exchange status, BBO, spreads, and signals.
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)


class Dashboard:
    """Rich-based terminal dashboard for multi-exchange monitoring."""

    def __init__(self, refresh_ms: int = 500):
        self.console = Console()
        self.refresh_ms = refresh_ms

        # State caches (updated by main loop)
        self._exchange_status: dict[str, dict] = {}
        self._bbo_data: dict[str, dict] = {}
        self._spreads: list[dict] = []
        self._signals: list[dict] = []
        self._stats: dict = {}
        self._start_time: float = time.time()
        self._live: Optional[Live] = None
        self._running = False

    # --- Data Update Methods ---

    def update_exchange_status(self, name: str, connected: bool, ws_active: bool,
                                tier: int, error_count: int = 0) -> None:
        self._exchange_status[name] = {
            "connected": connected,
            "ws_active": ws_active,
            "tier": tier,
            "error_count": error_count,
        }

    def update_bbo(self, exchange: str, symbol: str, bid, ask) -> None:
        self._bbo_data[exchange] = {
            "symbol": symbol,
            "bid": str(bid),
            "ask": str(ask),
            "time": time.time(),
        }

    def update_spreads(self, spreads: list[dict]) -> None:
        self._spreads = spreads[:10]  # top 10 only

    def update_signals(self, signals: list[dict]) -> None:
        # Keep last 20 signals
        self._signals = (signals + self._signals)[:20]

    def update_stats(self, stats: dict) -> None:
        self._stats = stats

    # --- Rendering ---

    def _build_exchange_table(self) -> Table:
        table = Table(title="Exchange Status", expand=True)
        table.add_column("Exchange", style="cyan", width=14)
        table.add_column("Tier", width=4, justify="center")
        table.add_column("Status", width=10, justify="center")
        table.add_column("WS", width=6, justify="center")
        table.add_column("Errors", width=6, justify="right")

        for name, info in sorted(self._exchange_status.items()):
            status = "[green]ONLINE[/]" if info["connected"] else "[red]OFFLINE[/]"
            ws = "[green]LIVE[/]" if info["ws_active"] else "[dim]--[/]"
            err_style = "[red]" if info["error_count"] > 0 else ""
            err_end = "[/]" if info["error_count"] > 0 else ""
            table.add_row(
                name,
                str(info["tier"]),
                status,
                ws,
                f"{err_style}{info['error_count']}{err_end}",
            )
        return table

    def _build_bbo_table(self) -> Table:
        table = Table(title="Best Bid/Offer", expand=True)
        table.add_column("Exchange", style="cyan", width=14)
        table.add_column("Bid", width=14, justify="right", style="green")
        table.add_column("Ask", width=14, justify="right", style="red")
        table.add_column("Spread", width=10, justify="right")
        table.add_column("Age(s)", width=6, justify="right")

        now = time.time()
        for exchange, data in sorted(self._bbo_data.items()):
            try:
                bid = Decimal(data["bid"])
                ask = Decimal(data["ask"])
                spread = ask - bid
                age = now - data["time"]
                age_style = "[red]" if age > 5 else ""
                age_end = "[/]" if age > 5 else ""
                table.add_row(
                    exchange,
                    data["bid"],
                    data["ask"],
                    str(spread),
                    f"{age_style}{age:.1f}{age_end}",
                )
            except Exception:
                table.add_row(exchange, data.get("bid", "?"), data.get("ask", "?"), "?", "?")

        return table

    def _build_spread_table(self) -> Table:
        table = Table(title="Top Spreads (net, bps)", expand=True)
        table.add_column("Buy @", width=12)
        table.add_column("Sell @", width=12)
        table.add_column("Gross", width=8, justify="right")
        table.add_column("Net", width=8, justify="right", style="bold")

        for s in self._spreads[:8]:
            net = float(s.get("net_spread_bps", 0))
            net_style = "green" if net > 0 else "red"
            table.add_row(
                s.get("buy_exchange", "?"),
                s.get("sell_exchange", "?"),
                f"{float(s.get('spread_bps', 0)):.1f}",
                f"[{net_style}]{net:.1f}[/]",
            )
        return table

    def _build_signal_table(self) -> Table:
        table = Table(title="Recent Signals", expand=True)
        table.add_column("Direction", width=30)
        table.add_column("Spread", width=8, justify="right")
        table.add_column("Conf", width=6, justify="right")

        for s in self._signals[:8]:
            conf = float(s.get("confidence", 0))
            conf_style = "green" if conf >= 0.5 else "yellow"
            table.add_row(
                s.get("direction", "?"),
                f"{float(s.get('spread_bps', 0)):.1f}",
                f"[{conf_style}]{conf:.2f}[/]",
            )
        return table

    def _build_stats_panel(self) -> Panel:
        runtime = time.time() - self._start_time
        hours = runtime / 3600
        lines = [
            f"Runtime: {hours:.1f}h",
            f"Exchanges: {sum(1 for e in self._exchange_status.values() if e['connected'])}/{len(self._exchange_status)}",
            f"Ticks: {self._stats.get('tick_count', 0)}",
            f"Signals: {self._stats.get('signal_count', 0)}",
            f"Warmed up: {self._stats.get('warmed_up', False)}",
        ]
        return Panel("\n".join(lines), title="System Stats")

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=12),
        )

        # Header
        header_text = Text("DEX_total - Multi-Exchange Data Collection & Analysis", style="bold white on blue")
        layout["header"].update(Panel(header_text, style="blue"))

        # Body: exchanges + BBO
        layout["body"].split_row(
            Layout(name="left"),
            Layout(name="right"),
        )
        layout["left"].split_column(
            Layout(self._build_exchange_table(), name="exchanges"),
            Layout(self._build_bbo_table(), name="bbo"),
        )
        layout["right"].split_column(
            Layout(self._build_spread_table(), name="spreads"),
            Layout(self._build_stats_panel(), name="stats"),
        )

        # Footer: signals
        layout["footer"].update(self._build_signal_table())

        return layout

    # --- Lifecycle ---

    async def start(self) -> None:
        """Start the live dashboard."""
        self._running = True
        self._start_time = time.time()
        logger.info("Dashboard started")

    def render_once(self) -> Layout:
        """Render one frame. Call from main loop."""
        return self._build_layout()

    async def stop(self) -> None:
        """Stop the dashboard."""
        self._running = False
        logger.info("Dashboard stopped")

    @property
    def is_running(self) -> bool:
        return self._running
