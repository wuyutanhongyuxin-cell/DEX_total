"""
Telegram notification system for DEX_total.
Ported from grvt_lighter/helpers/telegram.py with multi-exchange support.

Key patterns:
- Lazy aiohttp session creation
- Non-blocking send with timeout
- Graceful degradation if disabled
- Markdown formatting
"""

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

SEND_TIMEOUT = 10  # seconds


class TelegramNotifier:
    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.bot_token and self.chat_id)
        self._session: Optional[aiohttp.ClientSession] = None
        self._closed = False  # shutdown 后禁止再发送
        if not self.enabled:
            logger.info("Telegram notifications disabled (no token/chat_id)")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send(self, text: str) -> None:
        if not self.enabled or self._closed:
            return
        # 去除 Markdown 特殊字符避免 parse entities 错误: 改用纯文本
        text = text.replace("*", "").replace("_", "-")
        try:
            session = await self._get_session()
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                # parse_mode 已移除 — 使用纯文本避免 Markdown 转义问题
                "disable_web_page_preview": True,
            }
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=SEND_TIMEOUT)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Telegram send failed: {resp.status} {body}")
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")

    # ─── Notification Methods ────────────────────────────────

    async def notify_start(self, exchanges: list[str], symbols: list[str]) -> None:
        n = len(exchanges)
        ex_list = ", ".join(exchanges[:6])
        if n > 6:
            ex_list += f" +{n-6} more"
        await self._send(
            f"*DEX\\_total Started*\n"
            f"Exchanges: {n} ({ex_list})\n"
            f"Symbols: {', '.join(symbols)}"
        )

    async def notify_stop(self, reason: str, runtime_hours: float) -> None:
        await self._send(
            f"*DEX\\_total Stopped*\n"
            f"Reason: {reason}\n"
            f"Runtime: {runtime_hours:.1f}h"
        )

    async def notify_collector_status(self, name: str, status: str, detail: str = "") -> None:
        emoji = "OK" if status == "connected" else "WARN"
        msg = f"*[{emoji}] {name}*: {status}"
        if detail:
            msg += f"\n{detail}"
        await self._send(msg)

    async def notify_signal(self, signal: dict) -> None:
        await self._send(
            f"*Signal*\n"
            f"Pair: {signal.get('buy_exchange', '?')} -> {signal.get('sell_exchange', '?')}\n"
            f"Symbol: {signal.get('symbol', '?')}\n"
            f"Spread: {signal.get('spread_bps', 0):.1f} bps\n"
            f"Confidence: {signal.get('confidence', 0):.2f}\n"
            f"Direction: {signal.get('direction', '?')}"
        )

    async def notify_heartbeat(
        self,
        runtime_hours: float,
        active_exchanges: int,
        total_exchanges: int,
        signals_count: int,
        top_spread: Optional[dict] = None,
    ) -> None:
        msg = (
            f"*Heartbeat*\n"
            f"Runtime: {runtime_hours:.1f}h\n"
            f"Exchanges: {active_exchanges}/{total_exchanges} active\n"
            f"Signals: {signals_count}"
        )
        if top_spread:
            msg += (
                f"\nTop spread: {top_spread.get('pair', '?')} "
                f"{top_spread.get('spread_bps', 0):.1f} bps"
            )
        await self._send(msg)

    async def notify_error(self, source: str, error: str) -> None:
        await self._send(f"*ERROR [{source}]*\n{error}")

    async def notify_ws_stale(self, exchange: str, seconds_since: float) -> None:
        await self._send(
            f"*WS Stale [{exchange}]*\n"
            f"No update for {seconds_since:.0f}s"
        )

    # ─── Cleanup ─────────────────────────────────────────────

    async def close(self) -> None:
        self._closed = True  # 阻止 close 后再发送
        if self._session and not self._session.closed:
            await self._session.close()
