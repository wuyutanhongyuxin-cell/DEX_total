"""
IPC Bridge — abstraction layer for inter-process communication.

Designed for future Rust migration where hot-path modules (spread computation,
signal scoring) run in a Rust process and communicate via IPC.

Current implementation: InProcessBridge (same-process, zero-copy).
Future implementations: UDSBridge (Unix domain socket), SharedMemoryBridge.

When migrating to Rust:
1. Implement Rust side of the chosen IPC protocol
2. Switch from InProcessBridge to UDSBridge or SharedMemoryBridge
3. Python side serializes dataclasses → bytes → IPC → Rust
4. Rust side deserializes → compute → serialize → IPC → Python
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class IPCBridge(ABC):
    """Abstract IPC bridge for cross-process communication.

    All hot-path data flows through this bridge:
    - BBO updates → spread computation
    - Spread matrix → signal evaluation
    - Signal output → execution routing
    """

    @abstractmethod
    async def send(self, channel: str, data: dict) -> None:
        """Send data to a named channel."""
        ...

    @abstractmethod
    async def receive(self, channel: str) -> Optional[dict]:
        """Receive data from a named channel (non-blocking)."""
        ...

    @abstractmethod
    async def request(self, channel: str, data: dict) -> dict:
        """Send request and wait for response (RPC pattern)."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
        ...


class InProcessBridge(IPCBridge):
    """Same-process bridge — zero overhead, for current Python-only mode.

    Routes calls directly to registered handlers without serialization.
    This is the default until Rust modules are ready.
    """

    def __init__(self):
        self._handlers: dict[str, Callable] = {}
        self._queues: dict[str, list[dict]] = {}

    def register_handler(self, channel: str, handler: Callable) -> None:
        """Register a handler for incoming messages on a channel."""
        self._handlers[channel] = handler

    async def send(self, channel: str, data: dict) -> None:
        """Direct in-process delivery to handler or queue."""
        handler = self._handlers.get(channel)
        if handler:
            handler(data)
        else:
            if channel not in self._queues:
                self._queues[channel] = []
            self._queues[channel].append(data)

    async def receive(self, channel: str) -> Optional[dict]:
        """Pop from in-process queue."""
        queue = self._queues.get(channel)
        if queue:
            return queue.pop(0)
        return None

    async def request(self, channel: str, data: dict) -> dict:
        """Direct function call — zero serialization overhead."""
        handler = self._handlers.get(channel)
        if handler is None:
            raise RuntimeError(f"No handler registered for channel '{channel}'")
        result = handler(data)
        return result if isinstance(result, dict) else {"result": result}

    async def close(self) -> None:
        self._handlers.clear()
        self._queues.clear()


class UDSBridge(IPCBridge):
    """Unix Domain Socket bridge — placeholder for Rust IPC.

    Not implemented yet. When Rust modules are ready:
    1. Rust process listens on a UDS path
    2. Python sends JSON-serialized dataclasses
    3. Rust deserializes, computes, returns JSON
    """

    def __init__(self, socket_path: str = "/tmp/dex_total.sock"):
        self._socket_path = socket_path
        logger.warning("UDSBridge is a placeholder — not yet implemented")

    async def send(self, channel: str, data: dict) -> None:
        raise NotImplementedError("UDSBridge: Rust process not yet available")

    async def receive(self, channel: str) -> Optional[dict]:
        raise NotImplementedError("UDSBridge: Rust process not yet available")

    async def request(self, channel: str, data: dict) -> dict:
        raise NotImplementedError("UDSBridge: Rust process not yet available")

    async def close(self) -> None:
        pass


class SharedMemoryBridge(IPCBridge):
    """Shared memory bridge — placeholder for ultra-low-latency Rust IPC.

    Uses multiprocessing.shared_memory for zero-copy data transfer.
    Not implemented yet.
    """

    def __init__(self, shm_name: str = "dex_total_shm"):
        self._shm_name = shm_name
        logger.warning("SharedMemoryBridge is a placeholder — not yet implemented")

    async def send(self, channel: str, data: dict) -> None:
        raise NotImplementedError("SharedMemoryBridge: not yet implemented")

    async def receive(self, channel: str) -> Optional[dict]:
        raise NotImplementedError("SharedMemoryBridge: not yet implemented")

    async def request(self, channel: str, data: dict) -> dict:
        raise NotImplementedError("SharedMemoryBridge: not yet implemented")

    async def close(self) -> None:
        pass


def create_bridge(mode: str = "in_process", **kwargs) -> IPCBridge:
    """Factory for IPC bridges.

    Args:
        mode: "in_process" (default), "uds", or "shared_memory"
    """
    if mode == "in_process":
        return InProcessBridge()
    elif mode == "uds":
        return UDSBridge(**kwargs)
    elif mode == "shared_memory":
        return SharedMemoryBridge(**kwargs)
    else:
        raise ValueError(f"Unknown IPC bridge mode: {mode}")
