"""Read immutable Polygon block timestamps without manufacturing source age.

This public JSON-RPC adapter has no order transport or signing capability.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Callable
from math import isfinite
from typing import Protocol

_LOGGER = logging.getLogger(__name__)
_MAX_UNIX_SECONDS = 253402300799


class JsonRpcHttpReader(Protocol):
    """HTTP read transport sharing the application's managed session."""

    async def post_json(
        self,
        url: str,
        *,
        json_body: dict[str, object],
        silent: bool,
    ) -> object:
        """Perform the read-only RPC request without logging its credential-bearing URL."""
        ...


class RpcBlockTimestampReader:
    """Cache verified headers by hash with bounded concurrency and lookup duration."""

    def __init__(
        self,
        client: JsonRpcHttpReader,
        *,
        rpc_url: str,
        cache_capacity: int = 1024,
        timeout_seconds: float = 1.0,
    ) -> None:
        """Inject the managed transport and public chain endpoint.

        Args:
            client: Shared HTTP transport; this adapter does not close it.
            rpc_url: Configured Polygon RPC URL; never included in diagnostics.
            cache_capacity: Maximum immutable block timestamps retained.
            timeout_seconds: Total lookup budget, including concurrency wait.

        Raises:
            ValueError: Configuration is missing or its bounds are invalid.
        """
        if not isinstance(rpc_url, str) or not rpc_url.strip():
            raise ValueError("A block timestamp RPC endpoint is required")
        if (
            isinstance(cache_capacity, bool)
            or not isinstance(cache_capacity, int)
            or cache_capacity < 1
        ):
            raise ValueError("Block timestamp cache capacity must be positive")
        if (
            isinstance(timeout_seconds, bool)
            or not isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Block timestamp lookup duration must be finite and positive")
        self._client = client
        self._rpc_url = rpc_url
        self._capacity = cache_capacity
        self._timeout_seconds = timeout_seconds
        self._cache: OrderedDict[str, int] = OrderedDict()
        self._concurrency = asyncio.Semaphore(8)

    async def timestamp_ms(self, block_hash: str) -> int | None:
        """Return verified source time; failed evidence remains missing.

        Args:
            block_hash: Immutable 32-byte block hash from the source log/receipt.

        Returns:
            Coarse Unix milliseconds, never a local receipt or processing timestamp.
            None means malformed, unavailable, or timed-out evidence. Cancellation
            propagates so the owning detector can shut down deterministically.
        """
        if not isinstance(block_hash, str):
            return None
        identity = block_hash.lower()
        if (
            len(identity) != 66
            or not identity.startswith("0x")
            or any(char not in "0123456789abcdef" for char in identity[2:])
        ):
            return None
        cached = self._cache.get(identity)
        if cached is not None:
            self._cache.move_to_end(identity)
            return cached
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async with self._concurrency:
                    response = await self._client.post_json(
                        self._rpc_url,
                        json_body={
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_getBlockByHash",
                            "params": [identity, False],
                        },
                        silent=True,
                    )
        except Exception as exc:
            _LOGGER.warning("block_timestamp_unavailable", extra={"error_type": type(exc).__name__})
            return None
        value = self._parse(response, identity)
        if value is None:
            return None
        self._cache[identity] = value
        self._cache.move_to_end(identity)
        while len(self._cache) > self._capacity:
            self._cache.popitem(last=False)
        return value

    @staticmethod
    def _parse(response: object, block_hash: str) -> int | None:
        if not isinstance(response, dict) or response.get("error") is not None:
            return None
        result = response.get("result")
        if not isinstance(result, dict) or str(result.get("hash", "")).lower() != block_hash:
            return None
        timestamp = result.get("timestamp")
        if not isinstance(timestamp, str) or not timestamp.startswith("0x"):
            return None
        try:
            seconds = int(timestamp, 16)
        except ValueError:
            return None
        return seconds * 1000 if 0 <= seconds <= _MAX_UNIX_SECONDS else None


class RpcBlockTimestampReaderFactory:
    """Bind the existing HTTP session only when async runtime construction begins."""

    def __init__(self, *, rpc_url: str, client_provider: Callable[[], JsonRpcHttpReader]) -> None:
        """Inject the configured endpoint and application-owned transport provider."""
        self._rpc_url = rpc_url
        self._client_provider = client_provider

    def build(self) -> RpcBlockTimestampReader:
        """Construct one bounded reader without creating a separate session owner."""
        return RpcBlockTimestampReader(self._client_provider(), rpc_url=self._rpc_url)
