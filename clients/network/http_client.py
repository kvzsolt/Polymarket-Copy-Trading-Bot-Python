"""Shared bounded HTTP reads and writes with configurable response decoding."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import aiohttp


# aiohttp default is 8190 bytes; Polymarket can serve CSP headers larger than
# that (measured ~8.2 KB), which otherwise surfaces as a client-side HTTP 400.
MAX_HTTP_HEADER_SIZE = 64 * 1024


@dataclass
class HttpRetryPolicy:
    attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 5.0
    timeout_seconds: float = 10.0


class AsyncHttpClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        retry_policy: HttpRetryPolicy | None = None,
        logger=None,
    ):
        self.session = session
        self.retry_policy = retry_policy or HttpRetryPolicy()
        self.logger = logger

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        silent: bool = False,
        json_decoder: Callable[[str], object] | None = None,
    ) -> Any:
        """Fetch JSON, optionally preserving decimal numbers through a decoder."""
        return await self._request_json(
            "GET", url, params=params, headers=headers, silent=silent, json_decoder=json_decoder
        )

    async def post_json(
        self,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        silent: bool = False,
    ) -> Any:
        return await self._request_json("POST", url, json=json_body, headers=headers, silent=silent)

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        silent: bool = False,
        json_decoder: Callable[[str], object] | None = None,
        **kwargs,
    ) -> Any:
        last_exc = None

        for attempt in range(1, self.retry_policy.attempts + 1):
            started = time.perf_counter()
            try:
                timeout = aiohttp.ClientTimeout(total=self.retry_policy.timeout_seconds)
                async with self.session.request(method, url, timeout=timeout, **kwargs) as resp:
                    payload = (
                        await resp.json(content_type=None, loads=json_decoder)
                        if json_decoder is not None
                        else await resp.json(content_type=None)
                    )
                    latency_ms = round((time.perf_counter() - started) * 1000, 2)
                    if self.logger and not silent:
                        self.logger.debug(
                            "http_request_complete",
                            extra={
                                "method": method,
                                "url": url,
                                "status": resp.status,
                                "latency_ms": latency_ms,
                                "attempt": attempt,
                            },
                        )
                    resp.raise_for_status()
                    return payload
            except Exception as exc:  # pragma: no cover - covered via behavior tests
                last_exc = exc
                if self.logger and not silent:
                    self.logger.warning(
                        "http_request_failed",
                        extra={
                            "method": method,
                            "url": url,
                            "attempt": attempt,
                            "error": repr(exc),
                        },
                    )
                # Don't burn the retry budget on terminal 4xx responses.
                # 4xx (except 429 rate-limit) is the venue saying "this
                # request is wrong" — retrying with the same payload
                # produces the same 4xx 4 attempts later, burning
                # ~7 seconds and clouding logs. Status 429 stays in the
                # retry loop because backoff is the prescribed response
                # to rate-limit signals.
                if isinstance(exc, aiohttp.ClientResponseError):
                    status = exc.status
                    if 400 <= status < 500 and status != 429:
                        raise
                if attempt == self.retry_policy.attempts:
                    raise
                delay = min(
                    self.retry_policy.max_delay,
                    self.retry_policy.base_delay * (2 ** (attempt - 1)),
                )
                delay += random.uniform(0, delay * 0.2)
                await asyncio.sleep(delay)

        if last_exc is not None:
            raise last_exc
        # Unreachable in practice (the loop always sets last_exc before the
        # final attempt re-raises), but keep it fail-closed rather than
        # ``raise None`` if the invariant ever breaks.
        raise RuntimeError("http retry loop exhausted without a captured exception")


async def build_shared_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15),
        connector=aiohttp.TCPConnector(limit=100, keepalive_timeout=30),
        headers={"Accept": "application/json", "User-Agent": "polymarket-copy-bot/2.0"},
        max_line_size=MAX_HTTP_HEADER_SIZE,
        max_field_size=MAX_HTTP_HEADER_SIZE,
    )
