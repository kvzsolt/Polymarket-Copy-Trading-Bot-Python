from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any

import aiohttp


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
    ) -> Any:
        return await self._request_json("GET", url, params=params, headers=headers)

    async def post_json(
        self,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        return await self._request_json("POST", url, json=json_body, headers=headers)

    async def _request_json(self, method: str, url: str, **kwargs) -> Any:
        last_exc = None

        for attempt in range(1, self.retry_policy.attempts + 1):
            started = time.perf_counter()
            try:
                timeout = aiohttp.ClientTimeout(total=self.retry_policy.timeout_seconds)
                async with self.session.request(method, url, timeout=timeout, **kwargs) as resp:
                    payload = await resp.json(content_type=None)
                    latency_ms = round((time.perf_counter() - started) * 1000, 2)
                    if self.logger:
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
                if self.logger:
                    self.logger.warning(
                        "http_request_failed",
                        extra={
                            "method": method,
                            "url": url,
                            "attempt": attempt,
                            "error": repr(exc),
                        },
                    )
                if attempt == self.retry_policy.attempts:
                    raise
                delay = min(
                    self.retry_policy.max_delay,
                    self.retry_policy.base_delay * (2 ** (attempt - 1)),
                )
                delay += random.uniform(0, delay * 0.2)
                await asyncio.sleep(delay)

        raise last_exc


async def build_shared_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15),
        connector=aiohttp.TCPConnector(limit=100, keepalive_timeout=30),
        headers={"Accept": "application/json", "User-Agent": "polymarket-copy-bot/2.0"},
    )
