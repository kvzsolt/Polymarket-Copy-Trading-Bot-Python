"""Thin ABC-conforming wrapper around ``network.http_client.AsyncHttpClient``.

The wrapper exposes a narrow, service-facing surface (``get_json`` /
``post_json``) that satisfies ``AbstractHttpClient``. Retry/backoff and
timeout policy remain the concern of the underlying ``AsyncHttpClient``.
"""

from __future__ import annotations

from typing import Any

from domain.ports.clients import AbstractHttpClient
from clients.network.http_client import AsyncHttpClient


class HttpClient(AbstractHttpClient):
    def __init__(self, client: AsyncHttpClient):
        self._client = client

    async def get_json(self, url: str, *, silent: bool = False, **kwargs: Any) -> Any:
        return await self._client.get_json(url, silent=silent, **kwargs)

    async def post_json(self, url: str, *, silent: bool = False, **kwargs: Any) -> Any:
        return await self._client.post_json(url, silent=silent, **kwargs)
