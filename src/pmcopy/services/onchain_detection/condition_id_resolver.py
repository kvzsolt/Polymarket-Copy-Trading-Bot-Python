"""Resolve Polymarket condition IDs for CLOB token IDs with bounded LRU and singleflight."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import json
import time
from typing import Any
from urllib.parse import urlencode

from pmcopy.utils.fetchData import fetch_data
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("OnchainConditionIdResolver")

_DEFAULT_CACHE_CAPACITY = 8_192
_DEFAULT_NEGATIVE_TTL_SECONDS = 5.0
_DEFAULT_LOOKUP_TIMEOUT_SECONDS = 2.5


class GammaConditionIdResolver:
    """Bounded LRU + singleflight ``token_id -> condition_id`` resolver."""

    def __init__(
        self,
        *,
        base_url: str = "https://gamma-api.polymarket.com/markets",
        max_entries: int = _DEFAULT_CACHE_CAPACITY,
        negative_ttl_seconds: float = _DEFAULT_NEGATIVE_TTL_SECONDS,
        lookup_timeout_seconds: float = _DEFAULT_LOOKUP_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the bounded condition ID resolver.

        Args:
            base_url: Gamma markets endpoint URL.
            max_entries: Maximum positive token-to-condition mappings cached.
            negative_ttl_seconds: Cooldown window before retrying missing tokens.
            lookup_timeout_seconds: Per-lookup wall-clock deadline in seconds.
        """
        self._base_url = base_url.rstrip("?")
        self._max_entries = max(16, int(max_entries))
        self._negative_ttl_seconds = max(0.0, float(negative_ttl_seconds))
        self._lookup_timeout_seconds = max(0.1, float(lookup_timeout_seconds))
        self._condition_by_token: OrderedDict[str, str] = OrderedDict()
        self._negative_until: OrderedDict[str, float] = OrderedDict()
        self._inflight: dict[str, asyncio.Future[str]] = {}

    async def resolve(self, token_id: str) -> str:
        """Resolve ``token_id`` to ``condition_id`` with bounded cache and singleflight."""
        token = str(token_id or "").strip()
        if not token:
            return ""
        cached = self._condition_by_token.get(token)
        if cached is not None:
            self._condition_by_token.move_to_end(token)
            return cached

        now = time.monotonic()
        neg_expiry = self._negative_until.get(token)
        if neg_expiry is not None:
            if now < neg_expiry:
                return ""
            self._negative_until.pop(token, None)

        existing = self._inflight.get(token)
        if existing is not None:
            return await asyncio.shield(existing)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        self._inflight[token] = fut
        try:
            resolved = await self._fetch_from_gamma(token)
            fut.set_result(resolved)
            return resolved
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._inflight.pop(token, None)

    async def _fetch_from_gamma(self, token: str) -> str:
        query = urlencode((("clob_token_ids", token), ("limit", "1")))
        url = f"{self._base_url}?{query}"
        try:
            async with asyncio.timeout(self._lookup_timeout_seconds):
                payload = await fetch_data(url, silent=True)
        except Exception as exc:
            logger.debug(
                "gamma_condition_id_lookup_failed | token_id=%s error=%s",
                token,
                redact_error_detail(exc),
            )
            self._record_negative(token)
            return ""

        market = _first_market(payload)
        if not market:
            self._record_negative(token)
            return ""
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "").strip()
        if not condition_id:
            self._record_negative(token)
            return ""

        token_ids = _parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids") or market.get("tokens")
        )
        if not token_ids:
            token_ids = [token]
        for market_token in token_ids:
            key = str(market_token)
            self._condition_by_token[key] = condition_id
            self._condition_by_token.move_to_end(key)
            self._negative_until.pop(key, None)
        while len(self._condition_by_token) > self._max_entries:
            self._condition_by_token.popitem(last=False)
        return condition_id

    def _record_negative(self, token: str) -> None:
        if self._negative_ttl_seconds <= 0.0:
            return
        self._negative_until[token] = time.monotonic() + self._negative_ttl_seconds
        self._negative_until.move_to_end(token)
        while len(self._negative_until) > self._max_entries:
            self._negative_until.popitem(last=False)


def _first_market(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        return payload[0] if payload and isinstance(payload[0], dict) else None
    if not isinstance(payload, dict):
        return None
    if "conditionId" in payload or "condition_id" in payload:
        return payload
    for key in ("data", "markets", "results"):
        value = payload.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value[0]
    return None


def _parse_clob_token_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
    if isinstance(value, list):
        tokens: list[str] = []
        for item in value:
            if isinstance(item, dict):
                token = item.get("token_id") or item.get("tokenID") or item.get("id")
            else:
                token = item
            if token:
                tokens.append(str(token))
        return tokens
    return []
