"""Async, paginated and fail-closed client for the Falcon API."""

from __future__ import annotations

import asyncio
import inspect
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping

from pmcopy.integrations.falcon.errors import (
    FalconAuthenticationError,
    FalconBadRequestError,
    FalconConfigurationError,
    FalconPermissionError,
    FalconProtocolError,
    FalconRateLimitError,
    FalconUnavailableError,
)

_BASE_URL = "https://retriever.falconapi.net/api/v2/semantic/retrieve/parameterized"
_RETRYABLE_STATUS = {429, 500, 503}


@dataclass(frozen=True, slots=True)
class FalconPage:
    """Describe one sanitized page returned by the analytics provider."""

    results: tuple[dict[str, Any], ...]
    limit: int
    offset: int
    has_more: bool
    request_id: str | None
    timestamp: str | None
    raw: dict[str, Any]


def _secret(value: Any) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        value = getter()
    return str(value or "").strip()


def _param(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, datetime):
        return str(int(value.timestamp()))
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _pnl_boundary(value: Any) -> str | None:
    """Normalize PnL boundaries to the documented UTC ``YYYY-MM-DD`` form."""

    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float, Decimal)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).date().isoformat()
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return raw


def _normalize_params(params: Mapping[str, Any] | None) -> dict[str, str]:
    return {str(key): _param(value) for key, value in (params or {}).items() if value is not None}


class FalconClient:
    """One reusable transport for all documented Falcon agent IDs."""

    def __init__(
        self,
        *,
        api_key: Any,
        http_client: Any | None = None,
        base_url: str = _BASE_URL,
        timeout_seconds: float = 10.0,
        max_attempts: int = 4,
        backoff_base_seconds: float = 0.25,
        max_backoff_seconds: float = 30.0,
        jitter_seconds: float = 0.10,
        max_pages: int = 100,
        max_rows: int = 20_000,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_fn: Callable[[], float] = random.random,
    ) -> None:
        self._api_key = _secret(api_key)
        if not self._api_key:
            raise FalconConfigurationError("Falcon API key is required")
        if max_attempts < 1:
            raise FalconConfigurationError("max_attempts must be positive")
        if max_pages < 1 or max_rows < 1:
            raise FalconConfigurationError("pagination limits must be positive")
        if max_backoff_seconds <= 0:
            raise FalconConfigurationError("max_backoff_seconds must be positive")
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts
        self._backoff_base_seconds = backoff_base_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._jitter_seconds = jitter_seconds
        self._max_pages = max_pages
        self._max_rows = max_rows
        self._sleep = sleep
        self._random = random_fn
        self._closed = False
        self._owns_http = http_client is None
        if http_client is None:
            try:
                import httpx
            except ImportError as exc:  # pragma: no cover - repository ships httpx
                raise FalconConfigurationError("httpx is required for Falcon") from exc
            http_client = httpx.AsyncClient(timeout=timeout_seconds)
        self._http = http_client

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(base_url={self._base_url!r}, "
            f"timeout_seconds={self._timeout_seconds!r})"
        )

    async def close(self) -> None:
        """Close the injected provider or venue client."""
        if self._closed:
            return
        self._closed = True
        if self._owns_http:
            close = getattr(self._http, "aclose", None)
            if callable(close):
                close_result = close()
                if inspect.isawaitable(close_result):
                    await close_result

    async def __aenter__(self) -> "FalconClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def request(
        self,
        agent_id: int,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> FalconPage:
        """Send a validated request through the analytics provider adapter."""
        if self._closed:
            raise FalconConfigurationError("Falcon client is closed")
        if not 1 <= limit <= 200:
            raise FalconConfigurationError("limit must be between 1 and 200")
        if offset < 0:
            raise FalconConfigurationError("offset must not be negative")
        body = {
            "agent_id": int(agent_id),
            "params": _normalize_params(params),
            "pagination": {"limit": int(limit), "offset": int(offset)},
            "formatter_config": {"format_type": "raw"},
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        last_status: int | None = None
        for attempt in range(self._max_attempts):
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    response = await self._http.post(
                        self._base_url,
                        headers=headers,
                        json=body,
                    )
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                raise
            except Exception as exc:
                if attempt + 1 >= self._max_attempts:
                    raise FalconUnavailableError(
                        f"Falcon transport failed: {type(exc).__name__}"
                    ) from exc
                await self._backoff(attempt)
                continue

            status = int(getattr(response, "status_code", 0) or 0)
            last_status = status
            request_id = self._response_request_id(response)
            if 200 <= status < 300:
                return self._parse_page(response, requested_limit=limit, requested_offset=offset)
            if status == 400 or status == 404:
                raise FalconBadRequestError(
                    f"Falcon request rejected ({status})",
                    request_id=request_id,
                )
            if status == 401:
                raise FalconAuthenticationError(
                    "Falcon authentication rejected",
                    request_id=request_id,
                )
            if status == 403:
                raise FalconPermissionError(
                    "Falcon permission denied (403); verify plan access for the requested agent",
                    agent_id=body["agent_id"],
                )
            if status in _RETRYABLE_STATUS:
                if attempt + 1 < self._max_attempts:
                    await self._backoff(attempt, response=response)
                    continue
                if status == 429:
                    raise FalconRateLimitError(
                        "Falcon rate limit exceeded",
                        request_id=request_id,
                    )
                raise FalconUnavailableError(
                    f"Falcon server unavailable ({status})",
                    request_id=request_id,
                )
            if status >= 500:
                raise FalconUnavailableError(
                    f"Falcon server error ({status})",
                    request_id=request_id,
                )
            raise FalconBadRequestError(
                f"Falcon request rejected ({status})",
                request_id=request_id,
            )

        raise FalconUnavailableError(f"Falcon request failed ({last_status or 'unknown'})")

    async def _backoff(self, attempt: int, *, response: Any | None = None) -> None:
        delay = self._retry_after_seconds(response)
        if delay is None:
            delay = min(
                self._max_backoff_seconds,
                self._backoff_base_seconds * (2**attempt),
            )
        if self._jitter_seconds:
            delay = min(
                self._max_backoff_seconds,
                delay + self._random() * self._jitter_seconds,
            )
        await self._sleep(delay)

    @staticmethod
    def _response_header(response: Any, name: str) -> str | None:
        headers = getattr(response, "headers", None)
        if not isinstance(headers, Mapping):
            return None
        target = name.lower()
        for key, value in headers.items():
            if str(key).lower() == target and str(value).strip():
                return str(value).strip()
        return None

    def _retry_after_seconds(self, response: Any | None) -> float | None:
        if response is None:
            return None
        raw = self._response_header(response, "retry-after")
        if raw is None:
            raw = self._response_header(response, "x-ratelimit-reset")
        try:
            delay = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None
        if delay is None or not math.isfinite(delay) or delay <= 0:
            return None
        return min(delay, self._max_backoff_seconds)

    @classmethod
    def _response_request_id(cls, response: Any) -> str | None:
        return cls._response_header(response, "x-request-id")

    @staticmethod
    def _parse_page(response: Any, *, requested_limit: int, requested_offset: int) -> FalconPage:
        try:
            payload = response.json()
        except Exception as exc:
            raise FalconProtocolError("Falcon returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise FalconProtocolError("Falcon response root must be an object")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise FalconProtocolError("Falcon response omitted data object")
        results = data.get("results")
        if not isinstance(results, list):
            raise FalconProtocolError("Falcon data.results must be a list")
        if any(not isinstance(row, dict) for row in results):
            raise FalconProtocolError("Falcon results must contain objects")
        pagination = payload.get("pagination")
        if pagination is None:
            pagination = {}
        if not isinstance(pagination, dict):
            raise FalconProtocolError("Falcon pagination must be an object")
        try:
            limit = int(pagination.get("limit", requested_limit))
            offset = int(pagination.get("offset", requested_offset))
        except (TypeError, ValueError) as exc:
            raise FalconProtocolError("Falcon pagination is invalid") from exc
        has_more_raw = pagination.get("has_more", False)
        if isinstance(has_more_raw, bool):
            has_more = has_more_raw
        else:
            has_more = str(has_more_raw).strip().lower() in {"1", "true", "yes"}
        request_id = (
            str(payload.get("request_id") or "") or FalconClient._response_request_id(response)
        ) or None
        timestamp = str(payload.get("timestamp") or "") or None
        return FalconPage(
            results=tuple(dict(row) for row in results),
            limit=limit,
            offset=offset,
            has_more=has_more,
            request_id=request_id,
            timestamp=timestamp,
            raw=payload,
        )

    async def fetch_all(
        self,
        agent_id: int,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        max_pages: int | None = None,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all bounded provider pages with duplicate elimination."""
        page_cap = max_pages or self._max_pages
        row_cap = max_rows or self._max_rows
        if page_cap < 1 or row_cap < 1:
            raise FalconConfigurationError("pagination limits must be positive")
        rows: list[dict[str, Any]] = []
        offset = 0
        seen_offsets: set[int] = set()
        for _ in range(page_cap):
            if offset in seen_offsets:
                raise FalconProtocolError("Falcon pagination repeated an offset")
            seen_offsets.add(offset)
            page = await self.request(
                agent_id,
                params=params,
                limit=page_size,
                offset=offset,
            )
            remaining = row_cap - len(rows)
            if remaining <= 0:
                return rows
            rows.extend(page.results[:remaining])
            # ``max_rows`` is a caller-requested result bound, not a protocol
            # integrity limit.  A final page can legitimately cross the bound;
            # truncate deterministically instead of turning a successful query
            # into a vendor outage.
            if len(rows) >= row_cap:
                return rows
            if not page.has_more:
                return rows
            step = page.limit if page.limit > 0 else len(page.results)
            if step <= 0:
                raise FalconProtocolError("Falcon pagination made no progress")
            offset = page.offset + step
        raise FalconProtocolError("Falcon pagination exceeded max_pages")

    async def leaderboard(self, *, limit: int = 50, **params: Any) -> list[dict[str, Any]]:
        """Return the current provider wallet leaderboard."""
        return await self.fetch_all(584, params=params, page_size=min(limit, 200), max_rows=limit)

    async def wallet_360(
        self,
        proxy_wallet: str,
        window_days: int,
        *,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return Wallet360 evidence for a wallet and time window."""
        if window_days not in {1, 3, 7, 15}:
            raise FalconConfigurationError("Wallet360 window_days must be 1, 3, 7 or 15")
        return await self.fetch_all(
            581,
            params={"proxy_wallet": proxy_wallet, "window_days": window_days},
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def pnl_leaderboard(
        self,
        *,
        wallet_address: str = "ALL",
        leaderboard_period: str = "7d",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return the provider PnL leaderboard."""
        if leaderboard_period not in {"1d", "3d", "7d", "30d"}:
            raise FalconConfigurationError("leaderboard_period must be 1d, 3d, 7d or 30d")
        return await self.fetch_all(
            579,
            params={
                "wallet_address": wallet_address,
                "leaderboard_period": leaderboard_period,
            },
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def wallet_stats_all_time(
        self,
        wallet_address: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Return all-time statistics for a wallet."""
        return await self.fetch_all(
            586,
            params={"wallet_address": wallet_address},
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def trades(
        self,
        *,
        proxy_wallet: str | None = None,
        condition_id: str | None = None,
        market_slug: str | None = None,
        start_time: int | float | str | date | datetime | None = None,
        end_time: int | float | str | date | datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return sanitized provider trade evidence."""
        return await self.fetch_all(
            556,
            params={
                "proxy_wallet": proxy_wallet,
                "condition_id": condition_id,
                "market_slug": market_slug,
                "start_time": start_time,
                "end_time": end_time,
            },
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def markets(
        self,
        *,
        min_volume: Any = None,
        condition_id: str | None = None,
        market_slug: str | None = None,
        event_slug: str | None = None,
        end_date_min: int | datetime | None = None,
        end_date_max: int | datetime | None = None,
        closed: bool | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return sanitized provider market evidence."""
        return await self.fetch_all(
            574,
            params={
                "min_volume": min_volume,
                "condition_id": condition_id,
                "market_slug": market_slug,
                "event_slug": event_slug,
                "end_date_min": end_date_min,
                "end_date_max": end_date_max,
                "closed": closed,
            },
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def market_360(
        self,
        *,
        condition_id: str = "ALL",
        min_volume_24h: Any = None,
        min_liquidity_percentile: Any = None,
        volume_trend: str | None = None,
        min_top1_wallet_pct: Any = None,
        max_unique_traders_7d: Any = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return Market360 evidence for a market."""
        return await self.fetch_all(
            575,
            params={
                "condition_id": condition_id,
                "min_volume_24h": min_volume_24h,
                "min_liquidity_percentile": min_liquidity_percentile,
                "volume_trend": volume_trend,
                "min_top1_wallet_pct": min_top1_wallet_pct,
                "max_unique_traders_7d": max_unique_traders_7d,
            },
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def pnl(
        self,
        *,
        wallet: str,
        granularity: str = "1m",
        start_time: int | float | str | date | datetime | None = None,
        end_time: int | float | str | date | datetime | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return sanitized provider PnL evidence."""
        return await self.fetch_all(
            569,
            params={
                "wallet": wallet,
                "granularity": granularity,
                "start_time": _pnl_boundary(start_time),
                "end_time": _pnl_boundary(end_time),
            },
            page_size=min(limit, 200),
            max_rows=limit,
        )

    async def price_jumps(
        self,
        *,
        token_id: str,
        resolution: str = "5m",
        min_change_pct: Any = None,
        lookback_hours: int = 24,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return sanitized price-jump evidence."""
        return await self.fetch_all(
            596,
            params={
                "token_id": token_id,
                "resolution": resolution,
                "min_change_pct": min_change_pct,
                "lookback_hours": lookback_hours,
            },
            page_size=min(limit, 200),
            max_rows=limit,
        )


__all__ = ["FalconClient", "FalconPage"]
