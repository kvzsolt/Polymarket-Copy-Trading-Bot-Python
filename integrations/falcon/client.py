"""Pooled async client for the Heisenberg parameterized-agent endpoint."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import IntEnum
from typing import Any

import httpx
import orjson


class Agent(IntEnum):
    MARKETS = 574
    TRADES = 556
    CANDLES = 568
    ORDERBOOK = 572
    PNL = 569
    LEADERBOARD = 579
    HSCORE = 584
    WALLET360 = 581
    MARKET_INSIGHTS = 575
    KALSHI_MARKETS = 565
    KALSHI_TRADES = 573
    SOCIAL = 585


_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
)


@dataclass(frozen=True, slots=True)
class FalconPage:
    results: tuple[dict[str, Any], ...]
    returned: int


class TokenBucket:
    """Monotonic token bucket that never sleeps while holding its lock."""

    def __init__(self, rate: float, burst: float | None = None) -> None:
        if rate <= 0:
            raise ValueError("token bucket rate must be positive")
        capacity = float(burst if burst is not None else max(1.0, rate))
        if capacity < 1:
            raise ValueError("token bucket burst must hold at least one request")
        self.rate = float(rate)
        self.capacity = capacity
        self._tokens = capacity
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._updated_at)
                self._tokens = min(
                    self.capacity,
                    self._tokens + elapsed * self.rate,
                )
                self._updated_at = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                delay = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(delay)


class FalconClient:
    """One pooled HTTP/2 client shared by every Heisenberg agent call."""

    def __init__(self, config: Any, *, client: httpx.AsyncClient | None = None) -> None:
        self._url = str(config.base_url).rstrip("/")
        self._max_retries = int(config.max_retries)
        self._retry_base = float(config.retry_base_delay_seconds)
        self._retry_max = float(config.retry_max_delay_seconds)
        self._retry_jitter = float(config.retry_jitter_ratio)
        self._bucket = TokenBucket(config.requests_per_second, config.burst)
        self._closed = False
        self._owns_client = client is None
        token = (
            config.token.get_secret_value()
            if hasattr(config.token, "get_secret_value")
            else str(config.token)
        )
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "pmcopy-discovery/1",
        }
        self._client = client or httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(
                connect=3.0,
                read=float(config.timeout_seconds),
                write=5.0,
                pool=3.0,
            ),
            limits=httpx.Limits(
                max_connections=int(config.max_connections),
                max_keepalive_connections=int(config.max_keepalive_connections),
                keepalive_expiry=float(config.keepalive_expiry_seconds),
            ),
            headers=self._headers,
        )

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()

    async def close(self) -> None:
        """Idempotent lifecycle alias used by the application owner."""
        await self.aclose()

    async def __aenter__(self) -> "FalconClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    @staticmethod
    def build_body(
        agent_id: int,
        params: Mapping[str, str],
        *,
        limit: int,
        offset: int,
    ) -> bytes:
        non_strings = [key for key, value in params.items() if not isinstance(value, str)]
        if non_strings:
            raise TypeError(
                "all Heisenberg params values must be strings; invalid keys: "
                + ", ".join(sorted(non_strings))
            )
        if not 1 <= int(limit) <= 200:
            raise ValueError("Heisenberg pagination limit must be in [1, 200]")
        if int(offset) < 0:
            raise ValueError("Heisenberg pagination offset must be non-negative")
        return orjson.dumps(
            {
                "agent_id": int(agent_id),
                "params": dict(params),
                "pagination": {"limit": int(limit), "offset": int(offset)},
                "formatter_config": {"format_type": "raw"},
            }
        )

    async def query(
        self,
        *,
        agent_id: int,
        params: Mapping[str, str],
        limit: int = 100,
        offset: int = 0,
    ) -> FalconPage:
        if self._closed:
            raise RuntimeError("FalconClient is closed")
        bounded_limit = min(int(limit), 200)
        body = self.build_body(
            agent_id,
            params,
            limit=bounded_limit,
            offset=offset,
        )
        delay = self._retry_base
        for attempt in range(1, self._max_retries + 1):
            await self._bucket.acquire()
            try:
                response = await self._client.post(
                    self._url,
                    content=body,
                    headers=self._headers,
                )
            except _RETRYABLE_EXCEPTIONS:
                if attempt >= self._max_retries:
                    raise
                await asyncio.sleep(_jitter(delay, self._retry_jitter))
                delay = min(delay * 2.0, self._retry_max)
                continue

            if response.status_code in _RETRYABLE_STATUS and attempt < self._max_retries:
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                await asyncio.sleep(
                    min(retry_after, self._retry_max)
                    if retry_after is not None
                    else _jitter(delay, self._retry_jitter)
                )
                delay = min(delay * 2.0, self._retry_max)
                continue

            response.raise_for_status()
            payload = orjson.loads(response.content)
            if not isinstance(payload, dict):
                raise ValueError("Heisenberg response root must be an object")
            data = payload.get("data") or payload
            if isinstance(data, list):
                raw_results = data
            elif isinstance(data, dict):
                raw_results = (
                    data.get("results")
                    or data.get("records")
                    or data.get("items")
                    or payload.get("results")
                    or payload.get("records")
                    or []
                )
            else:
                raise ValueError("Heisenberg response data must be an object or list")
            if not isinstance(raw_results, list):
                raise ValueError("Heisenberg results must be a list")
            results = tuple(row for row in raw_results if isinstance(row, dict))
            return FalconPage(results=results, returned=len(results))

        raise RuntimeError(f"Heisenberg agent {int(agent_id)} exhausted retries")

    async def paginate(
        self,
        *,
        agent_id: int,
        params: Mapping[str, str],
        page_size: int = 100,
        max_pages: int = 10,
    ) -> AsyncIterator[dict[str, Any]]:
        limit = min(max(1, int(page_size)), 200)
        offset = 0
        for _ in range(max(1, int(max_pages))):
            page = await self.query(
                agent_id=agent_id,
                params=params,
                limit=limit,
                offset=offset,
            )
            for row in page.results:
                yield row
            if page.returned < limit:
                break
            offset += limit


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())


def _jitter(delay: float, ratio: float) -> float:
    return delay * random.uniform(max(0.0, 1.0 - ratio), 1.0 + ratio)


def row_value(row: Mapping[str, Any], *aliases: str, default: Any = None) -> Any:
    """Return the first populated alias from a raw agent row."""
    for alias in aliases:
        value = row.get(alias)
        if value is not None and value != "":
            return value
    return default


async def hscore_rows(client: FalconClient, config: Any) -> list[dict[str, Any]]:
    async def _fetch_mode(sort_by: str) -> list[dict[str, Any]]:
        params = {
            "min_win_rate_15d": str(config.min_win_rate_15d),
            "max_win_rate_15d": str(config.max_win_rate_15d),
            "min_roi_15d": str(config.min_roi_15d),
            "min_pnl_15d": str(config.min_pnl_15d),
            "min_total_trades_15d": str(config.min_total_trades_15d),
            "max_total_trades_15d": str(config.max_total_trades_15d),
            "sort_by": str(sort_by),
        }
        return [
            row
            async for row in client.paginate(
                agent_id=Agent.HSCORE,
                params=params,
                page_size=config.page_size,
                max_pages=config.max_pages,
            )
        ]

    # The shared token bucket remains the global rate authority; scheduling
    # sort modes together lets pooled HTTP/2 connections overlap network wait.
    modes = await asyncio.gather(
        *(_fetch_mode(str(sort_by)) for sort_by in config.sort_modes)
    )

    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    max_length = max((len(rows) for rows in modes), default=0)
    for index in range(max_length):
        for rows in modes:
            if index >= len(rows):
                continue
            row = rows[index]
            wallet = str(
                row_value(row, "proxy_wallet", "proxyWallet", "wallet", "address", default="")
            ).lower()
            if wallet in seen:
                continue
            seen.add(wallet)
            output.append(row)
            if len(output) >= config.candidate_pool_size:
                return output
    return output


async def wallet360(client: FalconClient, wallet: str, window_days: str) -> dict[str, Any]:
    page = await client.query(
        agent_id=Agent.WALLET360,
        params={"proxy_wallet": wallet.lower(), "window_days": str(window_days)},
        limit=1,
    )
    return dict(page.results[0]) if page.results else {}


async def wallet_trades(
    client: FalconClient,
    wallet: str,
    *,
    start_time: int,
    end_time: int,
    page_size: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for side in ("BUY", "SELL"):
        params = {
            "wallet_proxy": wallet.lower(),
            "condition_id": "ALL",
            "side": side,
            "start_time": str(int(start_time)),
            "end_time": str(int(end_time)),
        }
        async for row in client.paginate(
            agent_id=Agent.TRADES,
            params=params,
            page_size=page_size,
            max_pages=max_pages,
        ):
            identity = (
                str(row_value(row, "transaction_hash", "tx_hash", default="")),
                str(row_value(row, "trade_id", "id", default="")),
                side,
            )
            if identity in seen:
                continue
            seen.add(identity)
            output.append(row)
    return output


async def market_insights(
    client: FalconClient,
    condition_id: str,
    config: Any,
) -> dict[str, Any]:
    page = await client.query(
        agent_id=Agent.MARKET_INSIGHTS,
        params={
            "condition_id": condition_id,
            "min_volume_24h": str(config.min_volume_24h),
            "min_liquidity_percentile": str(config.min_liquidity_percentile),
            "volume_trend": str(config.volume_trend),
        },
        limit=1,
    )
    return dict(page.results[0]) if page.results else {}


async def social_pulse(
    client: FalconClient,
    keywords: tuple[str, ...],
    hours_back: int,
) -> dict[str, Any]:
    formatted = "{" + ",".join(keyword.strip() for keyword in keywords) + "}"
    page = await client.query(
        agent_id=Agent.SOCIAL,
        params={"keywords": formatted, "hours_back": str(int(hours_back))},
        limit=50,
    )
    return dict(page.results[0]) if page.results else {}


async def realized_pnl(
    client: FalconClient,
    wallet: str,
    *,
    start_date: str,
    end_date: str,
    page_size: int = 200,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    """Fetch realized-only PnL; agent 569 requires YYYY-MM-DD values."""
    params = {
        "proxy_wallet": wallet.lower(),
        "start_time": start_date,
        "end_time": end_date,
    }
    return [
        row
        async for row in client.paginate(
            agent_id=Agent.PNL,
            params=params,
            page_size=page_size,
            max_pages=max_pages,
        )
    ]


async def leaderboard_history(
    client: FalconClient,
    wallet: str,
    windows: tuple[int, ...] = (1, 3, 7, 30),
) -> list[tuple[int, dict[str, Any]]]:
    output: list[tuple[int, dict[str, Any]]] = []
    for window in windows:
        page = await client.query(
            agent_id=Agent.LEADERBOARD,
            params={
                "proxy_wallet": wallet.lower(),
                "window_days": str(int(window)),
            },
            limit=1,
        )
        if page.results:
            output.append((window, dict(page.results[0])))
    return output


__all__ = [
    "Agent",
    "FalconClient",
    "FalconPage",
    "TokenBucket",
    "hscore_rows",
    "leaderboard_history",
    "market_insights",
    "row_value",
    "realized_pnl",
    "social_pulse",
    "wallet360",
    "wallet_trades",
]
