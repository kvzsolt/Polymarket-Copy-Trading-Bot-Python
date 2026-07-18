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

from utils.logger import get_logger, redact_error_detail

logger = get_logger("FalconClient")


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
_MIN_PAGE_SIZE = 10
_MAX_PAGE_SIZE = 200
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


@dataclass(frozen=True, slots=True)
class HScoreShortlist:
    """Shortlist rows plus the sort modes that failed to contribute to them.

    ``degraded`` marks the row set as *incomplete* rather than merely small.
    The surviving modes still carry usable candidates, so callers should keep
    scoring them — but absence from ``rows`` no longer means a wallet stopped
    qualifying, only that nobody looked.
    """

    rows: tuple[dict[str, Any], ...]
    failed_sort_modes: tuple[str, ...] = ()

    @property
    def degraded(self) -> bool:
        return bool(self.failed_sort_modes)


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
        # A 429 applies to the API credential, not just the coroutine that
        # happened to receive it.  Coalesce concurrent retry delays so a
        # discovery fan-out cannot stampede Falcon again immediately.
        self._cooldown_until = 0.0
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
        if not _MIN_PAGE_SIZE <= int(limit) <= _MAX_PAGE_SIZE:
            raise ValueError(
                "Heisenberg pagination limit must be in "
                f"[{_MIN_PAGE_SIZE}, {_MAX_PAGE_SIZE}]"
            )
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
        # Falcon's generic docs say pagination starts at one, but the live
        # parameterized endpoint rejects values below ten (including for
        # Wallet360 and Market Insights) with a misleading HTTP 404.
        bounded_limit = min(max(_MIN_PAGE_SIZE, int(limit)), _MAX_PAGE_SIZE)
        body = self.build_body(
            agent_id,
            params,
            limit=bounded_limit,
            offset=offset,
        )
        delay = self._retry_base
        for attempt in range(1, self._max_retries + 1):
            await self._wait_for_shared_cooldown()
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

            if response.status_code == 429:
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                await self._extend_shared_cooldown(
                    min(retry_after, self._retry_max)
                    if retry_after is not None
                    else _jitter(delay, self._retry_jitter)
                )
                if attempt >= self._max_retries:
                    response.raise_for_status()
                delay = min(delay * 2.0, self._retry_max)
                continue

            if response.status_code in _RETRYABLE_STATUS and attempt < self._max_retries:
                await asyncio.sleep(_jitter(delay, self._retry_jitter))
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

    async def _extend_shared_cooldown(self, delay: float) -> None:
        # Event-loop tasks cannot interleave between this read and assignment,
        # so concurrent 429 responses safely extend one common deadline.
        self._cooldown_until = max(
            self._cooldown_until,
            time.monotonic() + max(0.0, delay),
        )

    async def _wait_for_shared_cooldown(self) -> None:
        while True:
            delay = self._cooldown_until - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    async def paginate(
        self,
        *,
        agent_id: int,
        params: Mapping[str, str],
        page_size: int = 100,
        max_pages: int = 10,
    ) -> AsyncIterator[dict[str, Any]]:
        limit = min(max(_MIN_PAGE_SIZE, int(page_size)), _MAX_PAGE_SIZE)
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


async def hscore_rows(client: FalconClient, config: Any) -> HScoreShortlist:
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

    sort_modes = tuple(str(sort_by) for sort_by in config.sort_modes)
    # The shared token bucket remains the global rate authority; scheduling
    # sort modes together lets pooled HTTP/2 connections overlap network wait.
    settled = await asyncio.gather(
        *(_fetch_mode(sort_by) for sort_by in sort_modes),
        return_exceptions=True,
    )

    # One sort mode failing is a partial degradation, not a shortlist outage:
    # the modes query the same agent and overlap heavily, so the survivors
    # still carry usable candidates. Discarding them would starve the funnel
    # and let a single transient 5xx look like "no wallet qualifies".
    # The failures are reported back on ``HScoreShortlist.failed_sort_modes``
    # so callers can tell "this wallet stopped qualifying" from "this wallet
    # was never looked at" — see ``DiscoveryFunnel.last_cycle_degraded``.
    modes: list[list[dict[str, Any]]] = []
    failed_modes: list[str] = []
    for sort_by, result in zip(sort_modes, settled):
        if isinstance(result, asyncio.CancelledError):
            # Never swallow cooperative cancellation into a degraded result.
            raise result
        if isinstance(result, BaseException):
            failed_modes.append(sort_by)
            logger.warning(
                "hscore_sort_mode_failed",
                extra={
                    "sort_by": sort_by,
                    # The bearer token can surface in transport errors.
                    "error": redact_error_detail(result),
                },
            )
            continue
        modes.append(result)

    if failed_modes and not modes:
        # Total outage must stay loud. Returning [] here would surface as
        # "H-Score returned no valid wallets" and let LeaderRegistry read an
        # agent outage as evidence that every active wallet deserves demotion.
        raise RuntimeError(
            "all H-Score sort modes failed: "
            + ", ".join(
                f"{sort_by}={redact_error_detail(result)}"
                for sort_by, result in zip(sort_modes, settled)
            )
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
                return HScoreShortlist(
                    rows=tuple(output),
                    failed_sort_modes=tuple(failed_modes),
                )
    return HScoreShortlist(
        rows=tuple(output),
        failed_sort_modes=tuple(failed_modes),
    )


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
    seen: set[tuple[str, ...]] = set()
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
            transaction_hash = str(
                row_value(
                    row,
                    "transaction_hash",
                    "transactionHash",
                    "tx_hash",
                    "txHash",
                    default="",
                )
            )
            trade_id = str(
                row_value(
                    row,
                    "trade_id",
                    "tradeID",
                    "tradeId",
                    "id",
                    default="",
                )
            )
            identity: tuple[str, ...]
            if trade_id:
                # A venue trade id already names exactly one fill, so key on
                # it alone. Folding in asset/condition would let harmless
                # metadata drift between overlapping pages (``asset`` on one
                # copy, ``token_id`` on the next; a condition id present only
                # once) split a single fill into two.
                identity = ("id", transaction_hash, trade_id, side)
            else:
                # No venue id. One transaction settles many fills at once, so
                # (tx_hash, side) would keep only the first leg and
                # under-report the leader's real activity. Keying on the whole
                # row keeps distinct fills apart — they differ in size, price,
                # or timestamp — while still collapsing the byte-identical
                # rows that overlapping pages re-emit.
                identity = (
                    "raw",
                    side,
                    orjson.dumps(row, option=orjson.OPT_SORT_KEYS).decode("utf-8"),
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
    "HScoreShortlist",
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
