"""Fresh Polymarket Data API fetch for the strategy profiler (PR3).

Fetches a leader's recent on-chain activity, trades, and closed positions
directly from the public Polymarket Data API
(``https://data-api.polymarket.com``) — no DB dependency, no reuse of the
discovery backfill stack (per the PR3 design decision). The loader returns an
in-memory :class:`LoadedHistory` consumed by
:mod:`services.strategy_profile.strategy_profiler`.

Endpoints used (verified via Firecrawl against
https://docs.polymarket.com/market-data/overview):
  * ``GET /activity?user={address}&type=TRADE&limit=&offset=&start=&end=``
  * ``GET /trades?user={address}&limit=&offset=``
  * ``GET /closed-positions?user={address}&limit=&offset=``

All endpoints are public (no auth). Pagination via ``limit``/``offset``. The
walk is bounded by ``POLYMARKET_HISTORY_LOOKBACK_DAYS`` /
``POLYMARKET_ACTIVITY_MAX_OFFSET`` (already typed in AppConfig) so a very
active wallet does not explode the fetch.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from integrations.polymarket.history_client import PolymarketHistoryClient

_DATA_API_BASE = "https://data-api.polymarket.com"


@dataclass(frozen=True)
class LoadedHistory:
    """In-memory normalized history for one leader wallet."""

    wallet: str
    activity: list[dict[str, Any]] = field(default_factory=list)
    trades: list[dict[str, Any]] = field(default_factory=list)
    closed_positions: list[dict[str, Any]] = field(default_factory=list)
    lookback_days: int = 30


class LeaderHistoryLoader:
    """Fetches a leader's recent activity / trades / closed positions."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 20.0,
        max_offset: int = 3000,
        page_limit: int = 500,
        lookback_days: int = 30,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_offset = max(0, max_offset)
        self._page_limit = max(1, page_limit)
        self._lookback_days = max(1, lookback_days)
        self._activity_client = PolymarketHistoryClient(timeout_seconds=timeout_seconds)

    async def load(self, wallet: str) -> LoadedHistory:
        wallet = (wallet or "").strip()
        if not wallet:
            return LoadedHistory(wallet="")
        wallet_lower = wallet.lower()

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - self._lookback_days * 86_400_000

        activity = await self._fetch_all_pages(
            self._fetch_activity_page, wallet_lower, start_ts=start_ms
        )
        trades = await self._fetch_all_pages(self._fetch_trades_page, wallet_lower)
        closed = await self._fetch_all_pages(self._fetch_closed_positions_page, wallet_lower)

        return LoadedHistory(
            wallet=wallet_lower,
            activity=activity,
            trades=trades,
            closed_positions=closed,
            lookback_days=self._lookback_days,
        )

    async def _fetch_all_pages(
        self,
        fetcher,
        wallet: str,
        *,
        start_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while offset <= self._max_offset:
            page = await fetcher(wallet=wallet, offset=offset, start_ts=start_ts)
            if not page:
                break
            rows.extend(page)
            if len(page) < self._page_limit:
                break
            offset += self._page_limit
        return rows

    async def _fetch_activity_page(
        self,
        *,
        wallet: str,
        offset: int,
        start_ts: int | None,
    ) -> list[dict[str, Any]]:
        page = await self._activity_client.fetch_activity_page(
            wallet=wallet,
            start_ts=start_ts,
            offset=offset,
            limit=self._page_limit,
        )
        return list(page.rows)

    async def _fetch_trades_page(
        self,
        *,
        wallet: str,
        offset: int,
        start_ts: int | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {
            "user": wallet,
            "limit": str(self._page_limit),
            "offset": str(offset),
        }
        _ = start_ts  # /trades does not support start/end in the same way
        return await self._fetch_json_list("/trades", params)

    async def _fetch_closed_positions_page(
        self,
        *,
        wallet: str,
        offset: int,
        start_ts: int | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {
            "user": wallet,
            "limit": str(self._page_limit),
            "offset": str(offset),
        }
        _ = start_ts
        return await self._fetch_json_list("/closed-positions", params)

    async def _fetch_json_list(self, path: str, params: dict[str, str]) -> list[dict[str, Any]]:
        url = f"{_DATA_API_BASE}{path}"
        timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status >= 400:
                        return []
                    payload = await resp.json()
        except (aiohttp.ClientError, TimeoutError):
            return []

        if isinstance(payload, list):
            return list(payload)
        if isinstance(payload, dict):
            for key in ("data", "trades", "positions", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return list(value)
        return []
