from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp


@dataclass(frozen=True)
class ActivityPage:
    rows: list[dict[str, Any]]


class PolymarketHistoryClient:
    BASE_URL = "https://data-api.polymarket.com/activity"

    def __init__(self, *, timeout_seconds: float = 20.0):
        self.timeout_seconds = timeout_seconds

    async def fetch_activity_page(
        self,
        *,
        wallet: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        offset: int = 0,
        limit: int = 500,
    ) -> ActivityPage:
        params: dict[str, str] = {
            "user": wallet,
            "type": "TRADE",
            "limit": str(limit),
            "offset": str(offset),
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        if start_ts is not None:
            params["start"] = str(start_ts)
        if end_ts is not None:
            params["end"] = str(end_ts)

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.BASE_URL, params=params) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Polymarket activity error {resp.status}: {text[:500]}")
                payload = await resp.json()

        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows = payload.get("data") or payload.get("activities") or payload.get("results") or []
        else:
            rows = []
        return ActivityPage(rows=list(rows))
