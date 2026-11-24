from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp


@dataclass(frozen=True)
class ActivityPage:
    rows: list[dict[str, Any]]
    next_cursor: str | None = None


def _normalize_activity_row(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    aliases = (
        ("transaction_hash", "transactionHash"),
        ("condition_id", "conditionId"),
        ("token_id", "asset"),
        ("token_id", "tokenId"),
        ("usdc_size", "usdcSize"),
        ("proxy_wallet", "proxyWallet"),
        ("event_slug", "eventSlug"),
        ("outcome_index", "outcomeIndex"),
    )
    for v2_key, v1_key in aliases:
        if v1_key not in row and v2_key in row:
            row[v1_key] = row[v2_key]
        elif v2_key not in row and v1_key in row:
            row[v2_key] = row[v1_key]
    return row


class PolymarketHistoryClient:
    BASE_URL = "https://data-api.polymarket.com/v2/activity"

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
        cursor: str | None = None,
    ) -> ActivityPage:
        del offset  # Data API v2 rejects offset; pagination uses cursor.
        params: dict[str, str] = {
            "user": wallet,
            "type": "TRADE",
            "limit": str(limit),
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        if cursor:
            params["cursor"] = cursor
        if start_ts is not None:
            start_sec = start_ts // 1000 if start_ts > 10_000_000_000 else start_ts
            params["start"] = str(start_sec)
        if end_ts is not None:
            end_sec = end_ts // 1000 if end_ts > 10_000_000_000 else end_ts
            params["end"] = str(end_sec)

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.BASE_URL, params=params) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Polymarket activity error {resp.status}: {text[:500]}")
                payload = await resp.json()

        next_cursor: str | None = None
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows = payload.get("data") or payload.get("activities") or payload.get("results") or []
            pagination = payload.get("pagination")
            if isinstance(pagination, dict):
                raw_cursor = pagination.get("next_cursor")
                if isinstance(raw_cursor, str) and raw_cursor.strip():
                    next_cursor = raw_cursor.strip()
        else:
            rows = []
        normalized = [
            _normalize_activity_row(r) if isinstance(r, dict) else r for r in rows
        ]
        return ActivityPage(rows=list(normalized), next_cursor=next_cursor)
