from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp


@dataclass(frozen=True)
class FalconPage:
    results: list[dict[str, Any]]
    has_more: bool
    raw: dict[str, Any]


class FalconClient:
    def __init__(self, *, api_url: str, token: str, timeout_seconds: float = 20.0):
        self.api_url = api_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds

    async def query(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        limit: int = 100,
        offset: int = 0,
        format_type: str = "raw",
    ) -> FalconPage:
        body = {
            "agent_id": int(agent_id),
            "params": params,
            "pagination": {"limit": int(limit), "offset": int(offset)},
            "formatter_config": {"format_type": format_type},
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.api_url, json=body, headers=headers) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Falcon API error {resp.status}: {text[:500]}")
                payload = await resp.json()

        data = payload.get("data") or {}
        pagination = payload.get("pagination") or data.get("pagination") or {}
        results = data.get("results") or payload.get("results") or []
        return FalconPage(
            results=list(results),
            has_more=bool(pagination.get("has_more")),
            raw=payload,
        )

