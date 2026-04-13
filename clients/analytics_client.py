"""Polymarket analytics adapter — ``AbstractAnalyticsClient`` implementation.

Issues HTTP requests against polymarketanalytics.com via the injected
``AbstractHttpClient`` (the same one services use for other endpoints),
so rate-limiting, retries, and timeouts are unified.
"""

from __future__ import annotations

from typing import Any

from domain.ports.clients import AbstractAnalyticsClient, AbstractHttpClient


class AnalyticsClient(AbstractAnalyticsClient):
    def __init__(
        self,
        *,
        http: AbstractHttpClient,
        base_url: str = "https://polymarketanalytics.com/api",
        leaderboard_params: str = "",
    ):
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._leaderboard_params = leaderboard_params

    async def fetch_top_traders(
        self,
        *,
        limit: int,
        min_pnl: float,
        min_win_rate: float,
        min_positions: int,
    ) -> list[dict[str, Any]]:
        query = self._leaderboard_params
        sep = "&" if query else ""
        url = f"{self._base_url}/traders-tag-performance?{query}{sep}limit={limit}&offset=0"
        try:
            data = await self._http.get_json(url, silent=True)
        except Exception:
            return []
        rows = data.get("data", []) if isinstance(data, dict) else []
        filtered: list[dict[str, Any]] = []
        for row in rows:
            try:
                pnl = float(row.get("overall_gain", 0) or 0)
                win_rate = float(row.get("win_rate", 0) or 0)
                positions = int(row.get("total_positions", 0) or 0)
            except (TypeError, ValueError):
                continue
            if pnl < min_pnl or win_rate < min_win_rate or positions < min_positions:
                continue
            filtered.append(dict(row))
        return filtered
