from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from services.discovery.candidate_scoring import RecentState


FALCON_PROFIT_AND_LOSS_AGENT_ID = 569


class FalconRecentStateService:
    """Fetch recent wallet PnL from Falcon's profit-and-loss agent."""

    def __init__(self, client):
        self.client = client

    async def fetch_recent_state(self, wallet: str) -> RecentState:
        pnl_7d = await self._fetch_window_pnl(wallet, days=7)
        pnl_3d = await self._fetch_window_pnl(wallet, days=3)
        return RecentState(pnl_7d=pnl_7d, pnl_3d=pnl_3d)

    async def _fetch_window_pnl(self, wallet: str, *, days: int) -> Decimal:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=max(int(days), 1) - 1)
        page = await self.client.query(
            agent_id=FALCON_PROFIT_AND_LOSS_AGENT_ID,
            params={
                "granularity": "1d",
                "wallet": str(wallet).lower(),
                "condition_id": "ALL",
                "start_time": start.isoformat(),
                "end_time": end.isoformat(),
            },
            limit=200,
            offset=0,
        )
        total = Decimal("0")
        for row in page.results:
            total += _decimal(row.get("pnl"))
        return total


def _decimal(value) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")
