from __future__ import annotations

from dataclasses import dataclass
from typing import Any


FALCON_LEADERBOARD_AGENT_ID = 584


@dataclass(frozen=True)
class FalconCandidate:
    wallet: str
    h_score: float
    roi_15d: float
    win_rate_15d: float
    sharpe_15d: float
    pnl_15d: float
    total_trades_15d: int
    markets_traded_15d: int
    trajectory: str
    raw: dict[str, Any]


class FalconCandidateService:
    def __init__(self, client, config):
        self.client = client
        self.config = config

    async def fetch_candidates(self) -> list[FalconCandidate]:
        params = {
            "min_win_rate_15d": str(self.config.discovery.falcon_min_win_rate_15d),
            "max_win_rate_15d": str(self.config.discovery.falcon_max_win_rate_15d),
            "min_roi_15d": str(self.config.discovery.falcon_min_roi_15d),
            "min_total_trades_15d": str(self.config.discovery.falcon_min_trades_15d),
            "max_total_trades_15d": str(self.config.discovery.falcon_max_trades_15d),
            "min_pnl_15d": str(self.config.discovery.falcon_min_pnl_15d),
            "sort_by": "h_score",
        }

        out: list[FalconCandidate] = []
        limit = int(self.config.discovery.falcon_page_size)
        max_pages = int(self.config.discovery.falcon_max_pages)
        for page_idx in range(max_pages):
            page = await self.client.query(
                agent_id=FALCON_LEADERBOARD_AGENT_ID,
                params=params,
                limit=limit,
                offset=page_idx * limit,
            )
            for row in page.results:
                candidate = self._parse_candidate(row)
                if candidate is not None:
                    out.append(candidate)
            if not page.has_more:
                break
        return out

    def _parse_candidate(self, row: dict[str, Any]) -> FalconCandidate | None:
        wallet = str(
            row.get("proxy_wallet")
            or row.get("proxyWallet")
            or row.get("wallet")
            or row.get("address")
            or ""
        ).lower().strip()
        if not wallet.startswith("0x") or len(wallet) != 42:
            return None

        return FalconCandidate(
            wallet=wallet,
            h_score=_float(row.get("h_score")),
            roi_15d=_float(row.get("roi_pct_15d") or row.get("roi_15d")),
            win_rate_15d=_float(
                row.get("win_rate_pct_15d") or row.get("win_rate_15d")
            ),
            sharpe_15d=_float(row.get("sharpe_ratio_15d") or row.get("sharpe_15d")),
            pnl_15d=_float(row.get("total_pnl_15d") or row.get("pnl_15d")),
            total_trades_15d=int(_float(row.get("total_trades_15d"))),
            markets_traded_15d=int(_float(row.get("markets_traded_15d"))),
            trajectory=str(row.get("trajectory") or "unknown"),
            raw=row,
        )


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

