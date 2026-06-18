from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from utils.logger import get_logger


FALCON_LEADERBOARD_AGENT_ID = 584
logger = get_logger("FalconCandidateService")


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
    total_volume_15d: float = 0.0
    leaderboard_rank: int | None = None
    tier: str = ""
    source_agent_id: int = FALCON_LEADERBOARD_AGENT_ID
    source_sort_by: str = "h_score"
    source_rank: int | None = None
    first_seen_sort_by: str = "h_score"
    all_seen_sort_modes: tuple[str, ...] = ("h_score",)


class FalconCandidateService:
    def __init__(self, client, config):
        self.client = client
        self.config = config

    async def fetch_candidates(self) -> list[FalconCandidate]:
        sort_modes = tuple(
            getattr(
                self.config.discovery,
                "falcon_sort_modes",
                ("h_score",),
            )
            or ("h_score",)
        )
        dedupe = bool(getattr(self.config.discovery, "falcon_dedupe_candidates", True))
        max_candidates = int(
            getattr(
                self.config.discovery,
                "falcon_max_candidates",
                int(self.config.discovery.falcon_page_size)
                * int(self.config.discovery.falcon_max_pages),
            )
            or 0
        )
        max_candidates = max(1, max_candidates)

        out: list[FalconCandidate] = []
        by_wallet: dict[str, FalconCandidate] = {}
        failures: list[Exception] = []
        successful_modes = 0

        for sort_by in sort_modes:
            try:
                mode_candidates = await self._fetch_sort_mode(str(sort_by))
            except Exception as exc:
                failures.append(exc)
                logger.warning(
                    "falcon_candidate_sort_failed",
                    extra={
                        "sort_by": str(sort_by),
                        "agent_id": FALCON_LEADERBOARD_AGENT_ID,
                        "error": repr(exc),
                    },
                )
                continue

            successful_modes += 1
            for candidate in mode_candidates:
                if dedupe:
                    existing = by_wallet.get(candidate.wallet)
                    if existing is None:
                        by_wallet[candidate.wallet] = candidate
                    else:
                        by_wallet[candidate.wallet] = _merge_candidate_seen_mode(
                            existing,
                            candidate,
                        )
                    if len(by_wallet) >= max_candidates:
                        break
                else:
                    out.append(candidate)
                    if len(out) >= max_candidates:
                        break
            if (dedupe and len(by_wallet) >= max_candidates) or (
                not dedupe and len(out) >= max_candidates
            ):
                break

        if successful_modes == 0:
            if failures:
                raise RuntimeError("all Falcon sort modes failed") from failures[-1]
            raise RuntimeError("no Falcon sort modes configured")

        if dedupe:
            return list(by_wallet.values())[:max_candidates]
        return out[:max_candidates]

    async def _fetch_sort_mode(self, sort_by: str) -> list[FalconCandidate]:
        params = self._base_params(sort_by)
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
                candidate = self._parse_candidate(row, sort_by=sort_by)
                if candidate is not None:
                    out.append(candidate)
            if not page.has_more:
                break
        return out

    def _base_params(self, sort_by: str) -> dict[str, str]:
        return {
            "min_win_rate_15d": str(self.config.discovery.falcon_min_win_rate_15d),
            "max_win_rate_15d": str(self.config.discovery.falcon_max_win_rate_15d),
            "min_roi_15d": str(self.config.discovery.falcon_min_roi_15d),
            "min_total_trades_15d": str(self.config.discovery.falcon_min_trades_15d),
            "max_total_trades_15d": str(self.config.discovery.falcon_max_trades_15d),
            "min_pnl_15d": str(self.config.discovery.falcon_min_pnl_15d),
            "sort_by": sort_by,
        }

    def _parse_candidate(
        self,
        row: dict[str, Any],
        *,
        sort_by: str = "h_score",
    ) -> FalconCandidate | None:
        wallet = str(
            row.get("proxy_wallet")
            or row.get("proxyWallet")
            or row.get("wallet")
            or row.get("address")
            or ""
        ).lower().strip()
        if not wallet.startswith("0x") or len(wallet) != 42:
            return None

        rank = _int_or_none(row.get("leaderboard_rank"))
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
            total_volume_15d=_float(row.get("total_volume_15d")),
            leaderboard_rank=rank,
            tier=str(row.get("tier") or ""),
            trajectory=str(row.get("trajectory") or "unknown"),
            source_agent_id=FALCON_LEADERBOARD_AGENT_ID,
            source_sort_by=sort_by,
            source_rank=rank,
            first_seen_sort_by=sort_by,
            all_seen_sort_modes=(sort_by,),
            raw=row,
        )


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_or_none(value) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _merge_candidate_seen_mode(
    existing: FalconCandidate,
    incoming: FalconCandidate,
) -> FalconCandidate:
    seen = list(existing.all_seen_sort_modes)
    for mode in incoming.all_seen_sort_modes:
        if mode not in seen:
            seen.append(mode)
    raw = dict(existing.raw)
    raw["all_seen_sort_modes"] = seen
    raw.setdefault("source_rows", [])
    if isinstance(raw["source_rows"], list):
        raw["source_rows"].append(
            {
                "source_sort_by": incoming.source_sort_by,
                "source_rank": incoming.source_rank,
                "leaderboard_rank": incoming.leaderboard_rank,
            }
        )
    return replace(
        existing,
        all_seen_sort_modes=tuple(seen),
        raw=raw,
    )
