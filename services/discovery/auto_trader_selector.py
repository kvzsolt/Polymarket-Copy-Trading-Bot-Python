from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from config.app_config import AppConfig
from services.metrics import metrics
from utils.logger import get_logger

logger = get_logger("AutoTraderSelector")

_LIVE_STATUSES = {"perfect_candidate", "live_eligible"}


@dataclass(frozen=True)
class AutoTraderSelection:
    wallets: list[str]
    source: str
    reason: str
    db_candidate_count: int = 0


class EvaluationRepo(Protocol):
    async def list_evaluations(
        self,
        *,
        statuses: list[str] | None = None,
        eligible_live: bool | None = None,
        limit: int = 50,
    ) -> list[Any]:
        ...


class AutoTraderSelector:
    """DB-only startup selector for live-eligible discovery evaluations."""

    def __init__(self, repo: EvaluationRepo, config: AppConfig):
        self.repo = repo
        self.config = config

    async def select_from_db(self) -> AutoTraderSelection:
        limit = int(self.config.discovery.max_auto_traders_count or 0)
        if limit <= 0:
            metrics.set_gauge("auto_trader_bootstrap_db_candidates", 0.0)
            return AutoTraderSelection(
                wallets=[],
                source="db",
                reason="max_auto_traders_count_zero",
            )

        rows = await self.repo.list_evaluations(
            statuses=["perfect_candidate", "live_eligible"],
            eligible_live=True,
            limit=limit,
        )

        wallets: list[str] = []
        for row in rows:
            if len(wallets) >= limit:
                break
            if not self._is_live_eligible(row):
                continue
            wallet = str(getattr(row, "wallet", "") or "").strip().lower()
            if wallet:
                wallets.append(wallet)

        metrics.set_gauge("auto_trader_bootstrap_db_candidates", float(len(rows)))

        logger.info(
            "auto_trader_db_selection_complete",
            extra={
                "requested_limit": limit,
                "db_candidate_count": len(rows),
                "selected_count": len(wallets),
                "selected_wallets": wallets,
            },
        )

        return AutoTraderSelection(
            wallets=wallets,
            source="db",
            reason="eligible_candidates_found" if wallets else "no_eligible_db_candidates",
            db_candidate_count=len(rows),
        )

    def _is_live_eligible(self, row: Any) -> bool:
        status = str(getattr(row, "status", "") or "").lower()
        if status not in _LIVE_STATUSES:
            return False
        if not bool(getattr(row, "eligible_live", False)):
            return False
        if getattr(row, "rejection_reason", None):
            return False

        if _as_float(getattr(row, "final_score", 0.0)) < float(
            self.config.leader.auto_discover_min_score
        ):
            return False
        if _as_float(getattr(row, "copyable_signal_ratio", 0.0)) < float(
            self.config.discovery.discovery_min_copyable_grouped_signal_ratio
        ):
            return False
        if _as_float(getattr(row, "copyable_notional_ratio", 0.0)) < float(
            self.config.discovery.discovery_min_copyable_notional_ratio
        ):
            return False

        if hasattr(row, "total_grouped_signals") and int(
            getattr(row, "total_grouped_signals") or 0
        ) <= 0:
            return False

        if self.config.discovery.discovery_require_7d_non_negative:
            pnl_7d = _as_decimal(getattr(row, "pnl_7d", Decimal("0")))
            min_pnl = Decimal(str(self.config.discovery.discovery_min_7d_pnl_usdc))
            if pnl_7d < min_pnl:
                return False

        return True


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _as_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")
