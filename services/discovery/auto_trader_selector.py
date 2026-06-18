from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import time
from typing import Any, Protocol

from config.app_config import AppConfig
from services.discovery.evaluation_context import build_evaluation_context_hash
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
        score_version: str | None = None,
        limit: int = 50,
    ) -> list[Any]:
        ...


class AutoTraderSelector:
    """DB-only startup selector for live-eligible discovery evaluations."""

    def __init__(self, repo: EvaluationRepo, config: AppConfig, balance_provider=None):
        self.repo = repo
        self.config = config
        self.balance_provider = balance_provider

    async def select_from_db(self) -> AutoTraderSelection:
        limit = int(self.config.discovery.max_auto_traders_count or 0)
        if limit <= 0:
            metrics.set_gauge("auto_trader_bootstrap_db_candidates", 0.0)
            return AutoTraderSelection(
                wallets=[],
                source="db",
                reason="max_auto_traders_count_zero",
            )

        query_limit = max(limit * 5, 25)
        query_kwargs: dict[str, Any] = {
            "statuses": ["perfect_candidate", "live_eligible"],
            "eligible_live": True,
            "limit": query_limit,
        }
        if bool(
            getattr(self.config.discovery, "auto_leader_require_score_version", True)
        ):
            query_kwargs["score_version"] = str(
                getattr(
                    self.config.discovery,
                    "leader_score_version",
                    "leader_score_v2",
                )
                or "leader_score_v2"
            ).lower()
        rows = await self.repo.list_evaluations(**query_kwargs)
        rows = sorted(
            rows,
            key=lambda row: (
                -_as_float(getattr(row, "final_score", 0.0)),
                -_as_float(getattr(row, "total_volume_15d", 0.0)),
                str(getattr(row, "wallet", "") or "").lower(),
            ),
        )
        current_config_hash: str | None = None
        if rows and self.config.discovery.discovery_reject_stale_evaluations:
            current_config_hash = await self._current_config_hash()
            if current_config_hash is None:
                metrics.set_gauge("auto_trader_bootstrap_db_candidates", float(len(rows)))
                return AutoTraderSelection(
                    wallets=[],
                    source="db",
                    reason="current_config_hash_unavailable",
                    db_candidate_count=len(rows),
                )

        wallets: list[str] = []
        for row in rows:
            if len(wallets) >= limit:
                break
            if not self._is_live_eligible(row, current_config_hash=current_config_hash):
                continue
            wallet = str(getattr(row, "wallet", "") or "").strip().lower()
            if wallet:
                wallets.append(wallet)

        metrics.set_gauge("auto_trader_bootstrap_db_candidates", float(len(rows)))

        logger.info(
            "auto_trader_db_selection_complete",
            extra={
                "requested_limit": limit,
                "query_limit": query_limit,
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

    async def _current_config_hash(self) -> str | None:
        if self.balance_provider is None:
            logger.warning(
                "auto_trader_selection_stale_check_failed",
                extra={"reason": "balance_provider_unavailable"},
            )
            return None
        try:
            balance = await self.balance_provider.get_available_balance_usdc()
        except Exception as exc:
            logger.warning(
                "auto_trader_selection_stale_check_failed",
                extra={"reason": "balance_fetch_failed", "error": repr(exc)},
            )
            return None
        config_hash, _context = build_evaluation_context_hash(
            self.config,
            balance_usdc=balance,
        )
        return config_hash

    def _is_live_eligible(self, row: Any, *, current_config_hash: str | None = None) -> bool:
        status = str(getattr(row, "status", "") or "").lower()
        if status not in _LIVE_STATUSES:
            self._log_rejection(row, "status_not_live")
            return False
        if not bool(getattr(row, "eligible_live", False)):
            self._log_rejection(row, "eligible_live_false")
            return False
        if getattr(row, "rejection_reason", None):
            self._log_rejection(
                row,
                "has_rejection_reason",
                row_rejection_reason=str(getattr(row, "rejection_reason", "") or ""),
            )
            return False

        if _as_float(getattr(row, "final_score", 0.0)) < float(
            self.config.leader.auto_discover_min_score
        ):
            self._log_rejection(
                row,
                "score_below_min",
                final_score=_as_float(getattr(row, "final_score", 0.0)),
                min_score=float(self.config.leader.auto_discover_min_score),
            )
            return False
        if bool(
            getattr(self.config.discovery, "auto_leader_require_score_version", True)
        ):
            expected_version = str(
                getattr(
                    self.config.discovery,
                    "leader_score_version",
                    "leader_score_v2",
                )
                or "leader_score_v2"
            ).lower()
            row_version = str(getattr(row, "score_version", "") or "").lower()
            if row_version != expected_version:
                self._log_rejection(
                    row,
                    "score_version_mismatch",
                    row_score_version=row_version,
                    expected_score_version=expected_version,
                )
                return False
        if (
            self.config.discovery.discovery_auto_prefer_sports_esports
            and _as_float(getattr(row, "sports_esports_ratio", 0.0))
            < self.config.discovery.discovery_min_auto_sports_esports_ratio
        ):
            self._log_rejection(
                row,
                "sports_esports_ratio_below_min",
                sports_esports_ratio=_as_float(
                    getattr(row, "sports_esports_ratio", 0.0)
                ),
                min_sports_esports_ratio=(
                    self.config.discovery.discovery_min_auto_sports_esports_ratio
                ),
            )
            return False
        if _as_float(getattr(row, "copyable_signal_ratio", 0.0)) < float(
            self.config.discovery.discovery_min_copyable_grouped_signal_ratio
        ):
            self._log_rejection(
                row,
                "copyable_signal_ratio_below_min",
                copyable_signal_ratio=_as_float(
                    getattr(row, "copyable_signal_ratio", 0.0)
                ),
                min_copyable_signal_ratio=float(
                    self.config.discovery.discovery_min_copyable_grouped_signal_ratio
                ),
            )
            return False
        if _as_float(getattr(row, "copyable_notional_ratio", 0.0)) < float(
            self.config.discovery.discovery_min_copyable_notional_ratio
        ):
            self._log_rejection(
                row,
                "copyable_notional_ratio_below_min",
                copyable_notional_ratio=_as_float(
                    getattr(row, "copyable_notional_ratio", 0.0)
                ),
                min_copyable_notional_ratio=float(
                    self.config.discovery.discovery_min_copyable_notional_ratio
                ),
            )
            return False

        if hasattr(row, "total_grouped_signals") and int(
            getattr(row, "total_grouped_signals") or 0
        ) <= 0:
            self._log_rejection(row, "total_grouped_signals_zero")
            return False

        if self.config.discovery.discovery_require_7d_non_negative:
            pnl_7d = _as_decimal(getattr(row, "pnl_7d", Decimal("0")))
            min_pnl = Decimal(str(self.config.discovery.discovery_min_7d_pnl_usdc))
            if pnl_7d < min_pnl:
                self._log_rejection(
                    row,
                    "recent_pnl_below_min",
                    pnl_7d=str(pnl_7d),
                    min_pnl_7d=str(min_pnl),
                )
                return False

        if self.config.discovery.discovery_reject_stale_evaluations:
            updated_at = _as_float(getattr(row, "updated_at", 0.0))
            ttl = int(self.config.discovery.discovery_evaluation_ttl_seconds or 0)
            if ttl > 0 and updated_at > 0 and time.time() - updated_at > ttl:
                self._log_rejection(
                    row,
                    "evaluation_expired",
                    updated_at=updated_at,
                    ttl_seconds=ttl,
                )
                return False
            row_hash = str(getattr(row, "config_hash", "") or "")
            if not current_config_hash or row_hash != current_config_hash:
                self._log_rejection(
                    row,
                    "config_hash_mismatch",
                    row_config_hash=row_hash,
                    current_config_hash=current_config_hash or "",
                )
                return False

        return True

    def _log_rejection(self, row: Any, reason: str, **extra: Any) -> None:
        logger.info(
            "auto_trader_candidate_rejected",
            extra={
                "wallet": str(getattr(row, "wallet", "") or "").lower(),
                "reason": reason,
                **extra,
            },
        )


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
