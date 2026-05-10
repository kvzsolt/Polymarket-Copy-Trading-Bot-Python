"""Reconciliation for pending redemption transactions."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RedemptionStatus(StrEnum):
    """Lifecycle states for a redemption transaction."""

    CREATED = "created"
    SUBMITTED = "submitted"
    CONFIRMED = "confirmed"
    REVERTED = "reverted"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"
    RECONCILIATION_REQUIRED = "reconciliation_required"


@dataclass(frozen=True)
class ReconciliationResult:
    """Result of reconciling a single pending redemption."""

    redemption_id: str
    previous_status: RedemptionStatus
    new_status: RedemptionStatus
    reason: str


class RedemptionReconciler:
    """Reconcile pending redemptions against on-chain state.

    Parameters
    ----------
    redemptions_repo:
        Repository that provides ``list_pending()`` and status update methods.
    web3_adapter:
        Optional async adapter for on-chain transaction lookups.
    """

    def __init__(self, redemptions_repo, web3_adapter=None) -> None:
        self.redemptions_repo = redemptions_repo
        self.web3_adapter = web3_adapter

    async def reconcile_pending(self) -> list[ReconciliationResult]:
        """Find all pending redemptions and check their on-chain status."""
        pending = self.redemptions_repo.list_pending()
        results: list[ReconciliationResult] = []
        for redemption in pending:
            result = await self._reconcile_one(redemption)
            if result:
                results.append(result)
        return results

    async def _reconcile_one(self, redemption) -> ReconciliationResult | None:
        """Reconcile a single redemption.

        Override in production with real chain checks. The base
        implementation returns None (no reconciliation needed).
        """
        return None
