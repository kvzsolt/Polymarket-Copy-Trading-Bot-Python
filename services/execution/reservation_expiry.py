"""Typed release decisions for expired reservations.

An expired reservation may only free its reserved quote/share capacity when
the expiry callback can classify the reservation into an outcome that PROVES
release is safe. Elapsed time alone is never such a proof: an ambiguous venue
POST (a ``submitted_unconfirmed`` order row without a venue identity) may
still be live, and freeing its capacity opens a double-spend window.

The four outcomes:

``RELEASE_PRE_SUBMIT_ORPHAN``
    POSITIVE proof of absence: the persisted-submission lookup SUCCEEDED and
    found no row for the reservation, and the reservation carries no venue
    order identity. Submissions persist a write-ahead row before any venue
    POST (``ExecutionService._save_submission`` with a ``pending`` response),
    so a successful empty lookup proves the crash happened between the
    reservation acquisition and that write — no POST can have occurred.

``KEEP_PENDING_RECOVERY``
    A submission row exists without a venue identity, in a status that does
    not prove termination (``submitted_unconfirmed``, pre-POST lock states,
    live/matched states...). The ambiguous-recovery machinery owns its
    resolution; the TTL never releases it.

``RELEASE_TERMINAL_EVIDENCE``
    Authoritative terminal evidence for THIS account: the linked order was
    reconciled to a terminal venue outcome, the row carries a terminal
    no-ID status (venue rejection, pre-POST denial, or a recovery-proven
    ``confirmed_absent``), or the reservation is already released.

``PERSISTENCE_UNAVAILABLE``
    A required lookup RAISED. A failed lookup is not absence: the decision
    keeps the reservation, surfaces the error, and retries on a later sweep.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any


class ReservationExpiryDecision(str, enum.Enum):
    """Classify one expired reservation before any capacity release."""

    RELEASE_PRE_SUBMIT_ORPHAN = "release_pre_submit_orphan"
    KEEP_PENDING_RECOVERY = "keep_pending_recovery"
    RELEASE_TERMINAL_EVIDENCE = "release_terminal_evidence"
    PERSISTENCE_UNAVAILABLE = "persistence_unavailable"

    @property
    def allows_release(self) -> bool:
        """Return whether this outcome authorizes releasing the reservation."""
        return self in (
            ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN,
            ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE,
        )


@dataclass(frozen=True)
class OrderRowLookup:
    """Typed persisted-submission lookup: a row, proven absence, or an error.

    Replaces the exception-to-``None`` collapse that made a database outage
    indistinguishable from "no submission row exists".
    """

    order_row: dict[str, Any] | None = None
    error: BaseException | None = None

    @classmethod
    def found(cls, order_row: dict[str, Any] | None) -> "OrderRowLookup":
        return cls(order_row=order_row if isinstance(order_row, dict) else None, error=None)

    @classmethod
    def absent(cls) -> "OrderRowLookup":
        """The lookup succeeded and proved no submission row exists."""
        return cls(order_row=None, error=None)

    @classmethod
    def unavailable(cls, error: BaseException) -> "OrderRowLookup":
        return cls(order_row=None, error=error)

    @property
    def lookup_succeeded(self) -> bool:
        """Return whether persistence answered (found row or proven absence)."""
        return self.error is None


# A no-ID submission row in one of these statuses is authoritative terminal
# evidence for the owning account: the venue rejected the order, the
# submission was denied before any POST, or the ambiguous-recovery machinery
# proved venue absence (``confirmed_absent``). Mirrors the coordinator's
# terminal-failure submit statuses plus the recovery-proven absent transition;
# anything else (including live/matched/pending states) stays unresolved.
TERMINAL_NO_ID_ROW_STATUSES = frozenset(
    {
        "confirmed_absent",
        "rejected",
        "failed",
        "submission_failed",
        "cancelled",
        "canceled",
        "expired",
        "book_no_liquidity",
        "book_spread_wide",
        "stale_market_data",
        "market_not_ready",
        "circuit_open",
        "price_limit_unexecutable",
        "task_deleted",
        "task_paused",
        "task_account_mismatch",
        "task_leader_mismatch",
        "task_version_conflict",
        "task_state_unavailable",
        "task_state_invalid",
        "outside_schedule",
        "trading_paused",
        "kill_switch",
    }
)


def classify_no_id_reservation_expiry(
    submission_lookup: OrderRowLookup,
) -> ReservationExpiryDecision:
    """Classify an expired reservation that carries no venue order identity.

    The caller must have already established that neither the reservation row
    nor the submission row carries a venue ``order_id`` (those cases take the
    linked-order reconciliation path instead).
    """

    if not submission_lookup.lookup_succeeded:
        return ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    order_row = submission_lookup.order_row
    if order_row is None:
        # Successful lookup, no persisted submission row: the write-ahead
        # submission row never existed, so no venue POST can have occurred.
        return ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN
    status = str(order_row.get("status") or "").strip().lower()
    if status in TERMINAL_NO_ID_ROW_STATUSES:
        return ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
    return ReservationExpiryDecision.KEEP_PENDING_RECOVERY


__all__ = [
    "OrderRowLookup",
    "ReservationExpiryDecision",
    "TERMINAL_NO_ID_ROW_STATUSES",
    "classify_no_id_reservation_expiry",
]
