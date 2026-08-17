"""Position drift domain types (PR7).

PR1–PR6.1 keep local state in step with the venue while the bot runs. PR7
adds the proactive cross-check: at startup and before each submit, the bot
compares its local position view + active reservations against the venue's
truth. Any unexplained drift pauses the scope until an operator resolves it.

Position identity is ``(user_id, condition_id, token_id)`` everywhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any


class PositionDriftStatus(StrEnum):
    CLEAN = "clean"
    LOCAL_MISSING_VENUE_POSITION = "local_missing_venue_position"
    VENUE_MISSING_LOCAL_POSITION = "venue_missing_local_position"
    SIZE_MISMATCH_POSITIVE = "size_mismatch_positive"  # venue > local
    SIZE_MISMATCH_NEGATIVE = "size_mismatch_negative"  # local > venue
    RESERVATION_ORPHANED = "reservation_orphaned"
    UNRESOLVED = "unresolved"


class PositionDriftAction(StrEnum):
    NO_ACTION = "no_action"
    UPDATE_LOCAL_READONLY = "update_local_readonly"
    PAUSE_SCOPE = "pause_scope"
    REQUIRE_OPERATOR_REVIEW = "require_operator_review"
    RECONCILE_RESERVATION = "reconcile_reservation"
    PRUNE_LOCAL_ZERO_POSITION = "prune_local_zero_position"


@dataclass(frozen=True)
class PositionDriftRecord:
    user_id: str
    condition_id: str
    token_id: str
    local_size: Decimal
    venue_size: Decimal
    drift_size: Decimal  # venue - local (signed)
    status: PositionDriftStatus
    action: PositionDriftAction
    reason: str = ""
    reservation_ids: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def scope_key(self) -> str:
        return f"{self.user_id}|{self.condition_id}|{self.token_id}"

    @property
    def is_clean(self) -> bool:
        return self.status == PositionDriftStatus.CLEAN

    @property
    def pauses_scope(self) -> bool:
        return self.action in (
            PositionDriftAction.PAUSE_SCOPE,
            PositionDriftAction.REQUIRE_OPERATOR_REVIEW,
        )


@dataclass(frozen=True)
class DriftReport:
    user_id: str
    clean: bool
    records: tuple[PositionDriftRecord, ...]
    generated_at: int
    paused_scopes: tuple[str, ...] = ()
