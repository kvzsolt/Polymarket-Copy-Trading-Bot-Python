"""Race-safe task-level spend reservations and one-entry accounting.

Task budgets are separate from the existing account-level reservation engine.
Both must approve a BUY. The task reservation protects Kreo-style task caps
and one-entry rules across concurrent leader workers, while the existing engine
remains authoritative for wallet collateral and portfolio risk.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol

from services.copy_tasks.domain import TaskSpendPolicy

_ZERO = Decimal("0")


def _decimal(value: object, *, name: str, positive: bool = False) -> Decimal:
    """Return one finite decimal validated at the service boundary."""

    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite() or result < 0 or (positive and result <= 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be finite and {qualifier}")
    return result


class TaskBudgetReservationStatus(StrEnum):
    """Persistent lifecycle of one task budget reservation."""

    RESERVED = "reserved"
    COMMITTED = "committed"
    RELEASED = "released"
    EXPIRED = "expired"


class TaskBudgetExecutionState(StrEnum):
    """Whether a reserved amount is still pre-POST or venue-retained."""

    PRE_POST = "pre_post"
    RETAINED = "retained"


class TaskBudgetInvariantError(RuntimeError):
    """Raised when fill evidence contradicts the reserved task budget."""


@dataclass(frozen=True, slots=True)
class TaskBudgetKey:
    """Task budget scope for one follower market."""

    follower_account: str
    task_id: str
    market_id: str
    sports_market_type: str = ""

    def __post_init__(self) -> None:
        for name in ("follower_account", "task_id", "market_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        object.__setattr__(
            self,
            "sports_market_type",
            str(self.sports_market_type).strip().casefold(),
        )


@dataclass(frozen=True, slots=True)
class TaskBudgetReservationRequest:
    """Request to reserve task spend before downstream order submission."""

    reservation_id: str
    key: TaskBudgetKey
    amount: Decimal
    reserved_at: datetime
    expires_at: datetime
    prior_reservation_ids: tuple[str, ...] = field(default=(), compare=False)

    def __post_init__(self) -> None:
        if not str(self.reservation_id).strip():
            raise ValueError("reservation_id is required")
        if not isinstance(self.prior_reservation_ids, tuple) or any(
            not isinstance(identity, str) or not identity for identity in self.prior_reservation_ids
        ):
            raise ValueError("Prior reservation identities must be an immutable tuple of ids")
        if self.reserved_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("task budget timestamps must be timezone-aware")
        if self.expires_at <= self.reserved_at:
            raise ValueError("task budget expiry must be after reservation time")
        object.__setattr__(
            self,
            "amount",
            _decimal(self.amount, name="task budget amount", positive=True),
        )


@dataclass(frozen=True, slots=True)
class TaskBudgetReservation:
    """Persistent task budget reservation state."""

    request: TaskBudgetReservationRequest
    status: TaskBudgetReservationStatus = TaskBudgetReservationStatus.RESERVED
    execution_state: TaskBudgetExecutionState = TaskBudgetExecutionState.PRE_POST
    settled_notional: Decimal = _ZERO
    fill_id: str | None = None
    committed_notional: Decimal | None = None


@dataclass(frozen=True, slots=True)
class TaskBudgetFill:
    """Committed follower fill counted against task spend limits."""

    fill_id: str
    reservation_id: str
    key: TaskBudgetKey
    notional: Decimal
    occurred_at: datetime

    def __post_init__(self) -> None:
        if not self.fill_id or not self.reservation_id:
            raise ValueError("task budget fill identities are required")
        if self.occurred_at.tzinfo is None:
            raise ValueError("task budget fill timestamp must be timezone-aware")
        object.__setattr__(
            self,
            "notional",
            _decimal(self.notional, name="filled notional", positive=True),
        )


@dataclass(frozen=True, slots=True)
class TaskBudgetFillApplication:
    """Result of appending one authoritative fill to a reservation."""

    created: bool
    cumulative_notional: Decimal
    remaining_notional: Decimal
    status: TaskBudgetReservationStatus


@dataclass(frozen=True, slots=True)
class TaskBudgetSnapshot:
    """Committed plus active-reservation spend visible to one decision."""

    task_total: Decimal
    task_daily: Decimal
    task_market: Decimal
    task_has_entry: bool
    global_market_has_entry: bool
    task_has_sports_type_entry: bool = False


@dataclass(frozen=True, slots=True)
class TaskBudgetReservationDecision:
    """Auditable result of an atomic task budget reservation."""

    allowed: bool
    reason: str
    created: bool
    reservation: TaskBudgetReservation | None
    snapshot: TaskBudgetSnapshot


class TaskBudgetRepository(Protocol):
    """Atomic durable store required by task budget orchestration."""

    async def snapshot(
        self,
        key: TaskBudgetKey,
        now: datetime,
    ) -> TaskBudgetSnapshot:
        """Return current spend and entry state for admission."""

        ...

    async def reserve(
        self,
        request: TaskBudgetReservationRequest,
        policy: TaskSpendPolicy,
    ) -> TaskBudgetReservationDecision:
        """Reserve spend if every task limit remains valid."""

        ...

    async def commit(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
    ) -> bool:
        """Commit authoritative fill evidence exactly once."""

        ...

    async def record_fill(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
        terminal: bool,
    ) -> TaskBudgetFillApplication:
        """Append one fill while retaining or terminally settling the cap."""

        ...

    async def settle(
        self,
        reservation_id: str,
        *,
        terminal_fill_id: str,
    ) -> bool:
        """Mark existing partial evidence terminal without adding notional."""

        ...

    async def retain(self, reservation_id: str) -> bool:
        """Mark a POST-attempted reservation as non-expiring and blocking."""

        ...

    async def release(self, reservation_id: str) -> bool:
        """Release one still-pending reservation."""

        ...

    async def sweep_expired(self, now: datetime) -> int:
        """Expire stale pending reservations."""

        ...


class TaskBudgetPolicyEvaluator:
    """Pure Kreo-style task cap and one-entry decision policy."""

    def evaluate(
        self,
        *,
        request: TaskBudgetReservationRequest,
        policy: TaskSpendPolicy,
        snapshot: TaskBudgetSnapshot,
    ) -> str | None:
        """Return a rejection reason or ``None`` when the request fits."""

        if not policy.enabled:
            return None
        if policy.one_entry_per_market and snapshot.task_has_entry:
            return "task_one_entry_per_market"
        if policy.global_one_entry_per_market and snapshot.global_market_has_entry:
            return "global_one_entry_per_market"
        if (
            policy.one_entry_per_sports_type
            and request.key.sports_market_type
            and snapshot.task_has_sports_type_entry
        ):
            return "task_one_entry_per_sports_type"
        amount = request.amount
        checks = (
            (policy.max_total, snapshot.task_total, "task_max_total_spend_exceeded"),
            (policy.max_daily, snapshot.task_daily, "task_max_daily_spend_exceeded"),
            (
                policy.max_per_market,
                snapshot.task_market,
                "task_max_market_spend_exceeded",
            ),
        )
        for maximum, current, reason in checks:
            if maximum > 0 and current + amount > maximum:
                return reason
        return None


class InMemoryTaskBudgetRepository:
    """Deterministic atomic repository for unit tests and paper runtimes."""

    def __init__(
        self,
        *,
        evaluator: TaskBudgetPolicyEvaluator | None = None,
    ) -> None:
        self._evaluator = evaluator or TaskBudgetPolicyEvaluator()
        self._reservations: dict[str, TaskBudgetReservation] = {}
        self._fills: dict[str, TaskBudgetFill] = {}
        self._lock = asyncio.Lock()

    async def reserve(
        self,
        request: TaskBudgetReservationRequest,
        policy: TaskSpendPolicy,
    ) -> TaskBudgetReservationDecision:
        """Reserve task spend atomically in memory."""

        async with self._lock:
            existing = self._reservations.get(request.reservation_id)
            snapshot = self._snapshot(request.key, request.reserved_at)
            if any(identity in self._reservations for identity in request.prior_reservation_ids):
                return TaskBudgetReservationDecision(
                    False,
                    "task_budget_submission_retained",
                    False,
                    None,
                    snapshot,
                )
            if existing is not None:
                if existing.request != request:
                    return TaskBudgetReservationDecision(
                        False,
                        "task_budget_reservation_identity_conflict",
                        False,
                        None,
                        snapshot,
                    )
                if existing.status is TaskBudgetReservationStatus.RESERVED:
                    if existing.execution_state is TaskBudgetExecutionState.RETAINED:
                        return TaskBudgetReservationDecision(
                            False,
                            "task_budget_submission_retained",
                            False,
                            existing,
                            snapshot,
                        )
                    return TaskBudgetReservationDecision(
                        True,
                        "task_budget_already_reserved",
                        False,
                        existing,
                        snapshot,
                    )
                reason = (
                    "task_budget_already_committed"
                    if existing.status is TaskBudgetReservationStatus.COMMITTED
                    else "task_budget_reservation_not_active"
                )
                return TaskBudgetReservationDecision(
                    False,
                    reason,
                    False,
                    existing,
                    snapshot,
                )
            reason = self._evaluator.evaluate(
                request=request,
                policy=policy,
                snapshot=snapshot,
            )
            if reason is not None:
                return TaskBudgetReservationDecision(
                    False,
                    reason,
                    False,
                    None,
                    snapshot,
                )
            reservation = TaskBudgetReservation(request=request)
            self._reservations[request.reservation_id] = reservation
            return TaskBudgetReservationDecision(
                True,
                "task_budget_reserved",
                True,
                reservation,
                snapshot,
            )

    async def snapshot(
        self,
        key: TaskBudgetKey,
        now: datetime,
    ) -> TaskBudgetSnapshot:
        """Return one consistent in-memory budget snapshot."""

        if now.tzinfo is None:
            raise ValueError("task budget snapshot time must be timezone-aware")
        async with self._lock:
            return self._snapshot(key, now)

    async def commit(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
    ) -> bool:
        """Commit one terminal fill for backward-compatible callers."""

        application = await self.record_fill(
            reservation_id=reservation_id,
            fill_id=fill_id,
            filled_notional=filled_notional,
            occurred_at=occurred_at,
            terminal=True,
        )
        return application.created

    async def record_fill(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
        terminal: bool,
    ) -> TaskBudgetFillApplication:
        """Append one authoritative fill and retain any unsettled remainder."""

        amount = _decimal(filled_notional, name="filled_notional", positive=True)
        if occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        reservation_identity = str(reservation_id)
        fill_identity = str(fill_id)
        async with self._lock:
            existing_fill = self._fills.get(fill_identity)
            if existing_fill is not None:
                if (
                    existing_fill.reservation_id == reservation_identity
                    and existing_fill.notional == amount
                ):
                    reservation = self._reservations.get(reservation_identity)
                    if reservation is None:
                        raise TaskBudgetInvariantError("task budget reservation does not exist")
                    cumulative = self._filled_total(reservation_identity)
                    if terminal and reservation.status is TaskBudgetReservationStatus.RESERVED:
                        reservation = self._settled_reservation(
                            reservation,
                            terminal_fill_id=fill_identity,
                            cumulative=cumulative,
                        )
                        self._reservations[reservation_identity] = reservation
                    return self._fill_application(
                        reservation=reservation,
                        cumulative=cumulative,
                        created=False,
                    )
                raise TaskBudgetInvariantError("fill identity conflicts with task budget ledger")
            reservation = self._reservations.get(reservation_identity)
            if reservation is None:
                raise TaskBudgetInvariantError("task budget reservation does not exist")
            if reservation.status is TaskBudgetReservationStatus.COMMITTED:
                raise TaskBudgetInvariantError("task budget reservation already committed")
            if reservation.status is not TaskBudgetReservationStatus.RESERVED:
                raise TaskBudgetInvariantError(
                    f"cannot commit task budget reservation in {reservation.status.value} state"
                )
            cumulative = self._filled_total(reservation_identity) + amount
            if cumulative > reservation.request.amount:
                raise TaskBudgetInvariantError("filled notional exceeds task budget reservation")
            fill = TaskBudgetFill(
                fill_id=fill_identity,
                reservation_id=reservation_identity,
                key=reservation.request.key,
                notional=amount,
                occurred_at=occurred_at,
            )
            self._fills[fill.fill_id] = fill
            if terminal:
                reservation = self._settled_reservation(
                    reservation,
                    terminal_fill_id=fill.fill_id,
                    cumulative=cumulative,
                )
            else:
                reservation = TaskBudgetReservation(
                    request=reservation.request,
                    status=TaskBudgetReservationStatus.RESERVED,
                    execution_state=TaskBudgetExecutionState.RETAINED,
                    settled_notional=cumulative,
                )
            self._reservations[reservation_identity] = reservation
            return self._fill_application(
                reservation=reservation,
                cumulative=cumulative,
                created=True,
            )

    async def settle(
        self,
        reservation_id: str,
        *,
        terminal_fill_id: str,
    ) -> bool:
        """Mark an existing retained partial fill as terminal idempotently."""

        reservation_identity = str(reservation_id).strip()
        fill_identity = str(terminal_fill_id).strip()
        if not reservation_identity or not fill_identity:
            raise ValueError("reservation_id and terminal_fill_id are required")
        async with self._lock:
            reservation = self._reservations.get(reservation_identity)
            if reservation is None:
                raise TaskBudgetInvariantError("task budget reservation does not exist")
            if reservation.status is TaskBudgetReservationStatus.COMMITTED:
                if reservation.fill_id != fill_identity:
                    raise TaskBudgetInvariantError("terminal fill identity conflicts")
                return False
            if reservation.status is not TaskBudgetReservationStatus.RESERVED:
                raise TaskBudgetInvariantError(
                    f"cannot settle task budget reservation in {reservation.status.value} state"
                )
            fill = self._fills.get(fill_identity)
            if fill is None or fill.reservation_id != reservation_identity:
                raise TaskBudgetInvariantError("terminal task budget fill does not exist")
            cumulative = self._filled_total(reservation_identity)
            self._reservations[reservation_identity] = self._settled_reservation(
                reservation,
                terminal_fill_id=fill_identity,
                cumulative=cumulative,
            )
            return True

    async def retain(self, reservation_id: str) -> bool:
        """Make one active reservation non-expiring after a POST attempt."""

        async with self._lock:
            identity = str(reservation_id)
            existing = self._reservations.get(identity)
            if existing is None or existing.status is not TaskBudgetReservationStatus.RESERVED:
                return False
            if existing.execution_state is TaskBudgetExecutionState.RETAINED:
                return False
            self._reservations[identity] = TaskBudgetReservation(
                request=existing.request,
                status=existing.status,
                execution_state=TaskBudgetExecutionState.RETAINED,
                settled_notional=existing.settled_notional,
                fill_id=existing.fill_id,
                committed_notional=existing.committed_notional,
            )
            return True

    async def release(self, reservation_id: str) -> bool:
        """Release one pending reservation."""

        async with self._lock:
            existing = self._reservations.get(str(reservation_id))
            if existing is None or existing.status is not TaskBudgetReservationStatus.RESERVED:
                return False
            self._reservations[reservation_id] = TaskBudgetReservation(
                request=existing.request,
                status=TaskBudgetReservationStatus.RELEASED,
                execution_state=existing.execution_state,
                settled_notional=existing.settled_notional,
            )
            return True

    async def sweep_expired(self, now: datetime) -> int:
        """Expire pending reservations whose TTL elapsed."""

        if now.tzinfo is None:
            raise ValueError("sweep time must be timezone-aware")
        async with self._lock:
            count = 0
            for reservation_id, existing in tuple(self._reservations.items()):
                if (
                    existing.status is TaskBudgetReservationStatus.RESERVED
                    and existing.execution_state is TaskBudgetExecutionState.PRE_POST
                    and existing.request.expires_at <= now
                ):
                    self._reservations[reservation_id] = TaskBudgetReservation(
                        request=existing.request,
                        status=TaskBudgetReservationStatus.EXPIRED,
                        execution_state=existing.execution_state,
                        settled_notional=existing.settled_notional,
                    )
                    count += 1
            return count

    def _snapshot(self, key: TaskBudgetKey, now: datetime) -> TaskBudgetSnapshot:
        active = tuple(
            item
            for item in self._reservations.values()
            if item.status is TaskBudgetReservationStatus.RESERVED
            and (
                item.execution_state is TaskBudgetExecutionState.RETAINED
                or item.request.expires_at > now
            )
        )
        # An active reservation already contributes its full cap. Count its
        # partial fills only after the reservation settles, otherwise cap +
        # fills would double-count the same spend.
        fills = tuple(
            item
            for item in self._fills.values()
            if self._reservations[item.reservation_id].status
            is not TaskBudgetReservationStatus.RESERVED
        )
        same_task_reservations = tuple(
            item for item in active if item.request.key.task_id == key.task_id
        )
        same_task_fills = tuple(item for item in fills if item.key.task_id == key.task_id)
        day = now.astimezone(timezone.utc).date()
        return TaskBudgetSnapshot(
            task_total=sum(
                (item.request.amount for item in same_task_reservations),
                _ZERO,
            )
            + sum((item.notional for item in same_task_fills), _ZERO),
            task_daily=sum(
                (
                    item.request.amount
                    for item in same_task_reservations
                    if item.request.reserved_at.astimezone(timezone.utc).date() == day
                ),
                _ZERO,
            )
            + sum(
                (
                    item.notional
                    for item in same_task_fills
                    if item.occurred_at.astimezone(timezone.utc).date() == day
                ),
                _ZERO,
            ),
            task_market=sum(
                (
                    item.request.amount
                    for item in same_task_reservations
                    if item.request.key.market_id == key.market_id
                ),
                _ZERO,
            )
            + sum(
                (item.notional for item in same_task_fills if item.key.market_id == key.market_id),
                _ZERO,
            ),
            task_has_entry=any(
                item.request.key.market_id == key.market_id for item in same_task_reservations
            )
            or any(item.key.market_id == key.market_id for item in same_task_fills),
            global_market_has_entry=any(
                item.request.key.follower_account == key.follower_account
                and item.request.key.market_id == key.market_id
                for item in active
            )
            or any(
                item.key.follower_account == key.follower_account
                and item.key.market_id == key.market_id
                for item in fills
            ),
            task_has_sports_type_entry=(
                bool(key.sports_market_type)
                and (
                    any(
                        item.request.key.task_id == key.task_id
                        and item.request.key.sports_market_type == key.sports_market_type
                        for item in active
                    )
                    or any(
                        item.key.task_id == key.task_id
                        and item.key.sports_market_type == key.sports_market_type
                        for item in fills
                    )
                )
            ),
        )

    def _filled_total(self, reservation_id: str) -> Decimal:
        """Return exact accumulated notional for one reservation."""

        return sum(
            (
                fill.notional
                for fill in self._fills.values()
                if fill.reservation_id == reservation_id
            ),
            _ZERO,
        )

    @staticmethod
    def _settled_reservation(
        reservation: TaskBudgetReservation,
        *,
        terminal_fill_id: str,
        cumulative: Decimal,
    ) -> TaskBudgetReservation:
        """Return a terminal reservation carrying cumulative fill evidence."""

        return TaskBudgetReservation(
            request=reservation.request,
            status=TaskBudgetReservationStatus.COMMITTED,
            execution_state=TaskBudgetExecutionState.RETAINED,
            settled_notional=cumulative,
            fill_id=terminal_fill_id,
            committed_notional=cumulative,
        )

    @staticmethod
    def _fill_application(
        *,
        reservation: TaskBudgetReservation,
        cumulative: Decimal,
        created: bool,
    ) -> TaskBudgetFillApplication:
        """Build one immutable fill-application result."""

        return TaskBudgetFillApplication(
            created=created,
            cumulative_notional=cumulative,
            remaining_notional=max(_ZERO, reservation.request.amount - cumulative),
            status=reservation.status,
        )


class TaskBudgetReservationService:
    """Small application service around the atomic task budget repository."""

    def __init__(self, repository: TaskBudgetRepository) -> None:
        self._repository = repository

    async def snapshot(
        self,
        key: TaskBudgetKey,
        now: datetime,
    ) -> TaskBudgetSnapshot:
        """Return current spend state without mutating the budget ledger."""

        return await self._repository.snapshot(key, now)

    async def reserve(
        self,
        request: TaskBudgetReservationRequest,
        policy: TaskSpendPolicy,
    ) -> TaskBudgetReservationDecision:
        """Reserve task spend before downstream order submission."""

        return await self._repository.reserve(request, policy)

    async def commit(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
    ) -> bool:
        """Commit authoritative fill evidence exactly once."""

        return await self._repository.commit(
            reservation_id=reservation_id,
            fill_id=fill_id,
            filled_notional=filled_notional,
            occurred_at=occurred_at,
        )

    async def record_fill(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
        terminal: bool,
    ) -> TaskBudgetFillApplication:
        """Append one authoritative partial or terminal fill exactly once."""

        return await self._repository.record_fill(
            reservation_id=reservation_id,
            fill_id=fill_id,
            filled_notional=filled_notional,
            occurred_at=occurred_at,
            terminal=terminal,
        )

    async def settle(
        self,
        reservation_id: str,
        *,
        terminal_fill_id: str,
    ) -> bool:
        """Mark prior partial fill evidence terminal without new spend."""

        return await self._repository.settle(
            reservation_id,
            terminal_fill_id=terminal_fill_id,
        )

    async def retain(self, reservation_id: str) -> bool:
        """Retain a POST-attempted reservation without wall-clock expiry."""

        return await self._repository.retain(reservation_id)

    async def release(self, reservation_id: str) -> bool:
        """Release a rejected or definitely unsubmitted intent."""

        return await self._repository.release(reservation_id)

    async def sweep_expired(self, now: datetime) -> int:
        """Expire stale pending reservations."""

        return await self._repository.sweep_expired(now)


__all__ = [
    "InMemoryTaskBudgetRepository",
    "TaskBudgetExecutionState",
    "TaskBudgetFill",
    "TaskBudgetFillApplication",
    "TaskBudgetInvariantError",
    "TaskBudgetKey",
    "TaskBudgetPolicyEvaluator",
    "TaskBudgetRepository",
    "TaskBudgetReservation",
    "TaskBudgetReservationDecision",
    "TaskBudgetReservationRequest",
    "TaskBudgetReservationService",
    "TaskBudgetReservationStatus",
    "TaskBudgetSnapshot",
]
