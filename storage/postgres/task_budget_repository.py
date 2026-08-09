"""PostgreSQL adapter for race-safe copy-task spend reservations.

The adapter serializes task-total, task-market, global-market, and reservation
identity decisions with transaction-scoped advisory locks. Domain policy stays
in ``TaskBudgetPolicyEvaluator``; PostgreSQL supplies atomic snapshots and
persistence only.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

from services.copy_tasks.budget import (
    TaskBudgetInvariantError,
    TaskBudgetKey,
    TaskBudgetPolicyEvaluator,
    TaskBudgetReservation,
    TaskBudgetReservationDecision,
    TaskBudgetReservationRequest,
    TaskBudgetReservationStatus,
    TaskBudgetSnapshot,
)
from services.copy_tasks.domain import TaskSpendPolicy


class TransactionContext(Protocol):
    """Async transaction context used by the repository."""

    async def __aenter__(self) -> object:
        """Enter one database transaction."""

    async def __aexit__(self, *args: object) -> object:
        """Exit one database transaction."""


class DatabaseConnection(Protocol):
    """Narrow asyncpg-compatible connection surface."""

    def transaction(self) -> TransactionContext:
        """Return a transaction context manager."""

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one row."""

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return one scalar value."""

    async def execute(self, query: str, *args: object) -> str:
        """Execute one statement."""


class AcquireContext(Protocol):
    """Pool acquisition context manager."""

    async def __aenter__(self) -> DatabaseConnection:
        """Return one acquired connection."""

    async def __aexit__(self, *args: object) -> object:
        """Release one acquired connection."""


class DatabasePool(Protocol):
    """Narrow asyncpg-compatible pool surface."""

    def acquire(self) -> AcquireContext:
        """Acquire one connection."""


class PostgresTaskBudgetRepository:
    """Persist task budgets with transaction-scoped concurrency control."""

    def __init__(
        self,
        pool: DatabasePool,
        *,
        evaluator: TaskBudgetPolicyEvaluator | None = None,
    ) -> None:
        self._pool = pool
        self._evaluator = evaluator or TaskBudgetPolicyEvaluator()

    async def reserve(
        self,
        request: TaskBudgetReservationRequest,
        policy: TaskSpendPolicy,
    ) -> TaskBudgetReservationDecision:
        """Atomically evaluate and reserve task-level follower spend.

        Locks cover reservation identity, all spend for the follower task, and
        global one-entry state for the follower market. Sorting the lock names
        keeps concurrent workers from acquiring the same scopes in a different
        order.
        """

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                lock_names = [
                    f"reservation:{request.reservation_id}",
                    (
                        "task:"
                        f"{request.key.follower_account}:"
                        f"{request.key.task_id}"
                    ),
                    (
                        "market:"
                        f"{request.key.follower_account}:"
                        f"{request.key.market_id}"
                    ),
                ]
                if policy.one_entry_per_sports_type and request.key.sports_market_type:
                    lock_names.append(
                        f"sports:{request.key.task_id}:{request.key.sports_market_type}"
                    )
                await self._acquire_locks(connection, tuple(lock_names))
                existing_row = await connection.fetchrow(
                    self._reservation_select_sql(),
                    request.reservation_id,
                )
                snapshot = await self._load_snapshot(
                    connection,
                    key=request.key,
                    now=request.reserved_at,
                )
                if existing_row is not None:
                    return self._existing_decision(
                        request=request,
                        row=existing_row,
                        snapshot=snapshot,
                    )

                reason = self._evaluator.evaluate(
                    request=request,
                    policy=policy,
                    snapshot=snapshot,
                )
                if reason is not None:
                    return TaskBudgetReservationDecision(
                        allowed=False,
                        reason=reason,
                        created=False,
                        reservation=None,
                        snapshot=snapshot,
                    )

                inserted_row = await connection.fetchrow(
                    """
                    INSERT INTO copy_task_budget_reservations
                        (reservation_id, follower_account, task_id, market_id,
                         amount, sports_market_type, reserved_at, expires_at,
                         status, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'reserved', NOW())
                    RETURNING reservation_id, follower_account, task_id, market_id,
                              sports_market_type, amount, reserved_at, expires_at,
                              status, fill_id, committed_notional
                    """,
                    request.reservation_id,
                    request.key.follower_account,
                    request.key.task_id,
                    request.key.market_id,
                    request.amount,
                    request.key.sports_market_type,
                    request.reserved_at,
                    request.expires_at,
                )
                if inserted_row is None:
                    raise TaskBudgetInvariantError(
                        "task budget reservation insert returned no row"
                    )
                return TaskBudgetReservationDecision(
                    allowed=True,
                    reason="task_budget_reserved",
                    created=True,
                    reservation=self._reservation(inserted_row),
                    snapshot=snapshot,
                )

    async def commit(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
    ) -> bool:
        """Commit authoritative fill evidence exactly once."""

        reservation_identity = self._required_text(
            reservation_id,
            name="reservation_id",
        )
        fill_identity = self._required_text(fill_id, name="fill_id")
        amount = self._positive_decimal(
            filled_notional,
            name="filled_notional",
        )
        if occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._acquire_locks(
                    connection,
                    (
                        f"reservation:{reservation_identity}",
                        f"fill:{fill_identity}",
                    ),
                )
                existing_fill = await connection.fetchrow(
                    """
                    SELECT fill_id, reservation_id, notional
                      FROM copy_task_fill_ledger
                     WHERE fill_id = $1
                     FOR UPDATE
                    """,
                    fill_identity,
                )
                reservation_row = await connection.fetchrow(
                    self._reservation_select_sql(),
                    reservation_identity,
                )

                if existing_fill is not None:
                    existing_reservation_id = self._text(
                        existing_fill,
                        "reservation_id",
                    )
                    existing_amount = self._decimal(
                        existing_fill,
                        "notional",
                    )
                    if (
                        existing_reservation_id == reservation_identity
                        and existing_amount == amount
                    ):
                        return False
                    raise TaskBudgetInvariantError(
                        "fill identity conflicts with task budget ledger"
                    )
                if reservation_row is None:
                    raise TaskBudgetInvariantError(
                        "task budget reservation does not exist"
                    )

                reservation = self._reservation(reservation_row)
                if reservation.status is TaskBudgetReservationStatus.COMMITTED:
                    if (
                        reservation.fill_id == fill_identity
                        and reservation.committed_notional == amount
                    ):
                        return False
                    raise TaskBudgetInvariantError(
                        "task budget reservation already committed"
                    )
                if reservation.status is not TaskBudgetReservationStatus.RESERVED:
                    raise TaskBudgetInvariantError(
                        "cannot commit task budget reservation in "
                        f"{reservation.status.value} state"
                    )
                if amount > reservation.request.amount:
                    raise TaskBudgetInvariantError(
                        "filled notional exceeds task budget reservation"
                    )

                await connection.execute(
                    """
                    INSERT INTO copy_task_fill_ledger
                        (fill_id, reservation_id, follower_account, task_id,
                         market_id, sports_market_type, notional, occurred_at,
                         created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                    """,
                    fill_identity,
                    reservation_identity,
                    reservation.request.key.follower_account,
                    reservation.request.key.task_id,
                    reservation.request.key.market_id,
                    reservation.request.key.sports_market_type,
                    amount,
                    occurred_at,
                )
                await connection.execute(
                    """
                    UPDATE copy_task_budget_reservations
                       SET status = 'committed',
                           fill_id = $2,
                           committed_notional = $3,
                           updated_at = NOW()
                     WHERE reservation_id = $1
                    """,
                    reservation_identity,
                    fill_identity,
                    amount,
                )
                return True

    async def release(self, reservation_id: str) -> bool:
        """Release one pending reservation without touching committed spend."""

        identity = self._required_text(reservation_id, name="reservation_id")
        async with self._pool.acquire() as connection:
            count = await connection.fetchval(
                """
                WITH released AS (
                    UPDATE copy_task_budget_reservations
                       SET status = 'released', updated_at = NOW()
                     WHERE reservation_id = $1
                       AND status = 'reserved'
                    RETURNING 1
                )
                SELECT COUNT(*) FROM released
                """,
                identity,
            )
        return int(count or 0) > 0

    async def sweep_expired(self, now: datetime) -> int:
        """Expire pending task reservations whose TTL has elapsed."""

        if now.tzinfo is None:
            raise ValueError("sweep time must be timezone-aware")
        async with self._pool.acquire() as connection:
            count = await connection.fetchval(
                """
                WITH expired AS (
                    UPDATE copy_task_budget_reservations
                       SET status = 'expired', updated_at = NOW()
                     WHERE status = 'reserved'
                       AND expires_at <= $1
                    RETURNING 1
                )
                SELECT COUNT(*) FROM expired
                """,
                now,
            )
        return int(count or 0)

    async def _load_snapshot(
        self,
        connection: DatabaseConnection,
        *,
        key: TaskBudgetKey,
        now: datetime,
    ) -> TaskBudgetSnapshot:
        row = await connection.fetchrow(
            """
            WITH active_reservations AS (
                SELECT follower_account, task_id, market_id, sports_market_type,
                       amount, reserved_at
                  FROM copy_task_budget_reservations
                 WHERE status = 'reserved'
                   AND expires_at > $4
            ), committed_fills AS (
                SELECT follower_account, task_id, market_id, sports_market_type,
                       notional, occurred_at
                  FROM copy_task_fill_ledger
            )
            SELECT
                COALESCE((
                    SELECT SUM(amount)
                      FROM active_reservations
                     WHERE task_id = $1
                ), 0) + COALESCE((
                    SELECT SUM(notional)
                      FROM committed_fills
                     WHERE task_id = $1
                ), 0) AS task_total,
                COALESCE((
                    SELECT SUM(amount)
                      FROM active_reservations
                     WHERE task_id = $1
                       AND (reserved_at AT TIME ZONE 'UTC')::date =
                           ($4 AT TIME ZONE 'UTC')::date
                ), 0) + COALESCE((
                    SELECT SUM(notional)
                      FROM committed_fills
                     WHERE task_id = $1
                       AND (occurred_at AT TIME ZONE 'UTC')::date =
                           ($4 AT TIME ZONE 'UTC')::date
                ), 0) AS task_daily,
                COALESCE((
                    SELECT SUM(amount)
                      FROM active_reservations
                     WHERE task_id = $1 AND market_id = $3
                ), 0) + COALESCE((
                    SELECT SUM(notional)
                      FROM committed_fills
                     WHERE task_id = $1 AND market_id = $3
                ), 0) AS task_market,
                EXISTS (
                    SELECT 1 FROM active_reservations
                     WHERE task_id = $1 AND market_id = $3
                ) OR EXISTS (
                    SELECT 1 FROM committed_fills
                     WHERE task_id = $1 AND market_id = $3
                ) AS task_has_entry,
                EXISTS (
                    SELECT 1 FROM active_reservations
                     WHERE follower_account = $2 AND market_id = $3
                ) OR EXISTS (
                    SELECT 1 FROM committed_fills
                     WHERE follower_account = $2 AND market_id = $3
                ) AS global_market_has_entry,
                CASE
                    WHEN $5 = '' THEN FALSE
                    ELSE (
                        EXISTS (
                            SELECT 1 FROM active_reservations
                             WHERE task_id = $1 AND sports_market_type = $5
                        ) OR EXISTS (
                            SELECT 1 FROM committed_fills
                             WHERE task_id = $1 AND sports_market_type = $5
                        )
                    )
                END AS task_has_sports_type_entry
            """,
            key.task_id,
            key.follower_account,
            key.market_id,
            now,
            key.sports_market_type,
        )
        if row is None:
            raise TaskBudgetInvariantError("task budget snapshot returned no row")
        return TaskBudgetSnapshot(
            task_total=self._decimal(row, "task_total"),
            task_daily=self._decimal(row, "task_daily"),
            task_market=self._decimal(row, "task_market"),
            task_has_entry=self._boolean(row, "task_has_entry"),
            global_market_has_entry=self._boolean(
                row,
                "global_market_has_entry",
            ),
            task_has_sports_type_entry=self._boolean(
                row,
                "task_has_sports_type_entry",
            ),
        )

    async def _acquire_locks(
        self,
        connection: DatabaseConnection,
        names: tuple[str, ...],
    ) -> None:
        for name in sorted(set(names)):
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                name,
            )

    def _existing_decision(
        self,
        *,
        request: TaskBudgetReservationRequest,
        row: Mapping[str, object],
        snapshot: TaskBudgetSnapshot,
    ) -> TaskBudgetReservationDecision:
        reservation = self._reservation(row)
        if reservation.request != request:
            return TaskBudgetReservationDecision(
                allowed=False,
                reason="task_budget_reservation_identity_conflict",
                created=False,
                reservation=None,
                snapshot=snapshot,
            )
        if reservation.status is TaskBudgetReservationStatus.RESERVED:
            return TaskBudgetReservationDecision(
                allowed=True,
                reason="task_budget_already_reserved",
                created=False,
                reservation=reservation,
                snapshot=snapshot,
            )
        if reservation.status is TaskBudgetReservationStatus.COMMITTED:
            reason = "task_budget_already_committed"
        else:
            reason = "task_budget_reservation_not_active"
        return TaskBudgetReservationDecision(
            allowed=False,
            reason=reason,
            created=False,
            reservation=reservation,
            snapshot=snapshot,
        )

    def _reservation(self, row: Mapping[str, object]) -> TaskBudgetReservation:
        fill_id = row.get("fill_id")
        if fill_id is not None and not isinstance(fill_id, str):
            raise ValueError("task budget fill_id must be text or null")
        committed_raw = row.get("committed_notional")
        committed = (
            None
            if committed_raw is None
            else self._decimal(row, "committed_notional")
        )
        return TaskBudgetReservation(
            request=TaskBudgetReservationRequest(
                reservation_id=self._text(row, "reservation_id"),
                key=TaskBudgetKey(
                    follower_account=self._text(row, "follower_account"),
                    task_id=self._text(row, "task_id"),
                    market_id=self._text(row, "market_id"),
                    sports_market_type=self._optional_text(
                        row,
                        "sports_market_type",
                    ),
                ),
                amount=self._decimal(row, "amount"),
                reserved_at=self._datetime(row, "reserved_at"),
                expires_at=self._datetime(row, "expires_at"),
            ),
            status=TaskBudgetReservationStatus(self._text(row, "status")),
            fill_id=fill_id,
            committed_notional=committed,
        )

    @staticmethod
    def _reservation_select_sql() -> str:
        return """
            SELECT reservation_id, follower_account, task_id, market_id,
                   sports_market_type, amount, reserved_at, expires_at, status, fill_id,
                   committed_notional
              FROM copy_task_budget_reservations
             WHERE reservation_id = $1
             FOR UPDATE
        """

    @staticmethod
    def _required_text(value: object, *, name: str) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError(f"{name} is required")
        return result

    @staticmethod
    def _text(row: Mapping[str, object], name: str) -> str:
        value = row.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"task budget {name} must be non-empty text")
        return value

    @staticmethod
    def _optional_text(row: Mapping[str, object], name: str) -> str:
        """Return an optional normalized text field from one database row."""

        value = row.get(name, "")
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError(f"task budget {name} must be text")
        return value.strip().casefold()

    @staticmethod
    def _decimal(row: Mapping[str, object], name: str) -> Decimal:
        value = row.get(name)
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"task budget {name} must be decimal-like") from exc
        if not result.is_finite() or result < 0:
            raise ValueError(f"task budget {name} must be finite and non-negative")
        return result

    @staticmethod
    def _positive_decimal(value: object, *, name: str) -> Decimal:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be decimal-like") from exc
        if not result.is_finite() or result <= 0:
            raise ValueError(f"{name} must be finite and positive")
        return result

    @staticmethod
    def _datetime(row: Mapping[str, object], name: str) -> datetime:
        value = row.get(name)
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError(f"task budget {name} must be timezone-aware datetime")
        return value

    @staticmethod
    def _boolean(row: Mapping[str, object], name: str) -> bool:
        value = row.get(name)
        if not isinstance(value, bool):
            raise ValueError(f"task budget {name} must be boolean")
        return value


__all__ = ["PostgresTaskBudgetRepository"]
