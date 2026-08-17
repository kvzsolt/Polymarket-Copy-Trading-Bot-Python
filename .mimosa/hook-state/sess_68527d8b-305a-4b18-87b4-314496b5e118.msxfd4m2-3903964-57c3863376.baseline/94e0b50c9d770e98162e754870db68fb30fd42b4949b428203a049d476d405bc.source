"""Bounded, side-effect-free PostgreSQL projections for the control plane."""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol, TypeVar

from services.control_plane.contracts import (
    ActivityView,
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryCandidateView,
    DiscoveryCycleView,
    DiscoveryRosterView,
    DiscoveryStatusView,
    OrderView,
    Page,
    PageRequest,
    PositionView,
    ReentryLockView,
    ReservationView,
    TaskDetailView,
    TaskView,
)
from storage.postgres.copy_task_repository import CopyTaskCodec


T = TypeVar("T")


class DatabasePool(Protocol):
    """Small direct-query surface needed by read projections."""

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, object]]:
        """Return a sequence of rows."""

        ...

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one optional row."""

        ...

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the first scalar from an optional row."""

        ...


class Clock(Protocol):
    """Clock used only for local discovery status observation time."""

    def now(self) -> datetime:
        """Return the current timezone-aware time."""

        ...


class ControlPlaneCursorCodec:
    """Encode typed keyset state into an opaque URL-safe cursor."""

    VERSION = 1

    def encode(self, kind: str, values: tuple[str, ...]) -> str:
        """Return an opaque cursor bound to one query kind."""

        if not kind or not values or any(not isinstance(value, str) for value in values):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        payload = json.dumps(
            {"kind": kind, "values": list(values), "version": self.VERSION},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii")

    def decode(self, cursor: str | None, kind: str, width: int) -> tuple[str, ...]:
        """Decode a cursor only for its issuing query and tuple width."""

        if cursor is None:
            return tuple("" for _ in range(width))
        try:
            decoded = base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True)
            payload = json.loads(decoded.decode("utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError
            values = payload.get("values")
            if (
                payload.get("version") != self.VERSION
                or payload.get("kind") != kind
                or not isinstance(values, list)
                or len(values) != width
                or any(not isinstance(value, str) or not value for value in values)
            ):
                raise ValueError
            return tuple(values)
        except (UnicodeError, ValueError, TypeError, json.JSONDecodeError):
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE) from None


class PostgresControlPlaneQueryRepository:
    """Project existing durable truth into sanitized immutable DTO pages."""

    def __init__(
        self,
        pool: DatabasePool,
        *,
        clock: Clock,
        cursor_codec: ControlPlaneCursorCodec | None = None,
        task_codec: CopyTaskCodec | None = None,
    ) -> None:
        """Store narrow database, time, cursor, and task-codec collaborators."""

        self._pool = pool
        self._clock = clock
        self._cursor = cursor_codec or ControlPlaneCursorCodec()
        self._task_codec = task_codec or CopyTaskCodec()

    async def list_tasks(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        """Return tasks in stable identity order, including soft-deleted tasks."""

        (after_task_id,) = self._cursor.decode(page.cursor, "tasks", 1)
        rows = await self._fetch(
            """
            SELECT task_id, name, leader_wallet, follower_account, status,
                   monitoring_mode, sizing_mode, version,
                   notifications_muted
            FROM copy_tasks
            WHERE follower_account = $1
              AND task_id > $2
            ORDER BY task_id ASC
            LIMIT $3
            """,
            follower_account,
            after_task_id,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="tasks",
            mapper=self._task_view,
            cursor_values=lambda row: (self._text(row, "task_id"),),
        )

    async def get_task(self, task_id: str) -> TaskDetailView | None:
        """Return one complete typed task detail."""

        row = await self._fetchrow(
            """
            SELECT task_id, name, leader_wallet, follower_account, status,
                   monitoring_mode, sizing_mode, notifications_muted,
                   version, configuration
            FROM copy_tasks
            WHERE task_id = $1
            """,
            task_id,
        )
        if row is None:
            return None
        try:
            return TaskDetailView.from_domain(self._task_codec.decode_row(row))
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None

    async def count_active_tasks(self) -> int:
        """Return the current durable active-task count through one scalar query."""

        try:
            value = await self._pool.fetchval(
                """
                SELECT COUNT(*)
                FROM copy_tasks
                WHERE status = 'active'
                """
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return value

    async def list_positions(
        self,
        follower_account: str,
        page: PageRequest,
    ) -> Page[PositionView]:
        """Return open positions through condition/token keyset pagination."""

        condition_id, token_id = self._cursor.decode(page.cursor, "positions", 2)
        rows = await self._fetch(
            """
            SELECT user_id, condition_id, token_id, leader_id, size_tokens,
                   avg_entry_price, updated_at
            FROM positions
            WHERE user_id = $1
              AND size_tokens::numeric > 0
              AND (condition_id, token_id) > ($2, $3)
            ORDER BY condition_id ASC, token_id ASC
            LIMIT $4
            """,
            follower_account,
            condition_id,
            token_id,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="positions",
            mapper=self._position_view,
            cursor_values=lambda row: (
                self._text(row, "condition_id"),
                self._text(row, "token_id"),
            ),
        )

    async def get_position(
        self,
        follower_account: str,
        position_id: str,
    ) -> PositionView | None:
        """Return one open position by follower and raw outcome token ID."""

        row = await self._fetchrow(
            """
            SELECT user_id, condition_id, token_id, leader_id, size_tokens,
                   avg_entry_price, updated_at
            FROM positions
            WHERE user_id = $1
              AND token_id = $2
              AND size_tokens::numeric > 0
            """,
            follower_account,
            position_id,
        )
        return None if row is None else self._position_view(row)

    async def list_orders(
        self,
        follower_account: str,
        page: PageRequest,
    ) -> Page[OrderView]:
        """Return sanitized orders without raw request/response/error bodies."""

        updated_at_cursor, key_cursor = self._cursor.decode(page.cursor, "orders", 2)
        self._validate_cursor_datetime(updated_at_cursor)
        updated_at_argument: str | None = updated_at_cursor or None
        rows = await self._fetch(
            """
            SELECT idempotency_key, user_id, leader_id, order_id, status,
                   request_json, matched_total, created_at, updated_at
            FROM orders
            WHERE user_id = $1
              AND (
                    $2::timestamptz IS NULL
                    OR (updated_at, idempotency_key) < ($2::timestamptz, $3)
              )
            ORDER BY updated_at DESC, idempotency_key DESC
            LIMIT $4
            """,
            follower_account,
            updated_at_argument,
            key_cursor,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="orders",
            mapper=self._order_view,
            cursor_values=lambda row: (
                self._datetime(row, "updated_at").isoformat(),
                self._text(row, "idempotency_key"),
            ),
        )

    async def get_order(
        self,
        follower_account: str,
        order_id: str,
    ) -> OrderView | None:
        """Return one order by follower and durable idempotency key."""

        row = await self._fetchrow(
            """
            SELECT idempotency_key, user_id, leader_id, order_id, status,
                   request_json, matched_total, created_at, updated_at
            FROM orders
            WHERE user_id = $1
              AND idempotency_key = $2
            """,
            follower_account,
            order_id,
        )
        return None if row is None else self._order_view(row)

    async def list_activity(
        self,
        follower_account: str | None,
        page: PageRequest,
    ) -> Page[ActivityView]:
        """Return append-only activity without unstructured details."""

        (sequence_cursor,) = self._cursor.decode(page.cursor, "activity", 1)
        sequence = self._cursor_integer(sequence_cursor) if sequence_cursor else 9223372036854775807
        rows = await self._fetch(
            """
            SELECT audit_sequence, event_id, actor, action, target_type,
                   target_id, outcome, reason_code, occurred_at
            FROM control_plane_audit_log
            WHERE ($1::text IS NULL OR follower_account = $1)
              AND audit_sequence < $2
            ORDER BY audit_sequence DESC
            LIMIT $3
            """,
            follower_account,
            sequence,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="activity",
            mapper=self._activity_view,
            cursor_values=lambda row: (str(self._integer(row, "audit_sequence")),),
        )

    async def list_discovery_cycles(self, page: PageRequest) -> Page[DiscoveryCycleView]:
        """Return discovery cycles without stored error detail."""

        started_cursor, cycle_cursor = self._cursor.decode(page.cursor, "discovery_cycles", 2)
        started_argument = self._cursor_float(started_cursor) if started_cursor else None
        rows = await self._fetch(
            """
            SELECT cycle_id, started_at, completed_at, status, config_hash,
                   candidate_count, kept_count
            FROM discovery_cycles
            WHERE (
                    $1::double precision IS NULL
                    OR (started_at, cycle_id) < ($1::double precision, $2)
                  )
            ORDER BY started_at DESC, cycle_id DESC
            LIMIT $3
            """,
            started_argument,
            cycle_cursor,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="discovery_cycles",
            mapper=self._discovery_cycle_view,
            cursor_values=lambda row: (
                str(row["started_at"]),
                self._text(row, "cycle_id"),
            ),
        )

    async def get_discovery_status(self) -> DiscoveryStatusView:
        """Return state derived from the latest durable discovery cycle."""

        row = await self._fetchrow(
            """
            SELECT cycle_id, status
            FROM discovery_cycles
            ORDER BY started_at DESC, cycle_id DESC
            LIMIT 1
            """
        )
        if row is None:
            return DiscoveryStatusView(
                state="never_run",
                current_run_id=None,
                last_cycle_id=None,
                observed_at=self._clock.now(),
            )
        status = self._text(row, "status")
        cycle_id = self._text(row, "cycle_id")
        return DiscoveryStatusView(
            state=status,
            current_run_id=cycle_id if status in {"pending", "running"} else None,
            last_cycle_id=cycle_id,
            observed_at=self._clock.now(),
        )

    async def list_discovery_candidates(
        self,
        page: PageRequest,
    ) -> Page[DiscoveryCandidateView]:
        """Return the latest completed cycle's ranked candidates."""

        score_cursor, wallet_cursor = self._cursor.decode(page.cursor, "discovery_candidates", 2)
        score_argument = self._cursor_float(score_cursor) if score_cursor else None
        rows = await self._fetch(
            """
            WITH latest_cycle AS (
                SELECT cycle_id
                FROM discovery_cycles
                WHERE status = 'completed'
                ORDER BY completed_at DESC NULLS LAST, cycle_id DESC
                LIMIT 1
            )
            SELECT snapshots.cycle_id, snapshots.wallet,
                   snapshots.final_score, snapshots.rejected_reason
            FROM discovery_candidate_snapshots AS snapshots
            JOIN latest_cycle USING (cycle_id)
            WHERE (
                    $1::double precision IS NULL
                    OR snapshots.final_score < $1::double precision
                    OR (
                        snapshots.final_score = $1::double precision
                        AND snapshots.wallet > $2
                    )
                  )
            ORDER BY snapshots.final_score DESC, snapshots.wallet ASC
            LIMIT $3
            """,
            score_argument,
            wallet_cursor,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="discovery_candidates",
            mapper=self._discovery_candidate_view,
            cursor_values=lambda row: (
                str(row["final_score"]),
                self._text(row, "wallet"),
            ),
        )

    async def list_discovery_roster(
        self,
        page: PageRequest,
    ) -> Page[DiscoveryRosterView]:
        """Return the current roster without provider payloads."""

        score_cursor, wallet_cursor = self._cursor.decode(page.cursor, "discovery_roster", 2)
        score_argument = self._cursor_float(score_cursor) if score_cursor else None
        rows = await self._fetch(
            """
            SELECT wallet, tier, score, updated_at
            FROM leader_roster
            WHERE (
                    $1::double precision IS NULL
                    OR score < $1::double precision
                    OR (score = $1::double precision AND wallet > $2)
                  )
            ORDER BY score DESC, wallet ASC
            LIMIT $3
            """,
            score_argument,
            wallet_cursor,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="discovery_roster",
            mapper=self._discovery_roster_view,
            cursor_values=lambda row: (
                str(row["score"]),
                self._text(row, "wallet"),
            ),
        )

    async def list_reservations(
        self,
        follower_account: str,
        page: PageRequest,
    ) -> Page[ReservationView]:
        """Return task-budget reservations through identity keyset pagination."""

        (reservation_cursor,) = self._cursor.decode(page.cursor, "reservations", 1)
        rows = await self._fetch(
            """
            SELECT reservation_id, follower_account, task_id, market_id,
                   amount, status, reserved_at, expires_at
            FROM copy_task_budget_reservations
            WHERE follower_account = $1
              AND reservation_id > $2
            ORDER BY reservation_id ASC
            LIMIT $3
            """,
            follower_account,
            reservation_cursor,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="reservations",
            mapper=self._reservation_view,
            cursor_values=lambda row: (self._text(row, "reservation_id"),),
        )

    async def list_reentry_locks(
        self,
        follower_account: str,
        page: PageRequest,
    ) -> Page[ReentryLockView]:
        """Return protective locks with their exact CAS revisions."""

        task_cursor, leader_cursor, market_cursor = self._cursor.decode(
            page.cursor, "reentry_locks", 3
        )
        rows = await self._fetch(
            """
            SELECT follower_account, task_id, leader_wallet, market_id,
                   reason, fill_id, locked_at, revision
            FROM market_reentry_locks
            WHERE follower_account = $1
              AND (task_id, leader_wallet, market_id) > ($2, $3, $4)
            ORDER BY task_id ASC, leader_wallet ASC, market_id ASC
            LIMIT $5
            """,
            follower_account,
            task_cursor,
            leader_cursor,
            market_cursor,
            page.limit + 1,
        )
        return self._page(
            rows,
            page,
            kind="reentry_locks",
            mapper=self._reentry_lock_view,
            cursor_values=lambda row: (
                self._text(row, "task_id"),
                self._text(row, "leader_wallet"),
                self._text(row, "market_id"),
            ),
        )

    async def _fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, object]]:
        """Run a read and sanitize database failures."""

        try:
            return await self._pool.fetch(query, *args)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None

    async def _fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Run a single-row read and sanitize database failures."""

        try:
            return await self._pool.fetchrow(query, *args)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None

    def _page(
        self,
        rows: Sequence[Mapping[str, object]],
        page: PageRequest,
        *,
        kind: str,
        mapper: Callable[[Mapping[str, object]], T],
        cursor_values: Callable[[Mapping[str, object]], tuple[str, ...]],
    ) -> Page[T]:
        """Map limit-plus-one rows and issue a cursor from the last visible row."""

        visible = rows[: page.limit]
        try:
            items = tuple(mapper(row) for row in visible)
            next_cursor = (
                self._cursor.encode(kind, cursor_values(visible[-1]))
                if len(rows) > page.limit and visible
                else None
            )
            return Page(items=items, next_cursor=next_cursor)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None

    def _task_view(self, row: Mapping[str, object]) -> TaskView:
        """Translate one task summary row."""

        return TaskView(
            task_id=self._text(row, "task_id"),
            name=self._text(row, "name"),
            leader_wallet=self._text(row, "leader_wallet"),
            follower_account=self._text(row, "follower_account"),
            status=self._text(row, "status"),
            monitoring_mode=self._text(row, "monitoring_mode"),
            sizing_mode=self._text(row, "sizing_mode"),
            version=self._integer(row, "version"),
            notifications_muted=bool(row.get("notifications_muted")),
        )

    def _position_view(self, row: Mapping[str, object]) -> PositionView:
        """Translate one position row."""

        return PositionView(
            follower_account=self._text(row, "user_id"),
            condition_id=self._text(row, "condition_id"),
            token_id=self._text(row, "token_id"),
            leader_wallet=self._optional_text(row, "leader_id"),
            size=self._decimal_text(row, "size_tokens"),
            avg_entry_price=self._decimal_text(row, "avg_entry_price"),
            updated_at=self._datetime(row, "updated_at"),
        )

    def _order_view(self, row: Mapping[str, object]) -> OrderView:
        """Translate only allowlisted order fields."""

        request = row.get("request_json")
        safe_request = self._json_mapping(request)
        return OrderView(
            idempotency_key=self._text(row, "idempotency_key"),
            follower_account=self._text(row, "user_id"),
            leader_wallet=self._optional_text(row, "leader_id"),
            order_id=self._optional_text(row, "order_id"),
            status=self._text(row, "status"),
            side=self._optional_mapping_text(safe_request, "side"),
            amount=self._optional_mapping_decimal_names(safe_request, "amount", "size"),
            price_limit=self._optional_mapping_decimal_names(
                safe_request,
                "price_limit",
                "worst_price",
                "price",
            ),
            matched_total=self._decimal_text(row, "matched_total"),
            created_at=self._datetime(row, "created_at"),
            updated_at=self._datetime(row, "updated_at"),
        )

    def _activity_view(self, row: Mapping[str, object]) -> ActivityView:
        """Translate one append-only activity row."""

        return ActivityView(
            activity_id=self._text(row, "event_id"),
            actor=self._text(row, "actor"),
            action=self._text(row, "action"),
            target_type=self._text(row, "target_type"),
            target_id=self._text(row, "target_id"),
            outcome=self._text(row, "outcome"),
            reason_code=self._text(row, "reason_code"),
            occurred_at=self._datetime(row, "occurred_at"),
        )

    def _discovery_cycle_view(self, row: Mapping[str, object]) -> DiscoveryCycleView:
        """Translate one discovery cycle without error detail."""

        completed = row.get("completed_at")
        return DiscoveryCycleView(
            cycle_id=self._text(row, "cycle_id"),
            status=self._text(row, "status"),
            started_at=self._epoch_datetime(row.get("started_at")),
            completed_at=(self._epoch_datetime(completed) if completed is not None else None),
            candidate_count=self._integer(row, "candidate_count"),
            kept_count=self._integer(row, "kept_count"),
            config_revision=self._text(row, "config_hash"),
        )

    def _discovery_candidate_view(
        self,
        row: Mapping[str, object],
    ) -> DiscoveryCandidateView:
        """Translate one candidate without provider payloads."""

        rejected = self._optional_text(row, "rejected_reason")
        return DiscoveryCandidateView(
            wallet=self._text(row, "wallet"),
            score=self._decimal_value(row.get("final_score")),
            accepted=rejected is None,
            reason_code=rejected or "accepted",
            cycle_id=self._text(row, "cycle_id"),
        )

    def _discovery_roster_view(self, row: Mapping[str, object]) -> DiscoveryRosterView:
        """Translate one leader roster row."""

        tier = self._text(row, "tier")
        return DiscoveryRosterView(
            wallet=self._text(row, "wallet"),
            tier=tier,
            active=tier == "active",
            score=self._decimal_value(row.get("score")),
            updated_at=self._epoch_datetime(row.get("updated_at")),
        )

    def _reservation_view(self, row: Mapping[str, object]) -> ReservationView:
        """Translate one exact task-budget reservation row."""

        return ReservationView(
            reservation_id=self._text(row, "reservation_id"),
            follower_account=self._text(row, "follower_account"),
            task_id=self._text(row, "task_id"),
            market_id=self._text(row, "market_id"),
            amount=self._decimal_text(row, "amount"),
            status=self._text(row, "status"),
            reserved_at=self._datetime(row, "reserved_at"),
            expires_at=self._datetime(row, "expires_at"),
        )

    def _reentry_lock_view(self, row: Mapping[str, object]) -> ReentryLockView:
        """Translate one versioned protective lock row."""

        return ReentryLockView(
            follower_account=self._text(row, "follower_account"),
            task_id=self._text(row, "task_id"),
            leader_wallet=self._text(row, "leader_wallet"),
            market_id=self._text(row, "market_id"),
            reason=self._text(row, "reason"),
            fill_id=self._text(row, "fill_id"),
            locked_at=self._datetime(row, "locked_at"),
            revision=self._integer(row, "revision"),
        )

    @staticmethod
    def _text(row: Mapping[str, object], name: str) -> str:
        """Return non-empty text from a row."""

        value = row.get(name)
        if not isinstance(value, str) or not value:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return value

    @classmethod
    def _optional_text(cls, row: Mapping[str, object], name: str) -> str | None:
        """Return optional non-empty text from a row."""

        value = row.get(name)
        if value is None:
            return None
        return cls._text(row, name)

    @staticmethod
    def _datetime(row: Mapping[str, object], name: str) -> datetime:
        """Return a timezone-aware row timestamp."""

        value = row.get(name)
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return value

    @staticmethod
    def _epoch_datetime(value: object) -> datetime:
        """Convert an existing finite epoch value to a UTC timestamp."""

        try:
            timestamp = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        if not timestamp.is_finite():
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return datetime.fromtimestamp(float(timestamp), tz=UTC)

    @staticmethod
    def _integer(row: Mapping[str, object], name: str) -> int:
        """Return an integer field while rejecting booleans."""

        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        try:
            result = int(value)
        except (TypeError, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        return result

    @staticmethod
    def _cursor_integer(value: str) -> int:
        """Parse one integer cursor key or return a stable page error."""

        try:
            parsed = int(value)
        except ValueError:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE) from None
        if parsed < 0 or parsed > 9223372036854775807:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)
        return parsed

    @staticmethod
    def _cursor_float(value: str) -> float:
        """Parse one finite analytical cursor key."""

        try:
            parsed = Decimal(value)
        except InvalidOperation:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE) from None
        if not parsed.is_finite():
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)
        converted = float(parsed)
        if not math.isfinite(converted):
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)
        return converted

    @staticmethod
    def _validate_cursor_datetime(value: str) -> None:
        """Validate an optional timezone-aware ISO cursor key."""

        if not value:
            return
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE) from None
        if parsed.tzinfo is None:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)

    @classmethod
    def _decimal_text(cls, row: Mapping[str, object], name: str) -> str:
        """Return a finite decimal field as exact text."""

        return cls._decimal_value(row.get(name))

    @staticmethod
    def _decimal_value(value: object) -> str:
        """Return a finite decimal-compatible value as boundary text."""

        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
            return format(value, "f")
        if isinstance(value, str):
            try:
                parsed = Decimal(value)
            except InvalidOperation:
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
            if not parsed.is_finite():
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
            return value
        if isinstance(value, bool) or value is None:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        if not parsed.is_finite():
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return format(parsed, "f")

    @staticmethod
    def _optional_mapping_text(source: Mapping[str, object], name: str) -> str | None:
        """Extract one optional allowlisted text field from a JSON object."""

        value = source.get(name)
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return value

    @classmethod
    def _optional_mapping_decimal(
        cls,
        source: Mapping[str, object],
        name: str,
    ) -> str | None:
        """Extract one optional allowlisted decimal field from a JSON object."""

        value = source.get(name)
        if value is None:
            return None
        return cls._decimal_value(value)

    @classmethod
    def _optional_mapping_decimal_names(
        cls,
        source: Mapping[str, object],
        *names: str,
    ) -> str | None:
        """Extract the first present decimal from ordered safe aliases."""

        for name in names:
            if source.get(name) is not None:
                return cls._optional_mapping_decimal(source, name)
        return None

    @staticmethod
    def _json_mapping(value: object) -> Mapping[str, object]:
        """Decode a JSON object while discarding malformed or non-object data."""

        if isinstance(value, Mapping):
            return {str(key): item for key, item in value.items()}
        if not isinstance(value, str):
            return {}
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        if not isinstance(decoded, Mapping):
            return {}
        return {str(key): item for key, item in decoded.items()}


__all__ = ["ControlPlaneCursorCodec", "PostgresControlPlaneQueryRepository"]
