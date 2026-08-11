"""PostgreSQL control-plane audit, safety-control, and query contracts."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.control_plane.contracts import (
    AuditEvent,
    ControlPlaneError,
    ControlPlaneReason,
    LockResetCommand,
    LockResetOutcome,
    PageRequest,
)
from storage.postgres.control_plane_audit_repository import (
    PostgresControlPlaneAuditRepository,
)
from storage.postgres.control_plane_entry_control_repository import (
    PostgresEntryControlRepository,
)
from storage.postgres.control_plane_query_repository import (
    ControlPlaneCursorCodec,
    PostgresControlPlaneQueryRepository,
)
from storage.postgres.control_plane_reentry_lock_repository import (
    PostgresControlPlaneReentryLockRepository,
)


NOW = datetime(2026, 8, 11, 17, 0, tzinfo=UTC)
MIGRATION = (
    Path(__file__).parents[2]
    / "storage"
    / "postgres"
    / "migrations"
    / "20260811_03_control_plane.sql"
)


class FixedClock:
    """Return a deterministic timestamp for reset results."""

    def now(self) -> datetime:
        """Return a fixed UTC timestamp."""

        return NOW


class FakePool:
    """Minimal asyncpg-style direct pool with scripted results."""

    def __init__(self) -> None:
        self.fetch_results: list[list[dict[str, object]]] = []
        self.fetchrow_results: list[dict[str, object] | None] = []
        self.fetchval_results: list[object | None] = []
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        """Return the next row collection."""

        self.calls.append(("fetch", query, args))
        return self.fetch_results.pop(0)

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        """Return the next optional row."""

        self.calls.append(("fetchrow", query, args))
        return self.fetchrow_results.pop(0)

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the next scalar."""

        self.calls.append(("fetchval", query, args))
        return self.fetchval_results.pop(0)


def audit_event() -> AuditEvent:
    """Return one sanitized append-only mutation event."""

    return AuditEvent(
        event_id="event-a",
        actor="operator-a",
        action="task.pause",
        target_type="copy_task",
        target_id="task-a",
        outcome="succeeded",
        reason_code="ok",
        occurred_at=NOW,
        follower_account="account-a",
        details=(("expected_revision", "2"),),
    )


def test_migration_adds_only_minimal_audit_and_lock_revision() -> None:
    """The additive migration does not create a second runtime truth source."""

    sql = MIGRATION.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS control_plane_audit_log" in sql
    assert "CREATE TABLE IF NOT EXISTS entry_control" not in sql
    assert "INSERT INTO entry_control" not in sql
    assert "CREATE TABLE IF NOT EXISTS trading_control" not in sql
    assert "CREATE SEQUENCE IF NOT EXISTS market_reentry_lock_revision_seq" in sql
    assert "ADD COLUMN IF NOT EXISTS revision" in sql
    assert "SET DEFAULT nextval('market_reentry_lock_revision_seq')" in sql
    assert "ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1" not in sql
    assert "event_stream" not in sql.casefold()
    assert "aggregate_version" not in sql.casefold()
    assert "UPDATE control_plane_audit_log" not in sql
    assert "DELETE FROM control_plane_audit_log" not in sql


@pytest.mark.asyncio
async def test_audit_repository_has_an_insert_only_idempotent_surface() -> None:
    """Audit persistence never exposes update/delete behavior or raw detail."""

    pool = FakePool()
    pool.fetchval_results = ["event-a", None]
    repository = PostgresControlPlaneAuditRepository(pool)
    event = audit_event()

    await repository.append(event)
    await repository.append(event)

    assert not hasattr(repository, "update")
    assert not hasattr(repository, "delete")
    for method, sql, args in pool.calls:
        assert method == "fetchval"
        assert "INSERT INTO control_plane_audit_log" in sql
        assert "ON CONFLICT (event_id) DO NOTHING" in sql
        assert "UPDATE " not in sql
        assert "DELETE " not in sql
        assert json.loads(str(args[-1])) == {"expected_revision": "2"}


@pytest.mark.asyncio
async def test_entry_control_repository_get_and_compare_and_set_revision() -> None:
    """Entry pause/full stop transitions are one-row optimistic updates."""

    pool = FakePool()
    pool.fetchrow_results = [
        {
            "enabled": True,
            "generation": 1,
            "reason": "running",
            "updated_at": NOW,
            "updated_by": "bootstrap",
        },
        {
            "enabled": False,
            "generation": 2,
            "reason": "runtime.full_stop",
            "updated_at": NOW,
            "updated_by": "operator-a",
        },
        None,
    ]
    repository = PostgresEntryControlRepository(pool)

    current = await repository.get()
    stopped = await repository.compare_and_set(
        expected_revision=1,
        entries_paused=True,
        full_stop=True,
        actor="operator-a",
        reason_code="incident",
    )
    stale = await repository.compare_and_set(
        expected_revision=1,
        entries_paused=True,
        full_stop=False,
        actor="operator-a",
        reason_code="stale",
    )

    assert current.revision == 1
    assert current.entries_paused is False
    assert stopped is not None and stopped.full_stop is True and stopped.revision == 2
    assert stale is None
    update_sql = pool.calls[1][1]
    assert "UPDATE trading_control" in update_sql
    assert "generation = generation + 1" in update_sql
    assert "AND generation = $1" in update_sql
    assert pool.calls[1][2] == (1, False, "runtime.full_stop", "operator-a")


@pytest.mark.asyncio
async def test_lock_reset_is_one_statement_cas_with_distinct_outcomes() -> None:
    """A stale operator view cannot delete a newer protective lock."""

    pool = FakePool()
    pool.fetchrow_results = [
        {"outcome": "revision_mismatch"},
        {"outcome": "reset"},
        {"outcome": "not_found"},
    ]
    repository = PostgresControlPlaneReentryLockRepository(pool, clock=FixedClock())
    command = LockResetCommand(
        follower_account="account-a",
        task_id="task-a",
        leader_wallet="0x1111111111111111111111111111111111111111",
        market_id="condition-a",
        expected_revision=2,
        actor="operator-a",
        reason_code="manual_review",
    )

    stale = await repository.reset_if_current(command)
    reset = await repository.reset_if_current(command)
    missing = await repository.reset_if_current(command)

    assert stale.outcome is LockResetOutcome.REVISION_MISMATCH
    assert reset.outcome is LockResetOutcome.RESET and reset.released_at == NOW
    assert missing.outcome is LockResetOutcome.NOT_FOUND
    sql = pool.calls[0][1]
    assert "DELETE FROM market_reentry_locks" in sql
    assert "revision = $5" in sql
    assert "SELECT revision" in sql
    assert "CASE" in sql
    assert len(pool.calls) == 3


@pytest.mark.asyncio
async def test_position_keyset_pagination_round_trips_an_opaque_cursor() -> None:
    """The adapter fetches limit-plus-one and never accepts offset pagination."""

    pool = FakePool()
    pool.fetch_results = [
        [
            {
                "user_id": "account-a",
                "condition_id": "condition-a",
                "token_id": "token-a",
                "leader_id": None,
                "size_tokens": "1.25",
                "avg_entry_price": "0.50",
                "updated_at": NOW,
            },
            {
                "user_id": "account-a",
                "condition_id": "condition-b",
                "token_id": "token-b",
                "leader_id": "0x1111111111111111111111111111111111111111",
                "size_tokens": "2",
                "avg_entry_price": "0.25",
                "updated_at": NOW,
            },
            {
                "user_id": "account-a",
                "condition_id": "condition-c",
                "token_id": "token-c",
                "leader_id": None,
                "size_tokens": "3",
                "avg_entry_price": "0.10",
                "updated_at": NOW,
            },
        ],
        [
            {
                "user_id": "account-a",
                "condition_id": "condition-c",
                "token_id": "token-c",
                "leader_id": None,
                "size_tokens": "3",
                "avg_entry_price": "0.10",
                "updated_at": NOW,
            }
        ],
    ]
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())

    first = await repository.list_positions("account-a", PageRequest(limit=2))
    second = await repository.list_positions(
        "account-a", PageRequest(limit=2, cursor=first.next_cursor)
    )

    assert [position.condition_id for position in first.items] == ["condition-a", "condition-b"]
    assert first.next_cursor is not None
    assert [position.condition_id for position in second.items] == ["condition-c"]
    assert pool.calls[0][2] == ("account-a", "", "", 3)
    assert pool.calls[1][2] == ("account-a", "condition-b", "token-b", 3)
    assert "OFFSET" not in pool.calls[0][1].upper()


@pytest.mark.asyncio
async def test_active_task_count_uses_one_bounded_scalar_query() -> None:
    """System status reads a current count without dumping task rows."""

    pool = FakePool()
    pool.fetchval_results = [3]
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())

    count = await repository.count_active_tasks()

    assert count == 3
    assert pool.calls[0][0] == "fetchval"
    assert "COUNT(*)" in pool.calls[0][1]
    assert "status = 'active'" in pool.calls[0][1]


@pytest.mark.asyncio
async def test_cursor_kind_confusion_fails_before_query() -> None:
    """A cursor issued for orders cannot be replayed against positions."""

    pool = FakePool()
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())
    cursor = ControlPlaneCursorCodec().encode("orders", (NOW.isoformat(), "order-a"))

    with pytest.raises(ControlPlaneError) as captured:
        await repository.list_positions(
            "account-a",
            PageRequest(limit=10, cursor=cursor),
        )

    assert captured.value.reason is ControlPlaneReason.INVALID_PAGE
    assert pool.calls == []


@pytest.mark.asyncio
async def test_cursor_value_type_confusion_is_a_stable_page_error() -> None:
    """A valid envelope with an invalid key type never reaches PostgreSQL."""

    pool = FakePool()
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())
    cursor = ControlPlaneCursorCodec().encode("activity", ("not-an-integer",))

    with pytest.raises(ControlPlaneError) as captured:
        await repository.list_activity(None, PageRequest(limit=10, cursor=cursor))

    assert captured.value.reason is ControlPlaneReason.INVALID_PAGE
    assert pool.calls == []


@pytest.mark.asyncio
async def test_order_projection_drops_raw_payloads_and_keeps_decimal_strings() -> None:
    """Request secrets and response/error bodies never enter the DTO."""

    pool = FakePool()
    pool.fetch_results = [
        [
            {
                "idempotency_key": "intent-a",
                "user_id": "account-a",
                "leader_id": None,
                "order_id": "order-a",
                "status": "matched",
                "request_json": json.dumps(
                    {
                        "side": "BUY",
                        "amount": "5.00",
                        "price_limit": "0.45",
                        "private_key": "must-not-leak",
                    }
                ),
                "matched_total": "5.00",
                "created_at": NOW,
                "updated_at": NOW,
            }
        ]
    ]
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())

    page = await repository.list_orders("account-a", PageRequest(limit=10))

    order = page.items[0]
    assert order.amount == "5.00"
    assert order.price_limit == "0.45"
    assert order.size == order.amount
    assert order.price == order.price_limit
    assert not hasattr(order, "request_json")
    assert "private_key" not in repr(order)


@pytest.mark.asyncio
async def test_position_and_order_details_are_single_row_and_follower_scoped() -> None:
    """Resource identities cannot bypass the follower-account boundary."""

    pool = FakePool()
    pool.fetchrow_results = [
        {
            "user_id": "account-a",
            "condition_id": "condition-a",
            "token_id": "token-a",
            "leader_id": None,
            "size_tokens": "1.25",
            "avg_entry_price": "0.50",
            "updated_at": NOW,
        },
        {
            "idempotency_key": "intent-a",
            "user_id": "account-a",
            "leader_id": None,
            "order_id": None,
            "status": "submitted_unconfirmed",
            "request_json": json.dumps({"side": "BUY", "amount": "5", "price_limit": "0.45"}),
            "matched_total": "0",
            "created_at": NOW,
            "updated_at": NOW,
        },
    ]
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())

    position = await repository.get_position("account-a", "token-a")
    order = await repository.get_order("account-a", "intent-a")

    assert position is not None and position.token_id == "token-a"
    assert order is not None and order.status == "submitted_unconfirmed"
    assert pool.calls[0][2] == ("account-a", "token-a")
    assert pool.calls[1][2] == ("account-a", "intent-a")
    assert "user_id = $1" in pool.calls[0][1]
    assert "user_id = $1" in pool.calls[1][1]


@pytest.mark.asyncio
async def test_reservation_and_lock_queries_expose_exact_values_and_lock_revision() -> None:
    """Money remains text and reset clients receive the exact CAS token."""

    pool = FakePool()
    pool.fetch_results = [
        [
            {
                "reservation_id": "reservation-a",
                "follower_account": "account-a",
                "task_id": "task-a",
                "market_id": "condition-a",
                "amount": "10.2500",
                "status": "reserved",
                "reserved_at": NOW,
                "expires_at": NOW,
            }
        ],
        [
            {
                "follower_account": "account-a",
                "task_id": "task-a",
                "leader_wallet": "0x1111111111111111111111111111111111111111",
                "market_id": "condition-a",
                "reason": "stop_loss",
                "fill_id": "fill-a",
                "locked_at": NOW,
                "revision": 3,
            }
        ],
    ]
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())

    reservations = await repository.list_reservations("account-a", PageRequest(limit=10))
    locks = await repository.list_reentry_locks("account-a", PageRequest(limit=10))

    assert reservations.items[0].amount == "10.2500"
    assert locks.items[0].revision == 3
    assert locks.items[0].lock_id.startswith("lock_")
    assert len(locks.items[0].lock_id) == 69
    assert "locked_at, revision" in pool.calls[1][1]


@pytest.mark.asyncio
async def test_discovery_pages_preserve_existing_score_desc_wallet_asc_order() -> None:
    """Control-plane reads do not silently change canonical ranking tie order."""

    pool = FakePool()
    pool.fetch_results = [[], []]
    repository = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())

    await repository.list_discovery_candidates(PageRequest(limit=10))
    await repository.list_discovery_roster(PageRequest(limit=10))

    assert "final_score DESC, snapshots.wallet ASC" in pool.calls[0][1]
    assert "score DESC, wallet ASC" in pool.calls[1][1]
