"""Exercise the production ownership query against deterministic stored order states.

SQLite executes the shared SELECT syntax in memory; transaction and position-lock
behavior remains covered by the PostgreSQL integration suite.
"""

import json
import sqlite3
from collections.abc import Iterator
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.orchestrators.account_inventory_startup import InitialInventorySync
from pmcopy.services.healthServer import ReadinessTracker
from tests.test_manual_sync_inventory_convergence import SyncScenario
from tests.test_positions_repo_set_position_size import VALID_TOKEN, _repo_with_current


class StoredOwnership:
    """Execute the repository's ownership SELECT without external I/O."""

    def __init__(self) -> None:
        self._db = sqlite3.connect(":memory:")
        self._db.executescript(
            """
            CREATE TABLE orders (user_id TEXT, request_json TEXT, status TEXT);
            CREATE TABLE order_reservations (
                user_id TEXT, condition_id TEXT, token_id TEXT, status TEXT
            );
            """
        )

    def add_order(self, status: str) -> None:
        """Store an order that owns the position scope under test."""
        self._db.execute(
            "INSERT INTO orders VALUES (?, ?, ?)",
            ("user", json.dumps({"condition_id": "condition", "token_id": VALID_TOKEN}), status),
        )

    def add_reservation(self) -> None:
        """Retain active capacity independently of the order's terminal status."""
        self._db.execute(
            "INSERT INTO order_reservations VALUES (?, ?, ?, ?)",
            ("user", "condition", VALID_TOKEN, "active"),
        )

    async def fetchval(self, query: str, *params: str) -> bool:
        """Evaluate the unchanged production SELECT and return its EXISTS result."""
        bindings = {str(index): value for index, value in enumerate(params, start=1)}
        return bool(self._db.execute(query, bindings).fetchone()[0])

    def close(self) -> None:
        """Release the in-memory database after each test."""
        self._db.close()


@pytest.fixture
def ownership() -> Iterator[StoredOwnership]:
    """Provide an isolated order/reservation database for each case."""
    stored = StoredOwnership()
    try:
        yield stored
    finally:
        stored.close()


@pytest.mark.asyncio
async def test_initial_inventory_prunes_absent_claim_with_auto_released_order(
    monkeypatch: pytest.MonkeyPatch, ownership: StoredOwnership
) -> None:
    """A completed reconciliation release must not abort an authoritative startup sync."""
    ownership.add_order("auto_released_ambiguous")
    repo, conn, captured = _repo_with_current(Decimal("5"))
    conn.fetchval = ownership.fetchval
    scenario = SyncScenario(monkeypatch)
    scenario.hold(settled=True)
    scenario.positions.list_held_positions.return_value[0].token_id = VALID_TOKEN
    scenario.positions.delete_position_if_unchanged = AsyncMock(
        side_effect=repo.delete_position_if_unchanged
    )
    readiness = ReadinessTracker(initial={"reconciler_ok": True})

    await InitialInventorySync(readiness).initialize(scenario.service)

    assert captured["deletes"] == [("user", "condition", VALID_TOKEN)]
    assert readiness.snapshot()["reconciler_ok"] is True
    scenario.market_store.evict.assert_called_once_with(VALID_TOKEN)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        "manually_canceled",
        "manually_expired",
        "manually_confirmed_absent",
    ],
)
async def test_operator_resolved_order_does_not_block_pruning(
    ownership: StoredOwnership, status: str
) -> None:
    """An operator-validated ``bot.py resolve --apply`` status must end the claim.

    ``_TERMINAL_STATUS_BY_RESOLUTION`` in ``bot_cli/commands/resolve.py`` writes
    these ``manually_*`` statuses, so an operator-confirmed resolution has to be
    treated as terminal here or the documented unblock path is inert and the
    drift service keeps pausing the scope.
    """
    ownership.add_order(status)
    repo, conn, captured = _repo_with_current(Decimal("5"))
    conn.fetchval = ownership.fetchval

    assert await repo.delete_position_if_unchanged(
        "user", "condition", token_id=VALID_TOKEN, expected_size=Decimal("5")
    )
    assert captured["deletes"] == [("user", "condition", VALID_TOKEN)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        "ambiguous_timeout",
        "submitted_unconfirmed",
        "trade_mined",
        "matched_pending_confirmation",
        "reconciliation_required",
        "unknown_unconfirmed",
        "future_unknown_status",
    ],
)
async def test_unresolved_order_still_blocks_pruning(
    ownership: StoredOwnership, status: str
) -> None:
    """An older released order never overrides unresolved ownership of the same scope."""
    ownership.add_order("auto_released_ambiguous")
    ownership.add_order(status)
    repo, conn, captured = _repo_with_current(Decimal("5"))
    conn.fetchval = ownership.fetchval

    assert not await repo.delete_position_if_unchanged(
        "user", "condition", token_id=VALID_TOKEN, expected_size=Decimal("5")
    )
    assert captured["deletes"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["reservation", "concurrent_fill"])
async def test_auto_released_order_does_not_bypass_other_pruning_guards(
    ownership: StoredOwnership, guard: str
) -> None:
    """Terminal orders cannot override active reservations or a changed position size."""
    ownership.add_order("auto_released_ambiguous")
    if guard == "reservation":
        ownership.add_reservation()
    repo, conn, captured = _repo_with_current(Decimal("6" if guard == "concurrent_fill" else "5"))
    conn.fetchval = ownership.fetchval

    assert not await repo.delete_position_if_unchanged(
        "user", "condition", token_id=VALID_TOKEN, expected_size=Decimal("5")
    )
    assert captured["deletes"] == []
