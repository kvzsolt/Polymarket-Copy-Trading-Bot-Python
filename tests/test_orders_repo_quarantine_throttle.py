"""Observability throttle for the legacy-row quarantine count queries.

The legacy blank-``user_id`` quarantine count (``SELECT count(*) ...``) plus
its WARNING used to run on EVERY scoped unconfirmed call — one extra count
query per recovery cycle per account purely for a log line.  These contracts
pin the throttle:

* at most ONE count query per scope inside the TTL window;
* the window re-arms once the injected monotonic clock advances past the TTL;
* the scoped query RESULTS are never affected — the fetch runs every call.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pmcopy.storage.postgres.orders_repo import OrdersRepo

pytestmark = pytest.mark.asyncio

_OBSERVED_AT = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)


class _FakeClock:
    """Deterministic monotonic clock seam (adapters.clock.MonotonicClock)."""

    def __init__(self) -> None:
        self._now = 1000.0

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class _CountingConn:
    """Fake connection that serves one owned row and counts count-queries."""

    def __init__(self, *, count_value: int = 3) -> None:
        self.fetch_calls = 0
        self.count_queries = 0
        self._count_value = count_value

    async def fetch(self, query: str, *params: object) -> list[dict]:
        self.fetch_calls += 1
        return [_row()]

    async def fetchval(self, query: str, *params: object) -> int:
        if "count(*)" in query:
            self.count_queries += 1
            return self._count_value
        raise AssertionError(f"unexpected fetchval: {query}")


class _Acquire:
    def __init__(self, conn: _CountingConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _CountingConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _Pool:
    def __init__(self, conn: _CountingConn) -> None:
        self._conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self._conn)


def _row() -> dict:
    return {
        "idempotency_key": "copy:leader:event",
        "user_id": "user-1",
        "leader_id": "leader-1",
        "order_id": None,
        "status": "submitted_unconfirmed",
        "request_json": {
            "token_id": "token-1",
            "condition_id": "condition-1",
            "side": "BUY",
            "amount": "12.50",
            "price_limit": "0.625",
        },
        "response_json": {
            "status": "submitted_unconfirmed",
            "pending_since": 1_723_376_400.0,
        },
        "attempt_count": 1,
        "last_error": "venue response unavailable",
        "created_at": _OBSERVED_AT,
        "updated_at": _OBSERVED_AT,
        "matched_total": "0",
    }


async def test_two_ambiguous_recovery_calls_issue_one_quarantine_count() -> None:
    conn = _CountingConn()
    repo = OrdersRepo(
        _Pool(conn), quarantine_count_clock=_FakeClock(), quarantine_count_ttl_seconds=60.0
    )

    first = await repo.list_no_id_ambiguous_submissions(user_id="user-1", limit=5)
    second = await repo.list_no_id_ambiguous_submissions(user_id="user-1", limit=5)

    assert conn.count_queries == 1
    # Pure observability throttle: the scoped fetch still ran for both calls
    # and the batch results are identical.
    assert conn.fetch_calls == 2
    assert [s.idempotency_key for s in first] == [s.idempotency_key for s in second]


async def test_quarantine_count_rearms_after_the_ttl_window() -> None:
    conn = _CountingConn()
    clock = _FakeClock()
    repo = OrdersRepo(_Pool(conn), quarantine_count_clock=clock, quarantine_count_ttl_seconds=60.0)

    await repo.list_no_id_ambiguous_submissions(user_id="user-1", limit=5)
    await repo.list_no_id_ambiguous_submissions(user_id="user-1", limit=5)
    assert conn.count_queries == 1

    clock.advance(60.0)
    await repo.list_no_id_ambiguous_submissions(user_id="user-1", limit=5)
    assert conn.count_queries == 2


async def test_unconfirmed_trade_scopes_throttle_per_side() -> None:
    conn = _CountingConn()
    repo = OrdersRepo(
        _Pool(conn), quarantine_count_clock=_FakeClock(), quarantine_count_ttl_seconds=60.0
    )

    await repo.get_unconfirmed_buys(300.0, user_id="user-1")
    await repo.get_unconfirmed_buys(300.0, user_id="user-1")
    await repo.get_unconfirmed_sells(300.0, user_id="user-1")
    await repo.get_unconfirmed_sells(300.0, user_id="user-1")

    # One count per (scope, window): BUY and SELL are separate scopes, and
    # every scoped fetch still runs.
    assert conn.count_queries == 2
    assert conn.fetch_calls == 4


async def test_quarantine_throttle_never_changes_query_results() -> None:
    counted = _CountingConn(count_value=3)
    repo = OrdersRepo(
        _Pool(counted), quarantine_count_clock=_FakeClock(), quarantine_count_ttl_seconds=60.0
    )

    for _ in range(3):
        submissions = await repo.list_no_id_ambiguous_submissions(user_id="user-1", limit=5)
        assert [s.idempotency_key for s in submissions] == ["copy:leader:event"]
        trades = await repo.get_unconfirmed_buys(300.0, user_id="user-1")
        assert trades == [_row()]

    # The count query (and its warning) ran at most once per scope; the
    # scoped reads returned the same rows on every call regardless.
    assert counted.count_queries == 2
    assert counted.fetch_calls == 6
