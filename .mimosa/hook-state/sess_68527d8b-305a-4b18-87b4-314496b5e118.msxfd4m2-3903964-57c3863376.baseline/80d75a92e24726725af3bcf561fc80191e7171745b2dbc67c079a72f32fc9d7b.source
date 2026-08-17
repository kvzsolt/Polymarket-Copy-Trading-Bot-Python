"""Atomic persistence contracts for bounded ambiguous-recovery evidence."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AuthoritativeRecoveryScan,
    RecoveryMatchSource,
    SubmissionSide,
)
from storage.postgres.orders_repo import OrdersRepo

pytestmark = pytest.mark.asyncio

_OBSERVED_AT = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _Acquire:
    def __init__(self, conn: "_StatefulConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_StatefulConn":
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _Pool:
    def __init__(self, conn: "_StatefulConn") -> None:
        self._conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self._conn)


class _StatefulConn:
    """Tiny row-lock fake that records SQL and applies response/status writes."""

    def __init__(self, row: dict) -> None:
        self.row = dict(row)
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetch(self, query: str, *params: object) -> list[dict]:
        self.calls.append((query, params))
        return [dict(self.row)]

    async def fetchrow(self, query: str, *params: object) -> dict | None:
        self.calls.append((query, params))
        if "FOR UPDATE" in query:
            if self.row.get("status") != "submitted_unconfirmed" or self.row.get("order_id"):
                return None
        return dict(self.row)

    async def fetchval(self, query: str, *params: object) -> str | None:
        self.calls.append((query, params))
        if self.row.get("status") != "submitted_unconfirmed" or self.row.get("order_id"):
            return None
        if "SET order_id" in query:
            self.row["order_id"] = params[1]
            self.row["response_json"] = params[2]
        elif "SET status = 'confirmed_absent'" in query:
            self.row["status"] = "confirmed_absent"
            self.row["response_json"] = params[1]
        return str(self.row["idempotency_key"])

    async def execute(self, query: str, *params: object) -> str:
        self.calls.append((query, params))
        if self.row.get("status") != "submitted_unconfirmed" or self.row.get("order_id"):
            return "UPDATE 0"
        self.row["response_json"] = params[1]
        return "UPDATE 1"


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


def _clear_scan() -> AuthoritativeRecoveryScan:
    return AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )


async def test_lists_only_bounded_no_id_submitted_unconfirmed_candidates() -> None:
    conn = _StatefulConn(_row())
    repo = OrdersRepo(_Pool(conn))

    records = await repo.list_no_id_ambiguous_submissions(limit=7)

    assert len(records) == 1
    record = records[0]
    assert record.idempotency_key == "copy:leader:event"
    assert record.side is SubmissionSide.BUY
    assert str(record.amount) == "12.50"
    assert record.submitted_at == datetime.fromtimestamp(1_723_376_400.0, tz=timezone.utc)
    query, params = conn.calls[0]
    assert "status = 'submitted_unconfirmed'" in query
    assert "order_id IS NULL" in query
    assert "LIMIT $1" in query
    assert params == (7,)


async def test_records_only_a_bounded_evidence_tail_and_resets_on_inconclusive_scan() -> None:
    conn = _StatefulConn(_row())
    repo = OrdersRepo(_Pool(conn))

    for offset in range(4):
        progress = await repo.record_ambiguous_scan_evidence(
            idempotency_key="copy:leader:event",
            scan=_clear_scan(),
            observed_at=_OBSERVED_AT.replace(second=offset),
            required_scan_count=5,
            evidence_history_limit=2,
        )
        assert progress is not None

    response = json.loads(conn.row["response_json"])
    recovery = response["ambiguous_recovery"]
    assert recovery["consecutive_complete_absence_scans"] == 4
    assert len(recovery["scans"]) == 2
    assert [item["observed_at"] for item in recovery["scans"]] == [
        "2026-08-11T12:00:02+00:00",
        "2026-08-11T12:00:03+00:00",
    ]

    inconclusive = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=False,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )
    progress = await repo.record_ambiguous_scan_evidence(
        idempotency_key="copy:leader:event",
        scan=inconclusive,
        observed_at=_OBSERVED_AT.replace(second=4),
        required_scan_count=5,
        evidence_history_limit=2,
    )

    assert progress is not None
    assert progress.consecutive_complete_absence_scans == 0
    response = json.loads(conn.row["response_json"])
    assert len(response["ambiguous_recovery"]["scans"]) == 2


async def test_attach_recovered_order_id_is_guarded_and_preserves_ambiguous_status() -> None:
    conn = _StatefulConn(_row())
    repo = OrdersRepo(_Pool(conn))

    changed = await repo.attach_recovered_venue_order_id(
        idempotency_key="copy:leader:event",
        venue_order_id="venue-order-7",
        source=RecoveryMatchSource.PRIVATE_FILLS,
        recovered_at=_OBSERVED_AT,
    )

    assert changed is True
    assert conn.row["order_id"] == "venue-order-7"
    assert conn.row["status"] == "submitted_unconfirmed"
    response = json.loads(conn.row["response_json"])
    assert response["order_id"] == "venue-order-7"
    assert response["ambiguous_recovery"]["resolution"] == "recovered_order_id"
    update_query = next(query for query, _ in conn.calls if "SET order_id" in query)
    assert "status = 'submitted_unconfirmed'" in update_query
    assert "order_id IS NULL" in update_query


async def test_confirmed_absent_transition_requires_persisted_scan_threshold() -> None:
    conn = _StatefulConn(_row())
    repo = OrdersRepo(_Pool(conn))

    await repo.record_ambiguous_scan_evidence(
        idempotency_key="copy:leader:event",
        scan=_clear_scan(),
        observed_at=_OBSERVED_AT,
        required_scan_count=2,
        evidence_history_limit=2,
    )
    assert (
        await repo.mark_ambiguous_submission_confirmed_absent(
            idempotency_key="copy:leader:event",
            required_scan_count=2,
            confirmed_at=_OBSERVED_AT,
        )
        is False
    )

    await repo.record_ambiguous_scan_evidence(
        idempotency_key="copy:leader:event",
        scan=_clear_scan(),
        observed_at=_OBSERVED_AT.replace(second=1),
        required_scan_count=2,
        evidence_history_limit=2,
    )
    assert (
        await repo.mark_ambiguous_submission_confirmed_absent(
            idempotency_key="copy:leader:event",
            required_scan_count=2,
            confirmed_at=_OBSERVED_AT.replace(second=1),
        )
        is True
    )

    assert conn.row["status"] == "confirmed_absent"
    response = json.loads(conn.row["response_json"])
    assert response["status"] == "confirmed_absent"
    assert response["ambiguous_recovery"]["resolution"] == "confirmed_absent"
