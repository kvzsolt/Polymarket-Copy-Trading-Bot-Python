"""Regression: reconcile_risk_record must read previous_matched from DB.

Pre-fix the function pulled ``previous_matched`` from
``_matched_amount_from_response(record.get("response"))``. On restart the
in-memory record was reconstructed without the prior response payload,
so previous_matched silently fell back to zero. Cumulative venue
``size_matched`` was then re-applied as a fresh delta and every risk
SELL fill in-flight across the restart was double-counted.

The fix is one-liner: call
``self._previous_matched_for_order(self.orders_repo, order_id)`` which
reads the persisted ``response_json.size_matched`` from the orders
table — the same source-of-truth the trade-record path uses
(reconciliation_service.py:913).

Test pins the contract by passing a record whose in-memory response is
empty while the mocked orders_repo returns a prior matched value, and
asserting the computed delta uses the DB value.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from services.execution.reconciliation_service import ReconciliationService


def test_previous_matched_sourced_from_orders_repo_not_record():
    svc = ReconciliationService.__new__(ReconciliationService)
    orders_repo = MagicMock()
    orders_repo.get_by_order_id.return_value = {
        "response": {"size_matched": "5"},
    }
    svc.orders_repo = orders_repo

    matched = ReconciliationService._previous_matched_for_order(orders_repo, "venue-123")
    assert matched == Decimal("5"), (
        f"previous matched must come from orders_repo.get_by_order_id; got {matched}"
    )


def test_db_lookup_safe_when_response_missing_in_memory():
    """An empty in-memory record must NOT cause previous_matched to be zero
    when the DB has a prior cumulative value — protects against the
    restart-double-fill regression."""
    orders_repo = MagicMock()
    orders_repo.get_by_order_id.return_value = {
        "response": {"size_matched": "3.5"},
    }

    record_in_memory = {"order_id": "venue-X"}  # NO "response" key
    matched = ReconciliationService._previous_matched_for_order(orders_repo, "venue-X")
    assert matched == Decimal("3.5"), (
        "DB-sourced previous_matched must survive missing in-memory response; "
        f"got {matched!r} (record was {record_in_memory!r})"
    )
