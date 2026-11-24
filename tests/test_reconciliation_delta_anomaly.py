"""PR 4: ReconciliationService delta math + negative-delta anomaly.

Pins the production-path delta accounting for ``reconcile_trade_record``:
  - venue cumulative 5, local previous 0  -> apply 5
  - venue cumulative 8, local previous 5  -> apply 3
  - venue cumulative 8, local previous 8  -> apply 0 (terminal recorded, no new fill row)
  - venue cumulative 5, local previous 8  -> apply 0 + anomaly logged + metric incremented
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from pmcopy.services.execution.reconciliation_service import ReconciliationService


# ---------------------------------------------------------------------------
# Fakes for the production dict-shaped record path
# ---------------------------------------------------------------------------

TRADE_KEY = "tk-delta"
ORDER_ID = "ord-delta-1"
USER_ID = "user-1"
LEADER_ID = "0xleader"
COND_ID = "cond-delta"
TOKEN_ID = "0xtok-delta"


class _OrdersRepo:
    def __init__(self, previous_matched: str = "0"):
        # `get_by_order_id` returns the existing row whose response.size_matched
        # is the previous matched cumulative size.
        self._previous_matched = previous_matched
        self.update_calls: list[dict[str, Any]] = []
        self.mark_processed_calls: list[dict[str, Any]] = []
        self.delete_processed_calls: list[str] = []

    async def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        if order_id != ORDER_ID:
            return None
        return {"order_id": order_id, "response": {"size_matched": self._previous_matched}}

    async def update_submission_status(self, **kwargs) -> None:
        self.update_calls.append(kwargs)

    async def mark_processed_trade(self, **kwargs) -> None:
        self.mark_processed_calls.append(kwargs)

    async def delete_processed_trade(self, trade_key: str) -> None:
        self.delete_processed_calls.append(trade_key)


class _FillsRepo:
    def __init__(self):
        self.fills: list[Any] = []

    async def append_fill(self, fill, *, raw_payload: dict[str, Any]) -> None:
        self.fills.append({"fill": fill, "raw_payload": raw_payload})

    async def append_fill_and_apply(self, record, positions_repo, *, raw_payload=None):
        # Mirror FillsRepo.append_fill_and_apply (P0-1): dedup on the stable
        # dedup_key, else append + apply the position delta. (0, None) on a dup.
        seen = getattr(self, "_applied_keys", None)
        if seen is None:
            seen = self._applied_keys = set()
        key = (record.order_id, record.side, getattr(record, "dedup_key", None))
        if key[2] is not None and key in seen:
            return 0, None
        seen.add(key)
        await self.append_fill(record, raw_payload=raw_payload)
        updated = await positions_repo.apply_fill(
            user_id=record.user_id,
            leader_id=record.leader_id,
            condition_id=record.condition_id,
            token_id=record.token_id,
            side=record.side,
            fill_size=record.size_tokens,
            fill_price=record.price,
        )
        return 1, updated


class _PositionsRepo:
    def __init__(self):
        self.applied: list[dict[str, Any]] = []

    async def apply_fill(self, **kwargs):
        self.applied.append(kwargs)
        return None

    async def get_position(self, *args, **kwargs):
        return None


class _AuditLogger:
    def __init__(self):
        self.events: list[tuple[str, dict[str, Any]]] = []

    def log_event(self, event: str, payload: dict[str, Any]) -> None:
        self.events.append((event, payload))


def _record() -> dict[str, Any]:
    return {
        "trade_key": TRADE_KEY,
        "user_id": USER_ID,
        "leader_id": LEADER_ID,
        "tx_hash": "0xtx",
        "asset": TOKEN_ID,
        "condition_id": COND_ID,
        "order_id": ORDER_ID,
    }


def _make_service(
    previous_matched: str = "0",
) -> tuple[ReconciliationService, _OrdersRepo, _FillsRepo, _PositionsRepo, _AuditLogger]:
    orders = _OrdersRepo(previous_matched=previous_matched)
    fills = _FillsRepo()
    positions = _PositionsRepo()
    audit = _AuditLogger()
    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
        audit_logger=audit,
    )
    return svc, orders, fills, positions, audit


async def _confirm_factory(size_matched: str, status: str = "matched"):
    async def _cf(order_id: str) -> dict[str, Any]:
        return {"status": status, "size_matched": size_matched, "avg_price": "0.50"}

    return _cf


# ---------------------------------------------------------------------------
# Delta math
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delta_full_apply_when_previous_zero() -> None:
    svc, orders, fills, positions, _ = _make_service(previous_matched="0")
    confirm_fill = await _confirm_factory("5", status="matched")

    applied = await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

    assert applied is True
    assert len(fills.fills) == 1
    assert fills.fills[0]["fill"].size_tokens == Decimal("5")
    assert len(positions.applied) == 1
    assert positions.applied[0]["fill_size"] == Decimal("5")


@pytest.mark.asyncio
async def test_delta_increment_applies_only_difference() -> None:
    svc, orders, fills, positions, _ = _make_service(previous_matched="5")
    confirm_fill = await _confirm_factory("8", status="confirmed")

    applied = await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

    assert applied is True
    assert len(fills.fills) == 1
    assert fills.fills[0]["fill"].size_tokens == Decimal("3")
    assert positions.applied[0]["fill_size"] == Decimal("3")


@pytest.mark.asyncio
async def test_delta_zero_when_already_applied_records_terminal_only() -> None:
    svc, orders, fills, positions, audit = _make_service(previous_matched="8")
    confirm_fill = await _confirm_factory("8", status="confirmed")

    applied = await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

    assert applied is True
    # No new fill rows or position updates — cumulative already accounted for.
    assert fills.fills == []
    assert positions.applied == []
    # Terminal state is still recorded on the order.
    assert orders.update_calls, "expected update_submission_status for terminal record"
    statuses = [c["status"] for c in orders.update_calls]
    assert "filled_reconciled" in statuses
    audit_events = [name for name, _ in audit.events]
    assert "reconciliation_terminal_no_new_delta" in audit_events


# ---------------------------------------------------------------------------
# Negative delta anomaly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_negative_delta_clamps_to_zero_logs_and_audits(caplog) -> None:
    svc, orders, fills, positions, audit = _make_service(previous_matched="8")
    confirm_fill = await _confirm_factory("5", status="confirmed")

    with (
        patch("pmcopy.services.observability.metrics.metrics.inc_sync") as inc_sync,
        caplog.at_level(logging.WARNING, logger="TradePipeline"),
    ):
        applied = await svc.reconcile_trade_record(
            _record(),
            "BUY",
            confirm_fill=confirm_fill,
            logger=logging.getLogger("TradePipeline"),
        )

    # Order still gets resolved (no new fill, but terminal is recorded).
    assert applied is True
    assert fills.fills == []
    assert positions.applied == []

    # Anomaly metric was incremented.
    metric_calls = [
        c for c in inc_sync.call_args_list if "reconciliation_negative_delta" in str(c.args)
    ]
    assert metric_calls, (
        f"expected reconciliation_negative_delta_total inc_sync, got {inc_sync.call_args_list}"
    )

    # Audit event recorded.
    assert any(name == "reconciliation_negative_delta_anomaly" for name, _ in audit.events)

    # Structured warning emitted.
    assert any(
        "reconciliation_negative_delta_anomaly" in record.getMessage() for record in caplog.records
    )


# ---------------------------------------------------------------------------
# Risk path also flags negative delta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_risk_path_flags_negative_delta_anomaly() -> None:
    # previous_matched MUST come from the persisted orders row (the
    # _previous_matched_for_order helper) — not from record["response"].
    # Pre-fix this test asserted the buggy in-memory source by setting
    # the orders_repo to "0" and the record to "8"; the fix flipped the
    # source of truth so the test setup flips too.
    svc, orders, fills, positions, audit = _make_service(previous_matched="8")
    record = {
        "order_id": ORDER_ID,
        "request": {"signal_id": "risk:abc", "token_id": TOKEN_ID, "reason": "stop_loss"},
        # ``response`` is intentionally absent — proves we no longer read
        # previous_matched from the in-memory record on the risk path.
    }
    confirm_fill = await _confirm_factory("5", status="matched")

    with patch("pmcopy.services.observability.metrics.metrics.inc_sync") as inc_sync:
        await svc.reconcile_risk_record(
            record,
            user_id=USER_ID,
            condition_id=COND_ID,
            token_id=TOKEN_ID,
            confirm_fill=confirm_fill,
        )

    metric_calls = [
        c for c in inc_sync.call_args_list if "reconciliation_negative_delta" in str(c.args)
    ]
    assert metric_calls
    assert any(name == "reconciliation_negative_delta_anomaly" for name, _ in audit.events)
