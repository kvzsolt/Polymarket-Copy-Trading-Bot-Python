"""Test that 'live' status with size_matched does NOT produce a confirmed fill.

This guards against Qodo PR#64 finding: execute() must not treat a
non-terminal status (e.g. 'live') with size_matched/filled_size as a
confirmed fill, because that would corrupt local accounting.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.executionService import ExecutionRequest, ExecutionService
from services.orderRepository import OrderRepository
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from utils.createClobClient import MarketMeta


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


class _MetaSvc:
    async def get(self, token_id: str, refresh: bool = False):  # noqa: ARG002
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


class _LiveFillClob:
    """Simulates a CLOB that returns status='live' with size_matched present."""

    def __init__(self):
        self.get_price = AsyncMock(return_value={"price": "0.50"})
        self.post_signed_order = AsyncMock(
            return_value={"success": True, "orderID": "ord-live-fill", "status": "submitted"}
        )
        self.client = SimpleNamespace(
            get_order=lambda _order_id: {
                "status": "live",
                "size_matched": "5",
                "avg_price": "0.50",
            }
        )

    async def create_market_order(self, **kwargs):  # noqa: ARG002
        return {"signed": True}


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


def _count_fills(db_path: str) -> int:
    with create_connection(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM fills").fetchone()
        return row["cnt"] if row else 0


@pytest.mark.asyncio
async def test_live_fill_not_confirmed(tmp_path):
    """When _confirm_fill returns status='live' + size_matched, execute()
    must NOT persist a fill and should report filled_amount=0."""
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)

    repo = OrderRepository(str(db_path))
    clob = _LiveFillClob()
    svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
    req = ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-live-fill",
    )

    result = await svc.execute(req)

    # The order was accepted and submitted to the venue
    assert result.accepted is True
    assert result.order_id == "ord-live-fill"

    # But the fill is NOT confirmed because status='live' is non-terminal
    assert result.filled_amount == Decimal("0"), (
        "filled_amount should be 0 when fill status is 'live', "
        f"got {result.filled_amount}"
    )
    assert result.avg_price is None, (
        "avg_price should be None when fill is not confirmed, "
        f"got {result.avg_price}"
    )

    # No fill rows should have been persisted
    assert _count_fills(str(db_path)) == 0, (
        "mark_fill should NOT be called when fill_info status is 'live', "
        "because that would corrupt local accounting"
    )