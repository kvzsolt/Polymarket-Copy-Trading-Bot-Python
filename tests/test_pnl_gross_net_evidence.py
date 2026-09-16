"""A session's gross cash result must not masquerade as fee-adjusted net performance."""

from decimal import Decimal

import pytest

from services.pnlTracker import PnLTracker
from tests.control_plane.test_extended_reads import pnl_service


@pytest.mark.asyncio
async def test_session_gross_pnl_and_missing_net_evidence_are_explicit(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_buy("market", ".50", "200", "100")
    tracker.record_sell("market", ".55", "100", "55", entry_price=".50", realized_pnl="5")
    metrics = tracker.metrics()
    assert metrics["gross_realized_pnl"] == Decimal(5)
    assert metrics["net_pnl"] is None
    view = await pnl_service(quotes={}, positions=(), tracker=tracker).get("account-a")
    assert view.realized.realized_pnl_usdc == "5.00"
    assert view.realized.realized_basis == "gross_before_fees"
    assert view.realized.net_realized_pnl_usdc is None


@pytest.mark.asyncio
async def test_unknown_cost_basis_cannot_be_reported_as_complete_account_realized_pnl(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_sell("unknown", ".55", "100", "55", entry_price=None, realized_pnl=None)
    view = await pnl_service(quotes={}, positions=(), tracker=tracker).get("account-a")
    assert view.realized.realized_pnl_usdc is None
    assert tracker.metrics()["gross_realized_pnl"] is None
