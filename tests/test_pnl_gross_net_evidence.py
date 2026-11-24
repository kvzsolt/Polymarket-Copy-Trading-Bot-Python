"""A session's gross cash result must not masquerade as fee-adjusted net performance."""

from decimal import Decimal

import pytest

from pmcopy.services.accounting.pnl_tracker import PnLTracker


@pytest.mark.asyncio
async def test_session_gross_pnl_and_missing_net_evidence_are_explicit(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_buy("market", ".50", "200", "100")
    tracker.record_sell("market", ".55", "100", "55", entry_price=".50", realized_pnl="5")
    metrics = tracker.metrics()
    assert metrics["gross_realized_pnl"] == Decimal(5)
    assert metrics["net_pnl"] is None


@pytest.mark.asyncio
async def test_unknown_cost_basis_cannot_be_reported_as_complete_account_realized_pnl(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_sell("unknown", ".55", "100", "55", entry_price=None, realized_pnl=None)
    assert tracker.metrics()["gross_realized_pnl"] is None
