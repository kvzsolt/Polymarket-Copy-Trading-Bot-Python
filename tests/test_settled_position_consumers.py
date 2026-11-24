"""Settled inventory contributes redemption equity without active trading exposure."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.config.app_config import AppConfig
from pmcopy.domain.position_lifecycle import PositionLifecycle
from pmcopy.orchestrators.lifecycle import runtime_summary
from pmcopy.orchestrators.pipeline import trade_pipeline
from pmcopy.services.risk.account_snapshot import AccountRiskSnapshotBuilder
from tests.test_legacy_marked_risk_snapshot import FixedQuote


@dataclass(frozen=True)
class Holding:
    """Expose an exact lifecycle-bearing row independent of durable storage."""

    token_id: str
    size_tokens: Decimal
    avg_entry_price: Decimal
    lifecycle: PositionLifecycle = PositionLifecycle.UNKNOWN
    settlement_price: Decimal | None = None
    user_id: str = "account"
    condition_id: str = "condition"
    leader_id: str = "leader"


class Quotes:
    """Track mark requests and return injected deterministic market evidence."""

    def __init__(self, quote: FixedQuote | None = None) -> None:
        self._quote = quote
        self.requested: list[str] = []

    def get_quote(self, token_id: str) -> FixedQuote | None:
        """Return an active market mark and record its raw token identity."""
        self.requested.append(token_id)
        return self._quote


@pytest.mark.parametrize("payout", [Decimal(0), Decimal(1), Decimal("0.5")])
def test_settled_claims_are_equity_without_active_risk_or_book_requests(payout: Decimal) -> None:
    """Redeemable payouts must neither consume exposure nor become spendable cash."""
    quotes = Quotes()
    snapshot = AccountRiskSnapshotBuilder(quotes, max_age_seconds=1).build(
        account="account",
        free_cash=Decimal(100),
        positions=[Holding("settled", Decimal(10), Decimal(0), PositionLifecycle.SETTLED, payout)],
        condition_id="condition",
        leader_id="leader",
    )

    assert snapshot.equity_usdc == Decimal(100) + Decimal(10) * payout
    assert snapshot.free_cash_usdc == Decimal(100)
    assert snapshot.total_exposure_usdc == Decimal(0)
    assert snapshot.market_exposure_usdc == Decimal(0)
    assert snapshot.leader_exposure_usdc == Decimal(0)
    assert snapshot.open_positions_count == 0
    assert quotes.requested == []


def test_settled_missing_payout_keeps_equity_unknown_without_active_exposure() -> None:
    """An obsolete market quote cannot stand in for unknown settlement terms."""
    quotes = Quotes(FixedQuote())
    snapshot = AccountRiskSnapshotBuilder(quotes, max_age_seconds=1).build(
        account="account",
        free_cash=Decimal(100),
        positions=[Holding("settled", Decimal(10), Decimal("0.4"), PositionLifecycle.SETTLED)],
        condition_id="condition",
        leader_id="leader",
    )

    assert snapshot.equity_usdc is None
    assert snapshot.total_exposure_usdc == Decimal(0)
    assert snapshot.open_positions_count == 0
    assert quotes.requested == []


@pytest.mark.parametrize("lifecycle", [PositionLifecycle.ACTIVE, PositionLifecycle.UNKNOWN])
def test_unknown_or_active_lifecycle_keeps_cost_and_count_when_quote_is_missing(
    lifecycle: PositionLifecycle,
) -> None:
    """Missing or zero market value never proves that inventory is settled."""
    snapshot = AccountRiskSnapshotBuilder(None, max_age_seconds=1).build(
        account="account",
        free_cash=Decimal(100),
        positions=[Holding("active", Decimal(10), Decimal("0.4"), lifecycle)],
        condition_id="condition",
        leader_id="leader",
    )

    assert snapshot.equity_usdc is None
    assert snapshot.total_exposure_usdc == Decimal(4)
    assert snapshot.open_positions_count == 1


def test_zero_share_rows_are_not_open_positions() -> None:
    """A zero-size repository row must not consume active position capacity."""
    snapshot = AccountRiskSnapshotBuilder(None, max_age_seconds=1).build(
        account="account",
        free_cash=Decimal(100),
        positions=[Holding("zero", Decimal(0), Decimal("0.4"))],
        condition_id="condition",
        leader_id="leader",
    )

    assert snapshot.equity_usdc == Decimal(100)
    assert snapshot.open_positions_count == 0


@pytest.mark.asyncio
async def test_buy_admission_reads_complete_held_inventory(monkeypatch: pytest.MonkeyPatch) -> None:
    """The account snapshot includes settled claims omitted by the active view."""
    held = AsyncMock(
        return_value=[
            Holding("settled", Decimal(10), Decimal(0), PositionLifecycle.SETTLED, Decimal(1))
        ]
    )
    opened = AsyncMock(return_value=[])
    context = SimpleNamespace(
        user_id="account",
        wallet_address="0x" + "1" * 40,
        extra={},
        config=AppConfig(),
        market_quotes=None,
        positions_repo=SimpleNamespace(list_held_positions=held, list_open_positions=opened),
    )
    monkeypatch.setattr(trade_pipeline, "get_my_balance", AsyncMock(return_value=Decimal(100)))

    snapshot = await trade_pipeline._fetch_balance_snapshot(context, "condition", "leader")

    assert snapshot is not None
    assert snapshot.equity_usdc == Decimal(110)
    held.assert_awaited_once_with("account")
    opened.assert_not_awaited()


def test_startup_summary_separates_76_settled_holdings_from_active_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator's settled zero-value inventory must display zero open risk."""
    logger = MagicMock()
    render = MagicMock()
    monkeypatch.setattr(runtime_summary, "logger", logger)
    monkeypatch.setattr(runtime_summary.console, "print", render)
    monkeypatch.setattr(runtime_summary, "print_startup_health_check", MagicMock())

    runtime_summary.print_startup_summary(
        wallet="0x" + "1" * 40,
        available_cash=Decimal(100),
        total_portfolio=Decimal(100),
        positions=[
            {
                "asset": str(index + 1),
                "size": Decimal(10),
                "currentValue": Decimal(0),
                "lifecycle": "settled",
                "settlement_price": Decimal(0),
            }
            for index in range(76)
        ],
        active_leaders=[],
        config=AppConfig(),
        health_report=SimpleNamespace(as_log_payload=lambda: {}),
    )

    extra = logger.info.call_args.kwargs["extra"]
    assert extra["openPositionCount"] == 0
    assert extra["settledPositionCount"] == 76
    assert extra["livePositions"] == []
    panel = render.call_args_list[0].args[0]
    assert "open_positions: 0" in panel.renderable
    assert "settled_holdings: 76" in panel.renderable
