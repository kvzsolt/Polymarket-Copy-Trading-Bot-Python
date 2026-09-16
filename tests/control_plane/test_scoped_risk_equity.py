"""Account risk reads cannot reuse a process-global baseline or invent zero daily loss."""

from decimal import Decimal

from services.control_plane.queries_trading import RiskQueryService
from tests.control_plane.test_extended_reads import (
    FakePortfolioCache,
    FakeRiskPort,
    FixedClock,
    build_config,
    portfolio,
)


async def test_daily_equity_probe_receives_the_requested_account() -> None:
    accounts = []

    def read(account: str) -> Decimal:
        accounts.append(account)
        return {"account-a": Decimal(400), "account-b": Decimal(800)}[account]

    query = RiskQueryService(
        port=FakeRiskPort(),
        portfolio=None,
        config=build_config(),
        clock=FixedClock(),
        day_start_equity=read,
    )
    assert (await query.get("account-a")).day_start_equity_usdc == "400"
    assert (await query.get("account-b")).day_start_equity_usdc == "800"
    assert accounts == ["account-a", "account-b"]


async def test_unscoped_legacy_probe_cannot_be_attributed_to_any_account() -> None:
    query = RiskQueryService(
        port=FakeRiskPort(),
        portfolio=None,
        config=build_config(),
        clock=FixedClock(),
        day_start_equity=lambda: Decimal(999),
    )
    assert (await query.get("account-a")).day_start_equity_usdc is None


async def test_missing_daily_baseline_withholds_drawdown_gated_headroom() -> None:
    query = RiskQueryService(
        port=FakeRiskPort(),
        portfolio=FakePortfolioCache(portfolio()),
        config=build_config(enable_balance_tiered_policy=True),
        clock=FixedClock(),
    )
    view = await query.get("account-a")
    assert view.day_start_equity_usdc is None
    assert view.headroom is None


async def test_ungated_headroom_does_not_present_missing_daily_loss_as_zero() -> None:
    query = RiskQueryService(
        port=FakeRiskPort(),
        portfolio=FakePortfolioCache(portfolio()),
        config=build_config(enable_balance_tiered_policy=False),
        clock=FixedClock(),
    )
    view = await query.get("account-a")
    assert view.headroom is not None
    assert view.headroom.daily_loss_usdc is None
