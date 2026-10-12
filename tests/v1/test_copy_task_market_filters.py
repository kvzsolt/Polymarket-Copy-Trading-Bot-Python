"""Contracts for Kreo-style market eligibility and sports-entry filters."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pmcopy.services.copy_tasks.admission import CopyTaskAdmissionEngine
from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    TaskAdmissionContext,
    TaskMarketFilter,
    TaskSpendPolicy,
)


def task(
    *,
    market_filter: TaskMarketFilter | None = None,
    spend: TaskSpendPolicy | None = None,
) -> CopyTask:
    """Return one deterministic task with selected filter policies."""

    return CopyTask(
        task_id="task-filter",
        name="Filter task",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        market_filter=market_filter or TaskMarketFilter(),
        spend=spend or TaskSpendPolicy(),
    )


def context(**overrides: object) -> TaskAdmissionContext:
    """Return one otherwise-eligible source market snapshot."""

    values: dict[str, object] = {
        "now": datetime(2026, 8, 8, tzinfo=UTC),
        "action": "BUY",
        "outcome_label": "YES",
        "leader_trade_notional": Decimal("20"),
        "leader_price": Decimal("0.50"),
        "market_id": "market-a",
        "market_title": "Bitcoin up or down",
        "category": "Crypto",
        "days_until_close": Decimal("1"),
        "task_total_spend": Decimal("0"),
        "task_daily_spend": Decimal("0"),
        "task_market_spend": Decimal("0"),
        "task_has_entered_market": False,
        "global_market_has_entry": False,
        "market_liquidity_usdc": Decimal("5000"),
        "market_volume_24h_usdc": Decimal("12000"),
        "token_symbol": "BTC",
        "market_timeframe": "5m",
        "sports_market_type": "moneyline",
        "task_has_entered_sports_type": False,
    }
    values.update(overrides)
    return TaskAdmissionContext(**values)  # type: ignore[arg-type]


def test_minimum_liquidity_and_volume_are_independent_hard_gates() -> None:
    engine = CopyTaskAdmissionEngine()
    candidate = task(
        market_filter=TaskMarketFilter(
            min_liquidity_usdc=Decimal("4000"),
            min_volume_24h_usdc=Decimal("10000"),
        )
    )

    assert engine.evaluate(candidate, context()).allowed is True
    assert (
        engine.evaluate(
            candidate,
            context(market_liquidity_usdc=Decimal("3999.99")),
        ).reason
        == "market_liquidity_below_minimum"
    )
    assert (
        engine.evaluate(
            candidate,
            context(market_volume_24h_usdc=Decimal("9999.99")),
        ).reason
        == "market_volume_below_minimum"
    )


def test_crypto_token_and_timeframe_filters_are_normalized() -> None:
    engine = CopyTaskAdmissionEngine()
    candidate = task(
        market_filter=TaskMarketFilter(
            tokens=("btc", "ETH"),
            timeframes=("5M", "15m"),
        )
    )

    assert engine.evaluate(
        candidate,
        context(token_symbol="BTC", market_timeframe="5m"),
    ).allowed
    assert engine.evaluate(candidate, context(token_symbol="SOL")).reason == "token_filtered"
    assert engine.evaluate(candidate, context(market_timeframe="1h")).reason == "timeframe_filtered"


def test_one_entry_per_sports_type_is_task_scoped_and_amount_independent() -> None:
    engine = CopyTaskAdmissionEngine()
    candidate = task(
        spend=TaskSpendPolicy(one_entry_per_sports_type=True),
    )

    blocked = engine.evaluate(
        candidate,
        context(
            sports_market_type="spread",
            task_has_entered_sports_type=True,
            proposed_notional=Decimal("5"),
        ),
    )
    unrelated = engine.evaluate(
        candidate,
        context(
            sports_market_type="total",
            task_has_entered_sports_type=False,
            proposed_notional=Decimal("5"),
        ),
    )

    assert blocked.allowed is False
    assert blocked.reason == "task_one_entry_per_sports_type"
    assert unrelated.allowed is True
