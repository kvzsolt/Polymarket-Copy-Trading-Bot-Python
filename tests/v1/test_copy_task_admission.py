"""Contracts for the pure copy-task admission engine."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pmcopy.services.copy_tasks.admission import CopyTaskAdmissionEngine
from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    TaskAdmissionContext,
    TaskMarketFilter,
    TaskPricePolicy,
    TaskSpendPolicy,
    TaskTriggerPolicy,
)


_NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
_LEADER = "0x" + "11" * 20


def _context(**changes: object) -> TaskAdmissionContext:
    base: dict[str, object] = {
        "now": _NOW,
        "action": "BUY",
        "outcome_label": "YES",
        "leader_trade_notional": Decimal("50"),
        "leader_price": Decimal("0.45"),
        "market_id": "cond-1",
        "market_title": "BTC above 100k",
        "category": "crypto",
        "days_until_close": Decimal("3"),
        "task_total_spend": Decimal("0"),
        "task_daily_spend": Decimal("0"),
        "task_market_spend": Decimal("0"),
        "task_has_entered_market": False,
        "global_market_has_entry": False,
    }
    base.update(changes)
    return TaskAdmissionContext(**base)  # type: ignore[arg-type]


def _task(**changes: object) -> CopyTask:
    base: dict[str, object] = {
        "task_id": "t1",
        "name": "t1",
        "leader_wallet": _LEADER,
        "follower_account": "acct",
    }
    base.update(changes)
    return CopyTask(**base)  # type: ignore[arg-type]


def test_deleted_task_is_rejected() -> None:
    decision = CopyTaskAdmissionEngine().evaluate(
        _task(status=CopyTaskStatus.DELETED),
        _context(),
    )
    assert decision.allowed is False
    assert decision.reason == "task_deleted"


def test_paused_task_is_rejected() -> None:
    decision = CopyTaskAdmissionEngine().evaluate(
        _task(status=CopyTaskStatus.PAUSED),
        _context(),
    )
    assert decision.allowed is False
    assert decision.reason == "task_paused"


def test_below_min_trigger_is_rejected() -> None:
    task = _task(trigger=TaskTriggerPolicy(min_notional=Decimal("100")))
    decision = CopyTaskAdmissionEngine().evaluate(task, _context())
    assert decision.allowed is False
    assert decision.reason == "below_min_trigger"


def test_above_max_trigger_is_rejected() -> None:
    task = _task(trigger=TaskTriggerPolicy(max_notional=Decimal("10")))
    decision = CopyTaskAdmissionEngine().evaluate(task, _context())
    assert decision.allowed is False
    assert decision.reason == "above_max_trigger"


def test_price_outside_band_is_rejected() -> None:
    task = _task(price=TaskPricePolicy(min_price=Decimal("0.20"), max_price=Decimal("0.30")))
    decision = CopyTaskAdmissionEngine().evaluate(task, _context())
    assert decision.allowed is False
    assert decision.reason == "price_outside_band"


def test_excluded_keyword_is_rejected() -> None:
    task = _task(market_filter=TaskMarketFilter(exclude_keywords=("scam",)))
    decision = CopyTaskAdmissionEngine().evaluate(
        task,
        _context(market_title="scam token moon"),
    )
    assert decision.allowed is False
    assert decision.reason == "excluded_keyword"


def test_required_keyword_missing_is_rejected() -> None:
    task = _task(market_filter=TaskMarketFilter(include_keywords=("btc",)))
    decision = CopyTaskAdmissionEngine().evaluate(
        task,
        _context(market_title="eth above 5k"),
    )
    assert decision.allowed is False
    assert decision.reason == "required_keyword_missing"


def test_category_filter_rejects_non_matching_category() -> None:
    task = _task(market_filter=TaskMarketFilter(categories=("sports",)))
    decision = CopyTaskAdmissionEngine().evaluate(task, _context(category="crypto"))
    assert decision.allowed is False
    assert decision.reason == "category_filtered"


def test_total_spend_cap_rejects_overspend() -> None:
    task = _task(spend=TaskSpendPolicy(max_total=Decimal("100")))
    decision = CopyTaskAdmissionEngine().evaluate(
        task,
        _context(task_total_spend=Decimal("80"), leader_trade_notional=Decimal("50")),
    )
    assert decision.allowed is False
    assert decision.reason == "total_spend_cap"


def test_one_entry_per_market_rejects_second_entry() -> None:
    task = _task(spend=TaskSpendPolicy(one_entry_per_market=True))
    decision = CopyTaskAdmissionEngine().evaluate(
        task,
        _context(task_has_entered_market=True),
    )
    assert decision.allowed is False
    assert decision.reason == "task_one_entry_per_market"


def test_global_one_entry_per_market_rejects_other_task_entry() -> None:
    task = _task(spend=TaskSpendPolicy(global_one_entry_per_market=True))
    decision = CopyTaskAdmissionEngine().evaluate(
        task,
        _context(global_market_has_entry=True),
    )
    assert decision.allowed is False
    assert decision.reason == "global_one_entry_per_market"


def test_admitted_when_all_gates_pass() -> None:
    decision = CopyTaskAdmissionEngine().evaluate(_task(), _context())
    assert decision.allowed is True
    assert decision.reason == "admitted"


def test_sell_action_skips_spend_gates() -> None:
    task = _task(spend=TaskSpendPolicy(max_total=Decimal("1")))
    decision = CopyTaskAdmissionEngine().evaluate(
        task,
        _context(action="SELL", task_total_spend=Decimal("100")),
    )
    assert decision.allowed is True
