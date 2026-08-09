"""Contracts for deterministic per-fill protective exits."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.copy_tasks.protective_exits import (
    FillRiskLot,
    LayeredLevel,
    ProtectiveExitDecision,
    ProtectiveExitEngine,
    ProtectiveRiskPolicy,
)


def _lot(**changes: object) -> FillRiskLot:
    base: dict[str, object] = {
        "lot_id": "lot-1",
        "task_id": "task-1",
        "market_id": "market",
        "outcome_id": "yes",
        "entry_price": Decimal("0.50"),
        "original_shares": Decimal("100"),
        "remaining_shares": Decimal("100"),
        "opened_at": datetime(2026, 8, 8, tzinfo=UTC),
        "peak_price": Decimal("0.60"),
    }
    base.update(changes)
    return FillRiskLot(**base)  # type: ignore[arg-type]


def test_percentage_stop_and_take_profit_are_based_on_each_fill_entry() -> None:
    engine = ProtectiveExitEngine()
    policy = ProtectiveRiskPolicy(
        stop_loss_percent=Decimal("20"),
        take_profit_percent=Decimal("30"),
    )
    assert engine.evaluate(_lot(), policy, current_price=Decimal("0.39")).reason == "stop_loss"
    assert engine.evaluate(_lot(), policy, current_price=Decimal("0.66")).reason == "take_profit"


def test_trailing_and_layered_exit_return_exact_share_fraction() -> None:
    engine = ProtectiveExitEngine()
    trailing = ProtectiveRiskPolicy(trailing_stop_percent=Decimal("10"))
    decision = engine.evaluate(_lot(), trailing, current_price=Decimal("0.53"))
    assert decision.reason == "trailing_stop"
    assert decision.shares_to_exit == Decimal("100")

    layered = ProtectiveRiskPolicy(
        layered_take_profit=(LayeredLevel(Decimal("20"), Decimal("0.50")),),
    )
    decision = engine.evaluate(_lot(), layered, current_price=Decimal("0.61"))
    assert decision.reason == "layered_take_profit"
    assert decision.shares_to_exit == Decimal("50.00")


def test_fixed_stop_loss_price_triggers_full_exit() -> None:
    engine = ProtectiveExitEngine()
    policy = ProtectiveRiskPolicy(stop_loss_price=Decimal("0.40"))
    decision = engine.evaluate(_lot(), policy, current_price=Decimal("0.39"))
    assert decision.reason == "stop_loss"
    assert decision.terminal is True
    assert decision.shares_to_exit == Decimal("100")


def test_fixed_take_profit_price_triggers_full_exit() -> None:
    engine = ProtectiveExitEngine()
    policy = ProtectiveRiskPolicy(take_profit_price=Decimal("0.65"))
    decision = engine.evaluate(_lot(), policy, current_price=Decimal("0.66"))
    assert decision.reason == "take_profit"
    assert decision.terminal is True


def test_disabled_policy_returns_no_trigger() -> None:
    engine = ProtectiveExitEngine()
    policy = ProtectiveRiskPolicy(enabled=False, stop_loss_percent=Decimal("20"))
    decision = engine.evaluate(_lot(), policy, current_price=Decimal("0.10"))
    assert isinstance(decision, ProtectiveExitDecision)
    assert decision.should_exit is False
    assert decision.reason == "protective_exit_disabled"


def test_layered_level_rejects_invalid_fraction() -> None:
    with pytest.raises(ValueError, match="exit_fraction"):
        LayeredLevel(Decimal("20"), Decimal("0"))


def test_lot_rejects_naive_opened_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _lot(opened_at=datetime(2026, 8, 8))


def test_lot_rejects_remaining_above_original() -> None:
    with pytest.raises(ValueError, match="remaining_shares"):
        _lot(remaining_shares=Decimal("200"))


def test_layered_exit_skips_already_triggered_levels() -> None:
    engine = ProtectiveExitEngine()
    policy = ProtectiveRiskPolicy(
        layered_stop_loss=(
            LayeredLevel(Decimal("10"), Decimal("0.50")),
            LayeredLevel(Decimal("20"), Decimal("1")),
        ),
    )
    lot = _lot(triggered_levels=("sl:0",))
    decision = engine.evaluate(lot, policy, current_price=Decimal("0.39"))
    assert decision.reason == "layered_stop_loss"
    assert decision.level_id == "sl:1"
    assert decision.shares_to_exit == Decimal("100")
    assert decision.terminal is True


def test_no_trigger_when_price_in_safe_zone() -> None:
    engine = ProtectiveExitEngine()
    policy = ProtectiveRiskPolicy(
        stop_loss_percent=Decimal("20"),
        take_profit_percent=Decimal("30"),
    )
    decision = engine.evaluate(_lot(), policy, current_price=Decimal("0.50"))
    assert decision.should_exit is False
    assert decision.reason == "no_protective_trigger"