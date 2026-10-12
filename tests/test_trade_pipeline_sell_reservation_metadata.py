"""Regression coverage for SELL reservation metadata propagation."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pmcopy.orchestrators.trade_pipeline import _submit_copy_through_coordinator
from pmcopy.services.execution.coordinator import ExecutionCoordinatorResult
from pmcopy.services.execution.types import ExecutionRequest


class _Coordinator:
    """Capture one production copy intent."""

    def __init__(self) -> None:
        self.intents: list[object] = []

    async def submit_copy_trade(self, intent: object) -> ExecutionCoordinatorResult:
        """Record and accept the intent without venue I/O."""

        self.intents.append(intent)
        return ExecutionCoordinatorResult(
            accepted=True,
            status="submitted_unconfirmed",
            raw_result=SimpleNamespace(status="submitted_unconfirmed"),
        )


class _TargetService:
    """Capture the fresh SELL intent before coordinator submission."""

    def __init__(self) -> None:
        self.intents: list[object] = []

    async def record_fresh_target(self, intent: object) -> object:
        """Record and return the submitted intent unchanged."""

        self.intents.append(intent)
        return intent


@pytest.mark.asyncio
async def test_pipeline_forwards_fraction_live_cap_and_minimum_policy() -> None:
    """The coordinator receives every input needed for atomic remainder sizing."""

    coordinator = _Coordinator()
    target_service = _TargetService()
    context = SimpleNamespace(
        user_id="user-1",
        execution_coordinator=coordinator,
        extra={"leader_sell_target_service": target_service},
        config=None,
    )
    request = ExecutionRequest(
        user_id="user-1",
        leader_id="0xleader",
        token_id="123456",
        side="SELL",
        amount=Decimal("62.29"),
        max_slippage_bps=200,
        reason="leader_sell",
        signal_id="0xsignal",
        condition_id="cond-1",
        price_limit=Decimal("0.20"),
        min_order_size=Decimal("5"),
        leader_sell_fraction=Decimal("1"),
        live_position_size=Decimal("62.2902"),
        sub_min_sell_policy="round_up",
    )
    signal = SimpleNamespace(
        price=Decimal("0.21"),
        tx_hash="0xsignal",
        condition_id="cond-1",
        token_id="123456",
    )

    with patch("pmcopy.orchestrators.trade_pipeline.logger.info") as info_log:
        await _submit_copy_through_coordinator(
            context,
            request,
            signal,
            leader_id="0xleader",
        )

    [intent] = coordinator.intents
    assert target_service.intents == [intent]
    assert intent.leader_sell_fraction == Decimal("1")
    assert intent.live_position_size == Decimal("62.2902")
    assert intent.min_order_size == Decimal("5")
    assert intent.sub_min_sell_policy == "round_up"
    intent_log = next(
        call
        for call in info_log.call_args_list
        if call.args and "coordinator_intent_built" in str(call.args[0])
    )
    assert intent_log.kwargs["extra"]["leader_price"] == "0.21"
    assert intent_log.kwargs["extra"]["protected_limit"] == "0.20"
