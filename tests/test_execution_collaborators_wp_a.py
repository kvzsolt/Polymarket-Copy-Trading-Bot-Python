"""Tests for WP-A extracted ExecutionService collaborators and WP-F CircuitBreaker."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from pmcopy.services.circuitBreaker import CircuitBreaker, CircuitState
from pmcopy.services.execution.admission_gates import (
    ExecutionAdmissionEvaluator,
    ValidatedAdmission,
)
from pmcopy.services.execution.types import ExecutionRequest, ExecutionResult


class TestCircuitBreakerStateObserverAndFactory:
    """WP-F: CircuitBreaker.for_venue_config and state-transition observer."""

    def test_state_observer_fires_on_open_half_open_and_close(self) -> None:
        observed: list[CircuitState] = []
        breaker = CircuitBreaker.for_venue_config(
            SimpleNamespace(execution=SimpleNamespace(circuit_failure_threshold=2)),
            cooldown_seconds=0.0,
            on_state_change=observed.append,
        )
        assert breaker.failure_threshold == 2
        breaker.record_failure()
        assert observed == []
        breaker.record_failure()
        assert observed == [CircuitState.OPEN]
        assert breaker.allow_request() is True
        assert observed == [CircuitState.OPEN, CircuitState.HALF_OPEN]
        breaker.record_success()
        assert observed == [CircuitState.OPEN, CircuitState.HALF_OPEN, CircuitState.CLOSED]


class TestExecutionAdmissionEvaluator:
    """WP-A: ExecutionAdmissionEvaluator preserves protective-exit vs global kill semantics."""

    @pytest.mark.asyncio
    async def test_execution_kill_switch_blocks_buy_but_allows_protective_sell(self) -> None:
        evaluator = ExecutionAdmissionEvaluator()
        breaker = CircuitBreaker(name="test", failure_threshold=5)
        cfg = SimpleNamespace(
            app=SimpleNamespace(kill_switch=False),
            execution=SimpleNamespace(kill_switch=True),
        )
        buy_req = ExecutionRequest(
            user_id="u1",
            leader_id="0xleader",
            token_id="tok-1",
            side="BUY",
            amount=Decimal("10"),
            max_slippage_bps=100,
            reason="copy_trade",
        )
        buy_res = await evaluator.evaluate(
            buy_req,
            config=cfg,
            trading_permission=None,
            circuit_breaker=breaker,
            geoblock_gate=None,
        )
        assert isinstance(buy_res, ExecutionResult)
        assert buy_res.status == "kill_switch"

        protective_req = ExecutionRequest(
            user_id="u1",
            leader_id="0xleader",
            token_id="tok-1",
            side="SELL",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="stop_loss",
        )
        sell_res = await evaluator.evaluate(
            protective_req,
            config=cfg,
            trading_permission=None,
            circuit_breaker=breaker,
            geoblock_gate=None,
        )
        assert isinstance(sell_res, ValidatedAdmission)
        assert sell_res.forced_sell_exit is True
