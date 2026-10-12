"""Phase 4 tests — services honour their new DI surface.

Each test constructs a service with explicit mocked ports and verifies:

* Module-level mutables that used to be implicit (readiness state,
  CLOB circuit breaker, arb basket store) are now injectable.
* ReadinessTracker is injected into key orchestrators (LeaderRegistry, TradeMonitor, RiskMonitor).
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from pmcopy.services.circuitBreaker import CircuitBreaker
from pmcopy.services.executionService import ExecutionService, _CLOB_CIRCUIT
from pmcopy.services.healthServer import ReadinessTracker
from pmcopy.application.trading_context import TradingContext
from types import SimpleNamespace


def _make_config() -> SimpleNamespace:
    return SimpleNamespace(
        raw={},
        execution=SimpleNamespace(
            execution_require_fresh_book=True,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=2500,
            max_book_spread_ticks=0,
            slippage_tolerance=2.0,
            transaction_fee_bps=20,
        ),
    )


# ---------------------------------------------------------------------------
# ReadinessTracker
# ---------------------------------------------------------------------------


class TestReadinessTracker:
    def test_default_keys_initialised(self) -> None:
        tracker = ReadinessTracker()
        snap = tracker.snapshot()
        for expected in ("db_ok", "api_ok", "last_trade_poll_ts", "mode"):
            assert expected in snap

    def test_snapshot_is_isolated(self) -> None:
        tracker = ReadinessTracker()
        snap = tracker.snapshot()
        snap["db_ok"] = True
        assert tracker.snapshot()["db_ok"] is False


# ---------------------------------------------------------------------------
# ExecutionService — circuit breaker is injectable
# ---------------------------------------------------------------------------


class TestExecutionCircuitBreakerDI:
    def test_default_uses_module_breaker(self) -> None:
        svc = ExecutionService(
            clob_client=object(),
            fee_service=object(),
            order_repo=object(),
            config=_make_config(),
            clob_gateway=object(),
        )
        assert svc.circuit_breaker is _CLOB_CIRCUIT

    def test_custom_breaker_is_respected(self) -> None:
        custom = CircuitBreaker(name="t", failure_threshold=1, cooldown_seconds=1)
        svc = ExecutionService(
            clob_client=object(),
            fee_service=object(),
            order_repo=object(),
            circuit_breaker=custom,
            config=_make_config(),
            clob_gateway=object(),
        )
        assert svc.circuit_breaker is custom


# ---------------------------------------------------------------------------
# Orchestrator DI Seams
# ---------------------------------------------------------------------------


class TestOrchestratorDISeams:
    def test_leader_registry_accepts_readiness_tracker(self) -> None:
        from pmcopy.orchestrators.leader_registry import LeaderRegistry

        sig = inspect.signature(LeaderRegistry.__init__)
        assert "readiness_tracker" in sig.parameters
        assert "config" in sig.parameters

    def test_trading_context_has_readiness_tracker(self) -> None:
        tracker = ReadinessTracker()
        ctx = TradingContext(
            user_id="test",
            wallet_address="0x" + "c" * 40,
            client=MagicMock(),
            positions_repo=MagicMock(),
            fills_repo=MagicMock(),
            orders_repo=MagicMock(),
            risk_blocks_repo=MagicMock(),
            pnl_tracker=MagicMock(),
            risk_engine=MagicMock(),
            accounting_service=MagicMock(),
            position_manager=MagicMock(),
            execution_service=MagicMock(),
            readiness=tracker,
        )
        assert ctx.readiness is tracker

    def test_trade_monitor_accepts_readiness_tracker(self) -> None:
        from pmcopy.services.tradeMonitor import trade_monitor

        sig = inspect.signature(trade_monitor)
        assert "readiness_tracker" in sig.parameters
        assert "config" in sig.parameters
