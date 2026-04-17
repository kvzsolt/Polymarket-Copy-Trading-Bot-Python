"""Phase 4 tests — services honour their new DI surface.

Each test constructs a service with explicit mocked ports and verifies:

* Module-level mutables that used to be implicit (readiness state,
  CLOB circuit breaker, arb basket store) are now injectable.
* ReadinessTracker is injected into key orchestrators (LeaderRegistry, TradeMonitor, RiskMonitor).
"""

from __future__ import annotations

import inspect
import pytest
from unittest.mock import MagicMock

from services.circuitBreaker import CircuitBreaker
from services.executionService import ExecutionService, _CLOB_CIRCUIT
from services.healthServer import ReadinessTracker
from domain.trading_context import TradingContext
from config.app_config import load_config


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
            market_meta_service=object(),
            fee_service=object(),
            order_repo=object(),
        )
        assert svc.circuit_breaker is _CLOB_CIRCUIT

    def test_custom_breaker_is_respected(self) -> None:
        custom = CircuitBreaker(name="t", failure_threshold=1, cooldown_seconds=1)
        svc = ExecutionService(
            clob_client=object(),
            market_meta_service=object(),
            fee_service=object(),
            order_repo=object(),
            circuit_breaker=custom,
        )
        assert svc.circuit_breaker is custom


# ---------------------------------------------------------------------------
# Orchestrator DI Seams
# ---------------------------------------------------------------------------

class TestOrchestratorDISeams:
    def test_leader_registry_accepts_readiness_tracker(self) -> None:
        from orchestrators.leader_registry import LeaderRegistry
        sig = inspect.signature(LeaderRegistry.__init__)
        assert "readiness_tracker" in sig.parameters
        assert "config" in sig.parameters

    def test_trading_context_has_readiness_tracker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PRIVATE_KEY", "0x" + "a" * 64)
        monkeypatch.setenv("FUNDER_ADDRESS", "0x" + "b" * 40)
        monkeypatch.setenv("RPC_URL", "https://polygon-rpc.example.com")
        monkeypatch.setenv("CHAIN_ID", "137")
        config = load_config()
        tracker = ReadinessTracker()
        ctx = TradingContext(
            user_id="test",
            wallet_address="0x" + "c" * 40,
            client=MagicMock(),
            positions_repo=MagicMock(),
            fills_repo=MagicMock(),
            orders_repo=MagicMock(),
            pnl_tracker=MagicMock(),
            risk_engine=MagicMock(),
            accounting_service=MagicMock(),
            position_manager=MagicMock(),
            execution_service=MagicMock(),
            readiness=tracker
        )
        assert ctx.readiness is tracker

    def test_trade_monitor_accepts_readiness_tracker(self) -> None:
        from services.tradeMonitor import trade_monitor
        sig = inspect.signature(trade_monitor)
        assert "readiness_tracker" in sig.parameters
        assert "config" in sig.parameters

    def test_risk_monitor_loop_accepts_readiness_tracker(self) -> None:
        from services.riskMonitor import risk_monitor_loop
        sig = inspect.signature(risk_monitor_loop)
        assert "readiness_tracker" in sig.parameters
        assert "config" in sig.parameters
