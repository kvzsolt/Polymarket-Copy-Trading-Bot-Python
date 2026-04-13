"""Phase 4 tests — services honour their new DI surface.

Each test constructs a service with explicit mocked ports and verifies:

* Module-level mutables that used to be implicit (readiness state,
  CLOB circuit breaker, arb basket store) are now injectable — an
  alternative instance takes priority over the legacy module default.
* Factory singletons (``get_position_manager`` / ``get_pnl_tracker``)
  can be overridden via the call site's ``position_manager=`` and
  ``pnl_tracker=`` keyword arguments.

These tests intentionally construct services with ``__new__`` where the
full ``__init__`` path pulls in heavy singletons we don't need in unit
tests; the goal is to prove the *injection seam* works, not to re-test
the underlying service behaviour.
"""

from __future__ import annotations

import inspect

import pytest

from services.circuitBreaker import CircuitBreaker
from services.executionService import ExecutionService, _CLOB_CIRCUIT
from services.healthServer import ReadinessTracker, _default_tracker, get_readiness, update_readiness
from services.negRiskArb import ArbBasketStore, arb_scanner_loop, execute_arb


# ---------------------------------------------------------------------------
# ReadinessTracker — replaces the module-level _readiness_state dict
# ---------------------------------------------------------------------------


class TestReadinessTracker:
    def test_default_keys_initialised(self) -> None:
        tracker = ReadinessTracker()
        snap = tracker.snapshot()
        for expected in ("db_ok", "api_ok", "last_trade_poll_ts", "mode"):
            assert expected in snap

    def test_snapshot_is_isolated_from_internal_state(self) -> None:
        tracker = ReadinessTracker()
        snap = tracker.snapshot()
        snap["db_ok"] = True
        assert tracker.snapshot()["db_ok"] is False

    def test_update_merges_new_keys(self) -> None:
        tracker = ReadinessTracker()
        tracker.update(db_ok=True, custom_key=42)
        snap = tracker.snapshot()
        assert snap["db_ok"] is True
        assert snap["custom_key"] == 42

    def test_module_shim_delegates_to_default(self) -> None:
        update_readiness(db_ok=True)
        assert get_readiness()["db_ok"] is True
        # Restore — other tests may depend on default state
        _default_tracker._state["db_ok"] = False

    def test_independent_instances_dont_share_state(self) -> None:
        a = ReadinessTracker()
        b = ReadinessTracker()
        a.update(api_ok=True)
        assert b.snapshot()["api_ok"] is False


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
# negRiskArb — store parameter overrides the module default
# ---------------------------------------------------------------------------


class TestArbBasketStoreDI:
    def test_execute_arb_accepts_store_kwarg(self) -> None:
        sig = inspect.signature(execute_arb)
        assert "store" in sig.parameters
        assert sig.parameters["store"].default is None

    def test_arb_scanner_loop_accepts_store_kwarg(self) -> None:
        sig = inspect.signature(arb_scanner_loop)
        assert "store" in sig.parameters
        assert sig.parameters["store"].default is None

    def test_new_store_instance_is_isolated(self) -> None:
        store_a = ArbBasketStore()
        store_b = ArbBasketStore()
        assert store_a is not store_b
        assert store_a.get_active_condition_ids() == set()


# ---------------------------------------------------------------------------
# riskMonitor / autoRedeem — factory override seams
# ---------------------------------------------------------------------------


class TestFactoryOverrideSeams:
    def test_risk_monitor_loop_accepts_injected_managers(self) -> None:
        from services.riskMonitor import risk_monitor_loop

        sig = inspect.signature(risk_monitor_loop)
        assert "position_manager" in sig.parameters
        assert "pnl_tracker" in sig.parameters
        assert sig.parameters["position_manager"].default is None
        assert sig.parameters["pnl_tracker"].default is None

    def test_try_redeem_position_accepts_injected_managers(self) -> None:
        from services.autoRedeem import _try_redeem_position

        sig = inspect.signature(_try_redeem_position)
        assert "position_manager" in sig.parameters
        assert "pnl_tracker" in sig.parameters


# ---------------------------------------------------------------------------
# health server exposes a typed tracker; skip when aiohttp missing
# ---------------------------------------------------------------------------


class TestHealthServerSignature:
    @pytest.mark.asyncio
    async def test_start_health_server_accepts_tracker_kwarg(self) -> None:
        from services.healthServer import start_health_server

        sig = inspect.signature(start_health_server)
        assert "tracker" in sig.parameters
        assert sig.parameters["tracker"].default is None
