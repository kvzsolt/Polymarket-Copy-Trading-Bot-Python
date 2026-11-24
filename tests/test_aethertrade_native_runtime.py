"""Production source composition is event-only and changes require restart."""

import ast
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

from pmcopy.domain.grouped_detection import TraderDetectionPolicy
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.storage.local.detection_state import SqliteDetectionState
from tests.test_aethertrade_native_owner import OWNER


def test_grouped_config_defaults_and_per_owner_policy_are_typed() -> None:
    """Current and future configured owners default to Alchemy mined detection."""
    from pmcopy.config.app_config import _LayeredModel

    config = _LayeredModel.model_validate(
        {"detection": {"native": {"traders": [{"wallet": OWNER, "mode": "pending"}]}}}
    )
    assert config.detection.native.traders[0].mode == "pending"
    assert config.detection.native.pending_enabled is False
    assert config.detection.native.policies_for((OWNER, "0x" + "22" * 20))[1].mode == "mined"
    with pytest.raises(ValueError):
        _LayeredModel.model_validate(
            {"detection": {"native": {"traders": [{"wallet": OWNER, "mode": "polling"}]}}}
        )


def test_production_composition_has_no_rest_trade_monitor_or_legacy_source_fallback() -> None:
    """Neither startup nor nested dynamic discovery can create REST trade loops."""
    tree = ast.parse(Path("src/pmcopy/orchestrators/app/runner.py").read_text())
    runtime = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_run_application"
    )
    names = {
        node.func.id
        for node in ast.walk(runtime)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "run_leader_monitor" not in names
    assert "_build_detection_source_factory" not in names
    assert "_build_pending_copy_tracker" not in names


@pytest.mark.asyncio
async def test_runtime_budget_failure_pauses_without_retries_or_restarting_risk(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hard provider budget stop affects entries while other application loops remain owned elsewhere."""
    from pmcopy.orchestrators.detection.native_detection_runtime import NativeDetectionRuntime
    from pmcopy.utils.logger import get_logger

    app_logger = get_logger()
    monkeypatch.setattr(app_logger, "handlers", [caplog.handler])
    monkeypatch.setattr(app_logger, "level", logging.INFO)

    state = SqliteDetectionState(tmp_path / "state")
    attempts = []

    class Session:
        connected = False

        async def run(self):
            attempts.append(1)
            raise BudgetExhausted("quota")

    runtime = NativeDetectionRuntime(
        policies=(TraderDetectionPolicy(OWNER),),
        state=state,
        session_factory=lambda _groups: Session(),
        observer=Mock(),
        status_changed=Mock(),
    )
    with caplog.at_level(logging.CRITICAL):
        caplog.handler.setLevel(logging.INFO)
        await runtime.start()
        await runtime.wait()
    assert attempts == [1]
    assert runtime.connected is False
    assert runtime.states()[0].degraded_reason == "provider_budget_exhausted"
    assert "native_detection_state" in caplog.text
    assert "active_leaders=0" in caplog.text
    assert "provider_budget_exhausted" in caplog.text
    await runtime.stop()
    state.close()


@pytest.mark.asyncio
async def test_empty_roster_never_opens_and_new_roster_stays_awaiting_restart(
    tmp_path: Path,
) -> None:
    """Dynamic owners remain visible but cannot start an unacknowledged source."""
    from pmcopy.orchestrators.detection.native_detection_runtime import NativeDetectionRuntime

    state = SqliteDetectionState(tmp_path / "state")
    factory = Mock()
    runtime = NativeDetectionRuntime(
        policies=(), state=state, session_factory=factory, observer=Mock(), status_changed=Mock()
    )
    await runtime.start()
    runtime.update_watched_wallets({OWNER})
    assert runtime.states()[0].effective_mode == "paused"
    assert runtime.states()[0].degraded_reason == "awaiting_restart"
    factory.assert_not_called()
    await runtime.stop()
    state.close()


@pytest.mark.asyncio
async def test_unsupported_trader_does_not_disable_other_approved_owner_groups(
    tmp_path: Path,
) -> None:
    """Provider unavailability is visible per trader and never widens a subscription."""
    from pmcopy.orchestrators.detection.native_detection_runtime import NativeDetectionRuntime

    state = SqliteDetectionState(tmp_path / "state")
    groups_seen = []
    other = "0x" + "22" * 20

    class Session:
        connected = False

        async def run(self):
            raise BudgetExhausted("quota")

    def factory(groups):
        groups_seen.extend(groups)
        return Session()

    with pytest.raises(ValueError, match="provider"):
        TraderDetectionPolicy(other, provider="infura")
    runtime = NativeDetectionRuntime(
        policies=(TraderDetectionPolicy(OWNER), TraderDetectionPolicy(other, enabled=False)),
        state=state,
        session_factory=factory,
        observer=Mock(),
        status_changed=Mock(),
    )
    await runtime.start()
    await runtime.wait()
    assert [g.wallets for g in groups_seen] == [(OWNER,)]
    await runtime.stop()
    state.close()
