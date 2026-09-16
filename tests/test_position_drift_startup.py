"""PR7: startup drift sweep wiring + structured logging."""

from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.app_config import PositionDriftConfig
from orchestrators.runtime_bootstrap import startup_drift_check
from services.healthServer import ReadinessTracker


def _drift_config(**overrides) -> PositionDriftConfig:
    base = dict(
        check_on_startup=True,
        fail_closed=True,
        allow_auto_prune_zero=True,
        allow_external_position_tracking=False,
        pause_scope_on_unresolved=True,
        max_size_epsilon=Decimal("0.000001"),
        prune_holdoff_seconds=60.0,
    )
    base.update(overrides)
    return PositionDriftConfig(**base)


def _app_config(*, drift: PositionDriftConfig | None = None) -> SimpleNamespace:
    return SimpleNamespace(reconciliation=SimpleNamespace(position_drift=drift or _drift_config()))


def _runtime_user(user_id: str = "u1", funder: str = "0xfunder") -> SimpleNamespace:
    return SimpleNamespace(
        user_id=user_id,
        wallet=SimpleNamespace(funder_address=funder),
    )


def _user_state(report) -> SimpleNamespace:
    drift_svc = MagicMock()
    drift_svc.scan_user = AsyncMock(return_value=report)
    return SimpleNamespace(position_drift_service=drift_svc)


@pytest.mark.asyncio
async def test_startup_drift_check_clean_sets_drift_ok_true():
    readiness = ReadinessTracker()
    report = SimpleNamespace(clean=True, paused_scopes=())
    runtime_users = [_runtime_user("u1")]
    user_states = {"u1": _user_state(report)}

    per_user = await startup_drift_check(
        runtime_users=runtime_users,
        user_states=user_states,
        config=_app_config(),
        readiness=readiness,
    )

    assert per_user == {"u1": True}
    assert readiness.snapshot()["drift_ok"] is True


@pytest.mark.asyncio
async def test_startup_drift_check_unresolved_sets_drift_ok_false():
    readiness = ReadinessTracker()
    report = SimpleNamespace(clean=False, paused_scopes=("u1|cond1|0xtok",))
    runtime_users = [_runtime_user("u1")]
    user_states = {"u1": _user_state(report)}

    per_user = await startup_drift_check(
        runtime_users=runtime_users,
        user_states=user_states,
        config=_app_config(),
        readiness=readiness,
    )

    assert per_user == {"u1": False}
    snap = readiness.snapshot()
    assert snap["drift_ok"] is False
    ok, failed = readiness.is_ready(strict=True)
    assert ok is False
    assert "drift_ok" in failed


@pytest.mark.asyncio
async def test_startup_drift_check_disabled_short_circuits():
    readiness = ReadinessTracker()
    cfg = _app_config(drift=_drift_config(check_on_startup=False))
    drift_svc = MagicMock()
    drift_svc.scan_user = AsyncMock()
    runtime_users = [_runtime_user("u1")]
    user_states = {"u1": SimpleNamespace(position_drift_service=drift_svc)}

    per_user = await startup_drift_check(
        runtime_users=runtime_users,
        user_states=user_states,
        config=cfg,
        readiness=readiness,
    )

    assert per_user == {}
    assert readiness.snapshot()["drift_ok"] is True
    drift_svc.scan_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_drift_check_propagates_failure_to_readiness():
    readiness = ReadinessTracker()
    drift_svc = MagicMock()
    drift_svc.scan_user = AsyncMock(side_effect=RuntimeError("boom"))
    runtime_users = [_runtime_user("u1")]
    user_states = {"u1": SimpleNamespace(position_drift_service=drift_svc)}

    per_user = await startup_drift_check(
        runtime_users=runtime_users,
        user_states=user_states,
        config=_app_config(),
        readiness=readiness,
    )

    assert per_user == {"u1": False}
    assert readiness.snapshot()["drift_ok"] is False


def test_log_position_drift_emits_structured_payload(caplog):
    from utils.logger import log_position_drift

    caplog.set_level(logging.INFO, logger="PMBot.Test.Drift")
    log_position_drift(
        logging.getLogger("PMBot.Test.Drift"),
        status="size_mismatch_negative",
        action="require_operator_review",
        user_id="u1",
        condition_id="cond1",
        asset="0xtok",
        local_size=10.0,
        venue_size=5.0,
        drift_size=-5.0,
        reason="off-bot sell",
    )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "position_drift"]
    assert len(matching) == 1
    rec = matching[0]
    assert rec.status == "size_mismatch_negative"
    assert rec.action == "require_operator_review"
    assert rec.userId == "u1"
    assert rec.localSize == 10.0
    assert rec.venueSize == 5.0
    assert rec.driftSize == -5.0
