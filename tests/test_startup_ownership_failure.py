"""A missing durable ownership projection cannot produce a usable trading context."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.orchestrators.runtime_bootstrap import (
    build_trading_context,
    default_follow,
    startup_drift_check,
)
from pmcopy.services.healthServer import ReadinessTracker
from tests.config_helpers import load_test_config


@pytest.mark.asyncio
async def test_reservation_read_failure_aborts_context_without_secret_error(caplog):
    config = load_test_config()
    marker = "synthetic-dsn-credential"
    reservations = SimpleNamespace(list_active=AsyncMock(side_effect=RuntimeError(marker)))
    coordinator = SimpleNamespace(
        reservation_service=SimpleNamespace(reservations_repo=reservations),
        reservations_repo=reservations,
    )
    state = SimpleNamespace(
        user=SimpleNamespace(user_id="account", wallet=SimpleNamespace(funder_address="account")),
        clob_gateway=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        leader_performance=None,
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=object(),
        risk_triggers_repo=None,
        execution_coordinator=coordinator,
    )
    readiness = ReadinessTracker()
    with pytest.raises(RuntimeError, match="reservation ownership recovery unavailable") as caught:
        await build_trading_context(
            state, default_follow("account", "leader", config), config, readiness=readiness
        )
    assert marker not in str(caught.value)
    assert marker not in caplog.text
    assert readiness.snapshot()["reconciler_ok"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("states", [{}, {"account": SimpleNamespace()}])
async def test_missing_account_or_drift_service_is_not_ready(states):
    readiness = ReadinessTracker()
    config = load_test_config()
    user = SimpleNamespace(user_id="account", wallet=SimpleNamespace(funder_address="account"))
    result = await startup_drift_check(
        runtime_users=[user], user_states=states, config=config, readiness=readiness
    )
    assert result == {"account": False}
    assert readiness.snapshot()["drift_ok"] is False
