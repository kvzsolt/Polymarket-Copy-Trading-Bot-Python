"""Restart must not admit entries with incomplete protective ownership restoration."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.services.risk.risk_monitor import rehydrate_position_manager


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["track", "trigger"])
async def test_protective_restore_failure_is_visible_and_sanitized(failure, caplog):
    marker = "synthetic-secret"
    manager = SimpleNamespace(get_position=Mock(return_value=None), track_buy=Mock())
    triggers = AsyncMock(has_trigger_for_reason=AsyncMock(return_value=False))
    if failure == "track":
        manager.track_buy.side_effect = RuntimeError(marker)
    else:
        triggers.has_trigger_for_reason.side_effect = RuntimeError(marker)
    context = SimpleNamespace(
        user_id="account",
        position_manager=manager,
        risk_triggers_repo=triggers,
        positions_repo=AsyncMock(
            list_open_positions=AsyncMock(
                return_value=[
                    SimpleNamespace(
                        condition_id="condition",
                        token_id="token",
                        leader_id="leader",
                        avg_entry_price=Decimal("0.5"),
                        size_tokens=Decimal(10),
                    )
                ]
            )
        ),
    )
    with pytest.raises(RuntimeError, match="position protection recovery unavailable") as caught:
        await rehydrate_position_manager(context)
    assert marker not in str(caught.value)
    assert marker not in caplog.text
