"""Account startup cannot continue after an unavailable inventory sync."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.orchestrators.lifecycle.account_inventory_startup import InitialInventorySync
from pmcopy.services.observability.health_server import ReadinessTracker


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["missing", "failed"])
async def test_failed_initial_inventory_inhibits_readiness_and_aborts(binding) -> None:
    readiness = ReadinessTracker(initial={"reconciler_ok": True})
    sync = (
        None
        if binding == "missing"
        else SimpleNamespace(
            initial_sync=AsyncMock(side_effect=ValueError("SYNTHETIC_STARTUP_SECRET"))
        )
    )
    with pytest.raises(RuntimeError, match="Initial account inventory is unavailable") as caught:
        await InitialInventorySync(readiness).initialize(sync)
    assert "SYNTHETIC_STARTUP_SECRET" not in str(caught.value)
    assert readiness.snapshot()["reconciler_ok"] is False


@pytest.mark.asyncio
async def test_applied_inventory_preserves_existing_readiness_inhibit() -> None:
    readiness = ReadinessTracker(initial={"reconciler_ok": False})
    sync = SimpleNamespace(initial_sync=AsyncMock())
    await InitialInventorySync(readiness).initialize(sync)
    sync.initial_sync.assert_awaited_once()
    assert readiness.snapshot()["reconciler_ok"] is False
