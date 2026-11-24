"""Provider failures cannot escape through market status or observer logs."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.clients.ws.market_ws import MarketWebSocket
from tests.test_book_update_atomicity import seeded_store
from tests.test_market_data_runtime import _make_runtime

SECRET = "SYNTHETIC_MARKET_PROVIDER_SECRET"


@pytest.mark.asyncio
async def test_failed_market_task_exposes_only_sanitized_state_and_exception(
    caplog, capsys
) -> None:
    runtime = _make_runtime(store=seeded_store())
    runtime._ws_client = SimpleNamespace(connect=AsyncMock(side_effect=RuntimeError(SECRET)))
    with pytest.raises(RuntimeError) as raised:
        await runtime._run_ws(["token"])
    assert SECRET not in str(raised.value)
    assert SECRET not in str(runtime.status())
    assert SECRET not in str(runtime.readiness.snapshot())
    assert SECRET not in caplog.text
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err


@pytest.mark.asyncio
async def test_failed_market_bootstrap_does_not_serialize_provider_error(caplog, capsys) -> None:
    runtime = _make_runtime(store=seeded_store())
    runtime.gateway.get_orderbook.side_effect = RuntimeError(SECRET)
    await runtime.bootstrap_tokens(["token"])
    assert runtime.status().last_error
    assert SECRET not in str(runtime.status())
    assert SECRET not in caplog.text
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err


def test_synchronous_market_observer_failure_does_not_log_provider_error(caplog, capsys) -> None:
    def failed_observer(token, event):
        raise RuntimeError(SECRET)

    socket = MarketWebSocket(seeded_store(), on_state_change=failed_observer)
    socket._handle_best_bid_ask({"asset_id": "token", "best_bid": ".4", "best_ask": ".6"})
    assert SECRET not in caplog.text
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
