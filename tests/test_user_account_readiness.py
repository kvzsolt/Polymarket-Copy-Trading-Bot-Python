"""Authenticated account streams keep separate failure state in real startup wiring."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import pmcopy.app as app
from pmcopy.services.observability.health_server import ReadinessTracker
from tests.test_book_update_atomicity import seeded_store
from tests.test_user_data_runtime import _make_runtime, VALID_CREDS


@pytest.mark.asyncio
async def test_peer_authentication_cannot_hide_disconnected_account(monkeypatch) -> None:
    stopped = asyncio.Event()

    async def connect(credentials):
        await stopped.wait()

    monkeypatch.setattr(
        "pmcopy.orchestrators.streaming.user_data_runtime.UserWebSocket",
        lambda **kwargs: SimpleNamespace(connect=connect, stop=AsyncMock()),
    )
    monkeypatch.setattr(app, "_user_ws_api_creds", lambda state: dict(VALID_CREDS))
    users = [SimpleNamespace(user_id=account) for account in ("a", "b")]
    readiness = ReadinessTracker()
    runtimes = await app._start_user_data_runtimes(
        runtime_users=users,
        user_states={user.user_id: SimpleNamespace(market_store=seeded_store()) for user in users},
        readiness=readiness,
    )
    try:
        await runtimes[0]._on_authenticated()
        assert readiness.snapshot()["wss_user_ok"] is False
        await runtimes[1]._on_authenticated()
        assert readiness.snapshot()["wss_user_ok"] is True
        await runtimes[1]._on_disconnected()
        await runtimes[0]._on_authenticated()
        assert readiness.snapshot()["wss_user_ok"] is False
    finally:
        stopped.set()
        for runtime in runtimes:
            await runtime.stop()


@pytest.mark.asyncio
async def test_user_runtime_fault_does_not_expose_credentials_or_report_running(caplog, capsys):
    marker = "SYNTHETIC_ACCOUNT_STREAM_SECRET"
    runtime = _make_runtime()
    runtime._status.running = True
    runtime._ws_client = SimpleNamespace(connect=AsyncMock(side_effect=RuntimeError(marker)))
    with pytest.raises(RuntimeError) as raised:
        await runtime._run_ws()
    assert marker not in str(raised.value)
    assert marker not in str(runtime.status())
    assert marker not in str(runtime.readiness.snapshot())
    assert marker not in caplog.text
    captured = capsys.readouterr()
    assert marker not in captured.out + captured.err
    assert runtime.status().running is False
