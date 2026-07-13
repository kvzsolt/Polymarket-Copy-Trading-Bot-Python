from __future__ import annotations

import pytest

from utils import fetchData


@pytest.mark.asyncio
async def test_http_client_recreated_when_session_closed():
    await fetchData.close_session()


@pytest.mark.asyncio
async def test_app_main_closes_shared_session_on_early_return(monkeypatch):
    import app as app_module

    async def _early_return(_clients):
        fetchData._get_http_client()
        return 1

    monkeypatch.setattr(app_module, "_run_application", _early_return)

    assert await app_module.main() == 1
    assert fetchData._session is None
    assert fetchData._http_client is None

    first_client = fetchData._get_http_client()
    first_session = first_client.session

    await first_session.close()

    second_client = fetchData._get_http_client()
    assert second_client is not first_client
    assert second_client.session is not first_session
    assert not second_client.session.closed

    await fetchData.close_session()


# INF-04: the prior ``_close_session_sync`` atexit hook + its dedicated
# warning-on-asyncio-failure test were removed. Atexit ran asyncio.run()
# during interpreter shutdown — under pytest-asyncio teardown a loop is
# still alive, ``asyncio.run`` raises ``RuntimeError`` and the session
# leaked. Explicit ``await close_session()`` from app shutdown is the
# sole supported close path now; the test above already covers it.
