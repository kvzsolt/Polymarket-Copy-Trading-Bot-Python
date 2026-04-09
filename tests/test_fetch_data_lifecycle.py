from __future__ import annotations

from unittest.mock import patch

import pytest

from utils import fetchData


@pytest.mark.asyncio
async def test_http_client_recreated_when_session_closed():
    await fetchData.close_session()

    first_client = fetchData._get_http_client()
    first_session = first_client.session

    await first_session.close()

    second_client = fetchData._get_http_client()
    assert second_client is not first_client
    assert second_client.session is not first_session
    assert not second_client.session.closed

    await fetchData.close_session()


@pytest.mark.asyncio
async def test_close_session_sync_warns_when_shutdown_close_fails():
    await fetchData.close_session()

    _ = fetchData._get_http_client()
    leaked_session = fetchData._session

    with patch("utils.fetchData.asyncio.run", side_effect=RuntimeError("loop closed")):
        with pytest.warns(UserWarning, match="Failed to close shared HTTP session"):
            fetchData._close_session_sync()

    assert fetchData._session is None
    assert fetchData._http_client is None

    if leaked_session and not leaked_session.closed:
        await leaked_session.close()
