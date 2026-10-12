"""``clients/network`` adapter tests.

``AsyncHttpClient`` is exercised with ``AsyncMock`` stand-ins for the
underlying aiohttp session. No network I/O, no DB writes, no ``ENV`` reads.

Goals:

* Delegation contract: the adapter forwards to the underlying session with
  the expected arguments and returns the underlying response.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.clients.network.http_client import AsyncHttpClient, HttpRetryPolicy


# ---------------------------------------------------------------------------
# AsyncHttpClient — thin passthrough to aiohttp session
# ---------------------------------------------------------------------------


class TestHttpClientDelegation:
    @pytest.mark.asyncio
    async def test_get_json_forwards(self) -> None:
        mock_resp = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_resp.json = AsyncMock(return_value={"ok": True})
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.request = MagicMock(return_value=mock_resp)

        client = AsyncHttpClient(session, retry_policy=HttpRetryPolicy(attempts=1))
        result = await client.get_json("https://x.example", silent=True, params={"a": 1})

        assert result == {"ok": True}
        session.request.assert_called_once()
        args, kwargs = session.request.call_args
        assert args == ("GET", "https://x.example")
        assert kwargs.get("params") == {"a": 1}

    @pytest.mark.asyncio
    async def test_post_json_forwards(self) -> None:
        mock_resp = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_resp.json = AsyncMock(return_value={"id": 1})
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.request = MagicMock(return_value=mock_resp)

        client = AsyncHttpClient(session, retry_policy=HttpRetryPolicy(attempts=1))
        result = await client.post_json("https://x.example", json_body={"a": 1})

        assert result == {"id": 1}
        session.request.assert_called_once()
        args, kwargs = session.request.call_args
        assert args == ("POST", "https://x.example")
        assert kwargs.get("json") == {"a": 1}
