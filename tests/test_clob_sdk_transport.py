from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

import utils.createClobClient as create_clob_client_module
from utils.createClobClient import ClobClientWrapper


def test_configure_sdk_http_transport_replaces_http2_singleton(monkeypatch):
    from py_clob_client_v2.http_helpers import helpers as sdk_http_helpers

    old_client = MagicMock()
    monkeypatch.setattr(sdk_http_helpers, "_http_client", old_client)
    monkeypatch.setattr(
        create_clob_client_module,
        "_SDK_HTTP_TRANSPORT_CONFIGURED",
        False,
    )

    create_clob_client_module._configure_sdk_http_transport()

    new_client = sdk_http_helpers._http_client
    try:
        assert isinstance(new_client, httpx.Client)
        assert new_client is not old_client
        assert new_client._transport._pool._http2 is False
        assert new_client._transport._pool._keepalive_expiry == 2.0
        old_client.close.assert_called_once()
    finally:
        new_client.close()


def test_configure_sdk_http_transport_is_idempotent(monkeypatch):
    from py_clob_client_v2.http_helpers import helpers as sdk_http_helpers

    old_client = MagicMock()
    monkeypatch.setattr(sdk_http_helpers, "_http_client", old_client)
    monkeypatch.setattr(
        create_clob_client_module,
        "_SDK_HTTP_TRANSPORT_CONFIGURED",
        False,
    )

    create_clob_client_module._configure_sdk_http_transport()
    first_client = sdk_http_helpers._http_client

    try:
        create_clob_client_module._configure_sdk_http_transport()
        assert sdk_http_helpers._http_client is first_client
        old_client.close.assert_called_once()
    finally:
        first_client.close()


@pytest.mark.asyncio
async def test_orderbook_404_returns_terminal_marker_without_sdk_fallback(monkeypatch):
    class _NotFoundResponse:
        status = 404

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class _Session:
        def get(self, _url):
            return _NotFoundResponse()

    raw_client = MagicMock()
    raw_client.get_order_book = MagicMock(side_effect=AssertionError("SDK fallback called"))
    wrapper = ClobClientWrapper(
        raw_client,
        SimpleNamespace(
            signature_type=0,
            funder_address="0x0000000000000000000000000000000000000000",
        ),
    )
    monkeypatch.setattr(create_clob_client_module, "_get_session", lambda: _Session())

    book = await wrapper.get_order_book("0xdead")

    assert book.orderbook_not_found is True
    assert book.bids == []
    assert book.asks == []
    raw_client.get_order_book.assert_not_called()
