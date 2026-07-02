from __future__ import annotations

from unittest.mock import MagicMock

import httpx

import utils.createClobClient as create_clob_client_module


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
