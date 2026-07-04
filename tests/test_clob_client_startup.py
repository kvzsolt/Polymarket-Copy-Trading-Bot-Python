from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from py_clob_client_v2.exceptions import PolyApiException

from utils.createClobClient import _create_or_derive_api_key_with_retry


@pytest.mark.asyncio
async def test_create_or_derive_api_key_retries_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    creds = object()
    client.create_or_derive_api_key.side_effect = [
        PolyApiException(error_msg="Request exception!"),
        creds,
    ]
    reconfigured: list[bool] = []
    monkeypatch.setattr(
        "utils.createClobClient._configure_sdk_http_transport",
        lambda *, force=False: reconfigured.append(force),
    )

    result = await _create_or_derive_api_key_with_retry(
        client,
        max_attempts=2,
        base_delay_seconds=0,
    )

    assert result is creds
    assert client.create_or_derive_api_key.call_count == 2
    assert reconfigured == [True]


@pytest.mark.asyncio
async def test_create_or_derive_api_key_does_not_retry_auth_failure() -> None:
    class AuthError(Exception):
        status_code = 401

    client = MagicMock()
    client.create_or_derive_api_key.side_effect = AuthError("unauthorized")

    with pytest.raises(AuthError):
        await _create_or_derive_api_key_with_retry(
            client,
            max_attempts=4,
            base_delay_seconds=0,
        )

    client.create_or_derive_api_key.assert_called_once()
