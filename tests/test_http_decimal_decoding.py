"""HTTP response decoding preserves exact financial JSON numbers."""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.clients.network.http_client import AsyncHttpClient, HttpRetryPolicy


class DecimalDecoder:
    """Decode decimal JSON values without binary floating point."""

    def __call__(self, payload: str) -> object:
        """Return the decoded public payload."""
        result: object = json.loads(payload, parse_float=Decimal)
        return result


@pytest.mark.asyncio
async def test_get_json_uses_injected_decoder_without_sending_it_to_server() -> None:
    payload = '{"size": 0.1234567890123456789012345678}'
    response = MagicMock(status=200)

    async def decode_response(**kwargs):
        return kwargs.get("loads", json.loads)(payload)

    response.json = AsyncMock(side_effect=decode_response)
    response.raise_for_status = MagicMock()
    request = MagicMock()
    request.__aenter__ = AsyncMock(return_value=response)
    request.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.request.return_value = request
    client = AsyncHttpClient(session, HttpRetryPolicy(attempts=1))
    result = await client.get_json("https://example.invalid", json_decoder=DecimalDecoder())
    assert result == {"size": Decimal("0.1234567890123456789012345678")}
    assert "json_decoder" not in session.request.call_args.kwargs
    assert "loads" not in session.request.call_args.kwargs
