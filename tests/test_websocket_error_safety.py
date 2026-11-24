"""Malformed provider frames and transport errors cannot leak credential material."""

from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.clients.ws import market_ws
from pmcopy.clients.ws import onchain_ws
from pmcopy.clients.ws import user_ws
from tests.test_book_update_atomicity import seeded_store

MARKER = "SYNTHETIC_PRIVATE_TOKEN_DO_NOT_LOG"


@pytest.mark.parametrize(
    "module,client_type", [(market_ws, market_ws.MarketWebSocket), (user_ws, user_ws.UserWebSocket)]
)
def test_malformed_market_and_account_frames_are_not_logged(module, client_type, monkeypatch):
    """A failed decoder provides no safe allowlist for raw frame content."""
    logger = Mock()
    monkeypatch.setattr(module, "logger", logger)
    store = seeded_store()
    client = client_type(store)
    client._handle_message(MARKER)
    assert MARKER not in str(logger.mock_calls)
    if client_type is market_ws.MarketWebSocket:
        assert store.get_state("token").stale


@pytest.mark.asyncio
async def test_onchain_transport_exception_is_sanitized_at_the_fault_boundary(monkeypatch):
    """Even a provider exception containing an unknown token format remains private."""
    logger = Mock()
    monkeypatch.setattr(onchain_ws, "logger", logger)
    client = onchain_ws.OnchainWebSocket(
        ws_url="wss://rpc.invalid",
        subscribe_params=["logs", {}],
        name="orderfilled",
        on_message=AsyncMock(),
    )

    class FailedConnect:
        async def __aenter__(self):
            client._running = False
            raise OSError(MARKER)

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(onchain_ws.websockets, "connect", lambda *args, **kwargs: FailedConnect())
    client._running = True
    await client._run()
    assert MARKER not in str(logger.mock_calls)
