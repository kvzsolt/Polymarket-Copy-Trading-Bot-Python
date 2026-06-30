"""True-async venue order-status / fills fetching (aiohttp, no thread executor).

These cover the rewrite of ``ClobV2Gateway.get_order`` / ``get_fills`` from a
blocking SDK call offloaded to a thread pool to a direct L2-authenticated REST
GET over the shared aiohttp session. The key property is that N order-status
fetches now OVERLAP on the event loop (concurrency) instead of serializing on a
handful of pool threads — that is what stops the risk pre-reconcile timeout.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import aiohttp
import pytest

from integrations.polymarket import clob_v2_gateway as gw_mod
from integrations.polymarket.clob_v2_gateway import ClobV2Gateway
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import VenueOrderState

HOST = "https://clob.polymarket.com"


# --- fakes -----------------------------------------------------------------

class _FakeResp:
    def __init__(self, status: int, body=None):
        self.status = status
        self._body = {} if body is None else body

    async def json(self, content_type=None):  # noqa: ARG002
        # Yield briefly so SonarCloud S7503 sees this as a genuine async
        # function (aiohttp's caller awaits ``response.json()``).
        await asyncio.sleep(0)
        return self._body

    async def text(self):
        await asyncio.sleep(0)
        return str(self._body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):  # noqa: ANN002
        return False


class _RaiseCM:
    """Async context manager whose entry raises — models a transport fault."""

    def __init__(self, exc: Exception):
        self._exc = exc

    async def __aenter__(self):
        raise self._exc

    async def __aexit__(self, *a):  # noqa: ANN002
        return False


class _FakeSession:
    """Returns queued responses/exceptions, one per ``get`` call."""

    def __init__(self, items):
        self._items = list(items)
        self.calls: list[SimpleNamespace] = []

    def get(self, url, headers=None, params=None, timeout=None):  # noqa: ANN001
        self.calls.append(
            SimpleNamespace(url=url, headers=headers, params=params, timeout=timeout)
        )
        item = self._items.pop(0) if self._items else _FakeResp(200, {})
        if isinstance(item, Exception):
            return _RaiseCM(item)
        return item


def _gateway(session, *, l2=None):
    """Build a gateway over a fake raw client + patched aiohttp session."""
    headers = l2 or {"POLY_API_KEY": "k", "POLY_SIGNATURE": "sig"}
    seen: list[tuple[str, str]] = []

    def _l2_headers(method, endpoint):  # noqa: ANN001
        seen.append((method, endpoint))
        return headers

    raw = SimpleNamespace(host=HOST, _l2_headers=_l2_headers)
    wrapper = SimpleNamespace(client=raw)
    gw = ClobV2Gateway(wrapper)
    gw._l2_seen = seen  # type: ignore[attr-defined]  # test introspection
    return gw


@pytest.fixture
def patch_session(monkeypatch):
    def _install(session):
        monkeypatch.setattr(gw_mod, "_get_session", lambda: session)
        return session
    return _install


# --- get_order: shape + parse ----------------------------------------------

@pytest.mark.asyncio
async def test_get_order_issues_signed_rest_get_and_parses(patch_session):
    session = patch_session(_FakeSession([
        _FakeResp(200, {
            "id": "0xORDER", "status": "live",
            "original_size": "5", "size_matched": "3", "avg_price": "0.42",
        }),
    ]))
    gw = _gateway(session)

    state = await gw.get_order("0xORDER")

    assert isinstance(state, VenueOrderState)
    assert session.calls[0].url == f"{HOST}/data/order/0xORDER"
    assert session.calls[0].headers == {"POLY_API_KEY": "k", "POLY_SIGNATURE": "sig"}
    # signed the exact /data/order/<id> path (matches the SDK)
    assert ("GET", "/data/order/0xORDER") in gw._l2_seen
    assert state.order_id == "0xORDER"
    assert state.status == "live"
    assert state.size == Decimal("5")
    assert state.filled_size == Decimal("3")
    assert state.avg_fill_price == Decimal("0.42")
    assert isinstance(state.filled_size, Decimal)


# --- get_order: status mapping ---------------------------------------------

@pytest.mark.parametrize(
    "status,exc",
    [
        (404, VenueOrderNotFoundError),
        (429, VenueRateLimitError),
        (401, VenueAuthError),
        (403, VenueAuthError),
        (400, VenueInvalidOrderError),
        (422, VenueInvalidOrderError),
    ],
)
@pytest.mark.asyncio
async def test_get_order_status_maps_to_typed_error_without_retry(patch_session, status, exc):
    session = patch_session(_FakeSession([_FakeResp(status, {"error": "x"})]))
    gw = _gateway(session)

    with pytest.raises(exc):
        await gw.get_order("0xORDER")

    assert len(session.calls) == 1  # terminal statuses are not retried


@pytest.mark.parametrize("status", [500, 502, 503, 504, 408])
@pytest.mark.asyncio
async def test_get_order_5xx_retries_once_then_unavailable(patch_session, status):
    session = patch_session(_FakeSession([
        _FakeResp(status), _FakeResp(status),
    ]))
    gw = _gateway(session)

    with pytest.raises(VenueUnavailableError) as info:
        await gw.get_order("0xORDER")

    assert len(session.calls) == 2  # original + one retry
    assert f"server_error_{status}" in str(info.value)


# --- get_order: transport retry --------------------------------------------

@pytest.mark.asyncio
async def test_get_order_transport_fault_retries_once_then_succeeds(patch_session):
    session = patch_session(_FakeSession([
        aiohttp.ServerDisconnectedError("server disconnected"),
        _FakeResp(200, {"id": "0xORDER", "status": "matched", "size_matched": "1"}),
    ]))
    gw = _gateway(session)

    state = await gw.get_order("0xORDER")

    assert len(session.calls) == 2
    assert state.status == "matched"
    assert state.filled_size == Decimal("1")


@pytest.mark.asyncio
async def test_get_order_persistent_transport_raises_unavailable(patch_session):
    session = patch_session(_FakeSession([
        aiohttp.ServerDisconnectedError("disc"),
        aiohttp.ServerDisconnectedError("disc"),
    ]))
    gw = _gateway(session)

    with pytest.raises(VenueUnavailableError) as info:
        await gw.get_order("0xORDER")

    assert len(session.calls) == 2
    assert "poly_api_transport" in str(info.value)


@pytest.mark.asyncio
async def test_get_order_timeout_retries_then_unavailable(patch_session):
    session = patch_session(_FakeSession([
        asyncio.TimeoutError(), asyncio.TimeoutError(),
    ]))
    gw = _gateway(session)

    with pytest.raises(VenueUnavailableError):
        await gw.get_order("0xORDER")

    assert len(session.calls) == 2


# --- the headline property: concurrency, not threads -----------------------

@pytest.mark.asyncio
async def test_get_order_calls_run_concurrently(monkeypatch):
    """N get_order calls overlap on the event loop. A serialized (thread-pool)
    path would yield max_in_flight == 1; true async I/O yields >= 2."""
    state = {"in_flight": 0, "max": 0}

    class _SlowResp(_FakeResp):
        async def json(self, content_type=None):  # noqa: ARG002
            state["in_flight"] += 1
            state["max"] = max(state["max"], state["in_flight"])
            try:
                await asyncio.sleep(0.05)
                return {"id": "x", "status": "live", "size_matched": "0"}
            finally:
                state["in_flight"] -= 1

    class _SlowSession:
        def get(self, url, headers=None, params=None, timeout=None):  # noqa: ANN001, ARG002
            return _SlowResp(200)

    monkeypatch.setattr(gw_mod, "_get_session", lambda: _SlowSession())
    gw = _gateway(_SlowSession())

    await asyncio.gather(*(gw.get_order(f"0x{i}") for i in range(8)))

    assert state["max"] >= 2, "order-status GETs must overlap (event-loop concurrency)"


# --- get_fills -------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_fills_signs_bare_path_and_passes_market_param(patch_session):
    session = patch_session(_FakeSession([
        _FakeResp(200, {"data": [
            {"id": "f1", "order_id": "o1", "asset_id": "t1", "side": "BUY",
             "size": "2", "price": "0.5", "fee": "0.01"},
        ], "next_cursor": "LTE="}),
    ]))
    gw = _gateway(session)

    fills = await gw.get_fills(market="0xMKT")

    assert session.calls[0].url == f"{HOST}/data/trades"
    assert session.calls[0].params == {"market": "0xMKT"}
    assert ("GET", "/data/trades") in gw._l2_seen  # bare path, not /data/trades?market=
    assert len(fills) == 1
    assert fills[0].size == Decimal("2")
    assert fills[0].price == Decimal("0.5")
    assert isinstance(fills[0].fee, Decimal)


@pytest.mark.asyncio
async def test_get_fills_404_returns_empty(patch_session):
    session = patch_session(_FakeSession([_FakeResp(404, {"error": "none"})]))
    gw = _gateway(session)

    assert await gw.get_fills(market="0xMKT") == []


@pytest.mark.asyncio
async def test_get_fills_follows_cursor_bounded(patch_session):
    # 3 pages then a terminating cursor; assert we stop and accumulate.
    session = patch_session(_FakeSession([
        _FakeResp(200, {"data": [{"id": "a"}], "next_cursor": "c1"}),
        _FakeResp(200, {"data": [{"id": "b"}], "next_cursor": "c2"}),
        _FakeResp(200, {"data": [{"id": "c"}], "next_cursor": "LTE="}),
    ]))
    gw = _gateway(session)

    fills = await gw.get_fills()
    assert len(session.calls) == 3
    assert [f.fill_id for f in fills] == ["a", "b", "c"]
