from __future__ import annotations

import json

import httpx
import pytest

from config.app_config import DiscoveryApiConfig
from integrations.falcon.client import (
    FalconClient,
    FalconPage,
    TokenBucket,
    wallet_trades,
)


def _config(**overrides) -> DiscoveryApiConfig:
    return DiscoveryApiConfig.model_validate(
        {
            "token": "secret",
            "requests_per_second": 1000,
            "burst": 1000,
            **overrides,
        }
    )


def test_body_requires_string_parameter_values() -> None:
    with pytest.raises(TypeError, match="must be strings"):
        FalconClient.build_body(584, {"limit": 3}, limit=100, offset=0)  # type: ignore[dict-item]


def test_body_uses_integer_pagination_and_raw_formatter() -> None:
    body = FalconClient.build_body(
        584,
        {"sort_by": "h_score"},
        limit=200,
        offset=400,
    )
    assert json.loads(body) == {
        "agent_id": 584,
        "params": {"sort_by": "h_score"},
        "pagination": {"limit": 200, "offset": 400},
        "formatter_config": {"format_type": "raw"},
    }


def test_body_rejects_page_sizes_below_live_falcon_minimum() -> None:
    with pytest.raises(ValueError, match=r"\[10, 200\]"):
        FalconClient.build_body(581, {"window_days": "3"}, limit=1, offset=0)


def test_token_bucket_always_has_capacity_for_one_request() -> None:
    bucket = TokenBucket(rate=0.1)
    assert bucket.capacity == 1.0
    with pytest.raises(ValueError, match="at least one"):
        TokenBucket(rate=1.0, burst=0.5)


@pytest.mark.asyncio
async def test_query_reuses_injected_pooled_client_and_parses_data() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": {"results": [{"wallet": "0x1"}]}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as transport_client:
        client = FalconClient(_config(), client=transport_client)
        page = await client.query(
            agent_id=584,
            params={"sort_by": "h_score"},
            limit=10,
        )
        await client.aclose()
    assert page.returned == 1
    assert page.results[0]["wallet"] == "0x1"
    assert len(requests) == 1
    assert requests[0].headers["authorization"] == "Bearer secret"


@pytest.mark.asyncio
async def test_query_clamps_page_size_to_live_falcon_minimum() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content)["pagination"]["limit"] == 10
        return httpx.Response(200, json={"data": {"results": []}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport_client:
        client = FalconClient(_config(), client=transport_client)
        await client.query(
            agent_id=581,
            params={"proxy_wallet": "0x" + "a" * 40, "window_days": "3"},
            limit=1,
        )




@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"records": [{"wallet": "0x2"}]}},
        {"data": [{"wallet": "0x2"}]},
    ],
)
async def test_query_accepts_documented_raw_record_shapes(payload: dict) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport_client:
        client = FalconClient(_config(), client=transport_client)
        page = await client.query(agent_id=584, params={"sort_by": "h_score"})
    assert page.results == ({"wallet": "0x2"},)


@pytest.mark.asyncio
async def test_pagination_stops_when_records_less_than_limit() -> None:
    client = FalconClient(_config(), client=httpx.AsyncClient())
    calls: list[int] = []

    async def query(**kwargs) -> FalconPage:
        calls.append(kwargs["offset"])
        rows = tuple({"id": value} for value in range(10)) if kwargs["offset"] == 0 else ({"id": 10},)
        return FalconPage(results=rows, returned=len(rows))

    client.query = query  # type: ignore[method-assign]
    rows = [
        row
        async for row in client.paginate(
            agent_id=584,
            params={"sort_by": "h_score"},
            page_size=2,
            max_pages=10,
        )
    ]
    await client.aclose()
    assert [row["id"] for row in rows] == list(range(11))
    assert calls == [0, 10]


@pytest.mark.asyncio
async def test_retry_after_is_honoured_without_leaking_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0.01"})
        return httpx.Response(200, json={"results": []})

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("integrations.falcon.client.asyncio.sleep", fake_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport_client:
        client = FalconClient(_config(max_retries=2), client=transport_client)
        await client.query(agent_id=584, params={"sort_by": "h_score"})
    assert calls == 2
    assert sleeps == [0.01]


@pytest.mark.asyncio
async def test_wallet_trades_preserves_camel_case_trade_ids() -> None:
    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "BUY":
                for row in (
                    {"tradeID": "trade-a", "asset": "asset-1"},
                    {"tradeID": "trade-b", "asset": "asset-1"},
                    {"tradeID": "trade-a", "asset": "asset-1"},
                ):
                    yield row

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert [row["tradeID"] for row in rows] == ["trade-a", "trade-b"]


@pytest.mark.asyncio
async def test_wallet_trades_keeps_distinct_anonymous_records() -> None:
    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "BUY":
                yield {"timestamp": "1", "asset": "asset-1", "size": "10"}
                yield {"timestamp": "2", "asset": "asset-1", "size": "10"}

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert len(rows) == 2
