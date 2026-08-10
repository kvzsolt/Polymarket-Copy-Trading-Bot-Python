from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from integrations.heisenberg.client import HeisenbergClient
from integrations.heisenberg.errors import (
    HeisenbergAuthenticationError,
    HeisenbergProtocolError,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, *, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class FakeHttpClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        self.closed = False

    async def post(
        self, url: str, *, headers: dict[str, str], json: dict[str, Any]
    ) -> FakeResponse:
        self.requests.append({"url": url, "headers": headers, "json": json})
        return self.responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


def page(
    results: list[dict[str, Any]], *, offset: int, has_more: bool, request_id: str = "req"
) -> dict[str, Any]:
    return {
        "timestamp": "2026-07-30T00:00:00Z",
        "params": {},
        "pagination": {"limit": 2, "offset": offset, "has_more": has_more},
        "data": {"results": results},
        "request_id": request_id,
    }


@pytest.mark.asyncio
async def test_request_normalizes_all_params_to_strings_and_never_logs_token() -> None:
    http = FakeHttpClient([FakeResponse(200, page([{"ok": True}], offset=0, has_more=False))])
    client = HeisenbergClient(api_key="super-secret", http_client=http)

    result = await client.request(
        574,
        params={"closed": False, "min_volume": Decimal("1000.50"), "skip": None, "count": 3},
        limit=2,
    )

    assert result.results == ({"ok": True},)
    request = http.requests[0]
    assert request["headers"]["Authorization"] == "Bearer super-secret"
    assert request["json"]["params"] == {
        "closed": "False",
        "min_volume": "1000.50",
        "count": "3",
    }
    assert request["json"]["formatter_config"] == {"format_type": "raw"}
    assert "super-secret" not in repr(client)


@pytest.mark.asyncio
async def test_paginate_walks_offset_until_has_more_is_false() -> None:
    http = FakeHttpClient(
        [
            FakeResponse(200, page([{"n": 1}, {"n": 2}], offset=0, has_more=True)),
            FakeResponse(200, page([{"n": 3}], offset=2, has_more=False)),
        ]
    )
    client = HeisenbergClient(api_key="token", http_client=http)

    rows = await client.fetch_all(556, params={"proxy_wallet": "0xabc"}, page_size=2)

    assert [row["n"] for row in rows] == [1, 2, 3]
    assert [req["json"]["pagination"]["offset"] for req in http.requests] == [0, 2]


@pytest.mark.asyncio
async def test_retryable_status_uses_exponential_backoff_then_succeeds() -> None:
    sleeps: list[float] = []

    async def sleep(value: float) -> None:
        sleeps.append(value)

    http = FakeHttpClient(
        [
            FakeResponse(429, {"error": "slow down"}),
            FakeResponse(503, {"error": "busy"}),
            FakeResponse(200, page([], offset=0, has_more=False)),
        ]
    )
    client = HeisenbergClient(
        api_key="token",
        http_client=http,
        max_attempts=3,
        backoff_base_seconds=0.25,
        jitter_seconds=0,
        sleep=sleep,
    )

    await client.request(584)

    assert sleeps == [0.25, 0.5]
    assert len(http.requests) == 3


@pytest.mark.asyncio
async def test_authentication_error_fails_fast_without_retry() -> None:
    http = FakeHttpClient([FakeResponse(401, {"error": "bad token"})])
    client = HeisenbergClient(api_key="secret", http_client=http, max_attempts=5)

    with pytest.raises(HeisenbergAuthenticationError):
        await client.request(584)

    assert len(http.requests) == 1


@pytest.mark.asyncio
async def test_malformed_success_payload_is_protocol_error() -> None:
    http = FakeHttpClient([FakeResponse(200, {"data": {"results": "not-a-list"}})])
    client = HeisenbergClient(api_key="token", http_client=http)

    with pytest.raises(HeisenbergProtocolError, match="results"):
        await client.request(581)


@pytest.mark.asyncio
async def test_endpoint_helpers_use_documented_agent_ids() -> None:
    responses = [FakeResponse(200, page([], offset=0, has_more=False)) for _ in range(8)]
    http = FakeHttpClient(responses)
    client = HeisenbergClient(api_key="token", http_client=http)

    await client.leaderboard(limit=1)
    await client.wallet_360("0xabc", 3)
    await client.wallet_stats_all_time("0xabc")
    await client.trades(proxy_wallet="0xabc", limit=1)
    await client.markets(condition_id="0xcondition", limit=1)
    await client.market_360(condition_id="0xcondition", limit=1)
    await client.pnl(wallet="0xabc", granularity="1m", limit=1)
    await client.price_jumps(token_id="123", lookback_hours=24, limit=1)

    assert [request["json"]["agent_id"] for request in http.requests] == [
        584,
        581,
        586,
        556,
        574,
        575,
        569,
        596,
    ]


@pytest.mark.asyncio
async def test_fetch_all_truncates_final_page_to_requested_row_cap() -> None:
    http = FakeHttpClient(
        [
            FakeResponse(200, page([{"n": 1}, {"n": 2}], offset=0, has_more=True)),
            FakeResponse(200, page([{"n": 3}, {"n": 4}], offset=2, has_more=True)),
        ]
    )
    client = HeisenbergClient(api_key="token", http_client=http)

    rows = await client.fetch_all(556, page_size=2, max_rows=3)

    assert [row["n"] for row in rows] == [1, 2, 3]
    assert len(http.requests) == 2


@pytest.mark.asyncio
async def test_pnl_helper_uses_documented_iso_date_boundaries() -> None:
    http = FakeHttpClient([FakeResponse(200, page([], offset=0, has_more=False))])
    client = HeisenbergClient(api_key="token", http_client=http)

    await client.pnl(
        wallet="0xabc",
        granularity="1d",
        start_time=datetime(2026, 2, 10, 23, 59, tzinfo=timezone.utc),
        end_time=int(datetime(2026, 2, 28, 12, 0, tzinfo=timezone.utc).timestamp()),
        limit=1,
    )

    assert http.requests[0]["json"]["params"]["start_time"] == "2026-02-10"
    assert http.requests[0]["json"]["params"]["end_time"] == "2026-02-28"
