"""Deterministic RPC recovery, request identity, and deadline regressions."""

from collections.abc import Mapping
from unittest.mock import AsyncMock

import aiohttp
import pytest

from pmcopy.domain.redemption import SettlementUnavailable
from pmcopy.integrations.polymarket.settlement_chain import PolygonSettlementReader
from tests.test_redemption_chain import (
    FakeRpcHttp,
    TestPolygonSettlementReader as ChainFixture,
    WALLET,
)
from tests.test_redemption_service import FakeClock


class ScriptedRpc:
    """Replay provider failures per logical request before returning actual ABI evidence."""

    def __init__(self, outcomes: list[object], clock: FakeClock, duration: float = 0) -> None:
        self.outcomes = outcomes
        self.clock = clock
        self.duration = duration
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.good = FakeRpcHttp()

    async def post_json(self, url: str, *, json_body: dict[str, object], silent: bool) -> object:
        """Return the next outcome; None delegates to a valid synthetic chain."""
        self.calls.append((url, json_body))
        self.clock.elapsed += self.duration
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, Mapping):
            return {"jsonrpc": "2.0", "id": json_body["id"], **outcome}
        return await self.good.post_json(url, json_body=json_body, silent=silent)


class TestSettlementRpcRecovery:
    """Retry transient reads only; strict evidence failures remain terminal."""

    @staticmethod
    def reader(http, clock, fallback: str = "https://fallback.example"):
        """Inject controlled time and zero jitter into the actual chain reader."""
        return PolygonSettlementReader(
            http,
            "https://primary.example/SECRET",
            fallback,
            clock=clock,
            jitter=lambda low, high: low,
        )

    @pytest.mark.parametrize("code", [-32005, -32002, -32603])
    async def test_second_pass_recovers_transient_json_rpc_errors(self, code) -> None:
        """HTTP 200 rate-limit/availability envelopes deserve bounded recovery."""
        clock = FakeClock()
        error = {"error": {"code": code, "message": "SECRET"}}
        http = ScriptedRpc([error, error, None], clock)
        assert await self.reader(http, clock).latest_timestamp() == 1700000000
        assert [url.split("/")[2] for url, _ in http.calls] == [
            "primary.example",
            "fallback.example",
            "primary.example",
        ]
        assert len({body["id"] for _, body in http.calls}) == 1
        assert 0 < clock.elapsed < 45

    @pytest.mark.parametrize(
        "failure",
        [
            TimeoutError("SECRET"),
            aiohttp.ClientConnectionError("SECRET"),
            *[
                aiohttp.ClientResponseError(None, (), status=status, message="SECRET")
                for status in (408, 429, 500, 502, 503, 504)
            ],
        ],
    )
    async def test_transient_http_failures_recover_without_nested_retries(self, failure) -> None:
        """Exactly one request per endpoint attempt is sufficient for recovery."""
        clock = FakeClock()
        http = ScriptedRpc([failure, None], clock)
        assert await self.reader(http, clock).latest_timestamp() == 1700000000
        assert len(http.calls) == 2

    async def test_retry_after_defers_that_endpoint_but_allows_fallback(self) -> None:
        """A busy primary cannot immediately be retried before its Retry-After."""
        clock = FakeClock()
        limited = aiohttp.ClientResponseError(None, (), status=429, headers={"Retry-After": "8"})
        http = ScriptedRpc([limited, TimeoutError(), None], clock)
        assert await self.reader(http, clock).latest_timestamp() == 1700000000
        assert clock.elapsed == 8
        assert len(http.calls) == 3

    async def test_retry_after_http_date_uses_injected_clock(self) -> None:
        """HTTP-date delays do not read wall time or turn into uncontrolled sleeps."""
        clock = FakeClock()
        limited = aiohttp.ClientResponseError(
            None, (), status=429, headers={"Retry-After": "Tue, 14 Nov 2023 22:13:28 GMT"}
        )
        http = ScriptedRpc([limited, None], clock)
        assert await self.reader(http, clock, "").latest_timestamp() == 1700000000
        assert clock.elapsed == 8

    async def test_total_call_budget_includes_requests_and_retry_after(self) -> None:
        """A Retry-After beyond the remaining 45 seconds cannot extend the call."""
        clock = FakeClock()
        limited = aiohttp.ClientResponseError(None, (), status=429, headers={"Retry-After": "50"})
        http = ScriptedRpc([limited] * 4, clock, duration=10)
        with pytest.raises(SettlementUnavailable, match="settlement_rpc_unavailable") as error:
            await self.reader(http, clock).latest_timestamp()
        assert len(http.calls) == 2 and clock.elapsed <= 45
        assert "budget" in str(error.value)
        assert "SECRET" not in str(error.value)

    async def test_exhausted_two_passes_report_only_safe_context(self) -> None:
        """Neither a credential-bearing endpoint nor provider message enters reports."""
        clock = FakeClock()
        http = ScriptedRpc([{"error": {"code": -32005, "message": "SECRET"}}] * 4, clock)
        with pytest.raises(SettlementUnavailable, match="settlement_rpc_unavailable") as error:
            await self.reader(http, clock).latest_timestamp()
        assert len(http.calls) == 4
        message = str(error.value)
        assert "eth_getBlockByNumber" in message and "fallback" in message
        assert "SECRET" not in message and "https://" not in message

    @pytest.mark.parametrize(
        "payload",
        [
            {"id": 999, "result": None},
            {"id": True, "result": None},
            {"jsonrpc": "1.0", "result": None},
            {},
            {"result": None, "error": {"code": -32005}},
            {"error": {"code": -32602, "message": "SECRET"}},
            {"error": {"code": -32000, "message": "execution reverted SECRET"}},
            {"error": {"code": "-32005", "message": "SECRET"}},
        ],
    )
    async def test_invalid_evidence_or_request_is_not_retried(self, payload) -> None:
        """Fallback must not hide invalid identity, schemas, or contract reverts."""
        clock = FakeClock()
        http = ScriptedRpc([payload], clock)
        with pytest.raises(SettlementUnavailable):
            await self.reader(http, clock).latest_timestamp()
        assert len(http.calls) == 1
        assert clock.elapsed == 0

    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    async def test_terminal_http_errors_do_not_retry(self, status) -> None:
        """Bad requests and endpoint authorization require operator correction."""
        clock = FakeClock()
        http = ScriptedRpc([aiohttp.ClientResponseError(None, (), status=status)], clock)
        with pytest.raises(SettlementUnavailable):
            await self.reader(http, clock).latest_timestamp()
        assert len(http.calls) == 1

    async def test_snapshot_retries_keep_the_pinned_block(self) -> None:
        """A retry cannot mix balances from a later chain height into one snapshot."""
        clock = FakeClock()
        http = ScriptedRpc([None, {"error": {"code": -32005}}, None], clock)
        snapshot = await self.reader(http, clock).snapshot(WALLET, ChainFixture.market())
        assert snapshot.balances == (0, 5)
        calls = [body for _, body in http.calls if body["method"] == "eth_call"]
        assert len(calls) == 7
        assert all(body["params"][1] == "0x64" for body in calls)


class TestSettlementRpcHttp:
    """The HTTP boundary makes a single request and preserves status before JSON parsing."""

    async def test_non_json_503_reaches_retry_classifier_as_http_error(self) -> None:
        """Reverse-proxy HTML outages must not be misclassified as invalid chain evidence."""
        from pmcopy.integrations.polymarket.settlement_rpc import SettlementRpcHttp

        response = AsyncMock()
        response.raise_for_status = lambda: (_ for _ in ()).throw(
            aiohttp.ClientResponseError(None, (), status=503)
        )
        context = AsyncMock()
        context.__aenter__.return_value = response
        from unittest.mock import Mock

        session = Mock()
        session.request.return_value = context
        with pytest.raises(aiohttp.ClientResponseError):
            await SettlementRpcHttp(session).post_json(
                "https://rpc.example", json_body={"id": 1}, silent=True
            )
        session.request.assert_called_once()
        response.json.assert_not_awaited()
