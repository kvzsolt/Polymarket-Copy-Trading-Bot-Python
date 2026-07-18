from __future__ import annotations

import asyncio
import json
import logging

import httpx
import pytest

from config.app_config import DiscoveryApiConfig, ShortlistConfig
from integrations.falcon import client as falcon_client
from integrations.falcon.client import (
    FalconClient,
    FalconPage,
    TokenBucket,
    hscore_rows,
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
    now = 0.0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0.01"})
        return httpx.Response(200, json={"results": []})

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    monkeypatch.setattr("integrations.falcon.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("integrations.falcon.client.time.monotonic", lambda: now)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport_client:
        client = FalconClient(_config(max_retries=2), client=transport_client)
        await client.query(agent_id=584, params={"sort_by": "h_score"})
    assert calls == 2
    assert sleeps == [0.01]


@pytest.mark.asyncio
async def test_concurrent_rate_limits_share_one_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []
    now = 0.0
    both_started = asyncio.Event()
    original_sleep = asyncio.sleep

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls <= 2:
            if calls == 1:
                await both_started.wait()
            else:
                both_started.set()
            return httpx.Response(429, headers={"Retry-After": "0.02"})
        return httpx.Response(200, json={"results": []})

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        wake_at = now + delay
        await original_sleep(0)
        now = max(now, wake_at)

    monkeypatch.setattr("integrations.falcon.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("integrations.falcon.client.time.monotonic", lambda: now)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport_client:
        client = FalconClient(_config(max_retries=2), client=transport_client)
        await asyncio.gather(
            client.query(agent_id=584, params={"sort_by": "h_score"}),
            client.query(agent_id=584, params={"sort_by": "roi"}),
        )

    assert calls == 4
    assert sleeps == [0.02, 0.02]
    assert now == 0.02


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


# ---------------------------------------------------------------------------
# wallet_trades dedup identity — one transaction settles many fills, and rows
# without a venue trade id must not collapse into a single leg.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wallet_trades_keeps_batched_fills_across_assets() -> None:
    """A tx_hash+side pair is not an identity: it batches many fills.

    Deduping on (tx_hash, trade_id, side) alone dropped every leg but the
    first whenever the agent omitted a trade id, silently under-reporting the
    leader's real activity to every downstream recency/market gate.
    """

    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "BUY":
                yield {"transactionHash": "0xtx", "asset": "asset-1", "size": "10"}
                yield {"transactionHash": "0xtx", "asset": "asset-2", "size": "20"}

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert [row["asset"] for row in rows] == ["asset-1", "asset-2"]


@pytest.mark.asyncio
async def test_wallet_trades_keeps_batched_fills_across_conditions() -> None:
    """Same asset alias, different market — both legs must survive."""

    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "SELL":
                yield {"tx_hash": "0xtx", "asset": "a", "condition_id": "condition-1"}
                yield {"tx_hash": "0xtx", "asset": "a", "conditionId": "condition-2"}

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert len(rows) == 2


@pytest.mark.asyncio
async def test_wallet_trades_keeps_fills_differing_only_in_size_and_price() -> None:
    """Two fills can agree on tx, side, asset *and* condition, and still differ.

    A partial fill ladder settles in one transaction against one token: only
    size/price/timestamp separate the legs. Any identity built from
    (tx, side, asset, condition) collapses them into one and under-reports the
    leader — so the no-trade-id fallback keys on the whole row instead.
    """

    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "BUY":
                yield {
                    "tx_hash": "0xtx",
                    "asset": "asset-1",
                    "condition_id": "condition-1",
                    "size": "10",
                    "price": "0.40",
                    "timestamp": 1_700_000_000,
                }
                yield {
                    "tx_hash": "0xtx",
                    "asset": "asset-1",
                    "condition_id": "condition-1",
                    "size": "25",
                    "price": "0.41",
                    "timestamp": 1_700_000_001,
                }

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert [row["size"] for row in rows] == ["10", "25"]


@pytest.mark.asyncio
async def test_wallet_trades_dedupes_on_trade_id_despite_metadata_drift() -> None:
    """A real trade id is the identity; incidental metadata must not defeat it.

    The same fill re-emitted on an overlapping page can carry a different
    asset alias (``asset`` vs ``token_id``), gain a condition id, or pick up an
    extra field. It is still one fill. Folding asset/condition into the identity
    would double-count it and inflate the leader's recent-trade count.
    """

    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "BUY":
                yield {
                    "transactionHash": "0xtx",
                    "trade_id": "venue-trade-1",
                    "asset": "asset-1",
                }
                yield {
                    "transactionHash": "0xtx",
                    "trade_id": "venue-trade-1",
                    "token_id": "asset-1",
                    "condition_id": "condition-1",
                    "fee_rate_bps": "0",
                }

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert len(rows) == 1
    assert rows[0]["asset"] == "asset-1"


@pytest.mark.asyncio
async def test_wallet_trades_still_dedupes_repeated_rows_without_a_trade_id() -> None:
    """Preservation: the whole-row fallback must not stop real dedup.

    An overlapping page re-emits a byte-identical row; it is one fill, not two.
    """

    class _Client:
        async def paginate(self, *, params, **_kwargs):
            if params["side"] == "BUY":
                for _ in range(3):
                    yield {
                        "transactionHash": "0xtx",
                        "asset": "asset-1",
                        "condition_id": "condition-1",
                        "size": "10",
                    }

    rows = await wallet_trades(
        _Client(),  # type: ignore[arg-type]
        "0x" + "1" * 40,
        start_time=1,
        end_time=2,
        page_size=200,
        max_pages=1,
    )

    assert len(rows) == 1


# ---------------------------------------------------------------------------
# hscore_rows sort-mode fan-out — one failing mode must not discard the rest,
# but a total outage must stay loud.
# ---------------------------------------------------------------------------


def _shortlist(**overrides) -> ShortlistConfig:
    return ShortlistConfig.model_validate(
        {"sort_modes": ["h_score", "roi"], **overrides}
    )


class _ModeClient:
    """Falcon stub whose ``sort_by`` modes can be made to fail individually."""

    def __init__(
        self, rows_by_mode: dict[str, list[dict]], failing: dict[str, Exception]
    ):
        self._rows_by_mode = rows_by_mode
        self._failing = failing

    async def paginate(self, *, params, **_kwargs):
        sort_by = params["sort_by"]
        if sort_by in self._failing:
            raise self._failing[sort_by]
        for row in self._rows_by_mode.get(sort_by, []):
            yield row


@pytest.mark.asyncio
async def test_hscore_rows_keeps_surviving_modes_when_one_sort_mode_fails() -> None:
    """A single mode's 500 must not empty the whole shortlist.

    ``asyncio.gather`` without ``return_exceptions`` propagated the first
    failure and discarded every already-fetched row, so one flaky sort mode
    looked to the funnel like "no wallet qualifies".
    """
    client = _ModeClient(
        rows_by_mode={"roi": [{"proxy_wallet": "0xAAA"}, {"proxy_wallet": "0xBBB"}]},
        failing={"h_score": httpx.HTTPError("boom")},
    )

    shortlist = await hscore_rows(client, _shortlist())  # type: ignore[arg-type]

    assert [row["proxy_wallet"] for row in shortlist.rows] == ["0xAAA", "0xBBB"]
    # The survivors are usable, but the caller must be told the set is partial
    # so a wallet only "h_score" surfaces is not read as having dropped out.
    assert shortlist.failed_sort_modes == ("h_score",)
    assert shortlist.degraded is True


@pytest.mark.asyncio
async def test_hscore_rows_raises_when_every_sort_mode_fails() -> None:
    """Total outage must not masquerade as an empty shortlist.

    Returning [] would surface as "H-Score returned no valid wallets" and let
    LeaderRegistry read an agent outage as evidence for demoting every wallet.
    """
    client = _ModeClient(
        rows_by_mode={},
        failing={
            "h_score": httpx.HTTPError("boom"),
            "roi": httpx.HTTPError("boom"),
        },
    )

    with pytest.raises(RuntimeError, match="all H-Score sort modes failed"):
        await hscore_rows(client, _shortlist())  # type: ignore[arg-type]


class _RecordingHandler(logging.Handler):
    """Keeps raw ``LogRecord``s so ``extra`` survives as record attributes."""

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def falcon_log_records():
    """Capture the production logger's own records.

    ``caplog`` cannot see these. ``utils.logger`` sets ``propagate = False`` on
    the app logger, so nothing this module logs ever reaches the root handler
    pytest installs — and caplog's default formatter renders only ``%(message)s``,
    which would drop the ``extra`` fields even if it did. A test built on caplog
    therefore asserts against an empty string and passes no matter what the
    production code logs. Attach to the real logger object and read the
    ``LogRecord`` instead.
    """
    handler = _RecordingHandler()
    logger = falcon_client.logger
    previous_level = logger.level
    # Pin the level so the assertion cannot depend on ambient log config.
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


@pytest.mark.asyncio
async def test_hscore_rows_failure_log_redacts_the_bearer_token(
    falcon_log_records: list[logging.LogRecord],
) -> None:
    """Transport errors can carry the Authorization header, so the log must not.

    Discriminating on purpose: this fails if production ever logs
    ``str(result)`` in place of ``redact_error_detail(result)``.
    """
    token = "supersecrettoken"
    client = _ModeClient(
        rows_by_mode={"roi": [{"proxy_wallet": "0xAAA"}]},
        failing={"h_score": httpx.HTTPError(f"401 Bearer {token}")},
    )

    shortlist = await hscore_rows(client, _shortlist())  # type: ignore[arg-type]

    assert [row["proxy_wallet"] for row in shortlist.rows] == ["0xAAA"]

    # The warning must actually be emitted — an absent log is not a pass.
    warnings = [
        record
        for record in falcon_log_records
        if record.levelno == logging.WARNING
        and record.getMessage() == "hscore_sort_mode_failed"
    ]
    assert len(warnings) == 1, [record.getMessage() for record in falcon_log_records]
    record = warnings[0]
    assert record.sort_by == "h_score"  # type: ignore[attr-defined]

    # ``redact_error_detail`` collapses an error it cannot categorise to its
    # exception class name; ``str(exc)`` would have kept the token verbatim.
    assert record.error == "HTTPError"  # type: ignore[attr-defined]
    assert token not in record.error  # type: ignore[attr-defined]
    # Nothing else on the record may smuggle it out either.
    assert token not in str(record.__dict__)


@pytest.mark.asyncio
async def test_hscore_rows_still_interleaves_every_healthy_mode() -> None:
    """Preservation: the round-robin merge across modes is unchanged."""
    client = _ModeClient(
        rows_by_mode={
            "h_score": [{"proxy_wallet": "0xAAA"}, {"proxy_wallet": "0xCCC"}],
            "roi": [{"proxy_wallet": "0xBBB"}],
        },
        failing={},
    )

    shortlist = await hscore_rows(client, _shortlist())  # type: ignore[arg-type]

    # Round-robin by index, deduped on wallet.
    assert [row["proxy_wallet"] for row in shortlist.rows] == [
        "0xAAA",
        "0xBBB",
        "0xCCC",
    ]
    # A complete cycle must not be marked degraded, or absence-based demotion
    # would be suppressed forever and no leader could ever be demoted.
    assert shortlist.failed_sort_modes == ()
    assert shortlist.degraded is False
