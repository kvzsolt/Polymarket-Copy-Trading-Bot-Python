"""Real HTTPX transport fakes, source clock integrity and transactional append seam."""

import asyncio
import json
import logging
from types import SimpleNamespace

import httpx
import pytest

from tests.test_aethertrade_evidence_outbox import event, outbox


@pytest.mark.parametrize(
    "origin",
    [
        "https://user:password@example.invalid",
        "https://example.invalid/path",
        "https://example.invalid?token=credential",
        "https://example.invalid/#fragment",
        "http://public.example.invalid",
        "file:///tmp/data",
        "https://example.invalid:invalid",
    ],
)
def test_transport_rejects_unsafe_origins_without_echoing(origin):
    from integrations.pmrecon.http_transport import PmreconHttpTransport

    with pytest.raises(ValueError) as caught:
        PmreconHttpTransport(origin, "private-service-token" * 2)
    assert origin not in str(caught.value)


async def test_http_posts_fixed_path_private_header_and_never_redirects():
    from integrations.pmrecon.http_transport import PmreconHttpTransport

    requests = []

    def receiver(request):
        requests.append(request)
        return httpx.Response(307, headers={"location": "https://elsewhere.invalid"})

    token = "private-service-token" * 2
    transport = PmreconHttpTransport(
        "https://hungary.invalid", token, transport=httpx.MockTransport(receiver)
    )
    response = await transport.post(b'{"events":[]}')
    assert response.status == 307
    assert len(requests) == 1 and str(requests[0].url) == "https://hungary.invalid/api/v1/evidence"
    assert requests[0].headers["authorization"] == "Bearer " + token
    assert "origin" not in requests[0].headers
    assert token not in repr(transport)
    await transport.aclose()


@pytest.mark.parametrize("scenario", ["large", "compressed", "exception"])
async def test_http_reply_and_error_paths_are_bounded_and_sanitized(scenario):
    from integrations.pmrecon.http_transport import PmreconHttpTransport

    def receiver(request):
        if scenario == "exception":
            raise httpx.ReadError("SECRET https://sensitive.invalid")
        if scenario == "compressed":
            return httpx.Response(200, content=b"{}", headers={"content-encoding": "gzip"})
        return httpx.Response(200, content=b"x" * 1025)

    transport = PmreconHttpTransport(
        "https://hungary.invalid",
        "token" * 10,
        max_response_bytes=1024,
        transport=httpx.MockTransport(receiver),
    )
    with pytest.raises(ValueError) as caught:
        await transport.post(b'{"events":[]}')
    assert "SECRET" not in str(caught.value) and "sensitive" not in str(caught.value)
    assert caught.value.__suppress_context__
    await transport.aclose()


async def test_http_total_timeout_is_bounded_even_without_a_response():
    from integrations.pmrecon.http_transport import PmreconHttpTransport

    async def receiver(request):
        await asyncio.Event().wait()

    transport = PmreconHttpTransport(
        "https://hungary.invalid",
        "token" * 10,
        timeout=0.01,
        transport=httpx.MockTransport(receiver),
    )
    with pytest.raises(ValueError, match="transport_unavailable"):
        await transport.post(b'{"events":[]}')
    await transport.aclose()


async def test_http_library_logging_does_not_expose_private_origin(caplog):
    from integrations.pmrecon.http_transport import PmreconHttpTransport

    caplog.set_level(logging.INFO, logger="httpx")
    mock = httpx.MockTransport(lambda request: httpx.Response(201, json={}))
    transport = PmreconHttpTransport(
        "https://private-evidence.invalid", "unit-test-token" * 3, transport=mock
    )
    await transport.post(b'{"events":[]}')
    await transport.aclose()
    assert "private-evidence.invalid" not in caplog.text
    async with httpx.AsyncClient(transport=mock) as other:
        await other.get("https://ordinary-public.invalid")
    assert "ordinary-public.invalid" in caplog.text


def test_sink_retains_native_source_ns_provider_recovery_and_unknown_fee(tmp_path):
    from integrations.pmrecon.observation_sink import PmreconObservationSink

    box = outbox(tmp_path / "box")
    sink = PmreconObservationSink(box)
    ns = 1_800_000_000_123_456_789
    receipt = SimpleNamespace(
        wall_timestamp_ns=ns, wall_timestamp_ms=ns // 1_000_000, boot_id="finland-test"
    )
    payload = {"provider": "bot", "reason": "disconnected"}
    sink.record("bot", "gap", payload, receipt, recovered=True)
    wire = box.peek()[0].event.wire()
    assert wire["received_ns"] == str(ns) and wire["clock_id"] == "finland-test"
    assert wire["provider"] == "bot" and wire["recovery"] is True
    box.close()


@pytest.mark.parametrize("native,kind", [(None, "gap"), (1_800_000_000_123_456_789, "unknown")])
def test_sink_missing_native_clock_or_unknown_kind_invalidates(tmp_path, native, kind):
    from integrations.pmrecon.observation_sink import PmreconObservationSink

    box = outbox(tmp_path / "box")
    sink = PmreconObservationSink(box)
    receipt = SimpleNamespace(wall_timestamp_ns=native, boot_id="finland-test")
    with pytest.raises(ValueError):
        sink.record("bot", kind, {}, receipt, recovered=False)
    assert not box.health().valid
    assert box.peek()[0].event.wire()["kind"] == "gap"
    box.close()


async def test_postgres_append_uses_existing_transaction_without_commit():
    from integrations.pmrecon.transaction_outbox import PostgresEvidenceOutbox

    class Connection:
        def __init__(self):
            self.calls = []

        def is_in_transaction(self):
            return True

        async def fetchrow(self, sql, *args):
            self.calls.append((sql, args))
            return {"seq": 7, "digest": args[1]}

    connection = Connection()
    item = event()
    assert await PostgresEvidenceOutbox().append(connection, item) == 7
    query, args = connection.calls[0]
    assert "ON CONFLICT" in query and "RETURNING" in query
    assert args[0] == item.event_id and args[1] == item.digest
    assert json.loads(args[2]) == item.wire()


async def test_postgres_missing_transaction_and_conflicting_id_fail_closed():
    from integrations.pmrecon.transaction_outbox import PostgresEvidenceOutbox

    class Connection:
        active = False

        def is_in_transaction(self):
            return self.active

        async def fetchrow(self, *args):
            return {"seq": 1, "digest": "other"}

    connection = Connection()
    seam = PostgresEvidenceOutbox()
    with pytest.raises(ValueError, match="transaction"):
        await seam.append(connection, event())
    connection.active = True
    with pytest.raises(ValueError, match="conflict"):
        await seam.append(connection, event())
