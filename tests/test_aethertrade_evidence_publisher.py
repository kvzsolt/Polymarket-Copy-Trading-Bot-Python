"""No-network publisher tests preserve ambiguous submissions and pause on conflicts."""

import asyncio

import pytest

from tests.test_aethertrade_evidence_outbox import ack, event, outbox


class FakeTransport:
    """Record immutable request bodies with controllable transport results."""

    def __init__(self, response):
        self.response, self.requests = response, []

    async def post(self, body):
        self.requests.append(body)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    async def aclose(self):
        pass


async def test_ambiguous_post_retries_exact_events_after_restart(tmp_path):
    from pmcopy.domain.evidence import EvidenceHttpResponse
    from pmcopy.services.evidence.evidence_publisher import EvidencePublisher

    path = tmp_path / "box"
    box = outbox(path)
    box.append(event())
    batch = box.peek()
    transport = FakeTransport(TimeoutError("sensitive upstream error"))
    publisher = EvidencePublisher(box, transport)
    assert await publisher.publish_once() == "retry"
    assert publisher.delivery_status == "retry"
    assert box.health().valid
    assert box.peek() == batch
    box.close()
    box = outbox(path)
    transport.response = EvidenceHttpResponse(201, ack(batch))
    publisher = EvidencePublisher(box, transport)
    assert await publisher.publish_once() == "acknowledged"
    assert publisher.delivery_status == "acknowledged"
    assert transport.requests[0] == transport.requests[1]
    assert box.peek() == ()
    box.close()


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 413, 422])
async def test_terminal_remote_rejection_blocks_without_silent_gap(tmp_path, status):
    from pmcopy.domain.evidence import EvidenceHttpResponse
    from pmcopy.services.evidence.evidence_publisher import EvidencePublisher

    path = tmp_path / "box"
    box = outbox(path, max_events=3)
    box.append(event())
    batch = box.peek()
    transport = FakeTransport(EvidenceHttpResponse(status, {"error": {"code": "conflict"}}))
    publisher = EvidencePublisher(box, transport)
    assert await publisher.publish_once() == "blocked"
    assert box.health().valid and box.health().blocked and box.health().quarantined == 1
    box.append(event(2))
    with pytest.raises(ValueError, match="nonprefix_ack"):
        box.acknowledge(batch, ack(batch))
    assert await publisher.publish_once() == "blocked"
    assert len(transport.requests) == 1
    box.close()
    box = outbox(path, max_events=3)
    assert box.health().valid and box.health().blocked
    box.append(event(3))
    assert box.health().pending == 2 and box.health().quarantined == 1
    assert box.peek() == ()
    with pytest.raises(ValueError, match="capacity"):
        box.append(event(4))
    assert not box.health().valid and box.health().blocked
    box.close()


async def test_malformed_success_retains_every_item(tmp_path):
    from pmcopy.domain.evidence import EvidenceHttpResponse
    from pmcopy.services.evidence.evidence_publisher import EvidencePublisher

    box = outbox(tmp_path / "box")
    box.append(event())
    rows = box.peek()
    publisher = EvidencePublisher(box, FakeTransport(EvidenceHttpResponse(201, {"results": []})))
    assert await publisher.publish_once() == "retry"
    assert box.peek() == rows
    box.close()


async def test_local_append_never_waits_for_hungary_response(tmp_path):
    from pmcopy.domain.evidence import EvidenceHttpResponse
    from pmcopy.services.evidence.evidence_publisher import EvidencePublisher

    started, release = asyncio.Event(), asyncio.Event()
    box = outbox(tmp_path / "box")
    box.append(event())
    batch = box.peek()

    class StalledTransport:
        async def post(self, body):
            started.set()
            await release.wait()
            return EvidenceHttpResponse(201, ack(batch))

    publishing = asyncio.create_task(EvidencePublisher(box, StalledTransport()).publish_once())
    await started.wait()
    box.append(event(2))
    assert len(box.peek()) == 2
    release.set()
    assert await publishing == "acknowledged"
    assert len(box.peek()) == 1
    box.close()
