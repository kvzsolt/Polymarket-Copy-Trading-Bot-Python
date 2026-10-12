"""Read-only bounded pending evaluation with real durable evidence and injected I/O."""

import asyncio
import json

import pytest

from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt
from tests.test_aethertrade_evidence_outbox import outbox
from tests.test_aethertrade_order_verification import (
    OWNER,
    OTHER,
    EXCHANGE,
    calldata,
    order,
    signed,
)

BASE = 1_800_000_000_000_000_000
HASH = "0x" + "aa" * 32


class Clock:
    def __init__(self):
        self.elapsed = 100

    def capture(self):
        ns = BASE + self.elapsed
        return TransportReceipt(ns // 1_000_000, self.elapsed, "finland-test", ns)

    def advance(self, ns):
        self.elapsed += ns


class Budget:
    def __init__(self):
        self.calls = []
        self.fail = False

    def charge_stream(self, provider, byte_count, *, category):
        self.calls.append((provider, byte_count, category))
        if self.fail:
            raise BudgetExhausted("test budget exhausted")


class Sink:
    def __init__(self, box):
        self.box = box

    def append_event(self, event):
        self.box.append(event)

    def invalidate(self, reason):
        self.box.invalidate(reason)


def frame(tx=HASH):
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {"subscription": "fixture", "result": tx},
        }
    )


def body(tx=HASH, **changes):
    return {
        "hash": tx,
        "to": EXCHANGE,
        "from": OTHER,
        "nonce": "0x1",
        "blockNumber": None,
        "input": calldata(
            taker=signed(order()), makers=[order(owner=OTHER), signed(order(salt=2))]
        ),
        **changes,
    }


class Gateway:
    def __init__(self, clock):
        self.clock, self.calls = clock, []
        self.result = body()
        self.error = None
        self.release = None
        self.started = asyncio.Event()

    async def call_observed(self, provider, method, params, *, category):
        self.calls.append((provider, method, params, category))
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error:
            raise self.error
        self.clock.advance(100)
        receipt = self.clock.capture()
        self.clock.advance(100)
        return ObservedRpcResult(self.result, receipt)


def setup(tmp_path, *, globally_enabled=True, policies=None, **limits):
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.domain.pending_evaluation import PendingEvaluationLimits
    from pmcopy.services.pending_evaluation import BoundedPendingEvaluator

    clock, budget, box = Clock(), Budget(), outbox(tmp_path / "box")
    gateway = Gateway(clock)
    evaluator = BoundedPendingEvaluator(
        policies if policies is not None else [TraderDetectionPolicy(OWNER, mode="pending")],
        globally_enabled=globally_enabled,
        gateway=gateway,
        stream_admission=budget,
        receipt_clock=clock,
        evidence_sink=Sink(box),
        limits=PendingEvaluationLimits(**limits),
    )
    return evaluator, clock, budget, gateway, box


def wires(box):
    return [row.event.wire() for row in box.peek()]


async def test_charge_every_frame_before_filter_and_durable_per_owner_attribution(tmp_path):
    evaluator, clock, budget, gateway, box = setup(tmp_path)
    ack = '{"jsonrpc":"2.0","id":1,"result":"fixture"}'
    assert await evaluator.observe_frame(ack, clock.capture()) == "ignored"
    first = clock.capture()
    assert await evaluator.observe_frame(frame(), first) == "attributed"
    assert budget.calls == [
        ("alchemy", len(ack.encode()), "pending"),
        ("alchemy", len(frame().encode()), "pending"),
    ]
    assert gateway.calls == [("alchemy", "eth_getTransactionByHash", [HASH], "pending")]
    observed = wires(box)
    assert [v["kind"] for v in observed] == [
        "pending_observation",
        "pending_body_observed",
        "order_verification",
        "order_verification",
        "pending_observation",
    ]
    assert [
        (v["order_role"], v["order_index"], v["owner"])
        for v in observed
        if v["kind"] == "order_verification"
    ] == [("taker", 0, OWNER), ("maker", 1, OWNER)]
    outcome = observed[-1]
    assert (
        int(outcome["hash_received_ns"])
        < int(outcome["body_received_ns"])
        < int(outcome["attributable_received_ns"])
    )
    assert outcome["source_event_id"] == observed[1]["event_id"]
    assert "score" not in outcome and "replacement_hash" not in outcome
    assert await evaluator.observe_frame(frame(), clock.capture()) == "duplicate"
    assert len(gateway.calls) == 1 and len(budget.calls) == 3
    await evaluator.aclose()
    box.close()


@pytest.mark.parametrize("reason", ["global", "mined", "disabled"])
async def test_desired_pending_flags_never_enable_unconfigured_traders(tmp_path, reason):
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy

    policy = TraderDetectionPolicy(
        OWNER, mode="mined" if reason == "mined" else "pending", enabled=reason != "disabled"
    )
    evaluator, clock, budget, gateway, box = setup(
        tmp_path, globally_enabled=reason != "global", policies=[policy]
    )
    await evaluator.observe_frame(frame(), clock.capture())
    assert len(budget.calls) == 1 and not gateway.calls and not wires(box)
    box.close()


async def test_hash_only_missing_body_is_durable_and_no_retry_or_fake_score(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    gateway.result = None
    assert await evaluator.observe_frame(frame(), clock.capture()) == "body_unavailable"
    assert [v["stage"] for v in wires(box)] == ["hash_observed", "body_unavailable"]
    assert "body_received_ns" in wires(box)[-1]  # Native null-body RPC response exists.
    assert "attributable_received_ns" not in wires(box)[-1]
    assert await evaluator.observe_frame(frame(), clock.capture()) == "duplicate"
    assert len(gateway.calls) == 1
    box.close()


@pytest.mark.parametrize(
    "mutation",
    [{"hash": "0x" + "bb" * 32}, {"input": "0x00"}, {"blockNumber": "0x1"}, {"nonce": True}],
)
async def test_invalid_or_already_mined_body_never_becomes_pending_candidate(tmp_path, mutation):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    gateway.result = body(**mutation)
    assert await evaluator.observe_frame(frame(), clock.capture()) == "body_invalid"
    assert not any(v["kind"] == "order_verification" for v in wires(box))
    assert wires(box)[-1]["stage"] == "body_invalid"
    box.close()


async def test_stream_budget_failure_stops_before_body_io_and_preserves_hash(tmp_path):
    evaluator, clock, budget, gateway, box = setup(tmp_path)
    budget.fail = True
    assert await evaluator.observe_frame(frame(), clock.capture()) == "budget_stopped"
    assert not gateway.calls and evaluator.stop_requested
    assert wires(box)[-1]["stage"] == "budget_stopped" and box.health().valid
    box.close()


async def test_rpc_budget_failure_is_not_swallowed_as_missing_body(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    gateway.error = BudgetExhausted("test admission blocked")
    assert await evaluator.observe_frame(frame(), clock.capture()) == "budget_stopped"
    assert evaluator.stop_requested and wires(box)[-1]["stage"] == "budget_stopped"
    box.close()


async def test_bounded_inflight_and_shutdown_preserve_unknown_without_waiting_for_network(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path, max_inflight=1)
    gateway.release = asyncio.Event()
    work = asyncio.create_task(evaluator.observe_frame(frame(), clock.capture()))
    await gateway.started.wait()
    assert evaluator.inflight == 1
    second = "0x" + "bb" * 32
    assert await evaluator.observe_frame(frame(second), clock.capture()) == "capacity_stopped"
    assert evaluator.stop_requested and len(gateway.calls) == 1
    await evaluator.aclose()
    assert work.cancelled() and evaluator.inflight == 0
    assert any(
        v.get("stage") == "deadline_unknown" and v["transaction_hash"] == HASH for v in wires(box)
    )
    assert box.health().valid
    box.close()


async def test_fake_clock_expiry_cannot_claim_late_body_at_hash_time(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path, body_timeout_ns=1_000_000)
    gateway.release = asyncio.Event()
    work = asyncio.create_task(evaluator.observe_frame(frame(), clock.capture()))
    await gateway.started.wait()
    clock.advance(2_000_000)
    evaluator.expire()
    assert wires(box)[-1]["stage"] == "deadline_unknown"
    gateway.release.set()
    assert await work == "deadline_unknown"
    assert not any(v["kind"] == "order_verification" for v in wires(box))
    assert any(v["kind"] == "pending_body_observed" for v in wires(box))
    box.close()


async def test_competing_nonce_bodies_are_not_asserted_replaced(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    await evaluator.observe_frame(frame(), clock.capture())
    other_hash = "0x" + "bb" * 32
    gateway.result = body(other_hash)
    await evaluator.observe_frame(frame(other_hash), clock.capture())
    sources = [v for v in wires(box) if v["kind"] == "pending_body_observed"]
    assert len(sources) == 2 and sources[0]["nonce"] == sources[1]["nonce"]
    assert not any("replacement" in json.dumps(v) for v in wires(box))
    box.close()


async def test_transport_failure_backs_off_without_new_polling_then_session_cap_stops(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path, max_body_lookups=1)
    gateway.error = RuntimeError("sensitive transport error")
    assert await evaluator.observe_frame(frame(), clock.capture()) == "body_unavailable"
    assert (
        await evaluator.observe_frame(frame("0x" + "bb" * 32), clock.capture())
        == "capacity_stopped"
    )
    assert len(gateway.calls) == 1 and evaluator.stop_requested
    assert "sensitive" not in json.dumps(wires(box))
    box.close()


def test_pending_stage_schema_rejects_backdating_and_fabricated_attribution():
    from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec

    base = {
        "kind": "pending_observation",
        "stage": "hash_observed",
        "transaction_hash": HASH,
        "received_ns": str(BASE + 10),
        "hash_received_ns": str(BASE),
        "clock_id": "finland-test",
    }
    assert PmreconEvidenceCodec.encode(base).wire()["schema_version"] == 2
    for changes in (
        {"stage": "attributed"},
        {"body_received_ns": str(BASE - 1)},
        {"raw_hash_frame": "é" * 8193},
        {"replacement_hash": HASH},
    ):
        with pytest.raises(ValueError):
            PmreconEvidenceCodec.encode(base | changes)


async def test_attribution_work_past_deadline_preserves_unknown(tmp_path):
    from pmcopy.integrations.pmrecon.pending_decoder import V2PendingCalldataDecoder

    evaluator, clock, _, _, box = setup(tmp_path, body_timeout_ns=1_000_000)

    class SlowDecoder(V2PendingCalldataDecoder):
        def decode(self, target, raw):
            result = super().decode(target, raw)
            clock.advance(2_000_000)
            return result

    evaluator._decoder = SlowDecoder()
    assert await evaluator.observe_frame(frame(), clock.capture()) == "deadline_unknown"
    assert not any(v["kind"] == "order_verification" for v in wires(box))
    assert wires(box)[-1]["stage"] == "deadline_unknown"
    box.close()


async def test_backoff_keeps_later_hash_evidence_without_extra_body_read(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    gateway.error = RuntimeError("provider unavailable")
    await evaluator.observe_frame(frame(), clock.capture())
    other_hash = "0x" + "bb" * 32
    assert await evaluator.observe_frame(frame(other_hash), clock.capture()) == "body_unavailable"
    assert len(gateway.calls) == 1 and not evaluator.stop_requested
    assert wires(box)[-1]["transaction_hash"] == other_hash
    assert "body_received_ns" not in wires(box)[-1]
    box.close()


async def test_deadline_port_timeout_is_durable_and_cancels_lookup(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)

    class Deadline:
        async def run(self, read, timeout_s):
            assert timeout_s == 3
            read.close()
            clock.advance(3_000_000_000)
            raise TimeoutError("private endpoint details")

    evaluator._deadline = Deadline()
    assert await evaluator.observe_frame(frame(), clock.capture()) == "deadline_unknown"
    assert evaluator.inflight == 0 and not gateway.calls
    assert "private" not in json.dumps(wires(box))
    box.close()


async def test_invalid_clock_stops_source_and_invalidates_durable_continuity(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    missing_ns = TransportReceipt(BASE // 1_000_000, 100, "finland-test")
    with pytest.raises(ValueError):
        await evaluator.observe_frame(frame(), missing_ns)
    assert evaluator.stop_requested and not box.health().valid and not gateway.calls
    box.close()


async def test_already_mined_body_retained_with_original_receipt_without_candidate(tmp_path):
    evaluator, clock, _, gateway, box = setup(tmp_path)
    gateway.result = body(blockNumber="0x123")
    assert await evaluator.observe_frame(frame(), clock.capture()) == "body_invalid"
    sources = [v for v in wires(box) if v["kind"] == "pending_body_observed"]
    assert len(sources) == 1 and sources[0]["block_number"] == 0x123
    assert sources[0]["received_ns"] == sources[0]["body_received_ns"]
    assert not any(v["kind"] == "order_verification" for v in wires(box))
    box.close()


def test_raw_body_v2_cannot_label_later_attribution_as_original_body_receipt():
    from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec
    from tests.test_aethertrade_order_verification import source

    raw = source().wire()
    raw.pop("event_id")
    raw["kind"] = "pending_body_observed"
    with pytest.raises(ValueError):
        PmreconEvidenceCodec.encode(raw)
    raw["received_ns"] = raw["body_received_ns"]
    assert PmreconEvidenceCodec.encode(raw).wire()["schema_version"] == 2


async def test_binary_frames_charge_exact_bytes_and_invalid_utf8_stops_visibly(tmp_path):
    evaluator, clock, budget, gateway, box = setup(tmp_path)
    raw = frame().encode()
    assert (
        await evaluator.observe_frame(raw, clock.capture(), expected_subscription="fixture")
        == "attributed"
    )
    assert budget.calls[0] == ("alchemy", len(raw), "pending")
    assert (
        await evaluator.observe_frame(b"\xff\xfe", clock.capture(), expected_subscription="fixture")
        == "stream_invalid"
    )
    assert budget.calls[-1] == ("alchemy", 2, "pending") and evaluator.stop_requested
    assert len(gateway.calls) == 1 and wires(box)[-1]["kind"] == "gap"
    box.close()


@pytest.mark.parametrize("subscription", ["", "foreign"])
async def test_no_lookup_until_own_subscription_acknowledged(tmp_path, subscription):
    evaluator, clock, budget, gateway, box = setup(tmp_path)
    assert (
        await evaluator.observe_frame(frame(), clock.capture(), expected_subscription=subscription)
        == "ignored"
    )
    assert len(budget.calls) == 1 and not gateway.calls and not wires(box)
    box.close()


async def test_malformed_rpc_envelope_cannot_lookup_before_runtime_validation(tmp_path):
    evaluator, clock, budget, gateway, box = setup(tmp_path)
    malformed = json.loads(frame())
    malformed["jsonrpc"] = "1.0"
    raw = json.dumps(malformed)
    assert await evaluator.observe_frame(raw, clock.capture(), expected_subscription="fixture") == "ignored"
    assert len(budget.calls) == 1 and not gateway.calls and not wires(box)
    box.close()
