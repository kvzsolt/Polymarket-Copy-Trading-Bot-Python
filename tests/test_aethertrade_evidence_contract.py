"""Producer parity against independently generated actual pmrecon/store goldens."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from integrations.pmrecon.codec import PmreconEvidenceCodec
from integrations.pmrecon.observation_sink import PmreconObservationSink
from tests.test_aethertrade_evidence_outbox import outbox


def test_all_v1_kinds_match_actual_receiver_canonical_bytes_ids_and_acks(tmp_path):
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/aethertrade_evidence_v1.json").read_text()
    )
    box = outbox(tmp_path / "box")
    for case in fixture["cases"]:
        item = PmreconEvidenceCodec.encode(case["input"])
        assert item.canonical_bytes.decode() == case["canonical"]
        assert item.event_id == case["event_id"]
        assert item.wire() == case["wire"] | {"event_id": case["event_id"]}
        box.append(item)
    batch = box.peek()
    box.acknowledge(
        batch,
        {
            "protocol_version": fixture["protocol_version"],
            "results": [case["ack"] for case in fixture["cases"]],
        },
    )
    assert box.peek() == ()
    box.close()


def test_raw_mined_sink_preserves_provider_log_and_original_clock(tmp_path):
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/aethertrade_evidence_v1.json").read_text()
    )
    case = fixture["cases"][0]
    value = case["input"]
    box = outbox(tmp_path / "box")
    sink = PmreconObservationSink(box)
    receipt = SimpleNamespace(
        wall_timestamp_ns=int(value["received_ns"]), boot_id=value["clock_id"]
    )
    sink.record(value["provider"], "mined", value["log"], receipt, recovered=False)
    item = box.peek()[0].event
    assert item.event_id == case["event_id"]
    assert item.wire()["log"] == value["log"]
    box.close()


def header():
    return {
        "number": "0x10",
        "hash": "0x" + "11" * 32,
        "parentHash": "0x" + "22" * 32,
        "timestamp": "0x65000000",
    }


def test_header_sink_retains_complete_bounded_raw_provider_block(tmp_path):
    box = outbox(tmp_path / "box")
    raw = header() | {"gasUsed": "0x1", "transactions": ["0x" + "33" * 32]}
    receipt = SimpleNamespace(wall_timestamp_ns=1_800_000_000_123_456_789, boot_id="finland-test")
    PmreconObservationSink(box).record("infura", "block_header", raw, receipt, recovered=True)
    wire = box.peek()[0].event.wire()
    assert wire["schema_version"] == 2 and wire["kind"] == "block_header"
    assert wire["header"] == header() and wire["raw_header"] == raw
    assert wire["received_ns"] == str(receipt.wall_timestamp_ns)
    assert wire["recovery"] is True
    box.close()


@pytest.mark.parametrize(
    "mutation",
    [
        {"number": "0x00"},
        {"hash": "0x12"},
        {"timestamp": 123},
        {"provider_url": "https://unsafe.invalid?token=private"},
        {"transactions": [{"hash": "0x" + "11" * 32}]},
    ],
)
def test_header_rejects_invalid_or_secret_bearing_raw_fields(tmp_path, mutation):
    box = outbox(tmp_path / "box")
    receipt = SimpleNamespace(wall_timestamp_ns=1_800_000_000_123_456_789, boot_id="finland-test")
    with pytest.raises(ValueError):
        PmreconObservationSink(box).record(
            "infura", "block_header", header() | mutation, receipt, recovered=True
        )
    assert not box.health().valid
    box.close()


def test_market_frame_keeps_original_bytes_and_stream_sequence_even_for_invalid_json():
    raw = '{ "event_type":"book", "price":"0.5000" '
    item = PmreconEvidenceCodec.encode(
        {
            "kind": "market_frame",
            "stream_id": "market-session-a",
            "sequence": 1,
            "frame": raw,
            "received_ns": "1800000000123456789",
            "clock_id": "finland-test",
        }
    )
    assert item.wire()["frame"] == raw and item.wire()["sequence"] == 1
    assert item.wire()["schema_version"] == 2


@pytest.mark.parametrize(
    "mutation",
    [
        {"sequence": True},
        {"sequence": 0},
        {"frame": "é" * 131073},
        {"schema_version": 1},
        {"stream_id": ""},
    ],
)
def test_market_frame_rejects_invalid_bounds(mutation):
    with pytest.raises(ValueError):
        PmreconEvidenceCodec.encode(
            {
                "kind": "market_frame",
                "stream_id": "market-a",
                "sequence": 1,
                "frame": "{}",
                "received_ns": "1800000000123456789",
                "clock_id": "finland-test",
                **mutation,
            }
        )


def test_observed_follower_fill_does_not_invent_source_action_or_unknown_fee():
    item = PmreconEvidenceCodec.encode(
        {
            "kind": "follower_fill_observed",
            "follower_account": "a",
            "fill_id": "observed-fill",
            "asset_id": "123",
            "side": "SELL",
            "price": "0.5000",
            "size": "2.0100",
            "received_ns": "1800000000123456789",
            "clock_id": "finland-test",
        }
    )
    assert item.wire()["schema_version"] == 2
    assert "source_action_id" not in item.wire() and "fee" not in item.wire()
    assert item.wire()["price"] == "0.5000"
    assert item.wire()["execution_mode"] == "unknown"


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_observed_fill_preserves_explicit_execution_mode(mode):
    item = PmreconEvidenceCodec.encode(
        {
            "kind": "follower_fill_observed",
            "follower_account": "a",
            "fill_id": "observed-fill",
            "asset_id": "123",
            "side": "SELL",
            "price": "0.5000",
            "size": "2.0100",
            "received_ns": "1800000000123456789",
            "clock_id": "finland-test",
            "execution_mode": mode,
        }
    )
    assert item.wire()["execution_mode"] == mode


@pytest.mark.parametrize(
    "method,arguments,value",
    [
        ("getCollectionId", ["0x" + "00" * 32, "0x" + "11" * 32, "1"], "0x" + "22" * 32),
        ("getOutcomeSlotCount", ["0x" + "11" * 32], "2"),
    ],
)
def test_pinned_collection_and_outcome_slot_state_methods(method, arguments, value):
    wire = PmreconEvidenceCodec.encode(
        {
            "kind": "contract_state",
            "provider": "infura",
            "contract": "0x" + "33" * 20,
            "block_number": 16,
            "block_hash": "0x" + "44" * 32,
            "method": method,
            "arguments": arguments,
            "value": value,
            "received_ns": "1800000000123456789",
            "clock_id": "finland-test",
        }
    ).wire()
    assert wire["value"] == value and wire["arguments"] == arguments
