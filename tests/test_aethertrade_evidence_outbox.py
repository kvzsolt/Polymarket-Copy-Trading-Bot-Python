"""Durable producer identity, bounded storage and exact acknowledgement regressions."""

import hashlib
import json
import sqlite3

import pytest


class FixedClock:
    """Deterministic native epoch clock for loss evidence."""

    def time_ns(self):
        return 1_800_000_000_123_456_789

    @property
    def clock_id(self):
        return "finland-test"


def event(number=1, **changes):
    """Build a standalone exact book event through the producer boundary."""
    from integrations.pmrecon.codec import PmreconEvidenceCodec

    return PmreconEvidenceCodec.encode(
        {
            "kind": "book",
            "received_ns": str(1_800_000_000_123_456_789 + number),
            "clock_id": "finland-test",
            "asset_id": "123",
            "bids": [],
            "asks": [["0.5000", "2.0100"]],
            **changes,
        }
    )


def outbox(path, **kwargs):
    """Inject a fixed clock and return a local SQLite producer."""
    from storage.evidence_outbox import SqliteEvidenceOutbox

    return SqliteEvidenceOutbox(path, clock=FixedClock(), **kwargs)


def ack(batch, **changes):
    """Construct the actual pmrecon atomic batch response."""
    return {
        "protocol_version": "aethertrade.evidence.v1",
        "results": [
            {"seq": 10 + index * 2, "event_id": row.event.event_id, "duplicate": False}
            for index, row in enumerate(batch)
        ],
        **changes,
    }


def test_v1_default_identity_and_exact_canonical_bytes():
    item = event()
    canonical = {
        "kind": "book",
        "received_ns": "1800000000123456790",
        "clock_id": "finland-test",
        "recovery": False,
        "asset_id": "123",
        "bids": [],
        "asks": [["0.5000", "2.0100"]],
        "safe": True,
    }
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    assert item.canonical_bytes == encoded
    assert item.event_id == hashlib.sha256(encoded).hexdigest()
    assert item.wire()["asks"] == [["0.5000", "2.0100"]]


@pytest.mark.parametrize(
    "changes",
    [
        {"asks": [[0.5, "2"]]},
        {"received_ns": "1.2"},
        {"clock_id": "unspecified"},
        {"unexpected": 1},
        {"safe": "true"},
        {"recovery": 1},
        {"asks": [["0.5", "-1"]]},
        {"asks": [["0.5", "2"], ["0.5", "3"]]},
    ],
)
def test_codec_rejects_unknown_precision_and_malformed_values(changes):
    with pytest.raises(ValueError):
        event(**changes)


def test_restart_replays_identical_bytes_and_idempotency_survives_ack(tmp_path):
    path = tmp_path / "outbox.sqlite3"
    item = event(event_id="stable-identity")
    box = outbox(path)
    assert box.append(item) == box.append(item)
    before = box.peek()
    box.close()
    box = outbox(path)
    assert box.peek() == before
    box.acknowledge(before, ack(before))
    assert box.peek() == ()
    assert box.append(item) == before[0].sequence
    assert box.peek() == ()
    with pytest.raises(ValueError, match="conflict"):
        box.append(event(event_id="stable-identity", asks=[["0.6", "2"]]))
    assert not box.health().valid
    box.close()


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "foreign",
        "reordered",
        "negative_seq",
        "bool_seq",
        "repeat_seq",
        "bad_version",
        "duplicate_string",
    ],
)
def test_ack_requires_exact_local_prefix_and_every_remote_identity(tmp_path, fault):
    box = outbox(tmp_path / "box")
    box.append(event(1))
    box.append(event(2))
    batch = box.peek()
    response = ack(batch)
    if fault == "missing":
        response["results"].pop()
    elif fault == "foreign":
        response["results"][0]["event_id"] = "foreign"
    elif fault == "reordered":
        response["results"].reverse()
    elif fault == "negative_seq":
        response["results"][0]["seq"] = -1
    elif fault == "bool_seq":
        response["results"][0]["seq"] = True
    elif fault == "repeat_seq":
        response["results"][1]["seq"] = response["results"][0]["seq"]
    elif fault == "bad_version":
        response["protocol_version"] = "other"
    else:
        response["results"][0]["duplicate"] = "true"
    with pytest.raises(ValueError):
        box.acknowledge(batch, response)
    assert box.peek() == batch
    box.close()


def test_cannot_ack_nonprefix_and_remote_sequence_need_not_be_contiguous(tmp_path):
    box = outbox(tmp_path / "box")
    for n in range(3):
        box.append(event(n))
    rows = box.peek()
    with pytest.raises(ValueError):
        box.acknowledge(rows[1:], ack(rows[1:]))
    box.acknowledge(rows[:2], ack(rows[:2]))
    assert box.peek() == rows[2:]
    box.close()


@pytest.mark.parametrize("limit", [0, 101, True])
def test_batch_size_is_bounded(tmp_path, limit):
    box = outbox(tmp_path / "box")
    with pytest.raises(ValueError):
        box.peek(limit)
    box.close()


@pytest.mark.parametrize("limits", [{"max_events": 1}, {"max_bytes": 300}, {"max_history": 1}])
def test_overflow_durably_invalidates_and_retains_reserved_gap(tmp_path, limits):
    path = tmp_path / "box"
    box = outbox(path, **limits)
    box.append(event(1))
    with pytest.raises(ValueError, match="capacity"):
        box.append(event(2))
    assert not box.health().valid
    rows = box.peek()
    assert rows[0].event == event(1)
    assert rows[-1].event.wire()["kind"] == "gap"
    assert rows[-1].event.wire()["reason"] == "overflow"
    box.close()
    box = outbox(path, **limits)
    assert not box.health().valid
    assert box.peek() == rows
    with pytest.raises(ValueError):
        box.append(event(3))
    box.close()


def test_sqlite_write_loss_and_unclean_restart_remain_invalid(tmp_path, monkeypatch):
    path = tmp_path / "box"
    box = outbox(path)

    def fail(_event):
        raise sqlite3.OperationalError("injected disk full")

    monkeypatch.setattr(box, "_insert", fail)
    with pytest.raises(ValueError, match="storage"):
        box.append(event())
    assert not box.health().valid
    box.close()
    reopened = outbox(path)
    assert not reopened.health().valid
    assert reopened.peek()[-1].event.wire()["kind"] == "gap"
    reopened.close()


def test_remote_conflict_quarantines_exact_prefix_without_deleting_evidence(tmp_path):
    path = tmp_path / "box"
    box = outbox(path)
    box.append(event())
    rows = box.peek()
    box.quarantine(rows, "remote_conflict")
    assert box.health().valid and box.health().blocked
    assert box.peek() == ()
    box.close()

    box = outbox(path)
    assert box.health().quarantined == 1
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT payload FROM evidence_outbox WHERE event_id=?", (rows[0].event.event_id,)
        ).fetchone()[0]
    box.close()


def test_batches_respect_receiver_default_one_megabyte_limit(tmp_path):
    box = outbox(tmp_path / "box")
    levels = [[str(n / 1001), "1" * 250] for n in range(1, 1001)]
    for n in range(4):
        box.append(event(n, asks=levels))
    batch = box.peek()
    body = json.dumps(
        {"events": [row.event.wire() for row in batch]}, separators=(",", ":")
    ).encode()
    assert len(body) <= 1_000_000
    assert 0 < len(batch) < 4
    box.close()


def test_single_event_cannot_exceed_a_deliverable_batch():
    with pytest.raises(ValueError):
        event(asks=[["0.5", "1" * 1_000_000]])


def test_direct_outbox_refuses_in_memory_sqlite():
    with pytest.raises(ValueError, match="durable"):
        outbox(":memory:")


def test_remote_quarantine_does_not_invent_a_local_loss_gap(tmp_path, monkeypatch):
    path = tmp_path / "box"
    box = outbox(path)
    box.append(event())
    rows = box.peek()

    def fail(_event):
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(box, "_insert", fail)
    box.quarantine(rows, "remote_conflict")
    box.close()
    reopened = outbox(path)
    assert reopened.health().blocked and reopened.health().valid
    assert reopened.health().pending == 0 and reopened.health().quarantined == 1
    assert reopened.peek() == ()
    reopened.close()


def test_interrupted_append_while_delivery_blocked_invalidates_on_restart(tmp_path):
    path = tmp_path / "box"
    box = outbox(path)
    box.append(event())
    box.quarantine(box.peek(), "remote_rejected")
    box._latch(True)  # Simulate process death between pre-write marker and commit.
    box.close()
    reopened = outbox(path)
    assert reopened.health().blocked and not reopened.health().valid
    assert reopened.health().reason == "storage_interrupted"
    with pytest.raises(ValueError, match="continuity_invalid"):
        reopened.append(event(2))
    reopened.close()


def test_append_work_does_not_scan_acknowledged_history(tmp_path):
    """A long-lived producer pays for its bounded queue, not every historical identity."""
    box = outbox(tmp_path / "box")
    box._db.execute("BEGIN IMMEDIATE")
    box._db.executemany(
        "INSERT INTO evidence_outbox(event_id,digest,payload,status) VALUES(?,?,NULL,'acked')",
        [(f"old-{n}", "retained-digest") for n in range(10_000)],
    )
    box._db.execute("COMMIT")
    steps = 0

    def progress():
        nonlocal steps
        steps += 100
        return 0

    box._db.set_progress_handler(progress, 100)
    box.append(event())
    box._db.set_progress_handler(None, 0)
    assert steps < 10_000
    assert box.health().valid
    box.close()


def test_failure_during_quarantine_transaction_never_reenables_delivery(tmp_path):
    path = tmp_path / "box"
    box = outbox(path)
    box.append(event())
    rows = box.peek()
    connection = box._db

    class FailingConnection:
        def __getattr__(self, name):
            return getattr(connection, name)

        def execute(self, statement, *args):
            if "SET blocked=1" in statement:
                raise sqlite3.OperationalError("injected failed quarantine update")
            return connection.execute(statement, *args)

    box._db = FailingConnection()
    with pytest.raises(ValueError):
        box.quarantine(rows, "remote_conflict")
    assert box.health().blocked
    box.close()
    reopened = outbox(path)
    assert reopened.health().blocked and not reopened.health().valid
    assert reopened.peek() == ()
    reopened.close()


def test_manually_corrupted_event_fails_safe_and_records_loss(tmp_path):
    from domain.evidence import EvidenceEvent

    box = outbox(tmp_path / "box")
    with pytest.raises(ValueError, match="evidence_invalid_payload"):
        box.append(EvidenceEvent(b"not json", "corrupt"))
    assert not box.health().valid
    assert box.peek()[0].event.wire()["kind"] == "gap"
    box.close()


@pytest.mark.parametrize("operation", ["acknowledge", "quarantine"])
def test_transaction_begin_failure_retains_batch_and_uses_safe_error(tmp_path, operation):
    box = outbox(tmp_path / "box")
    box.append(event())
    rows = box.peek()
    connection = box._db

    class FailingConnection:
        def __getattr__(self, name):
            return getattr(connection, name)

        def execute(self, statement, *args):
            if statement == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("injected busy transaction")
            return connection.execute(statement, *args)

    box._db = FailingConnection()
    with pytest.raises(ValueError, match="storage"):
        if operation == "acknowledge":
            box.acknowledge(rows, ack(rows))
        else:
            box.quarantine(rows, "remote_conflict")
    box._db = connection
    assert connection.execute(
        "SELECT payload FROM evidence_outbox WHERE seq=?", (rows[0].sequence,)
    ).fetchone()[0]
    if operation == "quarantine":
        assert box.health().blocked
    else:
        assert box.peek() == rows
    box.close()
