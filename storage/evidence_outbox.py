"""Bounded Finland-local evidence spool with sticky, crash-visible loss health.

Only confirmed payloads are removed. Their bounded identity tombstones remain so
restarts cannot silently reinterpret an old ID. A reserved gap sits outside the
ordinary queue quota; a failed local write also leaves a preallocated fsync latch.
"""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
import sqlite3
import threading

from domain.evidence import (
    PROTOCOL_VERSION,
    EvidenceError,
    EvidenceEvent,
    EvidenceHealth,
    OutboxItem,
)
from integrations.pmrecon.codec import PmreconEvidenceCodec
from interfaces.evidence import EvidenceClock


def _page_cap_pragma(page_count: int) -> str:
    """Render the SQLite page-cap PRAGMA for one int-validated page count.

    PRAGMA statements cannot bind parameters (sqlite3 rejects '?' in PRAGMA),
    so the count is rendered through an integer-only %d format. Every caller
    derives the count from int-validated capacities, so no text input can
    alter the statement structure.
    """
    return "PRAGMA max_page_count=%d" % page_count


class SqliteEvidenceOutbox:
    """One process owns a bounded durable FIFO; no method performs remote I/O."""

    def __init__(
        self,
        path: Path,
        *,
        clock: EvidenceClock,
        max_events: int = 10_000,
        max_bytes: int = 64 * 1024 * 1024,
        max_history: int = 1_000_000,
    ) -> None:
        """Open local storage and recover interrupted writes as permanent evidence loss."""
        if str(path) == ":memory:" or not str(path).strip() or "\x00" in str(path):
            raise EvidenceError("evidence_durable_path_required")
        if any(type(n) is not int or n <= 0 for n in (max_events, max_bytes, max_history)):
            raise EvidenceError("evidence_invalid_capacity")
        self._clock = clock
        self._max_events, self._max_bytes, self._max_history = max_events, max_bytes, max_history
        self._lock = threading.RLock()
        self._failure: str | None = None
        self._blocked_failure = False
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._latch_fd = os.open(str(path) + ".health", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self._latch_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if os.fstat(self._latch_fd).st_size == 0:
                os.pwrite(self._latch_fd, b"0" + bytes(4095), 0)
                os.fsync(self._latch_fd)
                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            marker = os.pread(self._latch_fd, 1, 0)
            interrupted = marker not in (b"0", b"3")
            self._blocked_failure = marker in (b"2", b"3", b"4")
            self._db = sqlite3.connect(
                path, isolation_level=None, timeout=0, check_same_thread=False
            )
            os.chmod(path, 0o600)
            self._db.row_factory = sqlite3.Row
            # DELETE avoids unbounded WAL growth when the publisher is stalled.
            self._db.execute("PRAGMA journal_mode=DELETE")
            self._db.execute("PRAGMA synchronous=FULL")
            pages = (max_bytes * 2 + max_history * 1024 + 131072) // 4096 + 1
            # The int-only PRAGMA renderer documents why this cannot be a
            # bound parameter; see _page_cap_pragma.
            self._db.execute(_page_cap_pragma(pages))
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS evidence_outbox (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
                    digest TEXT NOT NULL, payload BLOB, status TEXT NOT NULL,
                    remote_seq INTEGER);
                CREATE INDEX IF NOT EXISTS evidence_pending ON evidence_outbox(status,seq);
                CREATE TABLE IF NOT EXISTS evidence_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    valid INTEGER NOT NULL, blocked INTEGER NOT NULL, reason TEXT);
                INSERT OR IGNORE INTO evidence_health VALUES(1,1,0,NULL);
            """)
            if interrupted:
                self.invalidate("storage_interrupted")
        except (OSError, sqlite3.Error):
            if hasattr(self, "_db"):
                self._db.close()
            os.close(self._latch_fd)
            raise EvidenceError("evidence_storage_unavailable") from None

    def _latch(self, failed: bool) -> None:
        """Commit a preallocated failure marker even when normal queue space is gone."""
        # Keep delivery quarantine distinct from possible local evidence loss.
        # Legacy 2 is invalid+blocked; 3 is healthy+blocked; 4 marks an
        # interrupted local append while delivery remains blocked.
        if self._blocked_failure:
            marker = b"2" if self._failure else (b"4" if failed else b"3")
        else:
            marker = b"1" if failed or self._failure else b"0"
        os.pwrite(self._latch_fd, marker, 0)
        os.fsync(self._latch_fd)

    def _insert(self, event: EvidenceEvent) -> int:
        """Insert one already-canonical event inside the caller's transaction."""
        row = self._db.execute(
            "INSERT INTO evidence_outbox(event_id,digest,payload,status) VALUES(?,?,?,'pending')",
            (event.event_id, event.digest, event.canonical_bytes),
        )
        assert row.lastrowid is not None
        return row.lastrowid

    def append(self, event: EvidenceEvent) -> int:
        """Persist before returning; duplicates stay idempotent after acknowledgement."""
        # Refuse manually constructed objects that bypass the boundary codec.
        try:
            if PmreconEvidenceCodec.encode(event.wire()) != event:
                raise EvidenceError("evidence_invalid_payload")
        except (ValueError, TypeError, RecursionError):
            self.invalidate("invalid_payload")
            raise EvidenceError("evidence_invalid_payload") from None
        with self._lock:
            try:
                old = self._db.execute(
                    "SELECT seq,digest FROM evidence_outbox WHERE event_id=?", (event.event_id,)
                ).fetchone()
                if old:
                    if old["digest"] != event.digest:
                        self.invalidate("identity_conflict")
                        raise EvidenceError("evidence_identity_conflict")
                    return old["seq"]
                if not self.health().valid:
                    raise EvidenceError("evidence_continuity_invalid")
                self._latch(True)
                self._db.execute("BEGIN IMMEDIATE")
                count, size = self._db.execute(
                    "SELECT COUNT(*),COALESCE(SUM(length(payload)),0) FROM evidence_outbox "
                    "WHERE status IN ('pending','quarantined')"
                ).fetchone()
                # Rows are never deleted: the indexed local sequence is a safe
                # upper bound on retained identities without rescanning history.
                history = self._db.execute(
                    "SELECT COALESCE(MAX(seq),0) FROM evidence_outbox"
                ).fetchone()[0]
                if (
                    (count or 0) >= self._max_events
                    or size + len(event.canonical_bytes) > self._max_bytes
                    or history >= self._max_history
                ):
                    self._db.execute("ROLLBACK")
                    self.invalidate("capacity")
                    raise EvidenceError("evidence_capacity_exhausted")
                sequence = self._insert(event)
                self._db.execute("COMMIT")
                self._latch(False)
                return sequence
            except (sqlite3.Error, OSError):
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                self.invalidate("storage_failure")
                raise EvidenceError("evidence_storage_failure") from None

    def invalidate(self, reason: str) -> None:
        """Latch loss, retain original queue and persist one reserved v1 overflow gap."""
        allowed = {
            "capacity",
            "identity_conflict",
            "storage_failure",
            "storage_interrupted",
            "invalid_payload",
            "missing_native_clock",
            "remote_conflict",
            "remote_rejected",
        }
        safe_reason = reason if reason in allowed else "invalid_payload"
        with self._lock:
            self._failure = self._failure or safe_reason
            try:
                self._latch(True)
                self._db.execute("BEGIN IMMEDIATE")
                state = self._db.execute(
                    "SELECT valid FROM evidence_health WHERE singleton=1"
                ).fetchone()
                if state["valid"]:
                    self._db.execute(
                        "UPDATE evidence_health SET valid=0,reason=? WHERE singleton=1",
                        (safe_reason,),
                    )
                    gap = PmreconEvidenceCodec.encode(
                        {
                            "kind": "gap",
                            "provider": "bot",
                            "reason": "overflow",
                            "received_ns": str(self._clock.time_ns()),
                            "clock_id": self._clock.clock_id,
                        }
                    )
                    self._insert(gap)
                self._db.execute("COMMIT")
            except (sqlite3.Error, OSError, ValueError):
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                # The pre-write latch survives a full SQLite volume. If even that
                # medium fails, runtime health remains invalid and must stop capture.

    def health(self) -> EvidenceHealth:
        """Return continuity truth, never equating a surviving process with valid data."""
        with self._lock:
            try:
                state = self._db.execute(
                    "SELECT * FROM evidence_health WHERE singleton=1"
                ).fetchone()
                row = self._db.execute(
                    "SELECT SUM(status='pending'),COALESCE(SUM(length(payload)),0),"
                    "SUM(status='quarantined') FROM evidence_outbox "
                    "WHERE status IN ('pending','quarantined')"
                ).fetchone()
                return EvidenceHealth(
                    bool(state["valid"]) and self._failure is None,
                    bool(state["blocked"]) or self._blocked_failure,
                    state["reason"] or self._failure,
                    row[0] or 0,
                    row[1],
                    row[2] or 0,
                )
            except sqlite3.Error:
                return EvidenceHealth(False, True, "storage_failure", 0, 0, 0)

    def peek(self, limit: int = 100) -> tuple[OutboxItem, ...]:
        """Return at most 100 events and a body below the receiver's default 1 MiB limit."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise EvidenceError("evidence_invalid_batch_limit")
        with self._lock:
            if self.health().blocked:
                return ()
            rows = self._db.execute(
                "SELECT seq,event_id,payload FROM evidence_outbox WHERE status='pending' "
                "ORDER BY seq LIMIT ?",
                (limit,),
            )
            result: list[OutboxItem] = []
            size = 16
            for row in rows:
                size += len(row["payload"]) + len(row["event_id"].encode()) * 6 + 32
                if size > 1_000_000:
                    break
                result.append(
                    OutboxItem(row["seq"], EvidenceEvent(bytes(row["payload"]), row["event_id"]))
                )
            return tuple(result)

    def acknowledge(self, batch: tuple[OutboxItem, ...], response: object) -> None:
        """Delete only payloads of a fully matched local prefix; keep bounded ID tombstones."""
        if not isinstance(response, dict) or set(response) != {"protocol_version", "results"}:
            raise EvidenceError("evidence_invalid_ack")
        results = response["results"]
        if (
            response["protocol_version"] != PROTOCOL_VERSION
            or not isinstance(results, list)
            or not batch
            or len(results) != len(batch)
        ):
            raise EvidenceError("evidence_invalid_ack")
        sequences: set[int] = set()
        for row, item in zip(results, batch, strict=True):
            if (
                not isinstance(row, dict)
                or set(row) != {"seq", "event_id", "duplicate"}
                or row["event_id"] != item.event.event_id
                or type(row["seq"]) is not int
                or row["seq"] <= 0
                or row["seq"] in sequences
                or type(row["duplicate"]) is not bool
            ):
                raise EvidenceError("evidence_invalid_ack")
            sequences.add(row["seq"])
        with self._lock:
            try:
                if self.peek(len(batch)) != batch:
                    raise EvidenceError("evidence_nonprefix_ack")
                self._db.execute("BEGIN IMMEDIATE")
                for item, row in zip(batch, results, strict=True):
                    self._db.execute(
                        "UPDATE evidence_outbox SET status='acked',payload=NULL,remote_seq=? WHERE seq=?",
                        (row["seq"], item.sequence),
                    )
                self._db.execute("COMMIT")
            except sqlite3.Error:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise EvidenceError("evidence_ack_storage_failure") from None

    def quarantine(self, batch: tuple[OutboxItem, ...], reason: str) -> None:
        """Block on a rejected prefix; never discard a conflict to advance later evidence."""
        with self._lock:
            try:
                if not batch or self.peek(len(batch)) != batch:
                    raise EvidenceError("evidence_nonprefix_quarantine")
                # Persist a distinct delivery-stop latch before any fallible SQL.
                # Restart must not send this rejected prefix if quarantine rolls back.
                self._blocked_failure = True
                self._latch(False)
                self._db.execute("BEGIN IMMEDIATE")
                safe_reason = (
                    "remote_conflict" if reason == "remote_conflict" else "remote_rejected"
                )
                self._db.execute(
                    "UPDATE evidence_health SET blocked=1,reason=COALESCE(reason,?) WHERE singleton=1",
                    (safe_reason,),
                )
                self._db.executemany(
                    "UPDATE evidence_outbox SET status='quarantined' WHERE seq=?",
                    [(item.sequence,) for item in batch],
                )
                self._db.execute("COMMIT")
            except (sqlite3.Error, OSError):
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                self._blocked_failure = True
                self.invalidate("storage_failure")
                raise EvidenceError("evidence_quarantine_storage_failure") from None

    def close(self) -> None:
        """Release ownership; intentionally never clear an invalid continuity latch."""
        with self._lock:
            self._db.close()
            os.close(self._latch_fd)
