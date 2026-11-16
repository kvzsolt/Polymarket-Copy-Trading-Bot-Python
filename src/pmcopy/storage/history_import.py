"""Durable finite-job RPC observations, atomic chunk cursor, and local delivery spool."""

from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import threading
from collections.abc import Sequence

from pmcopy.domain.evidence import EvidenceEvent
from pmcopy.domain.history_import import HistoryImportError, HistoryImportPlan
from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.domain.source_time import TransportReceipt


def _page_cap_pragma(page_count: int) -> str:
    """Render the SQLite page-cap PRAGMA for one int-validated page count.

    PRAGMA statements cannot bind parameters (sqlite3 rejects '?' in PRAGMA),
    so the count is rendered through an integer-only %d format. The count is
    derived only from the plan's int-validated bounds, so no text input can
    alter the statement structure.
    """
    return "PRAGMA max_page_count=%d" % page_count


class HistoryImportJournal:
    """One exact reviewed plan owns a local journal; no method contacts a provider."""

    @classmethod
    def open_existing(cls, path: Path) -> "HistoryImportJournal":
        """Open one explicit job for delivery without creating or guessing its plan."""
        if not path.is_file():
            raise HistoryImportError("history_journal_missing")
        try:
            connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
            try:
                row = connection.execute(
                    "SELECT plan FROM history_job WHERE singleton=1"
                ).fetchone()
                if row is None:
                    raise HistoryImportError("invalid_evidence")
                plan = HistoryImportPlan.from_dict(json.loads(row[0]))
            finally:
                connection.close()
        except (sqlite3.Error, ValueError, TypeError) as exc:
            raise HistoryImportError("invalid_evidence") from exc
        return cls(path, plan)

    def __init__(self, path: Path, plan: HistoryImportPlan) -> None:
        """Bind existing state to its exact plan before allowing any resume."""
        if str(path) == ":memory:":
            raise HistoryImportError("invalid_evidence")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._plan = plan
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, isolation_level=None, timeout=2)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=DELETE")
        self._db.execute("PRAGMA synchronous=FULL")
        pages = (plan.max_bytes * 3 + plan.max_requests * 4096 + 2_000_000) // 4096 + 1
        # The int-only PRAGMA renderer documents why this cannot be a bound
        # parameter; see _page_cap_pragma.
        self._db.execute(_page_cap_pragma(pages))
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS history_job (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), digest TEXT NOT NULL,
                plan TEXT NOT NULL, cursor INTEGER NOT NULL, requests INTEGER NOT NULL,
                cu INTEGER NOT NULL, bytes INTEGER NOT NULL, rows INTEGER NOT NULL,
                complete INTEGER NOT NULL DEFAULT 0, coverage_key TEXT, coverage_id TEXT);
            CREATE TABLE IF NOT EXISTS history_responses (
                slot TEXT PRIMARY KEY, payload TEXT NOT NULL, receipt TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS history_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
                digest TEXT NOT NULL, payload BLOB NOT NULL, kind TEXT NOT NULL,
                published INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS history_headers (
                number INTEGER PRIMARY KEY, hash TEXT NOT NULL, event_id TEXT NOT NULL);
        """)
        try:
            self._db.execute("BEGIN IMMEDIATE")
            old = self._db.execute("SELECT digest FROM history_job WHERE singleton=1").fetchone()
            if old is not None and old["digest"] != plan.digest:
                raise HistoryImportError("journal_plan_conflict")
            if old is None:
                self._db.execute(
                    "INSERT INTO history_job(singleton,digest,plan,cursor,requests,cu,bytes,rows) "
                    "VALUES(1,?,?,?,0,0,0,0)",
                    (plan.digest, json.dumps(plan.wire(), sort_keys=True), plan.from_block - 1),
                )
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            self._db.close()
            raise

    def _job(self) -> sqlite3.Row:
        return self._db.execute("SELECT * FROM history_job WHERE singleton=1").fetchone()

    @property
    def cursor(self) -> int:
        """Return the last fully committed chunk end, never a partial read cursor."""
        return int(self._job()["cursor"])

    @property
    def complete(self) -> bool:
        """Return only a committed collector-completion claim."""
        return bool(self._job()["complete"])

    @property
    def request_count(self) -> int:
        """Count every reserved attempt, including failures and interrupted attempts."""
        return int(self._job()["requests"])

    def reserve_request(self, cost: int) -> None:
        """Charge finite job bounds durably before shared provider admission dispatch."""
        if type(cost) is not int or cost < 0:
            raise HistoryImportError("invalid_evidence")
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                job = self._job()
                if job["requests"] >= self._plan.max_requests:
                    raise HistoryImportError("request_limit")
                if job["cu"] + cost > self._plan.max_cu:
                    raise HistoryImportError("budget_exhausted")
                self._db.execute(
                    "UPDATE history_job SET requests=requests+1,cu=cu+? WHERE singleton=1", (cost,)
                )
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise

    def response(self, slot: str) -> ObservedRpcResult | None:
        """Reuse unchanged original body and native receipt instead of backdating a retry."""
        row = self._db.execute(
            "SELECT payload,receipt FROM history_responses WHERE slot=?", (slot,)
        ).fetchone()
        if row is None:
            return None
        return ObservedRpcResult(
            json.loads(row["payload"]), TransportReceipt(**json.loads(row["receipt"]))
        )

    def _room(self, size: int, rows: int, *, diagnostic: bool = False) -> None:
        job = self._job()
        if job["rows"] + rows > self._plan.max_rows + int(diagnostic):
            raise HistoryImportError("row_limit")
        if job["bytes"] + size > self._plan.max_bytes + (262144 if diagnostic else 0):
            raise HistoryImportError("byte_limit")

    def record_response(self, slot: str, observation: ObservedRpcResult) -> None:
        """Commit bounded original RPC evidence even when its chunk later fails validation."""
        if observation.receipt.wall_timestamp_ns is None:
            raise HistoryImportError("invalid_evidence")
        payload = json.dumps(
            observation.payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        receipt = json.dumps(asdict(observation.receipt), sort_keys=True, separators=(",", ":"))
        size = len(payload.encode()) + len(receipt.encode())
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                old = self._db.execute(
                    "SELECT payload,receipt FROM history_responses WHERE slot=?", (slot,)
                ).fetchone()
                if old is not None:
                    if (old["payload"], old["receipt"]) != (payload, receipt):
                        raise HistoryImportError("invalid_evidence")
                else:
                    self._room(size, 1)
                    self._db.execute(
                        "INSERT INTO history_responses VALUES(?,?,?)", (slot, payload, receipt)
                    )
                    self._db.execute(
                        "UPDATE history_job SET rows=rows+1,bytes=bytes+? WHERE singleton=1",
                        (size,),
                    )
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise

    def _append(self, event: EvidenceEvent, *, diagnostic: bool = False) -> None:
        old = self._db.execute(
            "SELECT digest FROM history_events WHERE event_id=?", (event.event_id,)
        ).fetchone()
        if old is not None:
            if old["digest"] != event.digest:
                raise HistoryImportError("invalid_evidence")
            return
        self._room(len(event.canonical_bytes), 1, diagnostic=diagnostic)
        self._db.execute(
            "INSERT INTO history_events(event_id,digest,payload,kind) VALUES(?,?,?,?)",
            (event.event_id, event.digest, event.canonical_bytes, event.wire()["kind"]),
        )
        self._db.execute(
            "UPDATE history_job SET rows=rows+1,bytes=bytes+? WHERE singleton=1",
            (len(event.canonical_bytes),),
        )

    def commit_chunk(
        self,
        expected_cursor: int,
        end: int,
        events: Sequence[EvidenceEvent],
        headers: Sequence[tuple[int, str, str]],
    ) -> None:
        """Commit original canonical events and contiguous pins before advancing cursor."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                if (
                    self.cursor != expected_cursor
                    or not expected_cursor < end <= self._plan.to_block
                ):
                    raise HistoryImportError("invalid_evidence")
                for event in events:
                    self._append(event)
                for number, digest, identity in headers:
                    old = self._db.execute(
                        "SELECT hash FROM history_headers WHERE number=?", (number,)
                    ).fetchone()
                    if old is not None and old["hash"] != digest:
                        raise HistoryImportError("chain_changed")
                    self._db.execute(
                        "INSERT INTO history_headers VALUES(?,?,?) ON CONFLICT(number) "
                        "DO UPDATE SET event_id=excluded.event_id",
                        (number, digest, identity),
                    )
                self._db.execute("UPDATE history_job SET cursor=? WHERE singleton=1", (end,))
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise

    def header(self, number: int) -> tuple[str, str] | None:
        """Read a committed block pin for continuity across chunk boundaries."""
        row = self._db.execute(
            "SELECT hash,event_id FROM history_headers WHERE number=?", (number,)
        ).fetchone()
        return None if row is None else (row["hash"], row["event_id"])

    def references(self) -> dict[str, list[str]]:
        """Enumerate only committed evidence, never partially fetched chunk data."""
        result = {
            "header_event_ids": [
                row[0]
                for row in self._db.execute("SELECT event_id FROM history_headers ORDER BY number")
            ]
        }
        for kind, field in (
            ("receipt", "receipt_event_ids"),
            ("contract_state", "state_event_ids"),
            ("protocol_log", "log_event_ids"),
        ):
            result[field] = [
                row[0]
                for row in self._db.execute(
                    "SELECT event_id FROM history_events WHERE kind=? ORDER BY seq", (kind,)
                )
            ]
        return result

    def coverage(self, status_key: str) -> EvidenceEvent | None:
        """Repeated bounded failures return their original durable completion observation."""
        job = self._job()
        if job["coverage_key"] != status_key:
            return None
        row = self._db.execute(
            "SELECT event_id,digest,payload FROM history_events WHERE event_id=?",
            (job["coverage_id"],),
        ).fetchone()
        return self._event(row)

    def append_coverage(self, event: EvidenceEvent, status_key: str, *, complete: bool) -> None:
        """Retain one bounded diagnostic allowance if regular storage reaches its limit."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._append(event, diagnostic=not complete)
                self._db.execute(
                    "UPDATE history_job SET complete=?,coverage_key=?,coverage_id=? WHERE singleton=1",
                    (int(complete), status_key, event.event_id),
                )
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise

    @staticmethod
    def _event(row: sqlite3.Row) -> EvidenceEvent:
        event = EvidenceEvent(bytes(row["payload"]), row["event_id"])
        if event.digest != row["digest"]:
            raise HistoryImportError("invalid_evidence")
        return event

    def pending(self, limit: int = 100) -> tuple[EvidenceEvent, ...]:
        """Read a bounded immutable local prefix for independent durable append."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise HistoryImportError("invalid_evidence")
        rows = self._db.execute(
            "SELECT event_id,digest,payload FROM history_events WHERE published=0 ORDER BY seq LIMIT ?",
            (limit,),
        )
        return tuple(self._event(row) for row in rows)

    def mark_published(self, identity: str) -> None:
        """Mark only after the existing local evidence publisher confirms durability."""
        self._db.execute("UPDATE history_events SET published=1 WHERE event_id=?", (identity,))

    def close(self) -> None:
        """Close local ownership without deleting raw observations or checkpoints."""
        self._db.close()
