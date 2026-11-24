"""Bounded durable source identity, reorg tombstones, and trader observation state.

This ledger suppresses repeat source dispatch; it does not replace the existing
per-follower/task reservation and order idempotency transactions. Recording source
admission before dispatch chooses visible under-copy over a duplicate submission
after a crash. Recovery events are always evidence-only.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Sequence
from dataclasses import asdict
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from pmcopy.domain.grouped_detection import (
    NativeOwnerFill,
    TraderDetectionPolicy,
    TraderDetectionState,
)
from pmcopy.interfaces.grouped_detection import ObservationDisposition


class DetectionEvidenceConflict(ValueError):
    """Existing source identity was replayed with conflicting immutable economics."""


class DetectionStateCapacity(RuntimeError):
    """Source evidence capacity is exhausted; no existing evidence was discarded."""


class SqliteDetectionState:
    """Own finite source admission state on the Finnish filesystem."""

    def __init__(self, path: Path, *, max_inclusions: int = 1_000_000) -> None:
        """Open durable local state without network access.

        Args:
            path: Dedicated local database, separate from operator account files.
            max_inclusions: Hard evidence capacity; exhaustion requires operator recovery.
        """
        if type(max_inclusions) is not int or max_inclusions < 1:
            raise ValueError("source state capacity must be positive")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, timeout=5, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.executescript("""
            CREATE TABLE IF NOT EXISTS native_actions (
                action_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS native_inclusions (
                inclusion_id TEXT PRIMARY KEY, action_id TEXT NOT NULL,
                block_number INTEGER NOT NULL, block_hash TEXT NOT NULL,
                removed INTEGER NOT NULL, recovered INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trader_source_state (
                wallet TEXT PRIMARY KEY, desired_mode TEXT NOT NULL,
                effective_mode TEXT NOT NULL, provider TEXT NOT NULL,
                on_budget_exhausted TEXT NOT NULL, enabled INTEGER NOT NULL,
                degraded_reason TEXT, last_observation_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS source_checkpoint (
                source TEXT PRIMARY KEY, block_number INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS native_transaction_inclusions (
                transaction_hash TEXT NOT NULL, block_number INTEGER NOT NULL,
                block_hash TEXT NOT NULL, removed INTEGER NOT NULL,
                PRIMARY KEY(transaction_hash,block_hash)
            );
        """)
        self._capacity = max_inclusions
        self._lock = threading.RLock()

    def configure(self, policies: Sequence[TraderDetectionPolicy]) -> None:
        """Persist desired state and require this process to acknowledge source coverage."""
        if len({policy.wallet for policy in policies}) != len(policies):
            raise ValueError("duplicate trader policy")
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE trader_source_state SET effective_mode='paused', degraded_reason='not_in_roster'"
            )
            for policy in policies:
                self._connection.execute(
                    """INSERT INTO trader_source_state VALUES (?,?,'paused',?,?,?,?,NULL)
                       ON CONFLICT(wallet) DO UPDATE SET
                       desired_mode=excluded.desired_mode, effective_mode='paused',
                       provider=excluded.provider, on_budget_exhausted=excluded.on_budget_exhausted,
                       enabled=excluded.enabled, degraded_reason=excluded.degraded_reason""",
                    (
                        policy.wallet,
                        policy.mode,
                        policy.provider,
                        policy.on_budget_exhausted,
                        int(policy.enabled),
                        "awaiting_subscription" if policy.enabled else "disabled",
                    ),
                )

    def observe(self, fill: NativeOwnerFill, *, recovered: bool) -> ObservationDisposition:
        """Persist one native inclusion atomically, retaining removed-first evidence.

        Args:
            fill: Strictly decoded native-owner evidence.
            recovered: Whether bounded historical/reconnect recovery supplied it.

        Returns:
            Only ``new`` permits fresh entry dispatch; every other disposition is evidence.

        Raises:
            DetectionEvidenceConflict: A stable action acquired different financial contents.
            DetectionStateCapacity: Storage cannot retain a new inclusion without dropping evidence.
        """
        if type(recovered) is not bool:
            raise ValueError("recovered must be a boolean")
        fingerprint = self._fingerprint(fill)
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            prior_transaction = self._connection.execute(
                """SELECT 1 FROM native_transaction_inclusions
                   WHERE transaction_hash=? AND (block_hash!=? OR removed=1) LIMIT 1""",
                (fill.transaction_hash, fill.block_hash),
            ).fetchone()
            self._connection.execute(
                """INSERT INTO native_transaction_inclusions VALUES (?,?,?,?)
                   ON CONFLICT(transaction_hash,block_hash) DO UPDATE
                   SET removed=max(removed,excluded.removed)""",
                (fill.transaction_hash, fill.block_number, fill.block_hash, int(fill.removed)),
            )
            previous_action = self._connection.execute(
                "SELECT fingerprint FROM native_actions WHERE action_id=?",
                (fill.action_id,),
            ).fetchone()
            if previous_action is not None and previous_action["fingerprint"] != fingerprint:
                raise DetectionEvidenceConflict("native source evidence identity conflict")
            previous = self._connection.execute(
                "SELECT removed FROM native_inclusions WHERE inclusion_id=?",
                (fill.inclusion_id,),
            ).fetchone()
            if previous is not None:
                if fill.removed:
                    self._connection.execute(
                        "UPDATE native_inclusions SET removed=1 WHERE inclusion_id=?",
                        (fill.inclusion_id,),
                    )
                return "removed" if fill.removed or previous["removed"] else "duplicate"
            count = self._connection.execute("SELECT count(*) FROM native_inclusions").fetchone()[0]
            if count >= self._capacity:
                raise DetectionStateCapacity("source evidence capacity exhausted")
            self._connection.execute(
                "INSERT OR IGNORE INTO native_actions VALUES (?,?)",
                (fill.action_id, fingerprint),
            )
            self._connection.execute(
                "INSERT INTO native_inclusions VALUES (?,?,?,?,?,?)",
                (
                    fill.inclusion_id,
                    fill.action_id,
                    fill.block_number,
                    fill.block_hash,
                    int(fill.removed),
                    int(recovered),
                ),
            )
            if fill.removed:
                return "removed"
            if previous_action is not None or prior_transaction is not None:
                return "reincluded"
            return "recovered" if recovered else "new"

    def set_effective(self, wallets: Sequence[str], mode: str, reason: str | None) -> None:
        """Persist actual mode without changing saved configuration or applying new settings."""
        if mode not in ("mined", "pending", "paused"):
            raise ValueError("unsupported effective mode")
        if reason is not None and (
            not reason or len(reason) > 128 or not reason.replace("_", "").isalnum()
        ):
            raise ValueError("invalid source degradation reason")
        with self._lock, self._connection:
            self._connection.executemany(
                "UPDATE trader_source_state SET effective_mode=?, degraded_reason=? WHERE wallet=?",
                ((mode, reason, wallet.lower()) for wallet in wallets),
            )

    def await_restart(self, policies: Sequence[TraderDetectionPolicy]) -> None:
        """Expose newly discovered owners without changing any acknowledged group."""
        with self._lock, self._connection:
            self._connection.executemany(
                """INSERT INTO trader_source_state VALUES (?,?,'paused',?,?,?,'awaiting_restart',NULL)
                   ON CONFLICT(wallet) DO UPDATE SET effective_mode='paused', degraded_reason='awaiting_restart'""",
                (
                    (
                        policy.wallet,
                        policy.mode,
                        policy.provider,
                        policy.on_budget_exhausted,
                        int(policy.enabled),
                    )
                    for policy in policies
                ),
            )

    def record_observation(self, wallet: str, received_ms: int) -> None:
        """Store original valid-evidence receipt time without inventing unknown observations."""
        if type(received_ms) is not int or received_ms < 0:
            raise ValueError("observation timestamp must be a nonnegative integer")
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE trader_source_state SET last_observation_ms=? WHERE wallet=?",
                (received_ms, wallet.lower()),
            )

    def states(self) -> tuple[TraderDetectionState, ...]:
        """Return durable desired/effective state without provider secrets or replay claims."""
        with self._lock:
            return tuple(
                TraderDetectionState(
                    wallet=row["wallet"],
                    desired_mode=row["desired_mode"],
                    effective_mode=row["effective_mode"],
                    provider=row["provider"],
                    on_budget_exhausted=row["on_budget_exhausted"],
                    enabled=bool(row["enabled"]),
                    degraded_reason=row["degraded_reason"],
                    last_observation_ms=row["last_observation_ms"],
                )
                for row in self._connection.execute(
                    "SELECT * FROM trader_source_state ORDER BY wallet"
                )
            )

    def checkpoint(self, source: str) -> int | None:
        """Return the last observed or fully recovered height for this fixed roster."""
        with self._lock:
            row = self._connection.execute(
                "SELECT block_number FROM source_checkpoint WHERE source=?", (source,)
            ).fetchone()
            return int(row[0]) if row is not None else None

    def advance_checkpoint(self, source: str, block_number: int) -> None:
        """Advance monotonically; recovery always includes a separate overlap window."""
        if not source or type(block_number) is not int or block_number < 0:
            raise ValueError("invalid source checkpoint")
        with self._lock, self._connection:
            self._connection.execute(
                """INSERT INTO source_checkpoint VALUES (?,?) ON CONFLICT(source)
                   DO UPDATE SET block_number=max(block_number,excluded.block_number)""",
                (source, block_number),
            )

    def included_blocks(self, start: int, end: int) -> tuple[tuple[int, str], ...]:
        """Return retained included block identities for bounded reorg verification."""
        with self._lock:
            return tuple(
                (int(row[0]), str(row[1]))
                for row in self._connection.execute(
                    """SELECT DISTINCT block_number,block_hash FROM native_inclusions
                       WHERE block_number BETWEEN ? AND ? AND removed=0 ORDER BY block_number""",
                    (start, end),
                )
            )

    def invalidate_block(self, number: int, block_hash: str) -> int:
        """Tombstone every replaced inclusion without deleting its prior evidence."""
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE native_transaction_inclusions SET removed=1 WHERE block_number=? AND block_hash=?",
                (number, block_hash),
            )
            result = self._connection.execute(
                """UPDATE native_inclusions SET removed=1
                   WHERE block_number=? AND block_hash=? AND removed=0""",
                (number, block_hash),
            )
            return result.rowcount

    def close(self) -> None:
        """Close this ledger's local connection."""
        with self._lock:
            self._connection.close()

    @staticmethod
    def _fingerprint(fill: NativeOwnerFill) -> str:
        data = asdict(fill)
        for field in ("block_number", "block_hash", "removed"):
            data.pop(field)
        wire = {
            key: format(value, "f") if isinstance(value, Decimal) else value
            for key, value in data.items()
        }
        return sha256(json.dumps(wire, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
