"""Durable source tombstones and desired/effective policy visibility."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from domain.grouped_detection import NativeOwnerFill, TraderDetectionPolicy


class FillFixture:
    """Construct immutable native evidence without a provider connection."""

    @staticmethod
    def fill() -> NativeOwnerFill:
        """Return one real-shaped native order owner fill."""
        return NativeOwnerFill(
            owner="0x" + "11" * 20,
            counterparty="0x" + "22" * 20,
            exchange="0x" + "33" * 20,
            transaction_hash="0x" + "44" * 32,
            log_index=1,
            block_number=100,
            block_hash="0x" + "55" * 32,
            order_hash="0x" + "66" * 32,
            token_id="123",
            side="BUY",
            shares=Decimal("100"),
            notional=Decimal("48"),
            price=Decimal("0.48"),
            fee=Decimal("0.001"),
            native_taker=False,
            removed=False,
        )


def test_source_consumption_survives_restart_and_does_not_collapse_sibling_logs(
    tmp_path: Path,
) -> None:
    """One durable action never dispatches twice after a process restart."""
    from storage.detection_state import SqliteDetectionState

    path = tmp_path / "state.sqlite3"
    state = SqliteDetectionState(path)
    fill = FillFixture.fill()
    assert state.observe(fill, recovered=False) == "new"
    state.close()
    state = SqliteDetectionState(path)
    assert state.observe(fill, recovered=False) == "duplicate"
    assert state.observe(replace(fill, log_index=2), recovered=False) == "new"
    state.close()


def test_removed_first_and_reinclusion_never_create_fresh_entries(tmp_path: Path) -> None:
    """A removal tombstone survives restart; re-inclusion is evidence, not recopy."""
    from storage.detection_state import SqliteDetectionState

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    fill = FillFixture.fill()
    assert state.observe(replace(fill, removed=True), recovered=False) == "removed"
    assert state.observe(fill, recovered=False) == "removed"
    assert (
        state.observe(replace(fill, block_hash="0x" + "77" * 32), recovered=False) == "reincluded"
    )
    state.close()


def test_recovery_evidence_cannot_later_be_dispatched_as_fresh(tmp_path: Path) -> None:
    """A buffered/live duplicate of recovered old evidence stays non-actionable."""
    from storage.detection_state import SqliteDetectionState

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    fill = FillFixture.fill()
    assert state.observe(fill, recovered=True) == "recovered"
    assert state.observe(fill, recovered=False) == "duplicate"
    state.close()


def test_source_capacity_and_conflicting_evidence_fail_visibly(tmp_path: Path) -> None:
    """No pruning or rewriting of prior financial evidence buys extra capacity."""
    from storage.detection_state import (
        DetectionEvidenceConflict,
        DetectionStateCapacity,
        SqliteDetectionState,
    )

    state = SqliteDetectionState(tmp_path / "state.sqlite3", max_inclusions=1)
    fill = FillFixture.fill()
    assert state.observe(fill, recovered=False) == "new"
    with pytest.raises(DetectionEvidenceConflict):
        state.observe(replace(fill, fee=Decimal("0.002")), recovered=False)
    with pytest.raises(DetectionStateCapacity):
        state.observe(replace(fill, log_index=2), recovered=False)
    assert state.observe(fill, recovered=False) == "duplicate"
    state.close()


def test_desired_effective_and_last_observation_persist_separately(tmp_path: Path) -> None:
    """A saved pending preference never claims pending service without a feed."""
    from storage.detection_state import SqliteDetectionState

    path = tmp_path / "state.sqlite3"
    policy = TraderDetectionPolicy(wallet=FillFixture.fill().owner, mode="pending")
    state = SqliteDetectionState(path)
    state.configure((policy,))
    status = state.states()[0]
    assert status.desired_mode == "pending"
    assert status.effective_mode == "paused"
    assert status.degraded_reason == "awaiting_subscription"
    state.set_effective((policy.wallet,), "mined", "pending_disabled")
    state.record_observation(policy.wallet, 123456)
    state.close()
    state = SqliteDetectionState(path)
    status = state.states()[0]
    assert status.desired_mode == "pending"
    assert status.effective_mode == "mined"
    assert status.degraded_reason == "pending_disabled"
    assert status.last_observation_ms == 123456
    state.configure((policy,))
    assert state.states()[0].effective_mode == "paused"
    assert state.states()[0].last_observation_ms == 123456
    state.close()


def test_checkpoints_and_reorg_invalidation_survive_restart(tmp_path: Path) -> None:
    """Recovery resumes conservatively and retains removed inclusion tombstones."""
    from storage.detection_state import SqliteDetectionState

    path = tmp_path / "state.sqlite3"
    state = SqliteDetectionState(path)
    fill = FillFixture.fill()
    assert state.checkpoint("owners") is None
    state.observe(fill, recovered=False)
    state.advance_checkpoint("owners", 100)
    state.advance_checkpoint("owners", 90)
    assert state.included_blocks(90, 100) == ((100, fill.block_hash),)
    assert state.invalidate_block(100, fill.block_hash) == 1
    assert state.included_blocks(90, 100) == ()
    state.close()
    state = SqliteDetectionState(path)
    assert state.checkpoint("owners") == 100
    assert state.observe(fill, recovered=False) == "removed"
    state.close()


def test_reincluded_transaction_with_changed_log_index_never_recopies(tmp_path: Path) -> None:
    """Transaction reordering changes logIndex; tombstones still suppress fresh entries."""
    from storage.detection_state import SqliteDetectionState

    state = SqliteDetectionState(tmp_path / "state")
    fill = FillFixture.fill()
    state.observe(fill, recovered=False)
    state.observe(replace(fill, removed=True), recovered=False)
    changed = replace(fill, block_hash="0x" + "88" * 32, log_index=9)
    assert state.observe(changed, recovered=False) == "reincluded"
    state.close()
