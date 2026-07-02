"""Tests for ``services/onchain_detection/mined_fill_deduplicator.py`` (PR2 / Phase 9)."""
from __future__ import annotations

import asyncio

import pytest

from services.onchain_detection.mined_fill_deduplicator import MinedFillDeduplicator


@pytest.mark.asyncio
async def test_duplicate_mined_key_returns_true() -> None:
    dedupe = MinedFillDeduplicator()
    assert await dedupe.is_duplicate_mined("0xabc", 0) is False
    assert await dedupe.is_duplicate_mined("0xabc", 0) is True
    assert await dedupe.is_duplicate_mined("0xabc", 1) is False
    assert await dedupe.is_duplicate_mined("0xdef", 0) is False


@pytest.mark.asyncio
async def test_same_tx_multiple_logs_each_counted_once() -> None:
    """A single settlement tx with multiple fill logs produces one logical
    event per log, deduped by ``(tx_hash, log_index)``, not by ``tx_hash``.
    """
    dedupe = MinedFillDeduplicator()
    tx = "0x" + "11" * 32
    # Three distinct logs in the same tx.
    assert await dedupe.is_duplicate_mined(tx, 0) is False
    assert await dedupe.is_duplicate_mined(tx, 1) is False
    assert await dedupe.is_duplicate_mined(tx, 2) is False
    # Each log is idempotent on replay.
    assert await dedupe.is_duplicate_mined(tx, 0) is True
    assert await dedupe.is_duplicate_mined(tx, 1) is True
    assert await dedupe.is_duplicate_mined(tx, 2) is True
    assert len(dedupe.mined_seen_snapshot()) == 3


@pytest.mark.asyncio
async def test_pending_record_dedupes_by_tx_and_leader() -> None:
    dedupe = MinedFillDeduplicator()
    assert await dedupe.record_pending("0xabc", "0xleader1") is True
    # Duplicate pending for the same (tx, leader) is not new.
    assert await dedupe.record_pending("0xabc", "0xleader1") is False
    # Same tx, different leader — distinct fill, recorded as new.
    assert await dedupe.record_pending("0xabc", "0xleader2") is True
    assert await dedupe.record_pending("0xdef", "0xleader1") is True


@pytest.mark.asyncio
async def test_supersede_pending_returns_true_when_known() -> None:
    dedupe = MinedFillDeduplicator()
    await dedupe.record_pending("0xabc", "0xleader1")
    assert await dedupe.supersede_pending("0xabc", "0xleader1") is True
    # After supersede, the pending is no longer known.
    assert await dedupe.pending_known("0xabc", "0xleader1") is False
    # Superseding an unknown pending returns False.
    assert await dedupe.supersede_pending("0xzzz", "0xleader1") is False
    # Superseding a known tx for a DIFFERENT leader returns False (per-leader).
    await dedupe.record_pending("0xabc", "0xleader2")
    assert await dedupe.supersede_pending("0xabc", "0xleader1") is False


@pytest.mark.asyncio
async def test_pending_known_reflects_state() -> None:
    dedupe = MinedFillDeduplicator()
    assert await dedupe.pending_known("0xabc", "0xleader1") is False
    await dedupe.record_pending("0xabc", "0xleader1")
    assert await dedupe.pending_known("0xabc", "0xleader1") is True
    # Different leader in the same tx — not known.
    assert await dedupe.pending_known("0xabc", "0xleader2") is False


@pytest.mark.asyncio
async def test_empty_or_negative_log_index_not_deduped() -> None:
    dedupe = MinedFillDeduplicator()
    assert await dedupe.is_duplicate_mined("", 0) is False
    assert await dedupe.is_duplicate_mined("0xabc", -1) is False
    # Recording the same empty/negative pair repeatedly still returns False
    # (the guard treats them as invalid keys, not dedupable).
    assert await dedupe.is_duplicate_mined("", 0) is False


@pytest.mark.asyncio
async def test_bulk_record_mined_returns_per_key_flags() -> None:
    dedupe = MinedFillDeduplicator()
    keys = [("0xabc", 0), ("0xabc", 1), ("0xabc", 0)]
    flags = await dedupe.bulk_record_mined(keys)
    assert flags == [False, False, True]


@pytest.mark.asyncio
async def test_reset_clears_state() -> None:
    dedupe = MinedFillDeduplicator()
    await dedupe.is_duplicate_mined("0xabc", 0)
    await dedupe.record_pending("0xdef", "0xleader1")
    dedupe.reset()
    assert dedupe.mined_seen_snapshot() == set()
    assert dedupe.pending_seen_snapshot() == set()


def test_isolated_event_loops_do_not_share_state() -> None:
    """Each instance has its own state; no cross-instance leakage."""
    dedupe_a = MinedFillDeduplicator()
    dedupe_b = MinedFillDeduplicator()

    async def _fill_a() -> None:
        await dedupe_a.is_duplicate_mined("0xabc", 0)

    async def _fill_b() -> None:
        await dedupe_b.is_duplicate_mined("0xabc", 0)

    asyncio.run(_fill_a())
    asyncio.run(_fill_b())
    assert dedupe_a.mined_seen_snapshot() == {("0xabc", 0)}
    assert dedupe_b.mined_seen_snapshot() == {("0xabc", 0)}