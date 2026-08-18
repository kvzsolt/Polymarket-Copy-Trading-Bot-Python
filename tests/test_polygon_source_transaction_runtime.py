"""Tests for Polygon source-transaction runtime lifecycle composition."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from domain.source_transactions import EvmHead
from orchestration.polygon_source_transaction_runtime import (
    PolygonSourceTransactionRuntime,
)


class FakeConnectionManager:
    """Capture session lifecycle calls."""

    def __init__(self) -> None:
        self.starts = 0
        self.stops = 0

    async def start(self) -> None:
        """Record one start."""

        self.starts += 1

    async def stop(self) -> None:
        """Record one stop."""

        self.stops += 1


class FakeSourcePipeline:
    """Capture watched-wallet snapshot changes."""

    def __init__(self) -> None:
        self.wallets: list[frozenset[str]] = []
        self.restores = 0

    async def restore(self) -> None:
        """Record durable state restoration."""

        self.restores += 1

    def update_watched_wallets(self, wallets: frozenset[str]) -> None:
        """Record one watched snapshot."""

        self.wallets.append(wallets)

    async def on_pending_transaction(self, transaction: dict[str, object]) -> None:
        """Ignore pending input in this lifecycle test."""

        del transaction

    async def on_new_head(self, head: EvmHead) -> None:
        """Ignore head input in this lifecycle test."""

        del head

    async def on_log(self, log: dict[str, object]) -> None:
        """Ignore log input in this lifecycle test."""

        del log

    async def on_disconnect(self) -> None:
        """Ignore disconnect input in this lifecycle test."""

    async def resynchronize(self) -> None:
        """Ignore resync input in this lifecycle test."""


@pytest.mark.asyncio
async def test_runtime_starts_once_stops_once_and_updates_watched_wallets() -> None:
    """Runtime lifecycle is idempotent and preserves hot-added leaders."""

    manager = FakeConnectionManager()
    pipeline = FakeSourcePipeline()
    runtime = PolygonSourceTransactionRuntime(manager=manager, pipeline=pipeline)

    await runtime.start()
    await runtime.start()
    runtime.update_watched_wallets({"0xABC", "0xDef"})
    await runtime.stop()
    await runtime.stop()

    assert manager.starts == 1
    assert manager.stops == 1
    assert pipeline.restores == 1
    assert pipeline.wallets == [frozenset({"0xabc", "0xdef"})]


def test_head_fixture_is_timezone_aware() -> None:
    """Runtime tests retain deterministic aware time values."""

    head = EvmHead(
        chain_id=137,
        number=1,
        block_hash="0x" + "11" * 32,
        parent_hash="0x" + "22" * 32,
        observed_at=datetime(2026, 8, 13, tzinfo=UTC),
    )

    assert head.observed_at.tzinfo is UTC


def test_connected_probe_reports_manager_state_and_tolerates_absence() -> None:
    """The control-plane probe never invents a confirmed-detection state."""

    class _ProbingManager(FakeConnectionManager):
        connected = True

    pipeline = FakeSourcePipeline()
    probed = PolygonSourceTransactionRuntime(_ProbingManager(), pipeline)  # type: ignore[arg-type]
    silent = PolygonSourceTransactionRuntime(FakeConnectionManager(), pipeline)  # type: ignore[arg-type]

    assert probed.connected is True
    assert silent.connected is None
