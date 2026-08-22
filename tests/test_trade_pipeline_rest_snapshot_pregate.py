"""The REST snapshot pre-gate is an opt-in switch, off by default.

Live evidence (2026-08-20): 25 copies were skipped by the pipeline pre-gate
with ``reason=wide_spread`` and several more with ``reason=stale`` because the
REST round trip itself (~520-570 ms) outran its own freshness budget. The REST
snapshot's job is to feed the price cache, not to block a FAK copy: the
executor's cache-first ``require_fresh_data`` is the single fetch on the entry
path, and the leader-price band is the price-quality gate.

``market_data.copy_entry_requires_rest_snapshot`` restores the blocking
pre-gate for operators who want it; the switch is checked as the first
statement of the pre-gate and returns without side effects when disabled.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from orchestrators.trade_pipeline import _prefetch_market_data_for_signals
from services.market_data_service import StaleDataError

TOKEN = "7132104567925221259462638553270691275033272857194253222353576478981233863739"


class _RecordingMDS:
    """Fake MarketDataService recording every pre-gate snapshot request."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self._error = error

    async def require_execution_snapshot(
        self,
        token_id: str,
        *,
        side: str,
        condition_id: str | None = None,
    ):
        self.calls.append((token_id, side))
        if self._error is not None:
            raise self._error
        return SimpleNamespace(
            quote=SimpleNamespace(best_bid=Decimal("0.49"), best_ask=Decimal("0.51")),
            book=SimpleNamespace(bids={}, asks={}),
            tick=SimpleNamespace(tick_size=Decimal("0.01"), min_order_size=Decimal("5")),
            source="rest_snapshot",
        )


class _RecordingRuntime:
    """Fake market-data runtime recording WSS subscription requests."""

    def __init__(self) -> None:
        self.subscribed: list[str] = []

    async def subscribe_tokens(self, tokens: list[str]) -> None:
        self.subscribed.extend(tokens)


def _context(*, requires_snapshot: bool, mds, runtime) -> SimpleNamespace:
    return SimpleNamespace(
        user_id="user-1",
        config=SimpleNamespace(
            market_data=SimpleNamespace(
                primary_source="rest_snapshot",
                copy_entry_requires_rest_snapshot=requires_snapshot,
                subscribe_timeout_seconds=2.0,
            ),
            execution=SimpleNamespace(trade_analysis_concurrency=6),
        ),
        market_data_service=mds,
        market_data_runtime=runtime,
        extra={},
    )


def _normalized() -> list:
    signal = SimpleNamespace(token_id=TOKEN, side="BUY", condition_id="0xcond")
    return [(signal, {})]


async def _drain_background_tasks() -> None:
    """Let fire-and-forget warm-up tasks run without blocking on them."""
    for _ in range(5):
        await asyncio.sleep(0)


class TestCopyEntryDoesNotBlockOnTheRestSnapshot:
    @pytest.mark.asyncio
    async def test_disabled_switch_skips_the_snapshot_call_entirely(self) -> None:
        mds = _RecordingMDS()
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=False, mds=mds, runtime=runtime)

        bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert mds.calls == []
        assert bundles == {}
        assert stale_tokens == set()
        assert stale_codes == {}

    @pytest.mark.asyncio
    async def test_disabled_switch_produces_no_skip_for_a_wide_book(self) -> None:
        """The gate that skipped 25 live copies must not fire when disabled."""
        mds = _RecordingMDS(error=StaleDataError("spread too wide", skip_code="book_spread_wide"))
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=False, mds=mds, runtime=runtime)

        _bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert stale_tokens == set()
        assert stale_codes == {}

    @pytest.mark.asyncio
    async def test_disabled_switch_still_warms_the_wss_subscription(self) -> None:
        mds = _RecordingMDS()
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=False, mds=mds, runtime=runtime)

        await _prefetch_market_data_for_signals(context, _normalized())
        await _drain_background_tasks()

        assert runtime.subscribed == [TOKEN]

    @pytest.mark.asyncio
    async def test_disabled_switch_swallows_a_failing_warm_up(self) -> None:
        class _BrokenRuntime:
            async def subscribe_tokens(self, tokens: list[str]) -> None:
                raise RuntimeError("wss down")

        mds = _RecordingMDS()
        context = _context(requires_snapshot=False, mds=mds, runtime=_BrokenRuntime())

        result = await _prefetch_market_data_for_signals(context, _normalized())
        await _drain_background_tasks()

        assert result == ({}, set(), {})

    @pytest.mark.asyncio
    async def test_enabled_switch_keeps_the_blocking_pre_gate(self) -> None:
        mds = _RecordingMDS()
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=True, mds=mds, runtime=runtime)

        bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert mds.calls == [(TOKEN, "BUY")]
        assert set(bundles) == {TOKEN}
        assert stale_tokens == set()
        assert stale_codes == {}
        assert runtime.subscribed == [TOKEN]

    @pytest.mark.asyncio
    async def test_enabled_switch_still_surfaces_the_rejection_skip_code(self) -> None:
        mds = _RecordingMDS(error=StaleDataError("spread too wide", skip_code="book_spread_wide"))
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=True, mds=mds, runtime=runtime)

        _bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert stale_tokens == {TOKEN}
        assert stale_codes == {(TOKEN, "BUY"): "book_spread_wide"}
