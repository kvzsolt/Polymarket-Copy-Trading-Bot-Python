"""Copy entries use metadata-only prefetch when the REST book gate is off.

Live evidence (2026-08-20): 25 copies were skipped by the pipeline pre-gate
with ``reason=wide_spread`` and several more with ``reason=stale`` because the
REST round trip itself (~520-570 ms) outran its own freshness budget. The REST
snapshot must not block or reprice a FAK copy. The disabled path fetches only
the dynamic tick, minimum-size, and negative-risk fields needed for safe
sizing; the copied price remains the sole order limit.

No operator switch can restore the removed book gate.
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
    """Fake market-data service recording metadata and snapshot requests."""

    def __init__(
        self,
        *,
        snapshot_error: Exception | None = None,
        metadata_error: Exception | None = None,
    ) -> None:
        self.snapshot_calls: list[tuple[str, str]] = []
        self.metadata_calls: list[tuple[str, str | None]] = []
        self._snapshot_error = snapshot_error
        self._metadata_error = metadata_error

    async def require_execution_snapshot(
        self,
        token_id: str,
        *,
        side: str,
        condition_id: str | None = None,
    ):
        self.snapshot_calls.append((token_id, side))
        if self._snapshot_error is not None:
            raise self._snapshot_error
        return SimpleNamespace(
            quote=SimpleNamespace(best_bid=Decimal("0.49"), best_ask=Decimal("0.51")),
            book=SimpleNamespace(bids={}, asks={}),
            tick=SimpleNamespace(tick_size=Decimal("0.01"), min_order_size=Decimal("5")),
            source="rest_snapshot",
        )

    async def require_execution_metadata(
        self,
        token_id: str,
        *,
        condition_id: str | None = None,
    ) -> SimpleNamespace:
        """Return deterministic execution metadata without an order book."""

        self.metadata_calls.append((token_id, condition_id))
        if self._metadata_error is not None:
            raise self._metadata_error
        return SimpleNamespace(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
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
    async def test_disabled_switch_fetches_metadata_without_a_snapshot(self) -> None:
        mds = _RecordingMDS()
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=False, mds=mds, runtime=runtime)

        bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert mds.snapshot_calls == []
        assert mds.metadata_calls == [(TOKEN, "0xcond")]
        assert bundles == {}
        assert stale_tokens == set()
        assert stale_codes == {}
        metadata = context.extra["_copy_execution_metadata_by_token"][TOKEN]
        assert metadata.tick_size == Decimal("0.01")
        assert metadata.min_order_size == Decimal("5")

    @pytest.mark.asyncio
    async def test_disabled_switch_produces_no_skip_for_a_wide_book(self) -> None:
        """The gate that skipped 25 live copies must not fire when disabled."""
        mds = _RecordingMDS(
            snapshot_error=StaleDataError("spread too wide", skip_code="book_spread_wide")
        )
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=False, mds=mds, runtime=runtime)

        _bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert stale_tokens == set()
        assert stale_codes == {}
        assert mds.snapshot_calls == []
        assert mds.metadata_calls == [(TOKEN, "0xcond")]

    @pytest.mark.asyncio
    async def test_disabled_switch_fails_closed_when_metadata_is_unavailable(self) -> None:
        """Do not size or submit a copy without the dynamic venue minimum."""

        mds = _RecordingMDS(
            metadata_error=StaleDataError(
                "metadata unavailable",
                skip_code="market_metadata_unavailable",
            )
        )
        context = _context(
            requires_snapshot=False,
            mds=mds,
            runtime=_RecordingRuntime(),
        )

        bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert bundles == {}
        assert stale_tokens == {TOKEN}
        assert stale_codes == {(TOKEN, "BUY"): "market_metadata_unavailable"}

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
        assert mds.metadata_calls == [(TOKEN, "0xcond")]

    @pytest.mark.asyncio
    async def test_legacy_enabled_value_cannot_restore_the_book_snapshot(self) -> None:
        """A stale context flag cannot re-enable book-derived copy pricing."""

        mds = _RecordingMDS()
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=True, mds=mds, runtime=runtime)

        bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert mds.snapshot_calls == []
        assert mds.metadata_calls == [(TOKEN, "0xcond")]
        assert bundles == {}
        assert stale_tokens == set()
        assert stale_codes == {}

    @pytest.mark.asyncio
    async def test_legacy_enabled_value_ignores_snapshot_rejections(self) -> None:
        """A wide REST book cannot reject a copied FAK."""

        mds = _RecordingMDS(
            snapshot_error=StaleDataError("spread too wide", skip_code="book_spread_wide")
        )
        runtime = _RecordingRuntime()
        context = _context(requires_snapshot=True, mds=mds, runtime=runtime)

        _bundles, stale_tokens, stale_codes = await _prefetch_market_data_for_signals(
            context, _normalized()
        )

        assert mds.snapshot_calls == []
        assert mds.metadata_calls == [(TOKEN, "0xcond")]
        assert stale_tokens == set()
        assert stale_codes == {}
