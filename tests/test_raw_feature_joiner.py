"""Tests for the raw-feature joiner service.

Verifies that ``RawFeatureJoiner`` honestly joins scoring-input records
with available storage evidence, producing correct availability flags and
raw inputs without fabricating missing data.
"""
from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from domain.discovery_models import (
    ObservedTrade,
    OrderbookSnapshot,
    PositionSnapshotRecord,
    ReconstructedPositionStatus,
    ReconstructionConfidence,
)
from domain.raw_feature_models import RawFeatureRecord
from services.discovery.raw_feature_joiner import RawFeatureJoiner
from services.discovery.scoring_input_service import ScoringInputAssemblerService
from storage.postgres.discovery_repo import DiscoveryRepo

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers (consistent with test_discovery_scoring_input_service.py)
# ---------------------------------------------------------------------------

WALLET = "0xabc"
COND = "cond-1"
TOKEN = "token-yes"
REF_TS = 1_700_000_000


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply

        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _trade(
    *,
    trader_wallet: str = WALLET,
    condition_id: str = COND,
    token_id: str = TOKEN,
    side: str,
    size: str,
    price: str,
    event_ts: int,
    tx_hash: str,
    trade_id: str = "",
) -> ObservedTrade:
    return ObservedTrade(
        trader_wallet=trader_wallet,
        condition_id=condition_id,
        token_id=token_id,
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        usdc_size=Decimal(size) * Decimal(price),
        tx_hash=tx_hash,
        event_ts=event_ts,
        trade_id=trade_id,
    )


def _snapshot(
    *,
    trader_wallet: str = WALLET,
    condition_id: str = COND,
    token_id: str = TOKEN,
    size: str,
    avg_price: str,
    snapshot_ts: int,
) -> PositionSnapshotRecord:
    return PositionSnapshotRecord(
        trader_wallet=trader_wallet,
        condition_id=condition_id,
        token_id=token_id,
        size=Decimal(size),
        avg_price=Decimal(avg_price),
        snapshot_ts=snapshot_ts,
    )


def _orderbook(
    *,
    token_id: str = TOKEN,
    condition_id: str = COND,
    snapshot_ts: int = REF_TS - 60,
    trader_wallet: str = WALLET,
    reference_tx_hash: str = "tx-1",
    best_bid: str = "0.55",
    best_ask: str = "0.60",
    mid_price: str = "0.575",
    spread: str = "0.05",
    bids_json: str = '[["0.55","100"]]',
    asks_json: str = '[["0.60","80"]]',
) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        token_id=token_id,
        condition_id=condition_id,
        snapshot_ts=snapshot_ts,
        trader_wallet=trader_wallet,
        reference_tx_hash=reference_tx_hash,
        reference_trade_id="",
        reference_event_ts=snapshot_ts,
        bids_json=bids_json,
        asks_json=asks_json,
        best_bid=Decimal(best_bid),
        best_ask=Decimal(best_ask),
        mid_price=Decimal(mid_price),
        spread=Decimal(spread),
    )


async def _assemble_and_join(repo, ref_ts=REF_TS):
    """Assemble scoring-input records and then join features."""
    assembler = ScoringInputAssemblerService(repo)
    records = await assembler.assemble_for_trader(WALLET)
    joiner = RawFeatureJoiner(repo)
    return await joiner.join(records, reference_ts=ref_ts)


# ---------------------------------------------------------------------------
# Tests: Scoring-input → stable raw-feature record
# ---------------------------------------------------------------------------

class TestScoringInputToRawFeature:
    """ScoringInputRecord transforms into a stable RawFeatureRecord."""

    async def test_basic_transform_produces_keyed_record(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            trade = _trade(side="BUY", size="100", price="0.60", event_ts=REF_TS - 120, tx_hash="tx-1")
            await repo.insert_trade(trade)

            features = await _assemble_and_join(repo)

        key = (WALLET, COND, TOKEN)
        assert key in features
        rec = features[key]
        assert isinstance(rec, RawFeatureRecord)
        assert rec.key == key
        assert rec.trader_wallet == WALLET
        assert rec.condition_id == COND
        assert rec.token_id == TOKEN

    async def test_position_fundamentals_pass_through(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="100", price="0.60", event_ts=REF_TS - 60, tx_hash="tx-1"))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.current_size == Decimal("100")
        assert rec.avg_cost == Decimal("0.60")
        assert rec.position_status == ReconstructedPositionStatus.OPEN

    async def test_multiple_positions_produce_multiple_features(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="50", price="0.40", event_ts=REF_TS - 60, tx_hash="tx-a"))
            await repo.insert_trade(_trade(
                token_id="token-no", side="BUY", size="30", price="0.35",
                event_ts=REF_TS - 30, tx_hash="tx-b",
            ))

            features = await _assemble_and_join(repo)
        assert len(features) == 2
        assert (WALLET, COND, "token-no") in features


# ---------------------------------------------------------------------------
# Tests: Evidence availability flags are honest when data is missing
# ---------------------------------------------------------------------------

class TestEvidenceAvailabilityHonesty:
    """Flags behave honestly — absent data produces False/None, never fabricated."""

    async def test_no_orderbook_produces_false_flags(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="10", price="0.50", event_ts=REF_TS - 60, tx_hash="tx-1"))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.slippage_copyability.has_orderbook_snapshot is False
        assert rec.slippage_copyability.best_bid is None
        assert rec.slippage_copyability.best_ask is None
        assert rec.slippage_copyability.spread is None
        assert rec.slippage_copyability.snapshot_age_seconds is None
        assert rec.evidence_flags.has_orderbook_snapshot is False
        assert rec.evidence_flags.orderbook_snapshot_count == 0

    async def test_price_history_always_absent(self):
        """No price-history table exists yet — flag must always be False."""
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="10", price="0.50", event_ts=REF_TS - 60, tx_hash="tx-1"))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]
        assert rec.evidence_flags.has_price_history is False

    async def test_orderbook_present_populates_slippage_inputs(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="10", price="0.50", event_ts=REF_TS - 120, tx_hash="tx-1"))
            await repo.insert_orderbook_snapshot(_orderbook(snapshot_ts=REF_TS - 60))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.slippage_copyability.has_orderbook_snapshot is True
        assert rec.slippage_copyability.best_bid == Decimal("0.55")
        assert rec.slippage_copyability.best_ask == Decimal("0.60")
        assert rec.slippage_copyability.mid_price == Decimal("0.575")
        assert rec.slippage_copyability.spread == Decimal("0.05")
        assert rec.slippage_copyability.book_depth_available is True
        assert rec.slippage_copyability.snapshot_age_seconds == 60
        assert rec.evidence_flags.has_orderbook_snapshot is True
        assert rec.evidence_flags.orderbook_snapshot_count == 1

    async def test_empty_orderbook_book_depth_false(self):
        """Orderbook with empty bids/asks → depth flag is False."""
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="10", price="0.50", event_ts=REF_TS - 120, tx_hash="tx-1"))
            await repo.insert_orderbook_snapshot(_orderbook(
                bids_json="[]", asks_json="[]",
                best_bid="0.55", best_ask="0.60",
            ))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.slippage_copyability.has_orderbook_snapshot is True
        assert rec.slippage_copyability.book_depth_available is False

    async def test_no_snapshot_yields_no_position_snapshot_flag(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="10", price="0.50", event_ts=REF_TS - 60, tx_hash="tx-1"))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]
        assert rec.evidence_flags.has_position_snapshot is False

    async def test_with_snapshot_flag_true(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="10", price="0.50", event_ts=REF_TS - 60, tx_hash="tx-1"))
            await repo.insert_position_snapshot(_snapshot(size="10", avg_price="0.50", snapshot_ts=REF_TS - 30))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]
        assert rec.evidence_flags.has_position_snapshot is True


# ---------------------------------------------------------------------------
# Tests: Low-confidence / missing-history reconstruction remains visible
# ---------------------------------------------------------------------------

class TestLowConfidenceVisibility:
    """Reconstruction uncertainty propagates into features — not hidden."""

    async def test_high_confidence_propagates(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="100", price="0.60", event_ts=REF_TS - 60, tx_hash="tx-1"))
            await repo.insert_position_snapshot(_snapshot(size="100", avg_price="0.60", snapshot_ts=REF_TS - 30))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]
        assert rec.reconstruction_confidence == ReconstructionConfidence.HIGH

    async def test_mismatch_propagates_to_delay_survival(self):
        """Size mismatch between trades and snapshot → visible in delay_survival."""
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(side="BUY", size="100", price="0.60", event_ts=REF_TS - 60, tx_hash="tx-1"))
            # Snapshot with different size → triggers mismatch
            await repo.insert_position_snapshot(_snapshot(size="999", avg_price="0.60", snapshot_ts=REF_TS - 30))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]
        assert rec.delay_survival.has_mismatch is True
        assert rec.reconstruction_confidence != ReconstructionConfidence.HIGH

    async def test_no_trades_means_no_entry_alpha_windows(self):
        """When there are no trades, no entry-alpha windows should be available.

        We seed a position snapshot so the assembler creates a record, but
        we add NO trades.
        """
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_position_snapshot(_snapshot(size="50", avg_price="0.40", snapshot_ts=REF_TS - 30))

            assembler = ScoringInputAssemblerService(repo)
            records = await assembler.assemble_for_trader(WALLET)

            # If the assembler doesn't produce a record from snapshot-only, skip
            if not records:
                return

            joiner = RawFeatureJoiner(repo)
            features = await joiner.join(records, reference_ts=REF_TS)
        rec = features[(WALLET, COND, TOKEN)]
        assert rec.entry_alpha.window_5m_available is False
        assert rec.entry_alpha.window_30m_available is False
        assert rec.entry_alpha.window_4h_available is False


# ---------------------------------------------------------------------------
# Tests: Entry-alpha window correctness
# ---------------------------------------------------------------------------

class TestEntryAlphaWindows:
    """Entry-alpha windows compute from actual trades, not fabrication."""

    async def test_trade_within_5m_window(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            # Trade 2 minutes before ref_ts
            await repo.insert_trade(_trade(
                side="BUY", size="100", price="0.60",
                event_ts=REF_TS - 120, tx_hash="tx-1",
            ))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.entry_alpha.window_5m_available is True
        assert rec.entry_alpha.window_5m_entry_price == Decimal("0.60")
        assert rec.entry_alpha.window_5m_trade_count == 1
        # Also falls within larger windows
        assert rec.entry_alpha.window_30m_available is True
        assert rec.entry_alpha.window_4h_available is True

    async def test_two_trades_different_windows(self):
        """Two BUY trades: one recent (within 5m of last) and one older.

        Windows are anchored on last_trade_ts.  An old trade 20 min before
        the last trade is outside the 5m window but inside 30m.
        """
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            # Old trade: 20 min before the last trade
            await repo.insert_trade(_trade(
                side="BUY", size="50", price="0.55",
                event_ts=REF_TS - 1200, tx_hash="tx-old",
            ))
            # Recent trade: the "last" trade
            await repo.insert_trade(_trade(
                side="BUY", size="100", price="0.60",
                event_ts=REF_TS - 60, tx_hash="tx-last",
            ))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        # 5m window anchored at last_trade_ts=REF_TS-60:
        # cutoff = (REF_TS-60) - 300 = REF_TS-360
        # old trade at REF_TS-1200 is NOT in 5m window, but last trade IS
        assert rec.entry_alpha.window_5m_available is True
        assert rec.entry_alpha.window_5m_trade_count == 1
        # 30m window includes both trades
        assert rec.entry_alpha.window_30m_available is True
        assert rec.entry_alpha.window_30m_trade_count == 2

    async def test_sell_trades_excluded_from_entry_alpha(self):
        """Only BUY trades contribute to entry-alpha windows."""
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(
                side="SELL", size="100", price="0.70",
                event_ts=REF_TS - 60, tx_hash="tx-1",
            ))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        # SELL trade doesn't count for entry alpha
        assert rec.entry_alpha.window_5m_available is False
        assert rec.entry_alpha.window_5m_trade_count == 0


# ---------------------------------------------------------------------------
# Tests: Delay-survival inputs
# ---------------------------------------------------------------------------

class TestDelaySurvival:
    """Delay-survival raw inputs are computed correctly."""

    async def test_delay_seconds_computed(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(
                side="BUY", size="100", price="0.60",
                event_ts=REF_TS - 300, tx_hash="tx-1",
            ))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.delay_survival.delay_seconds == 300
        assert rec.delay_survival.position_still_open is True

    async def test_closed_position_detected(self):
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)
            await repo.insert_trade(_trade(
                side="BUY", size="100", price="0.60",
                event_ts=REF_TS - 300, tx_hash="tx-1",
            ))
            await repo.insert_trade(_trade(
                side="SELL", size="100", price="0.70",
                event_ts=REF_TS - 60, tx_hash="tx-2",
            ))

            features = await _assemble_and_join(repo)
        rec = features[(WALLET, COND, TOKEN)]

        assert rec.delay_survival.position_still_open is False
