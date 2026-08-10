"""Issue #5 tests — Decimal precision, DB oversell clamp, atomic persistence."""

from __future__ import annotations

import contextlib
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.positionManager import PositionManager
from storage.postgres.orders_repo import OrdersRepo
from storage.postgres.positions_repo import PositionsRepo


def _make_pm() -> PositionManager:
    return PositionManager()


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
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


# ---------------------------------------------------------------------------
# Decimal precision in PositionManager — pure-function tests, no DB
# ---------------------------------------------------------------------------


class TestDecimalPrecision:
    # Phase 2 (Decimal sweep): TrackedPosition money fields are now
    # ``Decimal``. The legacy ``pytest.approx(float)`` form raises
    # ``TypeError`` against Decimal because the tolerance arithmetic
    # (``approx.expected - actual``) crosses the float / Decimal type
    # boundary. Cast Decimal → float at the assertion boundary so these
    # precision-oriented tests still check what they intend to check
    # (float-drift absence on the way out) without being sensitive to
    # the internal storage type.

    def test_repeating_decimal_buy_sell_no_drift(self):
        """Buy at 0.333333, sell at 0.666667 — Decimal should avoid float drift."""
        pm = _make_pm()
        pm.track_buy("cond-d1", "asset-1", 0.333333, 100.0, 33.3333)
        result = pm.track_sell("cond-d1", 0.666667, 100.0)
        assert result is not None  # position closed
        # PnL should be (0.666667 - 0.333333) * 100 = 33.3334
        assert float(result.realized_pnl) == pytest.approx(33.3334, rel=1e-4)

    def test_weighted_average_decimal_precision(self):
        """Multiple buys at different prices — weighted avg should be exact."""
        pm = _make_pm()
        pm.track_buy("cond-d2", "asset-1", 0.10, 100.0, 10.0)
        pm.track_buy("cond-d2", "asset-1", 0.30, 100.0, 30.0)
        pos = pm.get_position("cond-d2")
        # Weighted avg = (0.10*100 + 0.30*100) / 200 = 40/200 = 0.20
        assert float(pos.entry_price) == pytest.approx(0.20, abs=1e-6)
        assert float(pos.size_tokens) == pytest.approx(200.0)

    def test_partial_sell_preserves_avg_entry(self):
        """Partial sell should NOT change the average entry price."""
        pm = _make_pm()
        pm.track_buy("cond-d3", "asset-1", 0.40, 100.0, 40.0)
        pm.track_buy("cond-d3", "asset-1", 0.60, 100.0, 60.0)
        pos_before = pm.get_position("cond-d3")
        avg_before = float(pos_before.entry_price)  # 0.50

        # Partial sell 50 tokens
        pm.track_sell("cond-d3", 0.55, 50.0)
        pos_after = pm.get_position("cond-d3")
        assert pos_after is not None
        # Average entry should stay unchanged
        assert float(pos_after.entry_price) == pytest.approx(avg_before, abs=1e-6)
        assert float(pos_after.size_tokens) == pytest.approx(150.0)

    def test_many_buys_decimal_accumulation(self):
        """100 micro-buys should not accumulate float error."""
        pm = _make_pm()
        for i in range(100):
            pm.track_buy("cond-d4", "asset-1", 0.33, 1.0, 0.33)
        pos = pm.get_position("cond-d4")
        # 100 buys at 0.33 → avg should be exactly 0.33
        assert float(pos.entry_price) == pytest.approx(0.33, abs=1e-4)
        assert float(pos.size_tokens) == pytest.approx(100.0)


_TOKEN = "21742633143463906290569050155826241533067272736897614950488156847949938836495"


# ---------------------------------------------------------------------------
# DB oversell clamp in PositionsRepo (PG)
# ---------------------------------------------------------------------------


class TestDBOversellClamp:
    @pytest.mark.asyncio
    async def test_sell_more_than_held_clamps_to_held(self):
        """Selling more than held in DB should clamp to held amount."""
        async with _pg_pool() as pool:
            repo = PositionsRepo(pool)

            await repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-os",
                token_id=_TOKEN,
                side="BUY",
                fill_size=Decimal("50"),
                fill_price=Decimal("0.50"),
            )

            pos = await repo.get_position("u1", "cond-os", token_id=_TOKEN)
            assert pos is not None
            assert pos.size_tokens == Decimal("50")

            # Sell 80 tokens when only 50 held — should clamp to 50
            result = await repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-os",
                token_id=_TOKEN,
                side="SELL",
                fill_size=Decimal("80"),
                fill_price=Decimal("0.60"),
            )

            # Position should be deleted (closed)
            assert result is None
            assert await repo.get_position("u1", "cond-os", token_id=_TOKEN) is None

    @pytest.mark.asyncio
    async def test_sell_exactly_held_closes(self):
        """Selling exactly the held amount should close the position."""
        async with _pg_pool() as pool:
            repo = PositionsRepo(pool)

            await repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-exact",
                token_id=_TOKEN,
                side="BUY",
                fill_size=Decimal("100"),
                fill_price=Decimal("0.40"),
            )

            result = await repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-exact",
                token_id=_TOKEN,
                side="SELL",
                fill_size=Decimal("100"),
                fill_price=Decimal("0.60"),
            )

            assert result is None
            assert await repo.get_position("u1", "cond-exact", token_id=_TOKEN) is None


# ---------------------------------------------------------------------------
# Atomic execution persistence (PG)
# ---------------------------------------------------------------------------


class TestAtomicExecutionPersistence:
    @pytest.mark.asyncio
    async def test_persist_all_in_one_transaction(self):
        """Fill + position + processed_trade should all succeed atomically."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)

            ok = await repo.persist_execution_atomically(
                fill={"order_id": "ord-1"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-atom",
                token_id=_TOKEN,
                side="BUY",
                fill_size="50",
                fill_price="0.40",
                fill_fee="0",
                fill_ts=int(time.time()),
                fill_json='{"test": true}',
                trade_key="trade-atom-1",
                tx_hash=None,
                asset=_TOKEN,
                trade_status="filled",
            )
            assert ok is True

            async with pool.acquire() as conn:
                fills = await conn.fetchval(
                    "SELECT COUNT(*) FROM fills WHERE condition_id = 'cond-atom'"
                )
                assert fills == 1

                pos = await conn.fetchrow(
                    "SELECT size_tokens, avg_entry_price FROM positions "
                    "WHERE user_id = 'u1' AND condition_id = 'cond-atom'"
                )
                assert pos is not None
                assert pos["size_tokens"] == "50"
                assert pos["avg_entry_price"] == "0.40"

                pt = await conn.fetchrow(
                    "SELECT status FROM processed_trades WHERE trade_key = 'trade-atom-1'"
                )
                assert pt is not None
                assert pt["status"] == "filled"

    @pytest.mark.asyncio
    async def test_duplicate_fill_rejected(self):
        """Second fill with same unique key should be rejected."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            ts = int(time.time())

            ok1 = await repo.persist_execution_atomically(
                fill={"order_id": "ord-dup"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-dup",
                token_id=_TOKEN,
                side="BUY",
                fill_size="50",
                fill_price="0.40",
                fill_fee="0",
                fill_ts=ts,
                fill_json='{"test": true}',
                trade_key="trade-dup-1",
                tx_hash=None,
                asset=_TOKEN,
                trade_status="filled",
            )
            assert ok1 is True

            ok2 = await repo.persist_execution_atomically(
                fill={"order_id": "ord-dup"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-dup",
                token_id=_TOKEN,
                side="BUY",
                fill_size="50",
                fill_price="0.40",
                fill_fee="0",
                fill_ts=ts,
                fill_json='{"test": true}',
                trade_key="trade-dup-2",
                tx_hash=None,
                asset=_TOKEN,
                trade_status="filled",
            )
            assert ok2 is False  # rejected due to uniqueness constraint

    @pytest.mark.asyncio
    async def test_sell_closes_position_atomically(self):
        """Atomic SELL should delete position when fully closed."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            ts = int(time.time())

            await repo.persist_execution_atomically(
                fill={"order_id": "ord-buy"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-as",
                token_id=_TOKEN,
                side="BUY",
                fill_size="100",
                fill_price="0.30",
                fill_fee="0",
                fill_ts=ts,
                fill_json="{}",
                trade_key="trade-as-buy",
                tx_hash=None,
                asset=_TOKEN,
                trade_status="filled",
            )

            # Different fill_ts so the dedup unique index doesn't catch this
            # before the position-update step.
            await repo.persist_execution_atomically(
                fill={"order_id": "ord-sell"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-as",
                token_id=_TOKEN,
                side="SELL",
                fill_size="100",
                fill_price="0.60",
                fill_fee="0",
                fill_ts=ts + 1,
                fill_json="{}",
                trade_key="trade-as-sell",
                tx_hash=None,
                asset=_TOKEN,
                trade_status="filled",
            )

            async with pool.acquire() as conn:
                pos = await conn.fetchrow(
                    "SELECT * FROM positions WHERE user_id = 'u1' AND condition_id = 'cond-as'"
                )
                assert pos is None  # position should be deleted
