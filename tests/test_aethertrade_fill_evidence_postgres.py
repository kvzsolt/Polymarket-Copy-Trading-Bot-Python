"""Real isolated PostgreSQL verifies fill evidence atomicity and durable handoff."""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from storage.postgres.fills_repo import FillsRepo
from storage.postgres.positions_repo import PositionsRepo
from tests.test_aethertrade_fill_evidence import FillClock, fill_record
from tests.test_fills_repo_dedup_key import _pg_pool

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def projection():
    from storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

    return PostgresFillEvidenceProjection(clock=FillClock())


async def events(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT seq,event_id,digest,payload,drained_at FROM evidence_transaction_outbox ORDER BY seq"
        )
    return [dict(row) | {"payload": json.loads(row["payload"])} for row in rows]


@pytest.mark.parametrize("atomic_position", [False, True])
async def test_fresh_fill_and_evidence_commit_once_and_duplicates_never_retimestamp(
    atomic_position,
):
    async with _pg_pool() as pool:
        fills = FillsRepo(pool, evidence_projection=projection())
        record = fill_record()
        if atomic_position:
            fill_id, _ = await fills.append_fill_and_apply(record, PositionsRepo(pool))
            duplicate, _ = await fills.append_fill_and_apply(record, PositionsRepo(pool))
        else:
            fill_id = await fills.append_fill(record)
            duplicate = await fills.append_fill(record)
        assert fill_id > 0 and duplicate == 0
        observed = await events(pool)
        assert len(observed) == 1
        assert observed[0]["payload"]["fill_id"] == f"fill:{fill_id}"
        assert observed[0]["payload"].get("fee") is None
        assert observed[0]["payload"].get("source_action_id") is None
        assert observed[0]["payload"]["received_ns"] == "1800000000123456789"


@pytest.mark.parametrize("atomic_position", [False, True])
async def test_evidence_failure_rolls_back_fill_and_position(atomic_position):
    async with _pg_pool() as pool:
        failing = SimpleNamespace(
            apply=AsyncMock(side_effect=RuntimeError("local outbox unavailable"))
        )
        fills = FillsRepo(pool, evidence_projection=failing)
        positions = PositionsRepo(pool)
        with pytest.raises(RuntimeError, match="outbox unavailable"):
            if atomic_position:
                await fills.append_fill_and_apply(fill_record(), positions)
            else:
                await fills.append_fill(fill_record())
        assert await fills.list_fills_for_user("follower-a") == []
        assert await positions.get_position("follower-a", "condition", token_id="123") is None
        assert await events(pool) == []


async def test_reconciliation_links_only_scoped_explicit_order_metadata():
    async with _pg_pool() as pool:
        fills = FillsRepo(pool, evidence_projection=projection())
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orders(idempotency_key,user_id,order_id,request_json) VALUES('order-key',$1,$2,$3::jsonb)",
                "follower-a",
                "order-a",
                json.dumps({"recovery_metadata": {"source_action_id": "native-action"}}),
            )
        await fills.append_fill(fill_record())
        await fills.append_fill(fill_record(user_id="follower-b"))
        rows = await events(pool)
        assert rows[0]["payload"]["source_action_id"] == "native-action"
        assert rows[1]["payload"].get("source_action_id") is None
        assert rows[0]["event_id"] != rows[1]["event_id"]
        with pytest.raises(ValueError, match="attribution"):
            await fills.append_fill(fill_record(fill_ts=1001, source_action_id="different-action"))
        assert len(await events(pool)) == 2


async def test_local_append_then_crash_replays_same_event_and_only_marks_after_durability(tmp_path):
    from storage.postgres.evidence_drain import PostgresEvidenceDrain
    from tests.test_aethertrade_evidence_outbox import outbox

    async with _pg_pool() as pool:
        await FillsRepo(pool, evidence_projection=projection()).append_fill(fill_record())
        box = outbox(tmp_path / "durable")

        class CrashAfterAppend:
            def append_event(self, event):
                box.append(event)
                raise RuntimeError("crash before marking drained")

        with pytest.raises(RuntimeError, match="crash"):
            await PostgresEvidenceDrain(pool, CrashAfterAppend()).drain_once()
        assert (await events(pool))[0]["drained_at"] is None
        assert len(box.peek()) == 1
        sink = SimpleNamespace(append_event=box.append)
        drain = PostgresEvidenceDrain(pool, sink)
        assert await drain.drain_once() == 1
        assert await drain.drain_once() == 0
        assert len(box.peek()) == 1
        assert box.peek()[0].event.event_id == (await events(pool))[0]["event_id"]
        assert (await events(pool))[0]["drained_at"] is not None
        box.close()


async def test_drain_rejects_corrupted_identity_without_marking_or_publishing():
    from storage.postgres.evidence_drain import PostgresEvidenceDrain

    async with _pg_pool() as pool:
        await FillsRepo(pool, evidence_projection=projection()).append_fill(fill_record())
        async with pool.acquire() as conn:
            await conn.execute("UPDATE evidence_transaction_outbox SET digest=$1", "0" * 64)
        published = []
        with pytest.raises(ValueError, match="identity"):
            await PostgresEvidenceDrain(
                pool, SimpleNamespace(append_event=published.append)
            ).drain_once()
        assert published == []
        assert (await events(pool))[0]["drained_at"] is None


async def test_recovered_sell_keeps_inventory_absent_and_unattributed_evidence():
    """Risk recovery accounting requires no Hungary availability or guessed leader."""
    async with _pg_pool() as pool:
        fills = FillsRepo(pool, evidence_projection=projection())
        positions = PositionsRepo(pool)
        fill_id, position = await fills.append_recovered_sell(
            fill_record(side="SELL", dedup_key="2.0100", accounted_shares=Decimal(0)),
            positions,
        )
        assert fill_id > 0 and position is None
        assert await positions.get_position("follower-a", "condition", token_id="123") is None
        wire = (await events(pool))[0]["payload"]
        assert wire["recovery"] is True
        assert wire.get("source_action_id") is None
        assert wire["size"] == "2.0100" and wire["side"] == "SELL"


async def test_drain_bounds_each_batch_and_retains_later_fill_evidence(tmp_path):
    from storage.postgres.evidence_drain import PostgresEvidenceDrain
    from tests.test_aethertrade_evidence_outbox import outbox

    async with _pg_pool() as pool:
        fills = FillsRepo(pool, evidence_projection=projection())
        for timestamp in range(1000, 1003):
            await fills.append_fill(fill_record(fill_ts=timestamp))
        box = outbox(tmp_path / "durable")
        drain = PostgresEvidenceDrain(pool, SimpleNamespace(append_event=box.append), batch_size=2)
        assert await drain.drain_once() == 2
        assert len(box.peek()) == 2
        assert [row["drained_at"] is None for row in await events(pool)] == [False, False, True]
        assert await drain.drain_once() == 1
        assert len(box.peek()) == 3
        box.close()


@pytest.mark.parametrize("mode", ["paper", "live", "unknown"])
async def test_projection_never_relabels_execution_source(mode):
    from storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

    async with _pg_pool() as pool:
        fills = FillsRepo(
            pool,
            evidence_projection=PostgresFillEvidenceProjection(
                clock=FillClock(), execution_mode=mode
            ),
        )
        await fills.append_fill(fill_record())
        assert (await events(pool))[0]["payload"]["execution_mode"] == mode


async def test_recovery_uses_original_mode_and_legacy_unknown_after_mode_change():
    from storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

    async with _pg_pool() as pool:
        fills = FillsRepo(
            pool,
            evidence_projection=PostgresFillEvidenceProjection(
                clock=FillClock(), execution_mode="live"
            ),
        )
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orders(idempotency_key,user_id,order_id,request_json) VALUES('paper-key',$1,$2,$3::jsonb)",
                "follower-a",
                "order-a",
                json.dumps({"recovery_metadata": {"execution_mode": "paper"}}),
            )
        # Older reconciler variants may have a dedup key without an accounting watermark.
        await fills.append_fill(fill_record(dedup_key="2.0100"))
        await fills.append_fill(fill_record(order_id="legacy-order", dedup_key="2.0100"))
        rows = await events(pool)
        assert rows[0]["payload"]["execution_mode"] == "paper"
        assert rows[1]["payload"]["execution_mode"] == "unknown"
        assert all(row["payload"]["recovery"] for row in rows)
        with pytest.raises(ValueError, match="execution mode"):
            await fills.append_fill(fill_record(fill_ts=1002))
        assert len(await events(pool)) == 2
