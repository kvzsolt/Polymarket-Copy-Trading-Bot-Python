from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from storage.postgres.connection import get_pool


def _wallet(value: str) -> str:
    return str(value or "").strip().lower()


@dataclass
class _InsertResult:
    inserted_id: int | None = None


class _UserActivityCollection:
    def __init__(self, wallet_address: str):
        self.wallet_address = _wallet(wallet_address)

    async def insert_many(self, docs: list[dict[str, Any]]) -> None:
        if not docs:
            return
        now = time.time()
        pool = await get_pool()
        async with pool.acquire() as conn:
            # PR #108 finding: wrap the batch in a single transaction so a
            # mid-loop crash rolls back the whole batch instead of leaving
            # a partially-committed activity history.
            async with conn.transaction():
                for doc in docs:
                    external_id = str(doc.get("_id") or doc.get("id") or "")
                    tx_hash = str(doc.get("transactionHash") or doc.get("txHash") or "")
                    await conn.execute(
                        """
                        INSERT INTO user_activities
                        (wallet_address, external_id, transaction_hash, data_json, created_at, updated_at)
                        VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                        """,
                        self.wallet_address,
                        external_id,
                        tx_hash,
                        json.dumps(doc, separators=(",", ":"), default=str),
                        now,
                        now,
                    )

    async def update_one(self, filt: dict[str, Any], update: dict[str, Any]) -> None:
        set_fields = update.get("$set") or {}
        row_id = filt.get("_id")
        if row_id is None or not set_fields:
            return
        pool = await get_pool()
        async with pool.acquire() as conn:
            # PR #108 finding: serialise the SELECT + UPDATE read-modify-
            # write so two concurrent ``update_one`` calls on the same row
            # cannot both observe the same ``data_json`` snapshot and have
            # the second write silently overwrite the first. ``FOR UPDATE``
            # on the row + the surrounding transaction restore the
            # SQLite-era atomicity.
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT data_json FROM user_activities "
                    "WHERE id = $1 AND wallet_address = $2 FOR UPDATE",
                    int(row_id), self.wallet_address,
                )
                if not row:
                    return
                payload = json.loads(row["data_json"]) if isinstance(row["data_json"], str) else dict(row["data_json"])
                payload.update(set_fields)
                await conn.execute(
                    """
                    UPDATE user_activities
                    SET data_json = $1::jsonb, updated_at = $2
                    WHERE id = $3 AND wallet_address = $4
                    """,
                    json.dumps(payload, separators=(",", ":"), default=str),
                    time.time(),
                    int(row_id),
                    self.wallet_address,
                )


class _UserPositionCollection:
    def __init__(self, wallet_address: str):
        self.wallet_address = _wallet(wallet_address)

    async def delete_many(self, _: dict[str, Any]) -> None:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM user_positions WHERE wallet_address = $1",
                self.wallet_address,
            )

    async def insert_many(self, docs: list[dict[str, Any]]) -> None:
        if not docs:
            return
        now = time.time()
        pool = await get_pool()
        async with pool.acquire() as conn:
            # PR #108 finding: callers typically call ``delete_many`` then
            # ``insert_many``. Wrap the batch in a single transaction so a
            # crash mid-loop rolls back partial inserts; combine with the
            # caller's transaction (if any) for delete+insert atomicity.
            async with conn.transaction():
                for doc in docs:
                    condition_id = str(doc.get("conditionId") or doc.get("condition_id") or "")
                    key = condition_id or str(doc.get("tokenId") or doc.get("token_id") or "")
                    await conn.execute(
                        """
                        INSERT INTO user_positions (wallet_address, condition_id, data_json, updated_at)
                        VALUES ($1, $2, $3::jsonb, $4)
                        ON CONFLICT(wallet_address, condition_id) DO UPDATE SET
                          data_json = excluded.data_json,
                          updated_at = excluded.updated_at
                        """,
                        self.wallet_address,
                        key,
                        json.dumps(doc, separators=(",", ":"), default=str),
                        now,
                    )


class _TradingCollection:
    async def insert_one(self, doc: dict[str, Any]) -> _InsertResult:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trading_results
                (leader_wallet, transaction_hash, condition_id, side, title, outcome, price, size,
                 usdc_size, event_timestamp, result, reason, recorded_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING id
                """,
                doc.get("leaderWallet"),
                doc.get("transactionHash"),
                doc.get("conditionId"),
                doc.get("side"),
                doc.get("title"),
                doc.get("outcome"),
                doc.get("price"),
                doc.get("size"),
                doc.get("usdcSize"),
                doc.get("timestamp"),
                doc.get("result"),
                doc.get("reason"),
                float(doc.get("recordedAt") or time.time()),
            )
            inserted_id = int(row["id"]) if row else None
            return _InsertResult(inserted_id=inserted_id)


def get_user_position_model(wallet_address: str) -> _UserPositionCollection:
    return _UserPositionCollection(wallet_address)


def get_user_activity_model(wallet_address: str) -> _UserActivityCollection:
    return _UserActivityCollection(wallet_address)


async def find_activity_by_tx_hash(wallet_address: str, tx_hash: str) -> bool:
    """Return True if a user_activities row with this transaction_hash exists.

    Used by:
      * ManualSyncService to skip already-tracked trades during the funder
        activity sweep (single source of truth dedup).
      * Reconciler write-back to make the post-confirmation insert idempotent
        so retried reconciliations don't double-insert.
    """
    if not tx_hash:
        return False
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM user_activities "
            "WHERE wallet_address = $1 AND transaction_hash = $2 LIMIT 1",
            _wallet(wallet_address),
            str(tx_hash),
        )
        return row is not None


def get_trading_collection() -> _TradingCollection:
    return _TradingCollection()
