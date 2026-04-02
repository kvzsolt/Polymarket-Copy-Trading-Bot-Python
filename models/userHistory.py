from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from config.db import get_database


def _wallet(value: str) -> str:
    return str(value or "").strip().lower()


@dataclass
class _SQLiteResult:
    inserted_id: int | None = None


class _UserActivityCollection:
    def __init__(self, wallet_address: str):
        self.wallet_address = _wallet(wallet_address)

    def insert_many(self, docs: list[dict[str, Any]]) -> None:
        if not docs:
            return
        now = time.time()
        with get_database() as conn:
            for doc in docs:
                external_id = str(doc.get("_id") or doc.get("id") or "")
                tx_hash = str(doc.get("transactionHash") or doc.get("txHash") or "")
                conn.execute(
                    """
                    INSERT INTO user_activities
                    (wallet_address, external_id, transaction_hash, data_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.wallet_address,
                        external_id,
                        tx_hash,
                        json.dumps(doc, separators=(",", ":"), default=str),
                        now,
                        now,
                    ),
                )
            conn.commit()

    def update_one(self, filt: dict[str, Any], update: dict[str, Any]) -> None:
        set_fields = update.get("$set") or {}
        row_id = filt.get("_id")
        if row_id is None or not set_fields:
            return
        with get_database() as conn:
            row = conn.execute(
                "SELECT data_json FROM user_activities WHERE id = ? AND wallet_address = ? LIMIT 1",
                (int(row_id), self.wallet_address),
            ).fetchone()
            if not row:
                return
            payload = json.loads(row["data_json"])
            payload.update(set_fields)
            conn.execute(
                """
                UPDATE user_activities
                SET data_json = ?, updated_at = ?
                WHERE id = ? AND wallet_address = ?
                """,
                (
                    json.dumps(payload, separators=(",", ":"), default=str),
                    time.time(),
                    int(row_id),
                    self.wallet_address,
                ),
            )
            conn.commit()


class _UserPositionCollection:
    def __init__(self, wallet_address: str):
        self.wallet_address = _wallet(wallet_address)

    def delete_many(self, _: dict[str, Any]) -> None:
        with get_database() as conn:
            conn.execute(
                "DELETE FROM user_positions WHERE wallet_address = ?",
                (self.wallet_address,),
            )
            conn.commit()

    def insert_many(self, docs: list[dict[str, Any]]) -> None:
        if not docs:
            return
        now = time.time()
        with get_database() as conn:
            for doc in docs:
                condition_id = str(doc.get("conditionId") or doc.get("condition_id") or "")
                key = condition_id or str(doc.get("tokenId") or doc.get("token_id") or "")
                conn.execute(
                    """
                    INSERT INTO user_positions (wallet_address, condition_id, data_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(wallet_address, condition_id) DO UPDATE SET
                      data_json = excluded.data_json,
                      updated_at = excluded.updated_at
                    """,
                    (
                        self.wallet_address,
                        key,
                        json.dumps(doc, separators=(",", ":"), default=str),
                        now,
                    ),
                )
            conn.commit()


class _TradingCollection:
    def insert_one(self, doc: dict[str, Any]) -> _SQLiteResult:
        with get_database() as conn:
            cur = conn.execute(
                """
                INSERT INTO trading_results
                (leader_wallet, transaction_hash, condition_id, side, title, outcome, price, size,
                 usdc_size, event_timestamp, result, reason, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
                ),
            )
            conn.commit()
            return _SQLiteResult(inserted_id=cur.lastrowid)


def get_user_position_model(wallet_address: str) -> _UserPositionCollection:
    return _UserPositionCollection(wallet_address)


def get_user_activity_model(wallet_address: str) -> _UserActivityCollection:
    return _UserActivityCollection(wallet_address)


def get_trading_collection() -> _TradingCollection:
    return _TradingCollection()
