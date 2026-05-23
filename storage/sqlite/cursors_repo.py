from __future__ import annotations

import time
from dataclasses import dataclass

from domain.ports.repositories import AbstractCursorRepository
from storage.sqlite.connection import create_connection


@dataclass(frozen=True)
class TradeCursor:
    timestamp: int
    tx_hash: str
    trade_id: str | None = None


class CursorsRepo(AbstractCursorRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_cursor(self, leader_id: str) -> TradeCursor:
        leader_key = str(leader_id or "").lower()
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT last_timestamp, cursor_tx_hash, cursor_trade_id
                FROM leader_cursors
                WHERE leader_wallet = ?
                LIMIT 1
                """,
                (leader_key,),
            ).fetchone()

        if not row:
            return TradeCursor(timestamp=0, tx_hash="", trade_id=None)

        return TradeCursor(
            timestamp=int(float(row["last_timestamp"] or 0)),
            tx_hash=str(row["cursor_tx_hash"] or ""),
            trade_id=str(row["cursor_trade_id"] or "") or None,
        )

    def set_cursor(self, leader_id: str, cursor: TradeCursor) -> None:
        leader_key = str(leader_id or "").lower()
        with create_connection(self.db_path) as conn:
            # ``BEGIN IMMEDIATE`` so the cursor-regress guard (SELECT
            # → compare → INSERT/UPDATE) is atomic. Pre-fix two
            # concurrent ingestion threads could both read the same
            # ``existing``, both pass the ``<=`` check, and both upsert
            # — the smaller cursor wins if it commits last, rolling
            # the watermark backward and forcing the bot to re-process
            # already-seen trades. Taking the writer lock up-front
            # serialises the two callers.
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    """
                    SELECT last_timestamp, cursor_tx_hash, cursor_trade_id
                    FROM leader_cursors
                    WHERE leader_wallet = ?
                    LIMIT 1
                    """,
                    (leader_key,),
                ).fetchone()

                if row:
                    existing = TradeCursor(
                        timestamp=int(float(row["last_timestamp"] or 0)),
                        tx_hash=str(row["cursor_tx_hash"] or ""),
                        trade_id=str(row["cursor_trade_id"] or "") or None,
                    )
                    if _cursor_key(cursor) <= _cursor_key(existing):
                        conn.commit()
                        return

                conn.execute(
                    """
                    INSERT INTO leader_cursors (leader_wallet, last_timestamp, cursor_tx_hash, cursor_trade_id, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(leader_wallet) DO UPDATE SET
                        last_timestamp = excluded.last_timestamp,
                        cursor_tx_hash = excluded.cursor_tx_hash,
                        cursor_trade_id = excluded.cursor_trade_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        leader_key,
                        float(cursor.timestamp),
                        cursor.tx_hash,
                        cursor.trade_id,
                        time.time(),
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise



def _cursor_key(cursor: TradeCursor) -> tuple[int, str, str]:
    return (
        int(cursor.timestamp or 0),
        str(cursor.tx_hash or ""),
        str(cursor.trade_id or ""),
    )
