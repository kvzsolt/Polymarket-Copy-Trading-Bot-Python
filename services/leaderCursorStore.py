from __future__ import annotations

import time

from config.db import get_database
from utils.logger import get_logger

logger = get_logger("LeaderCursorStore")


class LeaderCursorStore:
    def __init__(self):
        self._mem: dict[str, float] = {}

    def get_last_timestamp(self, leader_wallet: str) -> float:
        key = str(leader_wallet or "").lower()
        try:
            with get_database() as conn:
                row = conn.execute(
                    """
                    SELECT last_timestamp
                    FROM leader_cursors
                    WHERE leader_wallet = ?
                    LIMIT 1
                    """,
                    (key,),
                ).fetchone()
            if row:
                return float(row["last_timestamp"] or 0.0)
        except Exception as exc:
            logger.warning(f"Failed reading cursor for {key[:10]}…: {exc}")
        return float(self._mem.get(key, 0.0))

    def set_last_timestamp(self, leader_wallet: str, ts: float) -> None:
        key = str(leader_wallet or "").lower()
        try:
            with get_database() as conn:
                conn.execute(
                    """
                    INSERT INTO leader_cursors (leader_wallet, last_timestamp, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(leader_wallet) DO UPDATE SET
                      last_timestamp = excluded.last_timestamp,
                      updated_at = excluded.updated_at
                    """,
                    (key, float(ts), time.time()),
                )
                conn.commit()
            return
        except Exception as exc:
            logger.warning(f"Failed writing cursor for {key[:10]}…: {exc}")
        self._mem[key] = float(ts)
