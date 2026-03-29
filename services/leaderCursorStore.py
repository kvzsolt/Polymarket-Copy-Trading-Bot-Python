from __future__ import annotations

from datetime import datetime, timezone

from config.db import get_database
from utils.logger import get_logger

logger = get_logger("LeaderCursorStore")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


class LeaderCursorStore:
    def __init__(self):
        self._db = get_database()
        if self._db is not None:
            self._collection = self._db["leader_cursors"]
            try:
                self._collection.create_index("leader_wallet", unique=True)
            except Exception as exc:
                logger.warning(f"leader_cursors index warning: {exc}")
        else:
            self._collection = None
            self._mem: dict[str, float] = {}

    def get_last_timestamp(self, leader_wallet: str) -> float:
        key = str(leader_wallet or "").lower()
        if self._collection is not None:
            doc = self._collection.find_one({"leader_wallet": key})
            if not doc:
                return 0.0
            return float(doc.get("last_timestamp", 0.0) or 0.0)
        return float(self._mem.get(key, 0.0))

    def set_last_timestamp(self, leader_wallet: str, ts: float) -> None:
        key = str(leader_wallet or "").lower()
        if self._collection is not None:
            self._collection.update_one(
                {"leader_wallet": key},
                {"$set": {"last_timestamp": float(ts), "updated_at": _utc_now()}},
                upsert=True,
            )
            return
        self._mem[key] = float(ts)
