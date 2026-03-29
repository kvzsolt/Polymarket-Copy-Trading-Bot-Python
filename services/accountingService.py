from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from config.db import get_database
from utils.logger import get_logger

logger = get_logger("AccountingService")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class PositionLot:
    user_id: str
    condition_id: str
    token_id: str
    size_tokens: Decimal
    avg_entry_price: Decimal
    leader_id: str
    category: str


class AccountingService:
    """MongoDB-backed accounting ledger for fills, positions, and equity snapshots."""

    def __init__(self):
        self._db = get_database()
        if self._db is None:
            self._collections = {}
            logger.info("AccountingService running in no-db mode")
            return
        self._collections = {
            "users": self._db["users"],
            "leaders_followed": self._db["leaders_followed"],
            "signals": self._db["signals"],
            "orders": self._db["orders"],
            "fills": self._db["fills"],
            "positions": self._db["positions_ledger"],
            "daily_equity_snapshots": self._db["daily_equity_snapshots"],
            "redemptions": self._db["redemptions"],
        }
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._collections["signals"].create_index("signal_id", unique=True)
            self._collections["orders"].create_index("order_id")
            self._collections["fills"].create_index("order_id")
            self._collections["positions"].create_index([("user_id", 1), ("condition_id", 1)], unique=True)
            self._collections["daily_equity_snapshots"].create_index([("user_id", 1), ("snapshot_date", 1)], unique=True)
            self._collections["redemptions"].create_index([("user_id", 1), ("condition_id", 1), ("attempt_id", 1)], unique=True)
        except Exception as exc:
            logger.warning(f"Accounting index setup warning: {exc}")

    def _coll(self, name: str):
        return self._collections.get(name)

    def save_signal(self, signal: dict[str, Any]) -> None:
        coll = self._coll("signals")
        if coll is None:
            return
        coll.update_one({"signal_id": signal["signal_id"]}, {"$setOnInsert": signal}, upsert=True)

    def book_fill(self, *, user_id: str, fill_doc: dict[str, Any]) -> None:
        coll = self._coll("fills")
        if coll is None:
            return
        doc = dict(fill_doc)
        doc["user_id"] = user_id
        doc["created_at"] = _utc_now()
        coll.insert_one(doc)

    def upsert_position(self, lot: PositionLot) -> None:
        coll = self._coll("positions")
        if coll is None:
            return
        coll.update_one(
            {"user_id": lot.user_id, "condition_id": lot.condition_id},
            {
                "$set": {
                    "token_id": lot.token_id,
                    "size_tokens": str(lot.size_tokens),
                    "avg_entry_price": str(lot.avg_entry_price),
                    "leader_id": lot.leader_id,
                    "category": lot.category,
                    "updated_at": _utc_now(),
                },
                "$setOnInsert": {"created_at": _utc_now()},
            },
            upsert=True,
        )

    def close_position(self, *, user_id: str, condition_id: str) -> None:
        coll = self._coll("positions")
        if coll is None:
            return
        coll.delete_one({"user_id": user_id, "condition_id": condition_id})

    def snapshot_daily_equity(
        self,
        *,
        user_id: str,
        equity_usdc: Decimal,
        free_cash_usdc: Decimal,
        reserved_cash_usdc: Decimal = Decimal("0"),
    ) -> None:
        coll = self._coll("daily_equity_snapshots")
        if coll is None:
            return
        now = _utc_now()
        snapshot_date = now.date().isoformat()
        coll.update_one(
            {"user_id": user_id, "snapshot_date": snapshot_date},
            {
                "$set": {
                    "equity_usdc": str(equity_usdc),
                    "free_cash_usdc": str(free_cash_usdc),
                    "reserved_cash_usdc": str(reserved_cash_usdc),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def record_redemption_attempt(self, redemption_doc: dict[str, Any]) -> None:
        coll = self._coll("redemptions")
        if coll is None:
            return
        doc = dict(redemption_doc)
        doc.setdefault("created_at", _utc_now())
        coll.update_one(
            {
                "user_id": doc["user_id"],
                "condition_id": doc["condition_id"],
                "attempt_id": doc["attempt_id"],
            },
            {"$setOnInsert": doc},
            upsert=True,
        )
