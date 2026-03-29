from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from config.db import get_database
from utils.logger import get_logger

logger = get_logger("OrderRepository")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


class OrderRepository:
    """Mongo-backed order persistence with an in-memory fallback for local runs."""

    def __init__(self):
        self._db = get_database()
        self._mem_orders: dict[str, dict[str, Any]] = {}
        self._mem_fills: dict[str, dict[str, Any]] = {}
        if self._db is not None:
            self._orders = self._db["orders"]
            self._fills = self._db["fills"]
            self._ensure_indexes()
        else:
            self._orders = None
            self._fills = None

    def _ensure_indexes(self) -> None:
        try:
            self._orders.create_index("idempotency_key", unique=True)
            self._orders.create_index("order_id")
            self._orders.create_index("created_at")
            self._fills.create_index("order_id")
            self._fills.create_index("created_at")
        except Exception as exc:
            logger.warning(f"OrderRepository index setup warning: {exc}")

    def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        if self._orders is not None:
            return self._orders.find_one({"idempotency_key": key})
        return self._mem_orders.get(key)

    def save_submission(
        self,
        *,
        idempotency_key: str,
        request: dict[str, Any],
        response: dict[str, Any],
    ) -> None:
        doc = {
            "idempotency_key": idempotency_key,
            "request": request,
            "response": response,
            "order_id": response.get("orderID"),
            "created_at": _utc_now(),
            "status": response.get("status", "submitted"),
        }
        if self._orders is not None:
            self._orders.update_one(
                {"idempotency_key": idempotency_key},
                {"$setOnInsert": doc},
                upsert=True,
            )
        else:
            self._mem_orders.setdefault(idempotency_key, doc)

    def mark_fill(self, *, order_id: str, fill: dict[str, Any]) -> None:
        doc = {"order_id": order_id, "fill": fill, "created_at": _utc_now()}
        if self._fills is not None:
            self._fills.insert_one(doc)
        else:
            self._mem_fills[order_id] = doc
