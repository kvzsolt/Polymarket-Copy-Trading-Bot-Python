from __future__ import annotations

from typing import Any

from config.db import get_db_path
from storage.sqlite.orders_repo import OrderSubmissionRecord, OrdersRepo
from utils.logger import get_logger

logger = get_logger("OrderRepository")


class OrderRepository:
    """Compatibility wrapper around storage.sqlite.orders_repo. """

    def __init__(self, db_path: str | None = None):
        self._repo = OrdersRepo(db_path or get_db_path())

    def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        return self._repo.get_by_idempotency_key(key)

    def try_acquire_submission_lock(
        self,
        key: str,
        user_id: str,
        leader_id: str | None,
        request_payload: dict[str, Any],
    ) -> bool:
        return self._repo.try_acquire_submission_lock(
            key=key,
            user_id=user_id,
            leader_id=leader_id,
            request_payload=request_payload,
        )

    def is_submission_locked(self, key: str) -> bool:
        return self._repo.is_submission_locked(key)

    def save_submission(
        self,
        *,
        idempotency_key: str,
        request: dict[str, Any],
        response: dict[str, Any],
    ) -> None:
        self._repo.save_submission_legacy(
            idempotency_key=idempotency_key,
            request=request,
            response=response,
        )

    def save_submission_record(self, record: OrderSubmissionRecord) -> None:
        self._repo.save_submission(record)

    def mark_fill(self, *, order_id: str, fill: dict[str, Any]) -> None:
        self._repo.mark_fill(order_id=order_id, fill=fill)

    def has_processed_trade(self, trade_key: str) -> bool:
        return self._repo.has_processed_trade(trade_key)

    def try_claim_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str = "in_progress",
        stale_claim_ttl_seconds: float = 300.0,
    ) -> bool:
        return self._repo.try_claim_processed_trade(
            trade_key=trade_key,
            user_id=user_id,
            leader_id=leader_id,
            tx_hash=tx_hash,
            side=side,
            asset=asset,
            condition_id=condition_id,
            status=status,
            stale_claim_ttl_seconds=stale_claim_ttl_seconds,
        )

    def get_processed_trade(self, trade_key: str) -> dict[str, Any] | None:
        return self._repo.get_processed_trade(trade_key)

    def mark_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str,
    ) -> None:
        self._repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=user_id,
            leader_id=leader_id,
            tx_hash=tx_hash,
            side=side,
            asset=asset,
            condition_id=condition_id,
            status=status,
        )
