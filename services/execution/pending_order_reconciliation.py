"""Build canonical reconciliation records from orders-table submissions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class PendingOrderReader(Protocol):
    """Read a bounded per-user batch of nonterminal order submissions."""

    async def get_unconfirmed_order_submissions(
        self,
        *,
        user_id: str | None,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return oldest-first sanitized pending orders."""

        ...


class PendingOrderReconciliationQueryService:
    """Translate orders-table rows into concrete reconciler input records."""

    def __init__(
        self,
        repository: PendingOrderReader,
        *,
        max_batch_size: int = 200,
    ) -> None:
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be positive")
        self._repository = repository
        self._max_batch_size = int(max_batch_size)

    async def list_records(
        self,
        *,
        user_id: str | None,
        limit: int,
    ) -> tuple[tuple[str, dict[str, object]], ...]:
        """Return a bounded normalized batch, skipping malformed rows.

        Args:
            user_id: Runtime account scope, or ``None`` for a maintenance scan.
            limit: Requested maximum, bounded by the constructor policy.

        Returns:
            ``(side, record)`` pairs accepted by ``ReconciliationService``.

        Raises:
            ValueError: If ``limit`` falls outside the configured bound.
        """

        if limit < 1 or limit > self._max_batch_size:
            raise ValueError(f"limit must be between 1 and {self._max_batch_size}")
        rows = await self._repository.get_unconfirmed_order_submissions(
            user_id=user_id,
            limit=limit,
        )
        records: list[tuple[str, dict[str, object]]] = []
        for row in rows:
            normalized = self._record(row)
            if normalized is not None:
                records.append(normalized)
        return tuple(records)

    @staticmethod
    def _record(
        row: Mapping[str, object],
    ) -> tuple[str, dict[str, object]] | None:
        request_value = row.get("request")
        if not isinstance(request_value, Mapping):
            return None
        request = dict(request_value)
        side = str(request.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
        order_id = str(row.get("order_id") or "").strip()
        user_id = str(row.get("user_id") or request.get("user_id") or "").strip()
        leader_id = str(row.get("leader_id") or request.get("leader_id") or "").strip()
        token_id = str(request.get("token_id") or request.get("asset") or "").strip()
        condition_id = str(request.get("condition_id") or request.get("market_id") or "").strip()
        trade_key = str(request.get("signal_id") or row.get("idempotency_key") or order_id).strip()
        if not all((order_id, user_id, token_id, condition_id, trade_key)):
            return None
        return (
            side,
            {
                "trade_key": trade_key,
                "user_id": user_id,
                "leader_id": leader_id or None,
                "tx_hash": str(request.get("signal_id") or trade_key),
                "asset": token_id,
                "condition_id": condition_id,
                "order_id": order_id,
                "request": request,
                "status": row.get("status"),
            },
        )


__all__ = ["PendingOrderReconciliationQueryService"]
