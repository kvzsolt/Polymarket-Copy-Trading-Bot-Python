from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from storage.postgres.connection import get_pool
from utils.logger import get_logger

logger = get_logger("AccountingService")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _required_decimal_str(doc: dict[str, Any], key: str) -> str:
    """Coerce a money-shaped doc field to a Decimal-validated string.

    CLAUDE.md non-negotiable: a fill row in the accounting ledger must
    not contain a non-numeric string (the prior ``str(doc.get(key, ""))``
    happily persisted ``"None"`` or ``""`` when the upstream doc was
    incomplete, then the next numeric cast fell over downstream). We
    fail closed at insertion: garbage in raises before reaching SQL.
    """
    value = doc.get(key)
    if value is None or value == "":
        raise ValueError(f"book_fill missing required money field: {key!r}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            f"book_fill field {key!r} is not a valid decimal: {value!r}"
        ) from exc
    # Review fix (cubic P2 on PR #110): ``Decimal("NaN")`` and
    # ``Decimal("Infinity")`` parse without raising, so an upstream
    # ``float('inf')`` or string literal would slip past the try/except
    # above and land in the ledger. Reject non-finite values explicitly.
    if not parsed.is_finite():
        raise ValueError(
            f"book_fill field {key!r} must be a finite decimal: {value!r}"
        )
    return str(parsed)


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
    """Postgres-backed accounting ledger for fills, positions, and equity snapshots."""

    async def save_signal(self, signal: dict[str, Any]) -> None:
        signal_id = str(signal.get("signal_id") or "")
        if not signal_id:
            return
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO signals (signal_id, payload_json, created_at)
                VALUES ($1, $2::jsonb, $3)
                ON CONFLICT (signal_id) DO NOTHING
                """,
                signal_id,
                json.dumps(signal, separators=(",", ":"), default=str),
                _utc_now().timestamp(),
            )

    async def book_fill(self, *, user_id: str, fill_doc: dict[str, Any]) -> None:
        doc = dict(fill_doc)
        doc["user_id"] = user_id
        # Fail closed on missing or non-decimal money fields before any
        # SQL runs (SHP-10): we cannot let ``"None"`` or ``""`` land in
        # the accounting ledger and corrupt downstream PnL replay.
        size_tokens_str = _required_decimal_str(doc, "size_tokens")
        price_str = _required_decimal_str(doc, "price")
        usdc_value_str = _required_decimal_str(doc, "usdc_value")
        now = _utc_now().timestamp()
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO accounting_fills
                (user_id, order_id, condition_id, token_id, side, size_tokens, price, usdc_value,
                 leader_id, category, reason, payload_json, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13)
                """,
                user_id,
                doc.get("order_id"),
                doc.get("condition_id"),
                doc.get("token_id"),
                doc.get("side"),
                size_tokens_str,
                price_str,
                usdc_value_str,
                doc.get("leader_id"),
                doc.get("category"),
                doc.get("reason"),
                json.dumps(doc, separators=(",", ":"), default=str),
                now,
            )

    async def upsert_position(self, lot: PositionLot) -> None:
        now = _utc_now().timestamp()
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO positions_ledger
                (user_id, condition_id, token_id, size_tokens, avg_entry_price, leader_id, category, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                  size_tokens = excluded.size_tokens,
                  avg_entry_price = excluded.avg_entry_price,
                  leader_id = excluded.leader_id,
                  category = excluded.category,
                  updated_at = excluded.updated_at
                """,
                lot.user_id,
                lot.condition_id,
                lot.token_id,
                str(lot.size_tokens),
                str(lot.avg_entry_price),
                lot.leader_id,
                lot.category,
                now,
                now,
            )

    async def close_position(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str | None = None,
    ) -> None:
        """Delete one position row.

        ``token_id`` is required to delete a specific token's row; if omitted
        (legacy callers), the entire condition is deleted across both YES and
        NO outcomes. New callers must always pass ``token_id`` to avoid
        collapsing positions on binary markets.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            if token_id is None:
                await conn.execute(
                    "DELETE FROM positions_ledger WHERE user_id = $1 AND condition_id = $2",
                    user_id, condition_id,
                )
            else:
                await conn.execute(
                    "DELETE FROM positions_ledger "
                    "WHERE user_id = $1 AND condition_id = $2 AND token_id = $3",
                    user_id, condition_id, token_id,
                )

    async def snapshot_daily_equity(
        self,
        *,
        user_id: str,
        equity_usdc: Decimal,
        free_cash_usdc: Decimal,
        reserved_cash_usdc: Decimal = Decimal("0"),
    ) -> None:
        now = _utc_now()
        snapshot_date = now.date().isoformat()
        ts = now.timestamp()
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO daily_equity_snapshots
                (user_id, snapshot_date, equity_usdc, free_cash_usdc, reserved_cash_usdc, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT(user_id, snapshot_date) DO UPDATE SET
                  equity_usdc = excluded.equity_usdc,
                  free_cash_usdc = excluded.free_cash_usdc,
                  reserved_cash_usdc = excluded.reserved_cash_usdc,
                  updated_at = excluded.updated_at
                """,
                user_id,
                snapshot_date,
                str(equity_usdc),
                str(free_cash_usdc),
                str(reserved_cash_usdc),
                ts,
                ts,
            )
