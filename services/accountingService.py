from __future__ import annotations

import json
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
    """SQLite-backed accounting ledger for fills, positions, and equity snapshots."""

    def save_signal(self, signal: dict[str, Any]) -> None:
        signal_id = str(signal.get("signal_id") or "")
        if not signal_id:
            return
        with get_database() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO signals (signal_id, payload_json, created_at)
                VALUES (?, ?, ?)
                """,
                (
                    signal_id,
                    json.dumps(signal, separators=(",", ":"), default=str),
                    _utc_now().timestamp(),
                ),
            )
            conn.commit()

    def book_fill(self, *, user_id: str, fill_doc: dict[str, Any]) -> None:
        doc = dict(fill_doc)
        doc["user_id"] = user_id
        now = _utc_now().timestamp()
        with get_database() as conn:
            conn.execute(
                """
                INSERT INTO accounting_fills
                (user_id, order_id, condition_id, token_id, side, size_tokens, price, usdc_value,
                 leader_id, category, reason, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    doc.get("order_id"),
                    doc.get("condition_id"),
                    doc.get("token_id"),
                    doc.get("side"),
                    str(doc.get("size_tokens", "")),
                    str(doc.get("price", "")),
                    str(doc.get("usdc_value", "")),
                    doc.get("leader_id"),
                    doc.get("category"),
                    doc.get("reason"),
                    json.dumps(doc, separators=(",", ":"), default=str),
                    now,
                ),
            )
            conn.commit()

    def upsert_position(self, lot: PositionLot) -> None:
        now = _utc_now().timestamp()
        with get_database() as conn:
            conn.execute(
                """
                INSERT INTO positions_ledger
                (user_id, condition_id, token_id, size_tokens, avg_entry_price, leader_id, category, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, condition_id) DO UPDATE SET
                  token_id = excluded.token_id,
                  size_tokens = excluded.size_tokens,
                  avg_entry_price = excluded.avg_entry_price,
                  leader_id = excluded.leader_id,
                  category = excluded.category,
                  updated_at = excluded.updated_at
                """,
                (
                    lot.user_id,
                    lot.condition_id,
                    lot.token_id,
                    str(lot.size_tokens),
                    str(lot.avg_entry_price),
                    lot.leader_id,
                    lot.category,
                    now,
                    now,
                ),
            )
            conn.commit()

    def close_position(self, *, user_id: str, condition_id: str) -> None:
        with get_database() as conn:
            conn.execute(
                "DELETE FROM positions_ledger WHERE user_id = ? AND condition_id = ?",
                (user_id, condition_id),
            )
            conn.commit()

    def snapshot_daily_equity(
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
        with get_database() as conn:
            conn.execute(
                """
                INSERT INTO daily_equity_snapshots
                (user_id, snapshot_date, equity_usdc, free_cash_usdc, reserved_cash_usdc, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, snapshot_date) DO UPDATE SET
                  equity_usdc = excluded.equity_usdc,
                  free_cash_usdc = excluded.free_cash_usdc,
                  reserved_cash_usdc = excluded.reserved_cash_usdc,
                  updated_at = excluded.updated_at
                """,
                (
                    user_id,
                    snapshot_date,
                    str(equity_usdc),
                    str(free_cash_usdc),
                    str(reserved_cash_usdc),
                    ts,
                    ts,
                ),
            )
            conn.commit()

    def record_redemption_attempt(self, redemption_doc: dict[str, Any]) -> None:
        doc = dict(redemption_doc)
        user_id = str(doc.get("user_id") or "")
        condition_id = str(doc.get("condition_id") or "")
        attempt_id = str(doc.get("attempt_id") or "")
        if not user_id or not condition_id or not attempt_id:
            return
        with get_database() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO redemptions
                (user_id, condition_id, attempt_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    condition_id,
                    attempt_id,
                    json.dumps(doc, separators=(",", ":"), default=str),
                    float(doc.get("created_at") or _utc_now().timestamp()),
                ),
            )
            conn.commit()
