import json
import sqlite3
import uuid
from decimal import Decimal
from typing import Any

from domain.order_lifecycle import OrderIntent, Reservation
from storage.sqlite.connection import create_connection


class ReservationsRepo:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def get_active_by_intent_key(self, intent_key: str) -> Reservation | None:
        with create_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM order_reservations
                WHERE order_intent_key = ? AND status = 'active'
                """,
                (intent_key,),
            ).fetchone()
            
            if not row:
                return None
                
            return self._row_to_reservation(row)

    def create_buy_reservation(self, intent: OrderIntent, reserved_usdc: Decimal, expires_at: int) -> Reservation:
        return self._create_reservation(intent, reserved_usdc=reserved_usdc, reserved_size=Decimal("0"), expires_at=expires_at)

    def create_sell_reservation(self, intent: OrderIntent, reserved_size: Decimal, expires_at: int) -> Reservation:
        return self._create_reservation(intent, reserved_usdc=Decimal("0"), reserved_size=reserved_size, expires_at=expires_at)

    def _create_reservation(self, intent: OrderIntent, reserved_usdc: Decimal, reserved_size: Decimal, expires_at: int) -> Reservation:
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        
        with create_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO order_reservations (
                    id, user_id, order_intent_key, condition_id, token_id, side, reason,
                    reserved_usdc, reserved_size, status, created_at, updated_at, expires_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    res_id,
                    intent.key.user_id,
                    intent.key.stable_key(),
                    intent.key.condition_id,
                    intent.key.token_id,
                    intent.key.side.value,
                    intent.key.reason.value,
                    str(reserved_usdc),
                    str(reserved_size),
                    "active",
                    created_at,
                    created_at,
                    expires_at,
                    json.dumps({"source_signal_id": intent.source_signal_id}),
                ),
            )
            conn.commit()
            
        row = self._get_by_id(res_id)
        if not row:
            raise RuntimeError(f"Failed to create reservation {res_id}")
        return self._row_to_reservation(row)

    def _get_by_id(self, res_id: str) -> sqlite3.Row | None:
        with create_connection(self._db_path) as conn:
            return conn.execute("SELECT * FROM order_reservations WHERE id = ?", (res_id,)).fetchone()

    def mark_released(self, reservation_id: str, reason: str, released_at: int) -> None:
        with create_connection(self._db_path) as conn:
            conn.execute(
                """
                UPDATE order_reservations
                SET status = 'released', updated_at = ?, metadata_json = json_insert(COALESCE(metadata_json, '{}'), '$.release_reason', ?)
                WHERE id = ? AND status = 'active'
                """,
                (released_at, reason, reservation_id),
            )
            conn.commit()

    def _row_to_reservation(self, row: sqlite3.Row) -> Reservation:
        from datetime import datetime, timezone
        from domain.order_lifecycle import IntentSide, IntentReason
        
        return Reservation(
            id=row["id"],
            user_id=row["user_id"],
            intent_key=row["order_intent_key"],
            condition_id=row["condition_id"],
            token_id=row["token_id"],
            side=IntentSide(row["side"]),
            reason=IntentReason(row["reason"]),
            reserved_usdc=Decimal(row["reserved_usdc"]),
            reserved_size=Decimal(row["reserved_size"]),
            status=row["status"],
            created_at=datetime.fromtimestamp(row["created_at"], tz=timezone.utc),
            expires_at=datetime.fromtimestamp(row["expires_at"], tz=timezone.utc),
        )
