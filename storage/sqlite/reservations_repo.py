import json
import sqlite3
import uuid
from collections.abc import Iterable
from decimal import Decimal

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
            return self._row_to_reservation(row) if row else None

    def create_buy_reservation(
        self, intent: OrderIntent, reserved_usdc: Decimal, expires_at: int
    ) -> Reservation:
        return self._create_reservation(
            intent, reserved_usdc=reserved_usdc, reserved_size=Decimal("0"), expires_at=expires_at
        )

    def create_sell_reservation(
        self, intent: OrderIntent, reserved_size: Decimal, expires_at: int
    ) -> Reservation:
        return self._create_reservation(
            intent, reserved_usdc=Decimal("0"), reserved_size=reserved_size, expires_at=expires_at
        )

    def create_buy_reservation_checked(
        self,
        intent: OrderIntent,
        reserved_usdc: Decimal,
        expires_at: int,
        available_balance: Decimal,
    ) -> Reservation | None:
        """Atomically check account-wide quote capacity and insert BUY reservation.

        Returns None if capacity is insufficient. Duplicate active intent keys still
        surface as sqlite3.IntegrityError via the unique active index.
        """
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        with create_connection(self._db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            already_reserved = self._sum_decimal_rows(
                conn.execute(
                    """
                    SELECT reserved_usdc FROM order_reservations
                    WHERE user_id = ? AND side = 'BUY' AND status = 'active'
                    """,
                    (intent.key.user_id,),
                ).fetchall(),
                "reserved_usdc",
            )
            if available_balance - already_reserved < reserved_usdc:
                conn.rollback()
                return None
            self._insert_reservation(
                conn, res_id, intent, reserved_usdc, Decimal("0"), created_at, expires_at
            )
            conn.commit()
        return self.get_by_id(res_id)

    def create_sell_reservation_checked(
        self,
        intent: OrderIntent,
        reserved_size: Decimal,
        expires_at: int,
        held_size: Decimal,
    ) -> Reservation | None:
        """Atomically check token-scoped share capacity and insert SELL reservation."""
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        with create_connection(self._db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            already_reserved = self._sum_decimal_rows(
                conn.execute(
                    """
                    SELECT reserved_size FROM order_reservations
                    WHERE user_id = ? AND condition_id = ? AND token_id = ?
                          AND side = 'SELL' AND status = 'active'
                    """,
                    (intent.key.user_id, intent.key.condition_id, intent.key.token_id),
                ).fetchall(),
                "reserved_size",
            )
            if held_size - already_reserved < reserved_size:
                conn.rollback()
                return None
            self._insert_reservation(
                conn, res_id, intent, Decimal("0"), reserved_size, created_at, expires_at
            )
            conn.commit()
        return self.get_by_id(res_id)

    def _create_reservation(
        self, intent: OrderIntent, reserved_usdc: Decimal, reserved_size: Decimal, expires_at: int
    ) -> Reservation:
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        with create_connection(self._db_path) as conn:
            self._insert_reservation(
                conn, res_id, intent, reserved_usdc, reserved_size, created_at, expires_at
            )
            conn.commit()
        row = self.get_by_id(res_id)
        if not row:
            raise RuntimeError(f"Failed to create reservation {res_id}")
        return row

    def _insert_reservation(
        self,
        conn: sqlite3.Connection,
        res_id: str,
        intent: OrderIntent,
        reserved_usdc: Decimal,
        reserved_size: Decimal,
        created_at: int,
        expires_at: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO order_reservations (
                id, user_id, order_intent_key, condition_id, token_id, side, reason,
                reserved_usdc, reserved_size, status, created_at, updated_at, expires_at,
                order_id, venue_order_id, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
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

    def get_by_id(self, res_id: str) -> Reservation | None:
        with create_connection(self._db_path) as conn:
            row = conn.execute(
                "SELECT * FROM order_reservations WHERE id = ?", (res_id,)
            ).fetchone()
            return self._row_to_reservation(row) if row else None

    # Backward-compatible private alias used by older tests/helpers.
    def _get_by_id(self, res_id: str) -> sqlite3.Row | None:
        with create_connection(self._db_path) as conn:
            return conn.execute(
                "SELECT * FROM order_reservations WHERE id = ?", (res_id,)
            ).fetchone()

    def list_expired_active(self, now_ts: int) -> list[Reservation]:
        with create_connection(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM order_reservations
                WHERE status = 'active' AND expires_at < ?
                """,
                (now_ts,),
            ).fetchall()
            return [self._row_to_reservation(row) for row in rows]

    def list_active(self) -> list[Reservation]:
        with create_connection(self._db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM order_reservations WHERE status = 'active'"
            ).fetchall()
            return [self._row_to_reservation(row) for row in rows]

    def list_active_for_scope(
        self, user_id: str, condition_id: str, token_id: str
    ) -> list[Reservation]:
        with create_connection(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM order_reservations
                WHERE user_id = ? AND condition_id = ? AND token_id = ?
                      AND status = 'active'
                """,
                (user_id, condition_id, token_id),
            ).fetchall()
            return [self._row_to_reservation(row) for row in rows]

    def sum_active_for_scope(
        self, user_id: str, condition_id: str, token_id: str, side: str
    ) -> tuple[Decimal, Decimal]:
        with create_connection(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT reserved_usdc, reserved_size FROM order_reservations
                WHERE user_id = ? AND condition_id = ? AND token_id = ?
                      AND side = ? AND status = 'active'
                """,
                (user_id, condition_id, token_id, side),
            ).fetchall()
        return (
            self._sum_decimal_rows(rows, "reserved_usdc"),
            self._sum_decimal_rows(rows, "reserved_size"),
        )

    def sum_active_quote_for_user(self, user_id: str) -> Decimal:
        with create_connection(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT reserved_usdc FROM order_reservations
                WHERE user_id = ? AND side = 'BUY' AND status = 'active'
                """,
                (user_id,),
            ).fetchall()
        return self._sum_decimal_rows(rows, "reserved_usdc")

    @staticmethod
    def _sum_decimal_rows(rows: Iterable[sqlite3.Row], field: str) -> Decimal:
        total = Decimal("0")
        for row in rows:
            total += Decimal(str(row[field] or "0"))
        return total

    def link_order(
        self, reservation_id: str, *, order_id: str | None = None, venue_order_id: str | None = None
    ) -> None:
        assignments: list[str] = ["updated_at = CAST(strftime('%s','now') AS INTEGER)"]
        params: list[str] = []
        if order_id is not None:
            assignments.append("order_id = ?")
            params.append(order_id)
        if venue_order_id is not None:
            assignments.append("venue_order_id = ?")
            params.append(venue_order_id)
        if len(assignments) == 1:
            return
        params.append(reservation_id)
        with create_connection(self._db_path) as conn:
            conn.execute(
                f"UPDATE order_reservations SET {', '.join(assignments)} WHERE id = ?",
                params,
            )
            conn.commit()

    def mark_released(self, reservation_id: str, reason: str, released_at: int) -> None:
        with create_connection(self._db_path) as conn:
            conn.execute(
                """
                UPDATE order_reservations
                SET status = 'released', updated_at = ?,
                    metadata_json = json_insert(COALESCE(metadata_json, '{}'), '$.release_reason', ?)
                WHERE id = ? AND status = 'active'
                """,
                (released_at, reason, reservation_id),
            )
            conn.commit()

    def _row_to_reservation(self, row: sqlite3.Row) -> Reservation:
        from datetime import datetime, timezone
        from domain.order_lifecycle import IntentReason, IntentSide

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
            order_id=row["order_id"],
            venue_order_id=row["venue_order_id"],
        )
