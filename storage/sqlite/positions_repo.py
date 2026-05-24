from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from domain.ports.repositories import AbstractPositionRepository
from storage.sqlite.connection import create_connection
from utils import is_valid_clob_token_id
from utils.logger import get_logger

logger = get_logger("PositionsRepo")


@dataclass
class PositionRecord:
    user_id: str
    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal


class PositionsRepo(AbstractPositionRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def list_open_positions(self, user_id: str) -> list[PositionRecord]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchall()

        out: list[PositionRecord] = []
        for row in rows:
            size = Decimal(str(row["size_tokens"] or "0"))
            if abs(size) <= Decimal("1e-12"):
                continue
            out.append(
                PositionRecord(
                    user_id=row["user_id"],
                    condition_id=row["condition_id"],
                    token_id=row["token_id"],
                    leader_id=row["leader_id"],
                    size_tokens=size,
                    avg_entry_price=Decimal(str(row["avg_entry_price"] or "0")),
                )
            )
        return out

    def get_position(self, user_id: str, condition_id: str, *, token_id: str | None = None) -> PositionRecord | None:
        with create_connection(self.db_path) as conn:
            return self._get_position_in_conn(conn, user_id, condition_id, token_id=token_id)

    @staticmethod
    def _get_position_in_conn(
        conn,
        user_id: str,
        condition_id: str,
        *,
        token_id: str | None = None,
    ) -> PositionRecord | None:
        """SELECT helper that uses an existing connection — caller owns the
        transaction. Required so apply_fill can run the SELECT + write
        inside one ``BEGIN IMMEDIATE`` for atomicity."""
        if token_id:
            row = conn.execute(
                """
                SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = ? AND condition_id = ? AND token_id = ?
                """,
                (user_id, condition_id, token_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = ? AND condition_id = ?
                ORDER BY token_id ASC, leader_id ASC
                LIMIT 1
                """,
                (user_id, condition_id),
            ).fetchone()

        if not row:
            return None

        return PositionRecord(
            user_id=row["user_id"],
            condition_id=row["condition_id"],
            token_id=row["token_id"],
            leader_id=row["leader_id"],
            size_tokens=Decimal(str(row["size_tokens"] or "0")),
            avg_entry_price=Decimal(str(row["avg_entry_price"] or "0")),
        )

    def list_positions_for_condition(self, user_id: str, condition_id: str) -> list[PositionRecord]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = ? AND condition_id = ?
                """,
                (user_id, condition_id),
            ).fetchall()

        out: list[PositionRecord] = []
        for row in rows:
            size = Decimal(str(row["size_tokens"] or "0"))
            if abs(size) <= Decimal("1e-12"):
                continue
            out.append(
                PositionRecord(
                    user_id=row["user_id"],
                    condition_id=row["condition_id"],
                    token_id=row["token_id"],
                    leader_id=row["leader_id"],
                    size_tokens=size,
                    avg_entry_price=Decimal(str(row["avg_entry_price"] or "0")),
                )
            )
        return out

    def upsert_position(self, record: PositionRecord) -> None:
        with create_connection(self.db_path) as conn:
            self._upsert_position_in_conn(conn, record)
            conn.commit()

    @staticmethod
    def _upsert_position_in_conn(conn, record: PositionRecord) -> None:
        """UPSERT helper that uses an existing connection — caller owns the
        transaction (see _get_position_in_conn for rationale)."""
        conn.execute(
            """
            INSERT INTO positions (
                user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                leader_id = excluded.leader_id,
                size_tokens = excluded.size_tokens,
                avg_entry_price = excluded.avg_entry_price,
                updated_at = excluded.updated_at
            """,
            (
                record.user_id,
                record.condition_id,
                record.token_id,
                record.leader_id,
                str(record.size_tokens),
                str(record.avg_entry_price),
                time.time(),
            ),
        )

    def delete_position(self, user_id: str, condition_id: str, *, token_id: str | None = None) -> None:
        with create_connection(self.db_path) as conn:
            self._delete_position_in_conn(conn, user_id, condition_id, token_id=token_id)
            conn.commit()

    @staticmethod
    def _delete_position_in_conn(
        conn, user_id: str, condition_id: str, *, token_id: str | None = None
    ) -> None:
        if token_id:
            conn.execute(
                "DELETE FROM positions WHERE user_id = ? AND condition_id = ? AND token_id = ?",
                (user_id, condition_id, token_id),
            )
        else:
            conn.execute(
                "DELETE FROM positions WHERE user_id = ? AND condition_id = ?",
                (user_id, condition_id),
            )

    def apply_fill(
        self,
        *,
        user_id: str,
        leader_id: str | None,
        condition_id: str,
        token_id: str,
        side: str,
        fill_size: Decimal,
        fill_price: Decimal,
    ) -> PositionRecord | None:
        if not is_valid_clob_token_id(token_id):
            raise ValueError(f"apply_fill: refusing invalid token_id={token_id!r}")

        side_u = str(side or "").upper()

        # Atomic SELECT + arithmetic + UPSERT/DELETE in a single connection
        # under BEGIN IMMEDIATE. Pre-fix the steps each opened/closed their
        # own connection, so two concurrent SELLs could both read the same
        # ``size`` snapshot and both upsert ``size - fill`` — the second
        # write silently overwrote the first (classic lost update).
        # BEGIN IMMEDIATE takes the writer lock up-front so the second
        # caller blocks until the first commits, then sees the updated row.
        with create_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                current = self._get_position_in_conn(
                    conn, user_id, condition_id, token_id=token_id
                )
                if current is None:
                    current = PositionRecord(
                        user_id=user_id,
                        condition_id=condition_id,
                        token_id=token_id,
                        leader_id=leader_id,
                        size_tokens=Decimal("0"),
                        avg_entry_price=Decimal("0"),
                    )

                if side_u == "BUY":
                    new_size = current.size_tokens + fill_size
                    if new_size > 0 and current.size_tokens <= 0:
                        avg = fill_price
                    elif new_size > 0:
                        avg = (
                            (current.size_tokens * current.avg_entry_price)
                            + (fill_size * fill_price)
                        ) / new_size
                    else:
                        avg = current.avg_entry_price
                    # CLAUDE.md immutability invariant: build a NEW record
                    # instead of mutating ``current`` in place, so a caller
                    # holding the pre-fill reference cannot observe
                    # half-mutated state if the UPSERT below raises.
                    updated = PositionRecord(
                        user_id=current.user_id,
                        condition_id=current.condition_id,
                        token_id=token_id or current.token_id,
                        leader_id=leader_id or current.leader_id,
                        size_tokens=new_size,
                        avg_entry_price=avg,
                    )
                    self._upsert_position_in_conn(conn, updated)
                    conn.commit()
                    return updated

                if side_u == "SELL":
                    # Clamp to held to prevent negative positions.
                    if fill_size > current.size_tokens:
                        logger.warning(
                            "sell_fill_exceeds_position_size user=%s condition=%s "
                            "token=%s fill_size=%s current_size=%s",
                            user_id,
                            condition_id,
                            token_id,
                            fill_size,
                            current.size_tokens,
                        )
                    effective_fill = min(fill_size, current.size_tokens)
                    new_size = current.size_tokens - effective_fill
                    if new_size <= Decimal("1e-12"):
                        self._delete_position_in_conn(
                            conn, user_id, condition_id, token_id=token_id
                        )
                        conn.commit()
                        return None
                    updated = PositionRecord(
                        user_id=current.user_id,
                        condition_id=current.condition_id,
                        token_id=current.token_id,
                        leader_id=current.leader_id,
                        size_tokens=new_size,
                        avg_entry_price=current.avg_entry_price,
                    )
                    self._upsert_position_in_conn(conn, updated)
                    conn.commit()
                    return updated

                # Unknown side: commit no-op and return current snapshot.
                conn.commit()
                return current
            except Exception:
                conn.rollback()
                raise

    def rebuild_user_positions_from_fills(self, user_id: str, fills_repo) -> list[PositionRecord]:
        fills = fills_repo.list_fills_for_user(user_id)

        with create_connection(self.db_path) as conn:
            conn.execute("DELETE FROM positions WHERE user_id = ?", (user_id,))
            conn.commit()

        for fill in fills:
            try:
                self.apply_fill(
                    user_id=user_id,
                    leader_id=fill.get("leader_id"),
                    condition_id=str(fill.get("condition_id") or ""),
                    token_id=str(fill.get("token_id") or ""),
                    side=str(fill.get("side") or ""),
                    fill_size=Decimal(str(fill.get("size_tokens") or "0")),
                    fill_price=Decimal(str(fill.get("price") or "0")),
                )
            except Exception:
                continue

        return self.list_open_positions(user_id)
