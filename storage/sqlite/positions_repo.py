from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from storage.sqlite.connection import create_connection
from utils import is_valid_clob_token_id


@dataclass
class PositionRecord:
    user_id: str
    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal


class PositionsRepo:
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

    def get_position(self, user_id: str, condition_id: str) -> PositionRecord | None:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = ? AND condition_id = ?
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

    def upsert_position(self, record: PositionRecord) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO positions (
                    user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, condition_id) DO UPDATE SET
                    token_id = excluded.token_id,
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
            conn.commit()

    def delete_position(self, user_id: str, condition_id: str) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                "DELETE FROM positions WHERE user_id = ? AND condition_id = ?",
                (user_id, condition_id),
            )
            conn.commit()

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

        current = self.get_position(user_id, condition_id)

        if current is None:
            current = PositionRecord(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                leader_id=leader_id,
                size_tokens=Decimal("0"),
                avg_entry_price=Decimal("0"),
            )

        side_u = str(side or "").upper()
        if side_u == "BUY":
            new_size = current.size_tokens + fill_size
            if new_size > 0:
                if current.size_tokens <= 0:
                    avg = fill_price
                else:
                    avg = (
                        (current.size_tokens * current.avg_entry_price)
                        + (fill_size * fill_price)
                    ) / new_size
                current.avg_entry_price = avg
            current.size_tokens = new_size
            current.leader_id = leader_id or current.leader_id
            current.token_id = token_id or current.token_id
            self.upsert_position(current)
            return current

        if side_u == "SELL":
            # Clamp to held amount to prevent negative positions
            effective_fill = min(fill_size, current.size_tokens)
            new_size = current.size_tokens - effective_fill
            if new_size <= Decimal("1e-12"):
                self.delete_position(user_id, condition_id)
                return None
            current.size_tokens = new_size
            self.upsert_position(current)
            return current

        # Unknown side: keep current untouched.
        return current

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
