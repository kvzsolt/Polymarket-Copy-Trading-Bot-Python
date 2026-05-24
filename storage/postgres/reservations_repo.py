"""Async asyncpg port of storage/sqlite/reservations_repo.py.

Behaviour invariants preserved verbatim:
  * BUY/SELL ``_checked`` capacity guards run SELECT-sum + INSERT
    inside one transaction with no intervening yield so two
    concurrent reservations cannot both observe enough headroom and
    both commit (Batch 3 atomic-capacity invariant). The SQLite
    version uses ``BEGIN IMMEDIATE``; PG equivalent is
    ``async with conn.transaction()`` with a serialised SELECT
    against the per-user / per-token slice. The duplicate-active
    intent_key unique index still surfaces collisions as
    asyncpg.UniqueViolationError (caller distinguishes that from
    over-capacity).
  * ``link_order`` keeps the three explicit UPDATE branches — no
    f-string SQL fragments — so a future caller passing a column
    name cannot enable injection (Batch 8 hardening).
  * ``mark_released`` writes the release reason into the jsonb
    ``metadata_json`` via ``jsonb_set`` (PG-native equivalent of
    SQLite's ``json_insert``).

Translation notes:
  * ``BEGIN IMMEDIATE`` → ``async with conn.transaction()`` — the
    transaction implicitly takes share-lock semantics; we explicitly
    SELECT the running-reservation slice once before the INSERT so
    the asyncpg driver round-trip serialises within the lock window.
    For the strict-capacity guarantee we wrap the SELECT-sum +
    INSERT in the transaction body; the unique active index on
    ``order_intent_key`` then catches any duplicate that slipped
    through.
  * ``CAST(strftime('%s','now') AS INTEGER)`` → ``EXTRACT(epoch FROM now())::bigint``.
  * ``json_insert`` → ``jsonb_set(metadata_json, '{release_reason}', to_jsonb($N::text), true)``.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

from domain.order_lifecycle import IntentReason, IntentSide, OrderIntent, Reservation


class ReservationsRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_active_by_intent_key(
        self, intent_key: str
    ) -> Reservation | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM order_reservations
                WHERE order_intent_key = $1 AND status = 'active'
                """,
                intent_key,
            )
        return self._row_to_reservation(row) if row else None

    async def create_buy_reservation(
        self, intent: OrderIntent, reserved_usdc: Decimal, expires_at: int
    ) -> Reservation:
        return await self._create_reservation(
            intent,
            reserved_usdc=reserved_usdc,
            reserved_size=Decimal("0"),
            expires_at=expires_at,
        )

    async def create_sell_reservation(
        self, intent: OrderIntent, reserved_size: Decimal, expires_at: int
    ) -> Reservation:
        return await self._create_reservation(
            intent,
            reserved_usdc=Decimal("0"),
            reserved_size=reserved_size,
            expires_at=expires_at,
        )

    async def create_buy_reservation_checked(
        self,
        intent: OrderIntent,
        reserved_usdc: Decimal,
        expires_at: int,
        available_balance: Decimal,
    ) -> Reservation | None:
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # greptile P1 + cubic P0/P1: serialise the check-and-
                # insert window with a transaction-scoped advisory lock
                # keyed on the user_id. Two concurrent BUY reservations
                # for the same user serialise here; the second observes
                # the first's already-committed-in-this-tx row when it
                # re-reads ``order_reservations``. Released at COMMIT
                # /ROLLBACK by Postgres automatically.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)",
                    f"buy_reservation:{intent.key.user_id}",
                )
                rows = await conn.fetch(
                    """
                    SELECT reserved_usdc FROM order_reservations
                    WHERE user_id = $1 AND side = 'BUY' AND status = 'active'
                    """,
                    intent.key.user_id,
                )
                already_reserved = self._sum_decimal_rows(rows, "reserved_usdc")
                if available_balance - already_reserved < reserved_usdc:
                    return None
                await self._insert_reservation(
                    conn,
                    res_id,
                    intent,
                    reserved_usdc,
                    Decimal("0"),
                    created_at,
                    expires_at,
                )
        return await self.get_by_id(res_id)

    async def create_sell_reservation_checked(
        self,
        intent: OrderIntent,
        reserved_size: Decimal,
        expires_at: int,
        held_size: Decimal,
    ) -> Reservation | None:
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # greptile P1: per-(user, condition, token) advisory
                # lock serialises concurrent SELL reservations against
                # the same held position so two cannot both pass the
                # held-size capacity check.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)",
                    f"sell_reservation:{intent.key.user_id}:"
                    f"{intent.key.condition_id}:{intent.key.token_id}",
                )
                rows = await conn.fetch(
                    """
                    SELECT reserved_size FROM order_reservations
                    WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                          AND side = 'SELL' AND status = 'active'
                    """,
                    intent.key.user_id,
                    intent.key.condition_id,
                    intent.key.token_id,
                )
                already_reserved = self._sum_decimal_rows(rows, "reserved_size")
                if held_size - already_reserved < reserved_size:
                    return None
                await self._insert_reservation(
                    conn,
                    res_id,
                    intent,
                    Decimal("0"),
                    reserved_size,
                    created_at,
                    expires_at,
                )
        return await self.get_by_id(res_id)

    async def _create_reservation(
        self,
        intent: OrderIntent,
        reserved_usdc: Decimal,
        reserved_size: Decimal,
        expires_at: int,
    ) -> Reservation:
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        async with self._pool.acquire() as conn:
            await self._insert_reservation(
                conn,
                res_id,
                intent,
                reserved_usdc,
                reserved_size,
                created_at,
                expires_at,
            )
        row = await self.get_by_id(res_id)
        if not row:
            raise RuntimeError(f"Failed to create reservation {res_id}")
        return row

    @staticmethod
    async def _insert_reservation(
        conn: asyncpg.Connection,
        res_id: str,
        intent: OrderIntent,
        reserved_usdc: Decimal,
        reserved_size: Decimal,
        created_at: int,
        expires_at: int,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO order_reservations (
                id, user_id, order_intent_key, condition_id, token_id, side, reason,
                reserved_usdc, reserved_size, status, created_at, updated_at, expires_at,
                order_id, venue_order_id, metadata_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13,
                NULL, NULL, $14::jsonb
            )
            """,
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
        )

    async def get_by_id(self, res_id: str) -> Reservation | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM order_reservations WHERE id = $1", res_id
            )
        return self._row_to_reservation(row) if row else None

    # Backward-compat alias used by older tests/helpers.
    async def _get_by_id(self, res_id: str) -> asyncpg.Record | None:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM order_reservations WHERE id = $1", res_id
            )

    async def list_expired_active(self, now_ts: int) -> list[Reservation]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM order_reservations
                WHERE status = 'active' AND expires_at < $1
                """,
                now_ts,
            )
        return [self._row_to_reservation(r) for r in rows]

    async def list_active(self) -> list[Reservation]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM order_reservations WHERE status = 'active'"
            )
        return [self._row_to_reservation(r) for r in rows]

    async def list_active_for_scope(
        self, user_id: str, condition_id: str, token_id: str
    ) -> list[Reservation]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM order_reservations
                WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                      AND status = 'active'
                """,
                user_id,
                condition_id,
                token_id,
            )
        return [self._row_to_reservation(r) for r in rows]

    async def sum_active_for_scope(
        self, user_id: str, condition_id: str, token_id: str, side: str
    ) -> tuple[Decimal, Decimal]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT reserved_usdc, reserved_size FROM order_reservations
                WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                      AND side = $4 AND status = 'active'
                """,
                user_id,
                condition_id,
                token_id,
                side,
            )
        return (
            self._sum_decimal_rows(rows, "reserved_usdc"),
            self._sum_decimal_rows(rows, "reserved_size"),
        )

    async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT reserved_usdc FROM order_reservations
                WHERE user_id = $1 AND side = 'BUY' AND status = 'active'
                """,
                user_id,
            )
        return self._sum_decimal_rows(rows, "reserved_usdc")

    @staticmethod
    def _sum_decimal_rows(
        rows: Iterable[asyncpg.Record], field: str
    ) -> Decimal:
        total = Decimal("0")
        for row in rows:
            total += Decimal(str(row[field] or "0"))
        return total

    async def link_order(
        self,
        reservation_id: str,
        *,
        order_id: str | None = None,
        venue_order_id: str | None = None,
    ) -> None:
        # Static-SQL cartesian — no f-string fragments. See Batch 8.
        if order_id is None and venue_order_id is None:
            return

        async with self._pool.acquire() as conn:
            if order_id is not None and venue_order_id is not None:
                await conn.execute(
                    "UPDATE order_reservations "
                    "SET updated_at = EXTRACT(epoch FROM now())::bigint, "
                    "    order_id = $1, venue_order_id = $2 "
                    "WHERE id = $3",
                    order_id,
                    venue_order_id,
                    reservation_id,
                )
            elif order_id is not None:
                await conn.execute(
                    "UPDATE order_reservations "
                    "SET updated_at = EXTRACT(epoch FROM now())::bigint, "
                    "    order_id = $1 "
                    "WHERE id = $2",
                    order_id,
                    reservation_id,
                )
            else:  # venue_order_id is not None
                await conn.execute(
                    "UPDATE order_reservations "
                    "SET updated_at = EXTRACT(epoch FROM now())::bigint, "
                    "    venue_order_id = $1 "
                    "WHERE id = $2",
                    venue_order_id,
                    reservation_id,
                )

    async def mark_released(
        self, reservation_id: str, reason: str, released_at: int
    ) -> None:
        async with self._pool.acquire() as conn:
            # jsonb_set with create-if-missing=true mirrors SQLite's
            # json_insert: insert the key only if absent. We coalesce
            # NULL → {} to handle rows that never wrote a payload.
            await conn.execute(
                """
                UPDATE order_reservations
                SET status = 'released',
                    updated_at = $1,
                    metadata_json = jsonb_set(
                        COALESCE(metadata_json, '{}'::jsonb),
                        '{release_reason}',
                        to_jsonb($2::text),
                        true
                    )
                WHERE id = $3 AND status = 'active'
                """,
                released_at,
                reason,
                reservation_id,
            )

    def _row_to_reservation(self, row: asyncpg.Record) -> Reservation:
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
