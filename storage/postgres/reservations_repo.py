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

import hashlib
import json
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

from domain.order_lifecycle import IntentReason, IntentSide, OrderIntent, Reservation


# DL-05: Advisory lock slot is a signed bigint. Hash the key to SHA-256 in
# Python and take the first 8 bytes as a signed 64-bit int — uniform 2^64
# slot distribution, no DB-side hash CPU cost, no asyncpg type confusion
# (Python ``int`` maps natively to PG ``bigint``). Same shape used in
# positions_repo so collisions stay symmetric across both repos.
_ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock($1)"
type PostgresConnection = asyncpg.Connection | asyncpg.pool.PoolConnectionProxy


def _lock_key(key: str) -> int:
    """Hash a lock-key string to a signed 64-bit int for ``pg_advisory_xact_lock``.

    SHA-256 truncated to 8 bytes gives a uniform 2^64 slot distribution;
    asyncpg sends Python ``int`` directly as PG ``bigint`` — no encoding
    dance, no ``bytea`` cast surface.
    """
    digest = hashlib.sha256(key.encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, "big", signed=True)


class ReservationsRepo:
    """Persist collateral and share reservations in PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_active_by_intent_key(self, intent_key: str) -> Reservation | None:
        """Return active by intent key."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM order_reservations
                WHERE order_intent_key = $1 AND status = 'active'
                """,
                intent_key,
            )
        return self._row_to_reservation(row) if row else None

    async def get_active_by_order_id(self, order_id: str) -> Reservation | None:
        """Return the active reservation linked to a venue/local order id.

        Reconciliation usually receives only the venue ``order_id`` from the
        processed-trade row. The reservation id may live only in the
        ``order_reservations`` link columns, especially after restart or when a
        legacy processed row lacks ``reservation_id`` metadata.
        """
        if not order_id:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM order_reservations
                WHERE status = 'active'
                  AND (
                    order_id = $1
                    OR venue_order_id = $1
                    OR $1 = ANY(regexp_split_to_array(COALESCE(order_id, ''), '\\s*,\\s*'))
                    OR $1 = ANY(regexp_split_to_array(COALESCE(venue_order_id, ''), '\\s*,\\s*'))
                  )
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                order_id,
            )
        return self._row_to_reservation(row) if row else None

    async def create_buy_reservation(
        self, intent: OrderIntent, reserved_usdc: Decimal, expires_at: int
    ) -> Reservation:
        """Create a quote-collateral reservation for a BUY intent."""
        return await self._create_reservation(
            intent,
            reserved_usdc=reserved_usdc,
            reserved_size=Decimal("0"),
            expires_at=expires_at,
        )

    async def create_sell_reservation(
        self, intent: OrderIntent, reserved_size: Decimal, expires_at: int
    ) -> Reservation:
        """Create an outcome-share reservation for a SELL intent."""
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
        """Create a BUY reservation after an atomic available-balance check."""
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
                    _ADVISORY_LOCK_SQL,
                    _lock_key(f"buy_reservation:{intent.key.user_id}"),
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

    async def create_buy_reservation_checked_by_intent(
        self,
        intent: OrderIntent,
        reserved_usdc: Decimal,
        expires_at: int,
        available_balance: Decimal,
    ) -> tuple["Reservation | None", "Reservation | None"]:
        """Atomic intent-key lookup + capacity check + insert.

        Returns ``(existing, new)``:
            * ``(active_row, None)`` — an active reservation for this
              ``intent_key`` already exists. Caller should treat as
              ``ReservationConflict``.
            * ``(None, None)`` — no active row but capacity insufficient.
            * ``(None, new_row)`` — created successfully.

        Closes the Greptile finding from PR #108: the previous
        ``get_active_by_intent_key`` + ``create_buy_reservation_checked``
        sequence read the active row on a separate pool connection,
        leaving a window in which the cleanup job could release an
        expired row and a concurrent caller could insert against a stale
        balance. This method serialises all three steps inside one
        ``conn.transaction()`` guarded by the same per-user advisory
        lock used by ``create_buy_reservation_checked``.
        """
        stable_key = intent.key.stable_key()
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    _ADVISORY_LOCK_SQL,
                    _lock_key(f"buy_reservation:{intent.key.user_id}"),
                )
                existing_row = await conn.fetchrow(
                    """
                    SELECT * FROM order_reservations
                    WHERE order_intent_key = $1 AND status = 'active'
                    FOR UPDATE
                    """,
                    stable_key,
                )
                if existing_row:
                    return self._row_to_reservation(existing_row), None
                rows = await conn.fetch(
                    """
                    SELECT reserved_usdc FROM order_reservations
                    WHERE user_id = $1 AND side = 'BUY' AND status = 'active'
                    """,
                    intent.key.user_id,
                )
                already_reserved = self._sum_decimal_rows(rows, "reserved_usdc")
                if available_balance - already_reserved < reserved_usdc:
                    return None, None
                await self._insert_reservation(
                    conn,
                    res_id,
                    intent,
                    reserved_usdc,
                    Decimal("0"),
                    created_at,
                    expires_at,
                )
        new_row = await self.get_by_id(res_id)
        return None, new_row

    async def create_sell_reservation_checked(
        self,
        intent: OrderIntent,
        reserved_size: Decimal,
        expires_at: int,
        held_size: Decimal,
    ) -> Reservation | None:
        """Create a SELL reservation after an atomic available-share check."""
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # greptile P1: per-(user, condition, token) advisory
                # lock serialises concurrent SELL reservations against
                # the same held position so two cannot both pass the
                # held-size capacity check.
                await conn.execute(
                    _ADVISORY_LOCK_SQL,
                    _lock_key(
                        f"sell_reservation:{intent.key.user_id}:"
                        f"{intent.key.condition_id}:{intent.key.token_id}"
                    ),
                )
                rows = await conn.fetch(
                    """
                    SELECT reserved_size FROM order_reservations
                    WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                          AND side = 'SELL' AND status = 'active'
                    FOR SHARE
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

    async def create_sell_reservation_checked_by_intent(
        self,
        intent: OrderIntent,
        reserved_size: Decimal,
        expires_at: int,
        held_size: Decimal,
        sell_fraction: Decimal | None = None,
        live_position_size: Decimal | None = None,
        min_order_size: Decimal | None = None,
        round_up_to_minimum: bool = False,
    ) -> tuple["Reservation | None", "Reservation | None"]:
        """SELL parity for ``create_buy_reservation_checked_by_intent``.

        Returns ``(existing, new)`` with the same conventions as the BUY
        variant: an active row for this intent key short-circuits with
        ``(active_row, None)``; insufficient capacity returns ``(None,
        None)``; success returns ``(None, new_row)``.

        Serialises the intent-key lookup, capacity SELECT and insert
        inside one ``conn.transaction()`` under the same per-(user,
        condition, token) advisory lock that ``create_sell_reservation_
        checked`` uses, closing the fast-path lookup race that the BUY
        variant already covers.
        """
        stable_key = intent.key.stable_key()
        res_id = str(uuid.uuid4())
        created_at = int(intent.created_at.timestamp())
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    _ADVISORY_LOCK_SQL,
                    _lock_key(
                        f"sell_reservation:{intent.key.user_id}:"
                        f"{intent.key.condition_id}:{intent.key.token_id}"
                    ),
                )
                existing_row = await conn.fetchrow(
                    """
                    SELECT * FROM order_reservations
                    WHERE order_intent_key = $1 AND status = 'active'
                    FOR UPDATE
                    """,
                    stable_key,
                )
                if existing_row:
                    return self._row_to_reservation(existing_row), None
                rows = await conn.fetch(
                    """
                    SELECT reserved_size FROM order_reservations
                    WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                          AND side = 'SELL' AND status = 'active'
                    FOR SHARE
                    """,
                    intent.key.user_id,
                    intent.key.condition_id,
                    intent.key.token_id,
                )
                already_reserved = self._sum_decimal_rows(rows, "reserved_size")
                capacity = held_size - already_reserved
                target_size = reserved_size
                if sell_fraction is not None:
                    if not sell_fraction.is_finite() or not (
                        Decimal("0") < sell_fraction <= Decimal("1")
                    ):
                        raise ValueError("sell_fraction must be finite and in (0, 1]")
                    if live_position_size is None or not live_position_size.is_finite():
                        raise ValueError(
                            "live_position_size is required for fractional SELL reservation"
                        )
                    free_size = min(
                        max(Decimal("0"), capacity),
                        max(Decimal("0"), live_position_size),
                    )
                    target_size = free_size * sell_fraction
                    if (
                        min_order_size is not None
                        and min_order_size > 0
                        and target_size < min_order_size
                    ):
                        target_size = (
                            min_order_size
                            if round_up_to_minimum and free_size >= min_order_size
                            else Decimal("0")
                        )
                    target_size = min(target_size, free_size)
                if target_size <= 0 or capacity < target_size:
                    return None, None
                await self._insert_reservation(
                    conn,
                    res_id,
                    intent,
                    Decimal("0"),
                    target_size,
                    created_at,
                    expires_at,
                )
        new_row = await self.get_by_id(res_id)
        return None, new_row

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
        conn: PostgresConnection,
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
        """Return by id."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM order_reservations WHERE id = $1", res_id)
        return self._row_to_reservation(row) if row else None

    # Backward-compat alias used by older tests/helpers.
    async def _get_by_id(self, res_id: str) -> asyncpg.Record | None:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM order_reservations WHERE id = $1", res_id)

    async def list_expired_active(
        self, now_ts: int, user_id: str | None = None
    ) -> list[Reservation]:
        """Return expired active."""
        query = """
            SELECT * FROM order_reservations
            WHERE status = 'active' AND expires_at < $1
        """
        params: list[object] = [now_ts]
        if user_id is not None:
            params.append(user_id)
            query += f" AND user_id = ${len(params)}"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        return [self._row_to_reservation(r) for r in rows]

    async def list_active(self) -> list[Reservation]:
        """Return active."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM order_reservations WHERE status = 'active'")
        return [self._row_to_reservation(r) for r in rows]

    async def list_active_for_scope(
        self, user_id: str, condition_id: str, token_id: str
    ) -> list[Reservation]:
        """Return active for scope."""
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
        """Return the total active for scope."""
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
        """Return the total active quote for user."""
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
    def _sum_decimal_rows(rows: Iterable[asyncpg.Record], field: str) -> Decimal:
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
        """Link order."""
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

    async def mark_released(self, reservation_id: str, reason: str, released_at: int) -> None:
        """Mark released."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Take the same per-scope advisory lock the capacity
                # checks use so a cleanup-job release cannot land between
                # an in-flight capacity SELECT and its INSERT. SELL uses
                # per-(user, condition, token); BUY uses per-user.
                meta = await conn.fetchrow(
                    """
                    SELECT user_id, condition_id, token_id, side
                    FROM order_reservations
                    WHERE id = $1
                    """,
                    reservation_id,
                )
                if meta is None:
                    return
                if meta["side"] == "SELL":
                    lock_key = (
                        f"sell_reservation:{meta['user_id']}:"
                        f"{meta['condition_id']}:{meta['token_id']}"
                    )
                else:
                    lock_key = f"buy_reservation:{meta['user_id']}"
                await conn.execute(
                    _ADVISORY_LOCK_SQL,
                    _lock_key(lock_key),
                )
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
