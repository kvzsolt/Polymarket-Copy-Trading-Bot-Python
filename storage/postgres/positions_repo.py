"""Async asyncpg port of storage/sqlite/positions_repo.py.

Behaviour invariants preserved verbatim:
  * ``apply_fill`` runs SELECT + arithmetic + UPSERT/DELETE inside one
    transaction with ``SELECT ... FOR UPDATE`` on the existing row so
    two concurrent SELLs cannot lose-update (review Batch 3 — the
    SQLite version uses ``BEGIN IMMEDIATE`` for the same purpose).
  * Immutability: BUY/SELL build a NEW ``PositionRecord`` instead of
    mutating ``current`` in place — a caller holding the pre-fill
    reference cannot observe half-mutated state if the UPSERT raises
    (CLAUDE.md mandate).
  * Sub-dust positions (``abs(size) <= 1e-12``) are filtered out of
    ``list_*`` and collapsed-to-DELETE inside SELL.
  * SELL clamps ``fill_size`` to held to prevent negative positions
    and logs ``sell_fill_exceeds_position_size`` when clamping
    activates.

Translation notes:
  * SQLite ``BEGIN IMMEDIATE`` → asyncpg
    ``async with conn.transaction()`` + explicit
    ``SELECT ... FOR UPDATE`` on the positions row read inside the
    transaction. The transaction context manager handles
    commit/rollback exactly as the legacy try/except/rollback block.
  * Parameter style ``?`` → ``$N``.
  * ``time.time()`` is still wall-clock; the ``updated_at`` column is
    ``timestamptz`` so asyncpg auto-converts the float to a TS
    (asyncpg handles ``datetime`` + accepts unix-epoch via
    ``to_timestamp($N)``); we pass ``datetime.now(timezone.utc)`` to
    keep the wire-type explicit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

from domain.ports.repositories import AbstractPositionRepository
from utils import is_valid_clob_token_id
from utils.logger import get_logger

logger = get_logger("PositionsRepo")


# DL-05: SHA-256-based 64-bit advisory-lock slot. Mirrors the constant in
# reservations_repo.py so collisions stay symmetric across both repos.
_ADVISORY_LOCK_SQL = (
    "SELECT pg_advisory_xact_lock("
    "('x' || encode(substring(sha256($1::bytea), 1, 8), 'hex'))"
    "::bit(64)::bigint)"
)


def _lock_key(key: str) -> bytes:
    """Encode a lock key to bytes for the ``$1::bytea`` parameter.

    asyncpg does not implicitly cast ``str`` to ``bytea``; passing a bare
    string to a ``bytea`` param raises ``DataError: a bytes-like object is
    required, not 'str'``.  Encode as UTF-8 so Postgres receives the
    correct wire type.
    """
    return key.encode("utf-8")


@dataclass
class PositionRecord:
    user_id: str
    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal


def _row_to_record(row: asyncpg.Record) -> PositionRecord:
    return PositionRecord(
        user_id=row["user_id"],
        condition_id=row["condition_id"],
        token_id=row["token_id"],
        leader_id=row["leader_id"],
        size_tokens=Decimal(str(row["size_tokens"] or "0")),
        avg_entry_price=Decimal(str(row["avg_entry_price"] or "0")),
    )


class PositionsRepo(AbstractPositionRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def list_open_positions(self, user_id: str) -> list[PositionRecord]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, condition_id, token_id, leader_id,
                       size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = $1
                """,
                user_id,
            )
        out: list[PositionRecord] = []
        for row in rows:
            record = _row_to_record(row)
            if abs(record.size_tokens) <= Decimal("1e-12"):
                continue
            out.append(record)
        return out

    async def get_position(
        self,
        user_id: str,
        condition_id: str,
        *,
        token_id: str | None = None,
    ) -> PositionRecord | None:
        async with self._pool.acquire() as conn:
            return await self._get_position_in_conn(
                conn, user_id, condition_id, token_id=token_id
            )

    # DL-02: queries are static literal strings — no f-string suffix
    # splice. ``for_update`` selects between four pre-built variants so
    # the SQL planner cache stays hot AND a future caller passing a
    # tampered "for_update" cannot inject SQL fragments. The without-
    # token form uses ``ORDER BY ... LIMIT 1`` which can lock the wrong
    # row when multiple positions share ``(user_id, condition_id)``; the
    # guard below refuses to combine ``for_update=True`` with a missing
    # token_id rather than silently picking a row by alphabetical order.
    _SELECT_POSITION_WITH_TOKEN = """
        SELECT user_id, condition_id, token_id, leader_id,
               size_tokens, avg_entry_price
        FROM positions
        WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
    """
    _SELECT_POSITION_WITH_TOKEN_FOR_UPDATE = (
        _SELECT_POSITION_WITH_TOKEN + " FOR UPDATE"
    )
    _SELECT_POSITION_WITHOUT_TOKEN = """
        SELECT user_id, condition_id, token_id, leader_id,
               size_tokens, avg_entry_price
        FROM positions
        WHERE user_id = $1 AND condition_id = $2
        ORDER BY token_id ASC, leader_id ASC
        LIMIT 1
    """

    @staticmethod
    async def _get_position_in_conn(
        conn: asyncpg.Connection,
        user_id: str,
        condition_id: str,
        *,
        token_id: str | None = None,
        for_update: bool = False,
    ) -> PositionRecord | None:
        """SELECT helper that uses an existing connection — caller owns the
        transaction. ``for_update=True`` adds ``FOR UPDATE`` so apply_fill
        can serialise the read+write step against concurrent writers
        (Postgres equivalent of the SQLite BEGIN IMMEDIATE pattern).

        ``for_update=True`` requires an explicit ``token_id`` so the lock
        always lands on the intended row — the no-token fallback picks the
        first row by ``ORDER BY token_id ASC LIMIT 1`` which would
        otherwise lock an arbitrary opposite leg of a binary market.
        """
        if for_update and not token_id:
            raise ValueError(
                "_get_position_in_conn(for_update=True) requires an explicit "
                "token_id to avoid locking the wrong row when multiple "
                "positions share (user_id, condition_id)"
            )
        if token_id:
            query = (
                PositionsRepo._SELECT_POSITION_WITH_TOKEN_FOR_UPDATE
                if for_update
                else PositionsRepo._SELECT_POSITION_WITH_TOKEN
            )
            row = await conn.fetchrow(query, user_id, condition_id, token_id)
        else:
            row = await conn.fetchrow(
                PositionsRepo._SELECT_POSITION_WITHOUT_TOKEN,
                user_id,
                condition_id,
            )
        if not row:
            return None
        return _row_to_record(row)

    async def list_positions_for_condition(
        self, user_id: str, condition_id: str
    ) -> list[PositionRecord]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, condition_id, token_id, leader_id,
                       size_tokens, avg_entry_price
                FROM positions
                WHERE user_id = $1 AND condition_id = $2
                """,
                user_id,
                condition_id,
            )
        out: list[PositionRecord] = []
        for row in rows:
            record = _row_to_record(row)
            if abs(record.size_tokens) <= Decimal("1e-12"):
                continue
            out.append(record)
        return out

    async def upsert_position(self, record: PositionRecord) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_position_in_conn(conn, record)

    @staticmethod
    async def _upsert_position_in_conn(
        conn: asyncpg.Connection, record: PositionRecord
    ) -> None:
        await conn.execute(
            """
            INSERT INTO positions (
                user_id, condition_id, token_id, leader_id,
                size_tokens, avg_entry_price, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                leader_id       = excluded.leader_id,
                size_tokens     = excluded.size_tokens,
                avg_entry_price = excluded.avg_entry_price,
                updated_at      = excluded.updated_at
            """,
            record.user_id,
            record.condition_id,
            record.token_id,
            record.leader_id,
            str(record.size_tokens),
            str(record.avg_entry_price),
            datetime.now(timezone.utc),
        )

    async def delete_position(
        self,
        user_id: str,
        condition_id: str,
        *,
        token_id: str | None = None,
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await self._delete_position_in_conn(
                    conn, user_id, condition_id, token_id=token_id
                )

    @staticmethod
    async def _delete_position_in_conn(
        conn: asyncpg.Connection,
        user_id: str,
        condition_id: str,
        *,
        token_id: str | None = None,
    ) -> None:
        if token_id:
            await conn.execute(
                "DELETE FROM positions WHERE user_id = $1 AND condition_id = $2 AND token_id = $3",
                user_id,
                condition_id,
                token_id,
            )
        else:
            await conn.execute(
                "DELETE FROM positions WHERE user_id = $1 AND condition_id = $2",
                user_id,
                condition_id,
            )

    async def apply_fill(
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

        # Postgres equivalent of the SQLite BEGIN IMMEDIATE lock:
        # async with conn.transaction() opens a transaction; the
        # SELECT ... FOR UPDATE on the positions row blocks any
        # concurrent writer until commit. Together they serialise the
        # SELECT + arithmetic + UPSERT/DELETE so two concurrent SELLs
        # cannot lost-update. asyncpg's transaction CM rolls back on
        # exception — no explicit try/except/rollback needed.
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                return await self._apply_fill_in_conn(
                    conn,
                    user_id=user_id,
                    leader_id=leader_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    side=side,
                    fill_size=fill_size,
                    fill_price=fill_price,
                )

    async def _apply_fill_in_conn(
        self,
        conn,
        *,
        user_id: str,
        leader_id: str | None,
        condition_id: str,
        token_id: str,
        side: str,
        fill_size: Decimal,
        fill_price: Decimal,
    ) -> PositionRecord | None:
        """In-conn apply_fill so callers (rebuild, batch flows) can
        compose multiple applies inside a single outer transaction."""
        side_u = str(side or "").upper()

        # cubic P1: ``SELECT ... FOR UPDATE`` cannot lock a row that
        # doesn't exist yet, so two concurrent first-BUY transactions
        # for the same ``(user, condition, token)`` both observe "no
        # row" and both compute their position from zero. The second
        # commit's ``ON CONFLICT DO UPDATE`` then overwrites the first
        # with stale arithmetic. A transaction-scoped advisory lock
        # serialises the gap-lock window so the second caller waits
        # until the first ``ON CONFLICT`` row is visible.
        await conn.execute(
            _ADVISORY_LOCK_SQL,
            _lock_key(f"position:{user_id}:{condition_id}:{token_id}"),
        )

        current = await self._get_position_in_conn(
            conn,
            user_id,
            condition_id,
            token_id=token_id,
            for_update=True,
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
            # CLAUDE.md immutability invariant: new PositionRecord
            # instead of mutating ``current`` in place.
            updated = PositionRecord(
                user_id=current.user_id,
                condition_id=current.condition_id,
                token_id=token_id or current.token_id,
                leader_id=leader_id or current.leader_id,
                size_tokens=new_size,
                avg_entry_price=avg,
            )
            await self._upsert_position_in_conn(conn, updated)
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
                await self._delete_position_in_conn(
                    conn, user_id, condition_id, token_id=token_id
                )
                return None
            updated = PositionRecord(
                user_id=current.user_id,
                condition_id=current.condition_id,
                token_id=current.token_id,
                leader_id=current.leader_id,
                size_tokens=new_size,
                avg_entry_price=current.avg_entry_price,
            )
            await self._upsert_position_in_conn(conn, updated)
            return updated

        return current

    async def rebuild_user_positions_from_fills(
        self, user_id: str, fills_repo
    ) -> list[PositionRecord]:
        # greptile P1 + cubic P1: DELETE + replay must be atomic.
        # Previously the DELETE committed in its own transaction and
        # the replay loop ran outside any transaction — a crash between
        # the two left the user with no positions until a manual
        # rebuild. Wrap both in a single ``conn.transaction()`` so
        # either the entire rebuild succeeds or the original positions
        # remain.
        #
        # cubic P2: the bare ``except Exception: continue`` previously
        # swallowed connection timeouts and pool exhaustion silently.
        # We now distinguish skip-able bad-fill records (ValueError /
        # InvalidOperation from Decimal parsing, invalid token_id) from
        # infrastructure failures (everything else), which propagate so
        # the caller sees the rebuild failed.
        from decimal import InvalidOperation

        fills = await fills_repo.list_fills_for_user(user_id)
        bad_fill_count = 0
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM positions WHERE user_id = $1", user_id
                )
                for fill in fills:
                    try:
                        await self._apply_fill_in_conn(
                            conn,
                            user_id=user_id,
                            leader_id=fill.get("leader_id"),
                            condition_id=str(fill.get("condition_id") or ""),
                            token_id=str(fill.get("token_id") or ""),
                            side=str(fill.get("side") or ""),
                            fill_size=Decimal(str(fill.get("size_tokens") or "0")),
                            fill_price=Decimal(str(fill.get("price") or "0")),
                        )
                    except (ValueError, InvalidOperation) as exc:
                        bad_fill_count += 1
                        logger.warning(
                            "rebuild_skipping_bad_fill user=%s fill=%s err=%s",
                            user_id,
                            fill,
                            exc,
                        )
                        continue
        if bad_fill_count:
            logger.info(
                "rebuild_user_positions_from_fills user=%s skipped=%s/%s bad fills",
                user_id,
                bad_fill_count,
                len(fills),
            )

        return await self.list_open_positions(user_id)
