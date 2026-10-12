"""PostgreSQL adapter for the cross-process account execution lease.

One row per copier account in ``account_execution_leases`` guards that at
most ONE process is the active writer for that account. All mutations are
single atomic statements (the ``trading_control_repo`` / advisory-lock house
style — no check-then-mutate split):

* ``acquire`` is a conditional upsert: it wins ONLY when the row is free
  (gracefully released, or TTL-expired) or already held by the same process
  token. An active lease held by another process returns a REJECTED outcome
  carrying the live holder — the caller fails closed.
* ``renew`` extends ``expires_at`` only for the current holder token; a
  zero-row update means the lease was taken over (or released).
* ``release`` marks the row released; only the holder can release, once.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


class DatabasePool(Protocol):
    """Small async database surface required by this repository."""

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one database row."""


@dataclass(frozen=True, slots=True)
class LeaseHolderInfo:
    """The durable state of one account's lease row."""

    user_id: str
    holder_host: str
    holder_token: str
    mode: str
    acquired_at: datetime
    renewed_at: datetime
    expires_at: datetime
    released_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeaseAcquisition:
    """The outcome of one lease acquire attempt.

    Exactly one of:
    * ``acquired`` — this process holds the lease. ``takeover`` flags a win
      over an EXPIRED foreign lease (logged as a takeover warning);
      ``reacquired`` flags an idempotent same-process win. ``lease`` carries
      the new row.
    * not ``acquired`` — an ACTIVE lease is held by another process;
      ``held_by`` describes it so the caller can fail closed with the
      operator-visible holder.
    """

    acquired: bool
    takeover: bool = False
    reacquired: bool = False
    lease: LeaseHolderInfo | None = None
    held_by: LeaseHolderInfo | None = None


class AccountLeaseRepo:
    """Persist the per-account execution lease atomically."""

    _ACQUIRE_SQL = """
    WITH prev AS (
        SELECT holder_token AS old_token,
               expires_at   AS old_expires_at,
               released_at  AS old_released_at
          FROM account_execution_leases
         WHERE user_id = $1
    ),
    upserted AS (
        INSERT INTO account_execution_leases
            (user_id, holder_host, holder_token, mode,
             acquired_at, renewed_at, expires_at)
        VALUES ($1, $2, $3, $4, now(), now(), now() + $5::interval)
        ON CONFLICT (user_id) DO UPDATE
            SET holder_host  = EXCLUDED.holder_host,
                holder_token = EXCLUDED.holder_token,
                mode         = EXCLUDED.mode,
                acquired_at  = now(),
                renewed_at   = now(),
                expires_at   = EXCLUDED.expires_at,
                released_at  = NULL
            WHERE account_execution_leases.released_at IS NOT NULL
               OR account_execution_leases.expires_at <= now()
               OR account_execution_leases.holder_token = EXCLUDED.holder_token
        RETURNING user_id, holder_host, holder_token, mode,
                  acquired_at, renewed_at, expires_at, released_at,
                  (xmax = 0) AS inserted
    )
    SELECT u.user_id, u.holder_host, u.holder_token, u.mode,
           u.acquired_at, u.renewed_at, u.expires_at, u.released_at,
           u.inserted, p.old_token, p.old_released_at
      FROM upserted u LEFT JOIN prev p ON (p.old_token IS NOT NULL)
    """

    _RENEW_SQL = """
    UPDATE account_execution_leases
       SET renewed_at = now(),
           expires_at = now() + $3::interval
     WHERE user_id = $1
       AND holder_token = $2
       AND released_at IS NULL
    RETURNING expires_at
    """

    _RELEASE_SQL = """
    UPDATE account_execution_leases
       SET released_at = now(),
           renewed_at = now()
     WHERE user_id = $1
       AND holder_token = $2
       AND released_at IS NULL
    RETURNING 1
    """

    _GET_SQL = """
    SELECT user_id, holder_host, holder_token, mode,
           acquired_at, renewed_at, expires_at, released_at
      FROM account_execution_leases
     WHERE user_id = $1
    """

    def __init__(self, pool: DatabasePool) -> None:
        self._pool = pool

    async def acquire(
        self,
        *,
        user_id: str,
        holder_host: str,
        holder_token: str,
        mode: str,
        ttl_seconds: float,
    ) -> LeaseAcquisition:
        """Atomically acquire (or re-acquire) one account's lease."""
        row = await self._pool.fetchrow(
            self._ACQUIRE_SQL,
            user_id,
            holder_host,
            holder_token,
            mode,
            timedelta(seconds=float(ttl_seconds)),
        )
        if row is not None:
            lease = self._holder(row)
            inserted = bool(row.get("inserted"))
            old_token = row.get("old_token")
            old_released = row.get("old_released_at")
            reacquired = (
                not inserted
                and isinstance(old_token, str)
                and old_token == holder_token
            )
            # Takeover = we won an EXPIRED foreign lease that was never
            # released (crash recovery — logged as a warning). A gracefully
            # released row is a clean handoff, not a takeover.
            takeover = (
                not inserted
                and not reacquired
                and isinstance(old_token, str)
                and old_released is None
            )
            return LeaseAcquisition(
                acquired=True,
                takeover=takeover,
                reacquired=reacquired,
                lease=lease,
            )
        # The conditional upsert lost: an ACTIVE foreign lease holds the row.
        # Read it for the fail-closed error. A missing row here would mean a
        # concurrent release raced the read — treat it as a hard error rather
        # than silently assuming the lease is free.
        held = await self.get(user_id)
        if held is None:
            raise RuntimeError(
                "account lease acquire lost the race and the holder row "
                "disappeared; refusing to assume the lease is free"
            )
        return LeaseAcquisition(acquired=False, held_by=held)

    async def renew(
        self,
        *,
        user_id: str,
        holder_token: str,
        ttl_seconds: float,
    ) -> datetime | None:
        """Extend the lease TTL for the holder.

        Returns the new ``expires_at``, or ``None`` when this holder no
        longer owns the row (taken over or released) — a confirmed loss.
        """
        row = await self._pool.fetchrow(
            self._RENEW_SQL,
            user_id,
            holder_token,
            timedelta(seconds=float(ttl_seconds)),
        )
        if row is None:
            return None
        return self._datetime(row.get("expires_at"), "expires_at")

    async def release(self, *, user_id: str, holder_token: str) -> bool:
        """Gracefully release the lease. Only the holder, exactly once."""
        row = await self._pool.fetchrow(self._RELEASE_SQL, user_id, holder_token)
        return row is not None

    async def get(self, user_id: str) -> LeaseHolderInfo | None:
        """Return the current lease row, if any (observability)."""
        row = await self._pool.fetchrow(self._GET_SQL, user_id)
        if row is None:
            return None
        return self._holder(row)

    @staticmethod
    def _holder(row: Mapping[str, object]) -> LeaseHolderInfo:
        released_at = row.get("released_at")
        return LeaseHolderInfo(
            user_id=_text(row, "user_id"),
            holder_host=_text(row, "holder_host"),
            holder_token=_text(row, "holder_token"),
            mode=str(row.get("mode") or ""),
            acquired_at=AccountLeaseRepo._datetime(row.get("acquired_at"), "acquired_at"),
            renewed_at=AccountLeaseRepo._datetime(row.get("renewed_at"), "renewed_at"),
            expires_at=AccountLeaseRepo._datetime(row.get("expires_at"), "expires_at"),
            released_at=(
                AccountLeaseRepo._datetime(released_at, "released_at")
                if released_at is not None
                else None
            ),
        )

    @staticmethod
    def _datetime(value: object, name: str) -> datetime:
        if not isinstance(value, datetime):
            raise ValueError(f"account lease {name} must be a datetime")
        return value


def _text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"account lease {name} must be non-empty text")
    return value


__all__ = ["AccountLeaseRepo", "LeaseAcquisition", "LeaseHolderInfo"]
