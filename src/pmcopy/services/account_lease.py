"""Cross-process account lease manager (Task 21).

Owns the per-process half of the ``account_execution_leases`` protocol so
two processes configured for the same copier account can never BOTH be
active writers:

* startup ACQUIRES one lease per account before any execution enablement
  and fails closed while another live lease holds the row;
* a supervised heartbeat RENEWS every held lease; a confirmed loss (another
  process took the expired lease over) raises ``AccountLeaseLost`` so the
  supervisor fail-closes the process instead of limping along un-leased.
  Transient database errors are logged and retried ONLY inside the TTL
  window: the manager also enforces a LOCAL deadline on a monotonic clock —
  if no renewal has succeeded for ``ttl_seconds``, it raises
  ``AccountLeaseLost`` regardless of database reachability, closing the
  fail-open tail where an outage longer than the TTL would otherwise keep
  the process trading on a lease that has certainly expired server-side;
* graceful shutdown RELEASES every held lease for a clean handoff; a
  crashed holder's lease simply expires and the next process takes over
  with a logged takeover warning.

Timing contract: the TTL (default 60s) must comfortably outlive the renewal
interval (default 15s) so a handful of transient renewal failures never
expire a live holder's lease. One lease per account regardless of trading
mode — paper and live both write money-shaped rows (reservations, orders,
positions) to the shared database — the holder's mode is recorded for
observability only.
"""

from __future__ import annotations

import asyncio
import socket
import uuid
from datetime import datetime
from typing import Any

from pmcopy.adapters.clock import MonotonicClock, SystemMonotonicClock
from pmcopy.domain.account_identity import CopierAccountId
from pmcopy.storage.postgres.account_lease_repo import (
    AccountLeaseRepo,
    LeaseAcquisition,
)
from pmcopy.utils.logger import get_logger

DEFAULT_LEASE_TTL_SECONDS = 60.0
DEFAULT_RENEWAL_INTERVAL_SECONDS = 15.0

logger = get_logger("AccountLease")


class AccountLeaseLost(RuntimeError):
    """A held lease was taken over by another process; this one must stop."""


class AccountLeaseManager:
    """Acquire, renew, and release one lease per copier account."""

    def __init__(
        self,
        pool: Any = None,
        *,
        repo: Any = None,
        holder_host: str | None = None,
        process_token: str | None = None,
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        renewal_interval_seconds: float = DEFAULT_RENEWAL_INTERVAL_SECONDS,
        monotonic_clock: MonotonicClock | None = None,
    ) -> None:
        if repo is None and pool is None:
            raise ValueError("AccountLeaseManager requires a pool or a repo")
        if repo is None:
            repo = AccountLeaseRepo(pool)
        ttl = float(ttl_seconds)
        interval = float(renewal_interval_seconds)
        if ttl <= 0:
            raise ValueError("lease ttl_seconds must be positive")
        if interval <= 0:
            raise ValueError("lease renewal_interval_seconds must be positive")
        if interval >= ttl:
            raise ValueError(
                "lease renewal_interval_seconds must be shorter than the TTL "
                "so transient renewal failures never expire a live lease"
            )
        self._repo = repo
        self._ttl_seconds = ttl
        self.renewal_interval_seconds = interval
        self._monotonic: MonotonicClock = monotonic_clock or SystemMonotonicClock()
        self._holder_host = holder_host or socket.gethostname()
        self.process_token = process_token or uuid.uuid4().hex
        self._held: dict[str, CopierAccountId] = {}
        self._closed = False
        # Monotonic timestamp of the last SUCCESSFUL acquire/renew per held
        # account — the local fail-close deadline (see renew_held).
        self._last_success_monotonic: dict[str, float] = {}

    @property
    def holder_description(self) -> str:
        """Operator-visible holder identity (never a secret)."""
        return f"host={self._holder_host} token={self.process_token}"

    @property
    def held_accounts(self) -> tuple[CopierAccountId, ...]:
        return tuple(self._held.values())

    async def acquire(self, account: CopierAccountId, *, mode: str) -> LeaseAcquisition:
        """Acquire the lease for one account (idempotent per process)."""
        # Capture the attempt start so the recorded success time is never
        # LATER than the server-side ``now()`` that set the row's expiry —
        # the local deadline then fires at or before the true expiry.
        attempt_started = self._monotonic.monotonic()
        outcome = await self._repo.acquire(
            user_id=account.value,
            holder_host=self._holder_host,
            holder_token=self.process_token,
            mode=str(mode or ""),
            ttl_seconds=self._ttl_seconds,
        )
        if not outcome.acquired:
            held_by = outcome.held_by
            logger.warning(
                "account_lease_held_by_other | user=%s mode=%s holder_host=%s "
                "holder_expires_at=%s — refusing to start a second writer",
                account.value,
                mode,
                getattr(held_by, "holder_host", "?"),
                getattr(held_by, "expires_at", "?"),
            )
            return outcome
        self._held[account.value] = account
        self._last_success_monotonic[account.value] = attempt_started
        if outcome.takeover:
            logger.warning(
                "account_lease_takeover | user=%s mode=%s ttl_seconds=%s — "
                "previous holder's lease expired without release",
                account.value,
                mode,
                self._ttl_seconds,
            )
        elif outcome.reacquired:
            logger.info("account_lease_reacquired | user=%s mode=%s", account.value, mode)
        else:
            logger.info(
                "account_lease_acquired | user=%s mode=%s ttl_seconds=%s %s",
                account.value,
                mode,
                self._ttl_seconds,
                self.holder_description,
            )
        return outcome

    async def renew_held(self) -> dict[str, datetime]:
        """Renew every held lease; raise on a CONFIRMED loss.

        Fail-close ladder (strictest first):

        1. LOCAL TTL deadline — checked on a monotonic clock BEFORE any
           database contact. If no acquire/renew has succeeded for
           ``ttl_seconds`` for a held account, the server-side lease has
           certainly expired, so this process raises ``AccountLeaseLost``
           regardless of database reachability. Without this, an outage
           longer than the TTL (partition, pool exhaustion) would let the
           process keep trading un-leased forever on "transient" errors
           while a rival lawfully took the lease over.
        2. Zero-row renew — the database answered and another holder owns
           the row: a confirmed takeover loss.
        3. Transient database error — logged and retried on the next tick;
           only ever observed INSIDE the deadline window because of (1).

        The success timestamp is recorded from the attempt START, so the
        local deadline fires at or before the server-side expiry (the
        server's ``now()`` cannot precede the client's send time).
        """
        now = self._monotonic.monotonic()
        lapsed: list[str] = []
        for user_id in sorted(self._held):
            last_success = self._last_success_monotonic.get(user_id)
            # A missing timestamp is impossible state; fail closed on it.
            if last_success is None or now - last_success >= self._ttl_seconds:
                lapsed.append(user_id)
        if lapsed:
            logger.error(
                "account_lease_local_deadline_passed | users=%s ttl_seconds=%s "
                "— no successful renewal within the TTL; failing closed even "
                "if the database is unreachable",
                ",".join(lapsed),
                self._ttl_seconds,
            )
            raise AccountLeaseLost(
                "account lease local TTL deadline passed without a successful "
                "renewal for: " + ", ".join(lapsed)
            )

        renewed: dict[str, datetime] = {}
        lost: list[str] = []
        for user_id in sorted(self._held):
            attempt_started = self._monotonic.monotonic()
            try:
                expires_at = await self._repo.renew(
                    user_id=user_id,
                    holder_token=self.process_token,
                    ttl_seconds=self._ttl_seconds,
                )
            except Exception as exc:
                logger.warning(
                    "account_lease_renewal_transient_failed | user=%s error_type=%s "
                    "— retrying next tick",
                    user_id,
                    type(exc).__name__,
                )
                continue
            if expires_at is None:
                lost.append(user_id)
                continue
            self._last_success_monotonic[user_id] = attempt_started
            renewed[user_id] = expires_at
        if lost:
            raise AccountLeaseLost("account lease lost to another process for: " + ", ".join(lost))
        return renewed

    async def release_all(self) -> int:
        """Release every held lease; return how many were released."""
        released = 0
        for user_id in sorted(self._held):
            try:
                ok = await self._repo.release(user_id=user_id, holder_token=self.process_token)
            except Exception as exc:
                logger.warning(
                    "account_lease_release_failed | user=%s error_type=%s — "
                    "lease will expire by TTL",
                    user_id,
                    type(exc).__name__,
                )
                continue
            if ok:
                released += 1
                logger.info("account_lease_released | user=%s", user_id)
        self._held.clear()
        self._last_success_monotonic.clear()
        return released

    async def close(self) -> None:
        """Graceful-shutdown hook (client-registry compatible, idempotent)."""
        if self._closed:
            return
        self._closed = True
        await self.release_all()


async def account_lease_renewal_loop(manager: AccountLeaseManager) -> None:
    """Heartbeat every held lease until cancelled or a lease is lost.

    Registered as a supervised loop (``_Supervised`` in ``app.py``): a clean
    return is a self-disable, ``AccountLeaseLost`` rides the bounded-restart
    machinery and, on a confirmed loss, the loss keeps re-raising until the
    supervisor fail-closes the process.
    """
    while True:
        await manager.renew_held()
        await asyncio.sleep(manager.renewal_interval_seconds)


__all__ = [
    "AccountLeaseLost",
    "AccountLeaseManager",
    "DEFAULT_LEASE_TTL_SECONDS",
    "DEFAULT_RENEWAL_INTERVAL_SECONDS",
    "account_lease_renewal_loop",
]
