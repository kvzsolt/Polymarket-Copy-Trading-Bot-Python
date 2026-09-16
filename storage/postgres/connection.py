"""Asyncpg connection pool — the single canonical Postgres entry point.

Module-level singleton pool. Constructor sites (config/db.py + tests) call
``await init_pool(dsn, …)`` exactly once per process; subsequent callers
fetch via ``await get_pool()``. Graceful shutdown via ``await
shutdown_pool()`` (called from the app shutdown path).

Pool defaults mirror sane production settings:
* ``min_size=2`` — keep at least one warm connection for the metrics
  endpoint and one for the trade loop.
* ``max_size=20`` — comfortable headroom for 12 repos × concurrent risk
  + reconciliation loops without thrashing the venue/database.
* ``command_timeout=30s`` — hard ceiling on any single query so a
  pathological lock or runaway scan cannot hang the loop forever.
* ``acquire_timeout=10s`` (P2-b) — bound the pool ``acquire()`` wait so a
  caller on an EXHAUSTED pool fails fast (``TimeoutError``) instead of
  livelocking forever with no error. asyncpg's pool has no default acquire
  timeout, so the bound is wired in two layers: ``create_pool(timeout=…)``
  bounds connection establishment, and ``acquire()`` itself is wrapped to
  inject the default for the 100+ ``self._pool.acquire()`` call sites that
  pass no explicit timeout.
* ``statement_timeout=30s`` / ``idle_in_transaction_session_timeout=60s``
  (P2-b) — server-side guards so a hung query or an abandoned open
  transaction (holding a backend + its locks) is bounded by Postgres even
  if the client-side ``command_timeout`` is somehow bypassed. Applied via
  ``server_settings`` so every pooled connection inherits them.

These are tuneable per-process via ``AppConfig.database`` fields.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from typing import Optional

import asyncpg

logger = logging.getLogger("PostgresPool")


# P2-b — sane bounded defaults. asyncpg leaves all three unbounded by
# default (acquire waits forever; no server-side statement / idle-txn cap),
# which lets a pool-exhaustion or a hung-transaction turn into a silent
# livelock. These are deliberately conservative ceilings, not normal-path
# limits: a healthy query/acquire finishes in milliseconds.
_DEFAULT_ACQUIRE_TIMEOUT_SECONDS: float = 10.0
_DEFAULT_STATEMENT_TIMEOUT_SECONDS: float = 30.0
_DEFAULT_IDLE_IN_TX_TIMEOUT_SECONDS: float = 60.0

# The acquire timeout the live singleton pool was built with. Read by
# ``_BoundedAcquirePool.acquire`` (below) for callers that pass no explicit
# timeout. Module-level rather than per-instance because the bot is
# single-pool by design and ``asyncpg.Pool`` is slotted + non-weakref-able,
# so there is nowhere on the instance to stash it.
_active_acquire_timeout: float = _DEFAULT_ACQUIRE_TIMEOUT_SECONDS


class _BoundedAcquirePool(asyncpg.Pool):
    """``asyncpg.Pool`` whose ``acquire()`` defaults to a bounded timeout.

    P2-b — asyncpg's ``Pool.acquire(*, timeout=None)`` waits forever when no
    timeout is given, so a caller on an exhausted pool livelocks silently.
    Roughly 100 ``self._pool.acquire()`` sites across the repos pass no
    timeout; rather than touch every one we re-home the pool's class to this
    subclass (empty ``__slots__`` keeps the memory layout identical, which is
    what makes the ``__class__`` swap legal on a slotted base) and inject the
    module-level default. An explicit ``acquire(timeout=…)`` is always honoured.
    """

    __slots__ = ()

    def acquire(self, *, timeout: Optional[float] = None):  # type: ignore[override]
        if timeout is None:
            timeout = _active_acquire_timeout
        return super().acquire(timeout=timeout)


# Single process-wide pool. asyncpg.Pool is already async-safe; the
# wrapping asyncio.Lock below only protects the lazy ``init_pool``
# happy-path against concurrent first-callers.
_pool: Optional[asyncpg.Pool] = None
_pool_dsn: Optional[str] = None
_pool_configuration: _PoolConfiguration | None = None
# INF-02: lazy lock init. ``asyncio.Lock()`` at module import time binds
# to whatever event loop is active at first import; in test environments
# that imported this module before pytest-asyncio created the loop the
# lock could end up bound to a closed / wrong loop. Build on first
# ``init_pool`` call instead, mirroring the lazy ``_pool`` pattern below.
_init_lock: Optional[asyncio.Lock] = None


@dataclass(frozen=True)
class _PoolConfiguration:
    """Validated singleton identity; credentials are excluded from representation."""

    dsn: str = field(repr=False)
    min_size: int
    max_size: int
    timeouts: tuple[float, ...]

    def __post_init__(self) -> None:
        if any(
            isinstance(value, bool) or not math.isfinite(value) or value <= 0
            for value in self.timeouts
        ):
            raise ValueError("Pool timeout values must be positive and finite")
        if (
            isinstance(self.min_size, bool)
            or isinstance(self.max_size, bool)
            or not isinstance(self.min_size, int)
            or not isinstance(self.max_size, int)
            or not 0 <= self.min_size <= self.max_size
            or self.max_size < 1
        ):
            raise ValueError("Pool sizes must be integers with 0 <= min_size <= max_size")

    def require_match(self, active: _PoolConfiguration | None) -> None:
        """Reject configuration drift without disclosing either DSN."""
        if active != self:
            raise RuntimeError(
                "init_pool called with different configuration; "
                "shutdown_pool() must run before configuration changes"
            )


async def init_pool(
    dsn: str,
    *,
    min_size: int = 2,
    max_size: int = 20,
    command_timeout_seconds: float = 30.0,
    acquire_timeout_seconds: float = _DEFAULT_ACQUIRE_TIMEOUT_SECONDS,
    statement_timeout_seconds: float = _DEFAULT_STATEMENT_TIMEOUT_SECONDS,
    idle_in_transaction_timeout_seconds: float = _DEFAULT_IDLE_IN_TX_TIMEOUT_SECONDS,
) -> asyncpg.Pool:
    """Lazily build the process pool and return it.

    Idempotent: a second call returns the same pool.  Raises
    ``RuntimeError`` if a previous call used a different DSN — the bot
    is single-pool by design.

    P2-b — bounded by construction:
    * ``acquire_timeout_seconds`` bounds the pool ``acquire()`` wait (no
      asyncpg default → would otherwise livelock under exhaustion). Wired
      both as ``create_pool(timeout=…)`` (connection establishment) and as
      a default injected into ``pool.acquire()`` for the call sites that
      pass none.
    * ``statement_timeout_seconds`` / ``idle_in_transaction_timeout_seconds``
      become Postgres ``server_settings`` GUCs so a hung query or abandoned
      open transaction is bounded server-side.
    """
    global _pool, _pool_dsn, _pool_configuration, _init_lock

    configuration = _PoolConfiguration(
        dsn,
        min_size,
        max_size,
        (
            command_timeout_seconds,
            acquire_timeout_seconds,
            statement_timeout_seconds,
            idle_in_transaction_timeout_seconds,
        ),
    )

    if _pool is not None:
        configuration.require_match(_pool_configuration)
        return _pool

    if _init_lock is None:
        _init_lock = asyncio.Lock()
    async with _init_lock:
        if _pool is not None:
            configuration.require_match(_pool_configuration)
            return _pool
        # Postgres GUCs are millisecond integers expressed as strings.
        server_settings = {
            "statement_timeout": str(int(statement_timeout_seconds * 1000)),
            "idle_in_transaction_session_timeout": str(
                int(idle_in_transaction_timeout_seconds * 1000)
            ),
        }
        try:
            pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=min_size,
                max_size=max_size,
                command_timeout=command_timeout_seconds,
                timeout=acquire_timeout_seconds,
                server_settings=server_settings,
            )
        except Exception:
            raise RuntimeError("PostgreSQL pool initialization failed") from None
        try:
            _install_default_acquire_timeout(pool, acquire_timeout_seconds)
        except Exception:
            try:
                await pool.close()
            except Exception:
                raise RuntimeError("Could not initialize bounded PostgreSQL pool") from None
            raise RuntimeError("Could not initialize bounded PostgreSQL pool") from None
        _pool = pool
        _pool_dsn = dsn
        _pool_configuration = configuration
        logger.info(
            "asyncpg pool initialised | min=%s max=%s command_timeout=%ss "
            "acquire_timeout=%ss statement_timeout=%ss idle_in_tx=%ss",
            min_size,
            max_size,
            command_timeout_seconds,
            acquire_timeout_seconds,
            statement_timeout_seconds,
            idle_in_transaction_timeout_seconds,
        )
        return _pool


def _install_default_acquire_timeout(pool: asyncpg.Pool, default_timeout: float) -> None:
    """Re-home ``pool`` to :class:`_BoundedAcquirePool` and record the default
    acquire timeout (P2-b).

    ``asyncpg.Pool`` is slotted and not weak-referenceable, so the timeout
    cannot live on the instance; it is stored module-side (single-pool bot).
    Skips gracefully if handed something that is not a stock ``asyncpg.Pool``
    (e.g. a test fake or an already-bounded pool) — the only hard requirement
    is that the timeout default is recorded.
    """
    global _active_acquire_timeout
    _active_acquire_timeout = default_timeout
    if type(pool) is asyncpg.Pool:
        try:
            pool.__class__ = _BoundedAcquirePool
        except TypeError:  # pragma: no cover - layout guard, should not happen
            raise RuntimeError("Could not install bounded pool acquisition") from None


async def get_pool() -> asyncpg.Pool:
    """Return the live pool. Raises ``RuntimeError`` if uninitialised.

    Fail-closed: the bot must not run repo queries before
    ``config.db.connect_db()`` has set up the pool.
    """
    if _pool is None:
        raise RuntimeError(
            "asyncpg pool not initialised — call config.db.connect_db() at "
            "startup before any repo access"
        )
    return _pool


async def shutdown_pool() -> None:
    """Close the pool and clear the singleton. Idempotent."""
    global _pool, _pool_dsn, _pool_configuration, _init_lock
    if _pool is None:
        return
    pool = _pool
    _pool = None
    _pool_dsn = None
    _pool_configuration = None
    _init_lock = None
    await pool.close()
    logger.info("asyncpg pool shut down")


def _reset_for_tests() -> None:
    """Hard-reset the module state. Test-only — tears down pool reference
    without closing (caller must close first to avoid leaking connections)."""
    global _pool, _pool_dsn, _pool_configuration, _init_lock
    _pool = None
    _pool_dsn = None
    _pool_configuration = None
    _init_lock = None
