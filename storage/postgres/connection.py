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

These are tuneable per-process via ``AppConfig.database`` fields.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger("PostgresPool")


# Single process-wide pool. asyncpg.Pool is already async-safe; the
# wrapping asyncio.Lock below only protects the lazy ``init_pool``
# happy-path against concurrent first-callers.
_pool: Optional[asyncpg.Pool] = None
_init_lock = asyncio.Lock()


async def init_pool(
    dsn: str,
    *,
    min_size: int = 2,
    max_size: int = 20,
    command_timeout_seconds: float = 30.0,
) -> asyncpg.Pool:
    """Lazily build the process pool and return it.

    Idempotent: a second call returns the same pool.  Raises
    ``RuntimeError`` if a previous call used a different DSN — the bot
    is single-pool by design.
    """
    global _pool

    if _pool is not None:
        # Defensive sanity: a second init with a different DSN is almost
        # certainly a test fixture or hot-reload bug.
        existing_dsn = getattr(_pool, "_polymarket_dsn", None)
        if existing_dsn and existing_dsn != dsn:
            raise RuntimeError(
                "init_pool called with a different DSN than the live pool; "
                "shutdown_pool() must run between dsn changes "
                f"(existing={existing_dsn!r}, new={dsn!r})"
            )
        return _pool

    async with _init_lock:
        if _pool is not None:
            return _pool
        pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=min_size,
            max_size=max_size,
            command_timeout=command_timeout_seconds,
        )
        # Stash the DSN on the pool object so re-init can verify equivalence.
        pool._polymarket_dsn = dsn  # type: ignore[attr-defined]
        _pool = pool
        logger.info(
            "asyncpg pool initialised | min=%s max=%s timeout=%ss",
            min_size,
            max_size,
            command_timeout_seconds,
        )
        return _pool


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
    global _pool
    if _pool is None:
        return
    pool = _pool
    _pool = None
    await pool.close()
    logger.info("asyncpg pool shut down")


def _reset_for_tests() -> None:
    """Hard-reset the module state. Test-only — tears down pool reference
    without closing (caller must close first to avoid leaking connections)."""
    global _pool
    _pool = None
