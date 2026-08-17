from __future__ import annotations

import logging

logger = logging.getLogger("Database")


async def connect_db() -> None:
    """Bring the Postgres pool online and apply migrations.

    Idempotent: a second call returns the existing pool (see
    ``storage.postgres.connection.init_pool``).
    """
    from config.app_config import load_config
    from storage.postgres.connection import init_pool
    from storage.postgres.migrations import apply_migrations as apply_pg_migrations

    cfg = load_config()
    pool = await init_pool(
        cfg.database.dsn,
        min_size=cfg.database.pool_min_size,
        max_size=cfg.database.pool_max_size,
        command_timeout_seconds=cfg.database.command_timeout_seconds,
    )
    async with pool.acquire() as conn:
        await apply_pg_migrations(conn)
    logger.info(
        "PostgreSQL ready",
        extra={
            "dsn_host": _safe_dsn_host(cfg.database.dsn),
            "pool_min": cfg.database.pool_min_size,
            "pool_max": cfg.database.pool_max_size,
        },
    )


def _safe_dsn_host(dsn: str) -> str:
    """Extract host:port for logging without leaking credentials."""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(dsn)
        host = parsed.hostname or ""
        port = parsed.port or ""
        return f"{host}:{port}" if port else host
    except Exception:
        return "<unparseable>"
