from __future__ import annotations

import logging
import threading
from pathlib import Path

from config.env import ENV
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations

logger = logging.getLogger("Database")

_LOCK = threading.Lock()
_INITIALIZED = False
_db_enabled_raw = ENV.get("BOT_STATE_DB_ENABLED", ENV.get("DB_ENABLED", "true"))
_DB_ENABLED = _db_enabled_raw.strip().lower() not in {"0", "false", "no", "off"}
_DB_PATH = (
    Path(ENV.get("BOT_STATE_DB", "data/bot_state.db"))
    if _DB_ENABLED
    else Path("/tmp/polymarket-copy-trading-bot-volatile.db")
)


def _connect():
    if not _DB_ENABLED and _DB_PATH.exists() and not _INITIALIZED:
        _DB_PATH.unlink()
    return create_connection(str(_DB_PATH))


def _init_schema() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return

    with _LOCK:
        if _INITIALIZED:
            return

        with _connect() as conn:
            apply_migrations(conn)
            conn.commit()

        _INITIALIZED = True


async def connect_db() -> None:
    """Bring the Postgres pool online and apply migrations.

    Order:
      1. Initialise the asyncpg pool (idempotent — second call is a no-op).
      2. Apply Postgres migrations under a single asyncpg transaction.
      3. Keep the SQLite path live as a transitional read-only fallback
         until PG-A4..A7 finish moving every repo over. Once
         storage/sqlite/ is deleted, the SQLite line + ``_init_schema``
         go away.
    """
    # --- Postgres path (primary going forward) ---
    try:
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
    except Exception as exc:  # pragma: no cover — bootstrap path
        # During the SQLite→PG transition, callers without a live PG
        # still want the SQLite legacy ready. Log loudly so an actual
        # PG misconfig is visible, then fall through to legacy init.
        logger.warning(
            "PostgreSQL init failed (transition mode — falling back to SQLite): %r",
            exc,
        )

    # --- Legacy SQLite path (removed in PG-A7) ---
    _init_schema()
    logger.info(
        "SQLite legacy ready (transition mode)",
        extra={
            "db_path": str(_DB_PATH),
            "persistent": _DB_ENABLED,
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


def get_client():
    _init_schema()
    return _connect()


def get_database():
    _init_schema()
    return _connect()


def get_db_path() -> str:
    _init_schema()
    return str(_DB_PATH)
