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
_DB_PATH = Path(ENV.get("BOT_STATE_DB", "data/bot_state.db"))


def _connect():
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
    _init_schema()
    logger.info(f"SQLite ready at {_DB_PATH}")


def get_client():
    _init_schema()
    return _connect()


def get_database():
    _init_schema()
    return _connect()


def get_db_path() -> str:
    _init_schema()
    return str(_DB_PATH)
