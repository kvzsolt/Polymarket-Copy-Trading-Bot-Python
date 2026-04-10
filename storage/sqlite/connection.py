from __future__ import annotations

import sqlite3
from pathlib import Path


def create_connection(path: str) -> sqlite3.Connection:
    is_memory = path == ":memory:"

    if not is_memory:
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connect_target = str(db_path)
    else:
        connect_target = path

    conn = sqlite3.connect(connect_target, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    if not is_memory:
        conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn
