"""Postgres schema — translated from storage/sqlite/migrations.py.

Filled in across PG-A3 (this commit lands an empty stub so config/db.py
imports succeed during the transition window). Every CREATE TABLE /
INDEX / ALTER gets ported with SQLite idioms swapped to Postgres
equivalents:

  INTEGER PRIMARY KEY AUTOINCREMENT  → BIGSERIAL PRIMARY KEY
  TIMESTAMP DEFAULT CURRENT_TIMESTAMP → timestamptz DEFAULT now()
  TEXT (storing JSON)                → jsonb
  json_extract(col, '$.x')           → col->>'x'
  json_insert / json_patch           → jsonb_set
  strftime('%s', 'now')              → extract(epoch from now())::bigint
  INSERT OR IGNORE                   → INSERT … ON CONFLICT DO NOTHING
  ON CONFLICT(k) DO UPDATE SET …     → identical
  PRAGMA …                           → drop (PG sets sane defaults)
  BEGIN IMMEDIATE                    → conn.transaction() +
                                       SELECT … FOR UPDATE on the
                                       contended row
  lastrowid / cur.rowcount           → INSERT … RETURNING id
"""

from __future__ import annotations

import asyncpg


async def apply_migrations(conn: asyncpg.Connection) -> None:
    """Apply the full Postgres schema migration atomically.

    PG-A3 fills the body. Until then this is a no-op so the transition
    fallback in config/db.py succeeds even before any Postgres schema
    is defined.
    """
    # PG-A3: wrap everything in ``async with conn.transaction():`` and
    # land every CREATE / INDEX / ALTER here.
    return None
