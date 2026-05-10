"""Release-gate: ``bot stale-locks list`` CLI (Issue #76).

Seeds two pending_lock orders — one fresh, one synthetically aged past TTL —
and asserts the listing only includes the aged row.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from bot_cli.commands.stale_locks import _list_stale_locks, run_stale_locks
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _seed(db_path: str) -> None:
    now_utc = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    old_ts = _iso(now_utc - timedelta(hours=1))
    fresh_ts = _iso(now_utc)
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.execute(
            """
            INSERT INTO orders
              (idempotency_key, user_id, leader_id, order_id, status,
               request_json, response_json, attempt_count, last_error,
               created_at, updated_at)
            VALUES
              ('k-old','u-test',NULL,NULL,'pending_lock','{}',NULL,0,NULL,?,?),
              ('k-fresh','u-test',NULL,NULL,'pending','{}',NULL,0,NULL,?,?)
            """,
            (old_ts, old_ts, fresh_ts, fresh_ts),
        )
        conn.commit()


def test_only_aged_locks_listed(tmp_path) -> None:
    db = tmp_path / "locks.db"
    _seed(str(db))
    rows = _list_stale_locks(str(db), ttl_seconds=120, user_id=None)
    assert len(rows) == 1
    assert rows[0]["idempotency_key"] == "k-old"
    assert rows[0]["age_seconds"] >= 120


def test_user_filter(tmp_path) -> None:
    db = tmp_path / "locks.db"
    _seed(str(db))
    other = _list_stale_locks(str(db), ttl_seconds=120, user_id="other")
    assert other == []


def test_run_stale_locks_emits_json(tmp_path, capsys, monkeypatch) -> None:
    db = tmp_path / "locks.db"
    _seed(str(db))
    fake_config = SimpleNamespace(
        database=SimpleNamespace(db_path=str(db)),
        execution=SimpleNamespace(pending_submission_ttl_seconds=120),
    )
    monkeypatch.setattr(
        "bot_cli.commands.stale_locks._load_config_or_fail",
        lambda: fake_config,
    )
    args = SimpleNamespace(
        stale_locks_subcommand="list",
        ttl_seconds=None,
        user_id=None,
    )
    rc = run_stale_locks(args)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["ttl_seconds"] == 120
