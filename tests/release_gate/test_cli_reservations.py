"""Release-gate: ``bot reservations list`` CLI (Issue #76).

Seeds a per-test sqlite DB with two active reservations (one BUY, one SELL)
plus one released reservation, then asserts the CLI summary matches the
expected per-user totals and that released rows are excluded by default.
"""
from __future__ import annotations

import json
import time
from types import SimpleNamespace

from bot_cli.commands.reservations import (
    _list_reservations,
    _summarize,
    run_reservations,
)
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


def _seed(db_path: str) -> None:
    now = int(time.time())
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.execute(
            """
            INSERT INTO order_reservations
            (id, user_id, order_intent_key, condition_id, token_id, side, reason,
             reserved_usdc, reserved_size, status, created_at, updated_at, expires_at,
             order_id, venue_order_id, metadata_json)
            VALUES
            ('r1','u-test','k1','cond-x','tok-x','BUY','copy','25.50','0','active',?,?,?,NULL,NULL,'{}'),
            ('r2','u-test','k2','cond-x','tok-x','SELL','exit','0','7.0','active',?,?,?,NULL,NULL,'{}'),
            ('r3','u-test','k3','cond-x','tok-x','BUY','copy','100','0','released',?,?,?,NULL,NULL,'{}')
            """,
            (now, now, now + 600, now, now, now + 600, now, now, now + 600),
        )
        conn.commit()


def test_list_reservations_filters_released(tmp_path) -> None:
    db = tmp_path / "res.db"
    _seed(str(db))
    rows = _list_reservations(str(db), user_id=None, include_expired=False)
    assert len(rows) == 2
    assert {r["id"] for r in rows} == {"r1", "r2"}


def test_summary_per_user_totals(tmp_path) -> None:
    db = tmp_path / "res.db"
    _seed(str(db))
    rows = _list_reservations(str(db), user_id=None, include_expired=False)
    summary = _summarize(rows)
    assert summary["total_active"] == 2
    assert summary["total_buy_usdc"] == "25.50"
    assert summary["total_sell_size"] == "7.0"
    user_bucket = summary["per_user"]["u-test"]
    assert user_bucket["count"] == 2
    assert user_bucket["buy_usdc"] == "25.50"
    assert user_bucket["sell_size"] == "7.0"


def test_user_filter_isolates(tmp_path) -> None:
    db = tmp_path / "res.db"
    _seed(str(db))
    none_rows = _list_reservations(
        str(db), user_id="other-user", include_expired=False
    )
    assert none_rows == []


def test_run_reservations_invocation_exits_zero(tmp_path, capsys, monkeypatch) -> None:
    db = tmp_path / "res.db"
    _seed(str(db))

    fake_config = SimpleNamespace(
        database=SimpleNamespace(db_path=str(db)),
    )

    monkeypatch.setattr(
        "bot_cli.commands.reservations._load_config_or_fail",
        lambda: fake_config,
    )

    args = SimpleNamespace(
        reservations_subcommand="list",
        user_id=None,
        include_expired=False,
    )
    rc = run_reservations(args)
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["summary"]["total_active"] == 2
