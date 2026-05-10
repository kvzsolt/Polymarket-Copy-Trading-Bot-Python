"""bot stale-locks — list orders held in submission lock past TTL."""
from __future__ import annotations

import argparse
import asyncio
import json

from config.app_config import EnvValidationError, load_config
from config.env import rehydrate_env
from config.runtime_env import load_runtime_env
from storage.sqlite.connection import create_connection


_LOCKED_STATUSES = (
    "pending_lock",
    "submitting",
    "pending",
    "submitted_unconfirmed",
    "delayed",
)


def register_stale_locks_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "stale-locks", help="List orders stuck in submission lock past TTL"
    )
    sub = parser.add_subparsers(dest="stale_locks_subcommand", required=True)

    list_parser = sub.add_parser("list", help="List stale submission locks")
    list_parser.add_argument(
        "--ttl-seconds",
        type=int,
        default=None,
        help="Override TTL (defaults to EXECUTION_PENDING_SUBMISSION_TTL_SECONDS)",
    )
    list_parser.add_argument("--user-id", default=None, help="Filter by user ID")


def _load_config_or_fail():
    try:
        load_runtime_env()
        rehydrate_env()
        return load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return None


def _list_stale_locks(
    db_path: str, *, ttl_seconds: int, user_id: str | None
) -> list[dict]:
    placeholders = ",".join("?" for _ in _LOCKED_STATUSES)
    query = f"""
        SELECT idempotency_key, user_id, leader_id, order_id, status,
               attempt_count, last_error, created_at, updated_at,
               response_json,
               COALESCE(
                   json_extract(response_json, '$.pending_since'),
                   updated_at,
                   created_at
               ) AS pending_since_ts,
               (strftime('%s','now') - CASE
                   WHEN json_extract(response_json, '$.pending_since') IS NOT NULL
                   THEN json_extract(response_json, '$.pending_since')
                   ELSE strftime('%s', COALESCE(updated_at, created_at))
               END) AS age_seconds
        FROM orders
        WHERE status IN ({placeholders})
          AND (strftime('%s','now') - CASE
               WHEN json_extract(response_json, '$.pending_since') IS NOT NULL
               THEN json_extract(response_json, '$.pending_since')
               ELSE strftime('%s', COALESCE(updated_at, created_at))
           END) > ?
    """
    params: list = list(_LOCKED_STATUSES) + [ttl_seconds]
    if user_id is not None:
        query += " AND user_id = ?"
        params.append(user_id)
    query += " ORDER BY age_seconds DESC"

    with create_connection(db_path) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='orders'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(query, tuple(params)).fetchall()

    return [
        {
            "idempotency_key": row["idempotency_key"],
            "user_id": row["user_id"],
            "leader_id": row["leader_id"],
            "order_id": row["order_id"],
            "status": row["status"],
            "attempt_count": row["attempt_count"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "pending_since_ts": row["pending_since_ts"],
            "age_seconds": int(row["age_seconds"] or 0),
        }
        for row in rows
    ]


async def _run_list_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    ttl = getattr(args, "ttl_seconds", None)
    if ttl is None:
        ttl = config.execution.pending_submission_ttl_seconds
    user_id = getattr(args, "user_id", None)

    try:
        rows = _list_stale_locks(
            config.database.db_path, ttl_seconds=int(ttl), user_id=user_id
        )
    except Exception as exc:
        print(json.dumps({"error": "db_error", "details": str(exc)}))
        return 1

    output = {
        "ttl_seconds": int(ttl),
        "count": len(rows),
        "stale_locks": rows,
    }
    print(json.dumps(output, indent=2))
    return 0


async def _run_stale_locks_async(args: argparse.Namespace) -> int:
    if args.stale_locks_subcommand == "list":
        return await _run_list_async(args)
    return 1


def run_stale_locks(args: argparse.Namespace) -> int:
    return asyncio.run(_run_stale_locks_async(args))
