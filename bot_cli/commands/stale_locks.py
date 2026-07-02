"""bot stale-locks — list orders held in submission lock past TTL."""
from __future__ import annotations

import argparse
import asyncio
import json
from typing import TYPE_CHECKING, Any

from config.app_config import EnvValidationError, load_config
from config.env import rehydrate_env
from config.runtime_env import load_runtime_env

if TYPE_CHECKING:
    import asyncpg


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

    resolve_parser = sub.add_parser(
        "resolve", help="Resolve stale submission locks that are safe to expire"
    )
    resolve_parser.add_argument(
        "--ttl-seconds",
        type=int,
        default=None,
        help="Override TTL (defaults to EXECUTION_PENDING_SUBMISSION_TTL_SECONDS)",
    )
    resolve_parser.add_argument("--user-id", default=None, help="Filter by user ID")
    mode = resolve_parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        dest="apply",
        action="store_false",
        default=False,
        help="Report actions without mutating rows (default)",
    )
    mode.add_argument(
        "--apply",
        dest="apply",
        action="store_true",
        help="Mark safe stale no-order locks expired",
    )


def _load_config_or_fail():
    try:
        load_runtime_env()
        rehydrate_env()
        return load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return None


async def _list_stale_locks(
    pool: "asyncpg.Pool",
    *,
    ttl_seconds: int,
    user_id: str | None,
) -> list[dict]:
    """List orders whose effective lock age exceeds ``ttl_seconds``.

    Effective lock age = ``now() - pending_since`` when
    ``response_json.pending_since`` is set, else
    ``now() - COALESCE(updated_at, created_at)``.

    Takes the asyncpg pool so callers (CLI + tests) own pool lifecycle.
    """
    from storage.postgres.orders_repo import OrdersRepo

    repo = OrdersRepo(pool)
    rows = await repo.list_stale_submission_locks(
        ttl_seconds=int(ttl_seconds),
        user_id=user_id,
    )
    return _serialize_lock_rows(rows)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_lock_rows(rows: list[dict]) -> list[dict]:
    def _ts(value):
        return _json_safe(value)

    return [
        {
            "idempotency_key": row["idempotency_key"],
            "user_id": row["user_id"],
            "leader_id": row["leader_id"],
            "order_id": row["order_id"],
            "status": row["status"],
            "attempt_count": row["attempt_count"],
            "last_error": row["last_error"],
            "created_at": _ts(row["created_at"]),
            "updated_at": _ts(row["updated_at"]),
            "pending_since_ts": int(row["pending_since_ts"])
            if row["pending_since_ts"] is not None
            else None,
            "age_seconds": int(row["age_seconds"] or 0),
            "classification": row.get("classification"),
            "action": row.get("action"),
        }
        for row in rows
    ]


async def _resolve_stale_locks(
    pool: "asyncpg.Pool",
    *,
    ttl_seconds: int,
    user_id: str | None,
    apply: bool,
) -> dict:
    from storage.postgres.orders_repo import OrdersRepo

    repo = OrdersRepo(pool)
    result = await repo.resolve_stale_submission_locks(
        ttl_seconds=int(ttl_seconds),
        user_id=user_id,
        apply=apply,
    )
    return {
        **result,
        "expired": _serialize_lock_rows(result["expired"]),
        "would_expire": _serialize_lock_rows(result["would_expire"]),
        "deferred_reconciliation_required": _serialize_lock_rows(
            result["deferred_reconciliation_required"]
        ),
    }


async def _run_list_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    ttl = getattr(args, "ttl_seconds", None)
    if ttl is None:
        ttl = config.execution.pending_submission_ttl_seconds
    user_id = getattr(args, "user_id", None)

    from storage.postgres.connection import init_pool, shutdown_pool

    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            rows = await _list_stale_locks(
                pool, ttl_seconds=int(ttl), user_id=user_id
            )
        finally:
            await shutdown_pool()
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


async def _run_resolve_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    ttl = getattr(args, "ttl_seconds", None)
    if ttl is None:
        ttl = config.execution.pending_submission_ttl_seconds
    user_id = getattr(args, "user_id", None)
    apply = bool(getattr(args, "apply", False))

    from storage.postgres.connection import init_pool, shutdown_pool

    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            output = await _resolve_stale_locks(
                pool,
                ttl_seconds=int(ttl),
                user_id=user_id,
                apply=apply,
            )
        finally:
            await shutdown_pool()
    except Exception as exc:
        print(json.dumps({"error": "db_error", "details": str(exc)}))
        return 1

    print(json.dumps(output, indent=2))
    return 0


async def _run_stale_locks_async(args: argparse.Namespace) -> int:
    if args.stale_locks_subcommand == "list":
        return await _run_list_async(args)
    if args.stale_locks_subcommand == "resolve":
        return await _run_resolve_async(args)
    return 1


def run_stale_locks(args: argparse.Namespace) -> int:
    return asyncio.run(_run_stale_locks_async(args))
