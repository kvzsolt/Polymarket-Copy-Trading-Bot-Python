"""bot orders — list and reconcile orders."""

from __future__ import annotations

import argparse
import asyncio
import json


from pmcopy.config.app_config import load_config, EnvValidationError


def register_orders_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the order inspection command with the CLI parser."""
    parser = subparsers.add_parser("orders", help="List and reconcile orders")
    orders_sub = parser.add_subparsers(dest="orders_subcommand", required=True)

    pending_parser = orders_sub.add_parser("pending", help="List orders with non-terminal status")
    pending_parser.add_argument("--user-id", default=None, help="Filter by user ID")

    reconcile_parser = orders_sub.add_parser(
        "reconcile", help="Trigger reconciliation for unconfirmed orders"
    )
    reconcile_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be reconciled without acting"
    )


def _load_config_or_fail():
    """Load config, returning None and printing error on failure."""
    try:
        return load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return None


async def _list_pending_orders(config, user_id: str | None = None) -> list[dict]:
    """Return orders with non-terminal status from the orders table."""
    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

    base_query = """
        SELECT idempotency_key, user_id, leader_id, order_id, status,
               attempt_count, last_error, created_at, updated_at
        FROM orders
        WHERE status IN ('submitted_unconfirmed', 'delayed', 'pending',
                         'pending_lock', 'submitting', 'submitted',
                         'partially_filled', 'reconciliation_required',
                         'unknown_unconfirmed')
    """
    params: list = []
    if user_id is not None:
        base_query += " AND user_id = $1"
        params.append(user_id)
    base_query += " ORDER BY updated_at ASC"

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=15.0,
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(base_query, *params)
    finally:
        await shutdown_pool()

    def _ts(value):
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

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
        }
        for row in rows
    ]


async def _run_orders_pending_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    user_id = getattr(args, "user_id", None)
    pending = await _list_pending_orders(config, user_id=user_id)
    output = {
        "count": len(pending),
        "orders": pending,
    }
    print(json.dumps(output, indent=2))
    return 0


async def _run_orders_reconcile_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    dry_run = getattr(args, "dry_run", False)

    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=15.0,
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status, updated_at
                FROM orders
                WHERE status IN ('submitted_unconfirmed', 'submitted', 'unknown_unconfirmed',
                                 'reconciliation_required')
                  AND order_id IS NOT NULL
                ORDER BY updated_at ASC
                """
            )
        unconfirmed = [
            {
                **dict(r),
                "updated_at": (
                    r["updated_at"].isoformat()
                    if hasattr(r["updated_at"], "isoformat")
                    else r["updated_at"]
                ),
            }
            for r in rows
        ]

        if dry_run:
            output = {
                "dry_run": True,
                "count": len(unconfirmed),
                "orders": unconfirmed,
            }
            print(json.dumps(output, indent=2, default=str))
            return 0

        reconciled = 0
        errors: list[str] = []
        if unconfirmed:
            try:
                from pmcopy.orchestrators.settlement.reconciliation_command import ReconciliationCommandService

                result = await ReconciliationCommandService(config=config, pool=pool).run()
                reconciled = result.stale_orders_cancelled
                if result.errors:
                    errors = result.errors
            except Exception as exc:
                errors.append(str(exc))
    finally:
        await shutdown_pool()

    output = {
        "dry_run": False,
        "unconfirmed_count": len(unconfirmed),
        "reconciled": reconciled,
        "errors": errors,
    }
    print(json.dumps(output, indent=2))
    return 1 if errors else 0


async def _run_orders_async(args: argparse.Namespace) -> int:
    if args.orders_subcommand == "pending":
        return await _run_orders_pending_async(args)
    elif args.orders_subcommand == "reconcile":
        return await _run_orders_reconcile_async(args)
    return 1


def run_orders(args: argparse.Namespace) -> int:
    """Query and render orders for the selected follower."""
    return asyncio.run(_run_orders_async(args))
