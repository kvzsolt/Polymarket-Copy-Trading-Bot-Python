"""bot reconcile — trigger reconciliation of all unconfirmed orders."""
from __future__ import annotations

import argparse
import asyncio
import json

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
from config.app_config import load_config, EnvValidationError


def register_reconcile_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("reconcile", help="Trigger reconciliation of unconfirmed orders")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be reconciled without acting")
    parser.add_argument("--user-id", default=None, help="Reconcile orders for a specific user only")


async def _fetch_unconfirmed_pg(dsn: str, user_id: str | None) -> list[dict]:
    from storage.postgres.connection import init_pool, shutdown_pool

    query = """
        SELECT idempotency_key, user_id, leader_id, order_id, status, updated_at
        FROM orders
        WHERE status IN ('submitted_unconfirmed', 'submitted', 'unknown_unconfirmed',
                         'reconciliation_required')
          AND order_id IS NOT NULL
    """
    params: list = []
    if user_id is not None:
        query += " AND user_id = $1"
        params.append(user_id)
    query += " ORDER BY updated_at ASC"

    pool = await init_pool(
        dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=15.0,
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
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
            "idempotency_key": r["idempotency_key"],
            "user_id": r["user_id"],
            "leader_id": r["leader_id"],
            "order_id": r["order_id"],
            "status": r["status"],
            "updated_at": _ts(r["updated_at"]),
        }
        for r in rows
    ]


async def _run_reconcile_async(args: argparse.Namespace) -> int:
    try:
        load_runtime_env()
        rehydrate_env()
        config = load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return 1

    dry_run = getattr(args, "dry_run", False)
    user_id = getattr(args, "user_id", None)

    # Collect unconfirmed orders via PG. Even when the reconcile action
    # path below routes through the legacy SQLite ReconciliationService,
    # this SELECT now reads from the canonical PG source of truth.
    unconfirmed = await _fetch_unconfirmed_pg(config.database.dsn, user_id)

    if dry_run:
        output = {
            "dry_run": True,
            "unconfirmed_count": len(unconfirmed),
            "orders": unconfirmed,
        }
        print(json.dumps(output, indent=2))
        return 0

    reconciled = 0
    errors: list[str] = []
    if unconfirmed:
        from storage.postgres.connection import init_pool, shutdown_pool
        from storage.postgres.orders_repo import OrdersRepo

        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            from utils.createClobClient import create_clob_client
            from services.reconciliation import ReconciliationService

            wallet = config.wallet
            client = await create_clob_client(wallet_config=wallet)
            orders_repo = OrdersRepo(pool)

            svc = ReconciliationService(
                clob_client=client,
                orders_repo=orders_repo,
                config=config,
            )
            result = await svc.reconcile()
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


def run_reconcile(args: argparse.Namespace) -> int:
    return asyncio.run(_run_reconcile_async(args))
