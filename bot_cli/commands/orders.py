"""bot orders — list and reconcile orders."""
from __future__ import annotations

import argparse
import asyncio
import json

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
from config.app_config import load_config, EnvValidationError
from storage.sqlite.connection import create_connection


def register_orders_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("orders", help="List and reconcile orders")
    orders_sub = parser.add_subparsers(dest="orders_subcommand", required=True)

    pending_parser = orders_sub.add_parser("pending", help="List orders with non-terminal status")
    pending_parser.add_argument("--user-id", default=None, help="Filter by user ID")

    reconcile_parser = orders_sub.add_parser("reconcile", help="Trigger reconciliation for unconfirmed orders")
    reconcile_parser.add_argument("--dry-run", action="store_true", help="Show what would be reconciled without acting")


def _load_config_or_fail():
    """Load config, returning None and printing error on failure."""
    try:
        load_runtime_env()
        rehydrate_env()
        return load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return None


def _list_pending_orders(config, user_id: str | None = None) -> list[dict]:
    """Return orders with non-terminal status from the orders table."""
    db_path = config.database.db_path
    query = """
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
        query += " AND user_id = ?"
        params.append(user_id)
    query += " ORDER BY updated_at ASC"

    with create_connection(db_path) as conn:
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
        }
        for row in rows
    ]


async def _run_orders_pending_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    user_id = getattr(args, "user_id", None)
    pending = _list_pending_orders(config, user_id=user_id)
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
    db_path = config.database.db_path

    # Find unconfirmed orders that need reconciliation
    unconfirmed: list[dict] = []
    with create_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT idempotency_key, user_id, leader_id, order_id, status, updated_at
            FROM orders
            WHERE status IN ('submitted_unconfirmed', 'submitted', 'unknown_unconfirmed',
                             'reconciliation_required')
              AND order_id IS NOT NULL
            ORDER BY updated_at ASC
            """
        ).fetchall()
        unconfirmed = [dict(r) for r in rows]

    if dry_run:
        output = {
            "dry_run": True,
            "count": len(unconfirmed),
            "orders": unconfirmed,
        }
        print(json.dumps(output, indent=2))
        return 0

    # Actually trigger reconciliation via ReconciliationService
    reconciled = 0
    errors: list[str] = []
    if unconfirmed:
        try:
            from utils.createClobClient import create_clob_client
            from services.reconciliation import ReconciliationService

            wallet = config.wallet
            client = await create_clob_client(wallet_config=wallet)
            orders_repo = None
            try:
                from storage.sqlite.orders_repo import OrdersRepo
                orders_repo = OrdersRepo(db_path)
            except Exception:
                pass

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
    return asyncio.run(_run_orders_async(args))
