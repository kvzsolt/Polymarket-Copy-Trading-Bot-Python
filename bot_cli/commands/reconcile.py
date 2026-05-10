"""bot reconcile — trigger reconciliation of all unconfirmed orders."""
from __future__ import annotations

import argparse
import asyncio
import json

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
from config.app_config import load_config, EnvValidationError
from storage.sqlite.connection import create_connection


def register_reconcile_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("reconcile", help="Trigger reconciliation of unconfirmed orders")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be reconciled without acting")
    parser.add_argument("--user-id", default=None, help="Reconcile orders for a specific user only")


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
    db_path = config.database.db_path

    # Collect unconfirmed orders
    unconfirmed: list[dict] = []
    with create_connection(db_path) as conn:
        query = """
            SELECT idempotency_key, user_id, leader_id, order_id, status, updated_at
            FROM orders
            WHERE status IN ('submitted_unconfirmed', 'submitted', 'unknown_unconfirmed',
                             'reconciliation_required')
              AND order_id IS NOT NULL
        """
        params: list = []
        if user_id is not None:
            query += " AND user_id = ?"
            params.append(user_id)
        query += " ORDER BY updated_at ASC"

        rows = conn.execute(query, tuple(params)).fetchall()
        unconfirmed = [dict(r) for r in rows]

    if dry_run:
        output = {
            "dry_run": True,
            "unconfirmed_count": len(unconfirmed),
            "orders": unconfirmed,
        }
        print(json.dumps(output, indent=2))
        return 0

    # Run actual reconciliation
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


def run_reconcile(args: argparse.Namespace) -> int:
    return asyncio.run(_run_reconcile_async(args))
