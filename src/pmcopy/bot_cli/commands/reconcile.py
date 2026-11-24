"""bot reconcile — trigger reconciliation of all unconfirmed orders."""

from __future__ import annotations

import argparse
import asyncio
import json

from pmcopy.config.app_config import load_config, EnvValidationError


def register_reconcile_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the reconciliation command with the CLI parser."""
    parser = subparsers.add_parser("reconcile", help="Trigger reconciliation of unconfirmed orders")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be reconciled without acting"
    )
    parser.add_argument("--user-id", default=None, help="Reconcile orders for a specific user only")


async def _fetch_unconfirmed_pg(pool, user_id: str | None) -> list[dict]:
    """Read unconfirmed orders. Caller owns ``pool`` lifecycle (INF-03)."""
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

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

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
        config = load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return 1

    dry_run = getattr(args, "dry_run", False)
    user_id = getattr(args, "user_id", None)

    # INF-03: single ``init_pool`` → ``try/finally shutdown_pool`` covering
    # both the read pass and the reconcile action. The pre-fix code took
    # two lifecycles per invocation; the second ``init_pool`` would either
    # raise on DSN-change guard or silently return a stale pool if the
    # interleaved ``shutdown_pool`` failed.
    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=15.0,
    )
    reconciled = 0
    errors: list[str] = []
    try:
        # Collect unconfirmed orders via PG. Even when the reconcile action
        # path below routes through the legacy SQLite ReconciliationService,
        # this SELECT now reads from the canonical PG source of truth.
        unconfirmed = await _fetch_unconfirmed_pg(pool, user_id)

        if dry_run:
            output = {
                "dry_run": True,
                "unconfirmed_count": len(unconfirmed),
                "orders": unconfirmed,
            }
            print(json.dumps(output, indent=2))
            return 0

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


def run_reconcile(args: argparse.Namespace) -> int:
    """Run the requested reconciliation operation."""
    return asyncio.run(_run_reconcile_async(args))
