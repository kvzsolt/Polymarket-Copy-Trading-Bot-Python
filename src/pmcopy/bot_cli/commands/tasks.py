"""pmcopy tasks — inspect and synchronize durable v1 copy-task configurations."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import cast

from pmcopy.config.app_config import EnvValidationError, load_config


def register_tasks_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``tasks`` subcommand (``list`` | ``sync``)."""
    parser = subparsers.add_parser(
        "tasks",
        help="Inspect or sync durable v1 copy-task definitions from config/traders/*.yml",
    )
    sub = parser.add_subparsers(dest="tasks_action", required=True)
    sub.add_parser("list", help="List non-deleted copy tasks stored in PostgreSQL")
    sync_parser = sub.add_parser(
        "sync",
        help="Create or update PostgreSQL copy_tasks rows from config/traders/*.yml",
    )
    sync_parser.add_argument(
        "--prune",
        action="store_true",
        help="Soft-delete PostgreSQL copy_tasks that no longer have a trader YAML file",
    )


async def _run_tasks_async(args: argparse.Namespace) -> int:
    try:
        config = load_config()
    except EnvValidationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    from pmcopy.orchestrators.copy_tasks.v1_runtime_bootstrap import sync_tasks_from_trader_configs
    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool
    from pmcopy.storage.postgres.copy_task_repository import (
        DatabasePool as CopyTaskDatabasePool,
        PostgresCopyTaskRepository,
    )

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=15.0,
    )
    try:
        repo = PostgresCopyTaskRepository(cast(CopyTaskDatabasePool, pool))
        if args.tasks_action == "list":
            tasks = await repo.list_active()
            payload = [
                {
                    "task_id": t.task_id,
                    "status": t.status.value,
                    "version": t.version,
                    "leader_wallet": t.leader_wallet,
                    "follower_account": t.follower_account,
                    "sizing_mode": t.sizing_mode,
                    "require_leader_balance": t.sizing.require_leader_balance,
                    "proportional_balance_cap_enabled": t.sizing.proportional_balance_cap_enabled,
                    "price_offset_mode": t.price.offset_mode.value,
                    "price_offset": str(t.price.offset),
                    "max_slippage_percent": str(t.price.max_slippage_percent),
                }
                for t in tasks
            ]
            print(json.dumps(payload, indent=2))
            return 0
        if args.tasks_action == "sync":
            created, updated, pruned = await sync_tasks_from_trader_configs(
                config=config,
                task_repository=repo,
                prune_orphaned=bool(getattr(args, "prune", False)),
            )
            print(
                json.dumps(
                    {"created": created, "updated": updated, "pruned": pruned},
                    indent=2,
                )
            )
            return 0
        print(f"Unknown tasks action: {args.tasks_action}", file=sys.stderr)
        return 1
    finally:
        await shutdown_pool()


def run_tasks(args: argparse.Namespace) -> int:
    """Execute the ``pmcopy tasks`` CLI command."""
    return asyncio.run(_run_tasks_async(args))


__all__ = ["register_tasks_command", "run_tasks"]
