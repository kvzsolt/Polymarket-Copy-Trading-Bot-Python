"""Database-backed live trading pause/resume control."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from datetime import datetime

from config.app_config import load_config


def register_trading_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "trading",
        help="Inspect or change the fail-closed live trading control",
    )
    actions = parser.add_subparsers(dest="trading_action", required=True)
    actions.add_parser("status", help="Show the current trading control")

    pause = actions.add_parser("pause", help="Block new live venue submissions")
    pause.add_argument("--reason", required=True)
    pause.add_argument("--operator", default=os.getenv("USER", "cli"))

    resume = actions.add_parser("resume", help="Allow live venue submissions")
    resume.add_argument("--confirm", required=True, help="Must be exactly LIVE")
    resume.add_argument("--reason", required=True)
    resume.add_argument("--operator", default=os.getenv("USER", "cli"))


def _json_state(state) -> str:
    payload = asdict(state)
    for key, value in tuple(payload.items()):
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
    return json.dumps(payload, indent=2, sort_keys=True)


async def _run_trading_async(args: argparse.Namespace) -> int:
    config = load_config()

    from storage.postgres.connection import init_pool, shutdown_pool
    from storage.postgres.migrations import apply_migrations
    from storage.postgres.trading_control_repo import TradingControlRepo

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=config.database.command_timeout_seconds,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations(conn)
        repo = TradingControlRepo(pool)
        if args.trading_action == "status":
            state = await repo.get()
        elif args.trading_action == "pause":
            state = await repo.set_enabled(
                False,
                reason=args.reason,
                updated_by=args.operator,
            )
        elif args.trading_action == "resume":
            if args.confirm != "LIVE":
                print("Refusing resume: --confirm must be exactly LIVE")
                return 2
            state = await repo.set_enabled(
                True,
                reason=args.reason,
                updated_by=args.operator,
            )
        else:  # argparse enforces this; defensive for direct callers
            return 2
        print(_json_state(state))
        return 0
    finally:
        await shutdown_pool()


def run_trading(args: argparse.Namespace) -> int:
    return asyncio.run(_run_trading_async(args))


__all__ = ["register_trading_command", "run_trading"]
