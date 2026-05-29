from __future__ import annotations

import argparse
import asyncio

from config.app_config import load_config
from config.env import rehydrate_env
from config.runtime_env import load_runtime_env


def register_rotate_traders_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "rotate-traders",
        help="Interactively rotate configured LEADER_WALLETS from discovery candidates",
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-active", type=int, default=None)


async def _run_rotate_traders_async(args: argparse.Namespace) -> int:
    load_runtime_env(override_secrets=True)
    rehydrate_env(load_dotenv_file=False)
    config = load_config()

    from storage.postgres.connection import init_pool, shutdown_pool
    from storage.postgres.migrations import apply_migrations
    from services.discovery.balance_provider import build_wallet_balance_provider
    from services.discovery.rotation_service import RotationService

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=config.database.command_timeout_seconds,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations(conn)
        svc = RotationService(
            pool=pool,
            config=config,
            balance_provider=build_wallet_balance_provider(config),
        )
        await svc.run_interactive(
            env_file=args.env_file or config.discovery.rotate_traders_env_file,
            dry_run=bool(args.dry_run),
            max_active=args.max_active,
        )
    finally:
        await shutdown_pool()
    return 0


def run_rotate_traders(args: argparse.Namespace) -> int:
    return asyncio.run(_run_rotate_traders_async(args))
