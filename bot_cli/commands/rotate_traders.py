from __future__ import annotations

import argparse
import asyncio


def register_rotate_traders_command(
    subparsers: argparse._SubParsersAction,
) -> None:
    parser = subparsers.add_parser(
        "rotate-traders",
        help="Run discovery and show the hysteresis-qualified roster",
    )
    parser.add_argument(
        "--rescore-only",
        action="store_true",
        help="Use cached snapshots without calling Heisenberg",
    )


async def _run_rotate_traders_async(args: argparse.Namespace) -> int:
    from bootstrap import build_app
    from storage.postgres.connection import shutdown_pool

    application = await build_app()
    try:
        if args.rescore_only:
            await application.registry.rescore([])
        else:
            await application.registry.refresh([], force_discovery=True)
        roster = await application.leaders_repo.list_roster()
        for entry in roster:
            print(
                f"{entry.tier:9} {entry.score:.4f} "
                f"streak=+{entry.promote_streak}/-{entry.demote_streak} "
                f"{entry.wallet}"
            )
    finally:
        await application.aclose()
        await shutdown_pool()
    return 0


def run_rotate_traders(args: argparse.Namespace) -> int:
    from utils.event_loop import new_event_loop

    with asyncio.Runner(loop_factory=new_event_loop) as runner:
        return runner.run(_run_rotate_traders_async(args))
