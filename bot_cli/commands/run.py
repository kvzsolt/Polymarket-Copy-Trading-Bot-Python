"""bot run — start the main trading loop."""
from __future__ import annotations

import argparse
import asyncio

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env


def register_run_command(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser(
        "run",
        help="Start the main loop using the fail-closed configured trading mode",
    )


async def _run_bot() -> int:
    """Delegate to app.py without a CLI mode override.

    The previous ``--live`` flag could conflict with the environment master
    switch and create two sources of truth.  Missing/unknown configuration now
    always remains paper mode; live also requires the database control to be
    explicitly resumed.
    """
    load_runtime_env()
    rehydrate_env()

    # Import and run app.main() — it handles its own event loop lifecycle.
    # Propagate app.main()'s exit code so a non-zero (fail-closed) app exit
    # surfaces through ``run_run`` → CLI runner → ``sys.exit`` instead of
    # being swallowed and reported as success (P2-q).
    from app import main as app_main

    code = await app_main()
    return int(code) if code is not None else 0


async def _run_run_async(args: argparse.Namespace) -> int:
    del args
    return await _run_bot()


def run_run(args: argparse.Namespace) -> int:
    """Start the main trading loop synchronously (wraps the async entrypoint)."""
    return asyncio.run(_run_run_async(args))
