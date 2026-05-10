"""bot run — start the main trading loop."""
from __future__ import annotations

import argparse
import asyncio

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env


def register_run_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("run", help="Start the main trading loop")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--paper", action="store_true", help="Run in paper-trading mode")
    mode_group.add_argument("--live", action="store_true", help="Run in live-trading mode")


async def _run_bot(mode: str) -> int:
    """Delegate to app.py's main() after setting the appropriate mode flag."""
    import os

    # Ensure PAPER_TRADE_ONLY reflects the requested mode before app.py loads config.
    if mode == "paper":
        os.environ["PAPER_TRADE_ONLY"] = "true"
    else:
        os.environ["PAPER_TRADE_ONLY"] = "false"

    # Re-initialise runtime env so the updated env var is picked up.
    load_runtime_env()
    rehydrate_env()

    # Import and run app.main() — it handles its own event loop lifecycle.
    from app import main as app_main

    await app_main()
    return 0


async def _run_run_async(args: argparse.Namespace) -> int:
    mode = "paper" if args.paper else "live"
    return await _run_bot(mode)


def run_run(args: argparse.Namespace) -> int:
    """Start the main trading loop synchronously (wraps the async entrypoint)."""
    return asyncio.run(_run_run_async(args))
