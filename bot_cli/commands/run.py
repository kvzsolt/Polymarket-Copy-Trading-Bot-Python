"""bot run — start the main trading loop."""

from __future__ import annotations

import argparse


def register_run_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the main-loop command and its process-local overrides.

    Args:
        subparsers: Parent CLI subparser registry.
    """
    parser = subparsers.add_parser(
        "run",
        help="Start the main loop using the fail-closed configured trading mode",
    )
    parser.add_argument(
        "--auto-trader",
        "--auto-follow",
        action="store_true",
        help="Restore the automatic DB roster; discover initially or after degradation",
    )


async def _run_bot(*, auto_trader: bool = False) -> int:
    """Delegate to app.py without a CLI mode override.

    The previous ``--live`` flag could conflict with the environment master
    switch and create two sources of truth.  Missing/unknown configuration now
    always remains paper mode; live also requires the database control to be
    explicitly resumed.
    """
    # Propagate app.main()'s exit code so a non-zero (fail-closed) app exit
    # surfaces through ``run_run`` → CLI runner → ``sys.exit`` instead of
    # being swallowed and reported as success (P2-q).
    from app import main as app_main

    code = await app_main(auto_trader=auto_trader)
    return int(code) if code is not None else 0


async def _run_run_async(args: argparse.Namespace) -> int:
    return await _run_bot(auto_trader=bool(args.auto_trader))


def run_run(args: argparse.Namespace) -> int:
    """Start the main trading loop synchronously.

    Args:
        args: Parsed command arguments.

    Returns:
        Process exit code from the asynchronous application entrypoint; ``0``
        when the operator requests shutdown with SIGINT or SIGTERM. Repeated
        signals leave asynchronous cleanup running to completion.
    """
    from utils.application_runner import ApplicationRunner
    from utils.event_loop import new_event_loop

    return ApplicationRunner(loop_factory=new_event_loop).run(_run_run_async(args))
