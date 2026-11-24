"""Database-backed live trading pause/resume control."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime

from pmcopy.config.app_config import load_config
from pmcopy.services.trading.trading_control import FULL_STOP_REASON


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

    stop = actions.add_parser(
        "full-stop",
        help=(
            "Durable full stop: block every live submission and cancel all "
            "resting venue orders (live mode only)"
        ),
    )
    stop.add_argument("--operator", default=os.getenv("USER", "cli"))

    resume = actions.add_parser("resume", help="Allow live venue submissions")
    resume.add_argument("--confirm", required=True, help="Must be exactly LIVE")
    resume.add_argument("--reason", required=True)
    resume.add_argument("--operator", default=os.getenv("USER", "cli"))

    clear_stop = actions.add_parser(
        "clear-full-stop",
        help=(
            "Explicitly clear a durable runtime full stop and allow live "
            "venue submissions again (resume refuses while it is active)"
        ),
    )
    clear_stop.add_argument("--confirm", required=True, help="Must be exactly LIVE")
    clear_stop.add_argument("--reason", required=True)
    clear_stop.add_argument("--operator", default=os.getenv("USER", "cli"))


def _json_state(state) -> str:
    payload = asdict(state)
    for key, value in tuple(payload.items()):
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
    return json.dumps(payload, indent=2, sort_keys=True)


async def _apply_full_stop_to_live_venues(config) -> None:
    """Cancel every resting order through one gateway per follower account.

    The durable ``trading_control`` row is the submission authority and is
    written before this runs, so a failed cancellation never weakens the stop;
    it only raises so the operator sees that resting orders remain. Paper mode
    never builds authenticated venue clients, so the apply is live-only, which
    matches how the remote runtime wired this safety path.
    """

    from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime
    from pmcopy.services.accounts.multi_user_config import load_runtime_users
    from pmcopy.services.trading.trading_control import (
        NullRuntimeTelemetry,
        TradingControlRuntimeSignal,
    )

    mode = str(getattr(config.execution, "trading_mode", "paper") or "paper").strip().lower()
    if mode != "live":
        print(f"full-stop venue cancel apply skipped (trading_mode={mode})")
        return

    runtimes = []
    try:
        for runtime_user in load_runtime_users([], config):
            runtimes.append(
                await build_polymarket_runtime(
                    runtime_user=runtime_user,
                    config=config,
                )
            )
        signal = TradingControlRuntimeSignal(
            gateways=tuple(runtime.gateway for runtime in runtimes),
            telemetry=NullRuntimeTelemetry(),
        )
        await signal.apply(entries_paused=True, full_stop=True)
    finally:
        for runtime in runtimes:
            try:
                await runtime.close()
            except Exception:
                pass


async def _run_trading_async(args: argparse.Namespace) -> int:
    config = load_config()

    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool
    from pmcopy.storage.postgres.migrations import apply_migrations
    from pmcopy.storage.postgres.trading_control_repo import TradingControlRepo

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
            current = await repo.get()
            if not current.enabled and current.reason == FULL_STOP_REASON:
                # Controller ruling: a durable full stop is already fully
                # paused; an ordinary pause reason must never overwrite it,
                # because that would let a routine resume clear the stop.
                print(
                    "Refusing pause: a durable full stop is active "
                    f"(reason={FULL_STOP_REASON}); it already pauses every "
                    "live submission. Run 'pmcopy trading clear-full-stop "
                    "--confirm LIVE --reason ...' to clear it explicitly."
                )
                return 2
            state = await repo.set_enabled(
                False,
                reason=args.reason,
                updated_by=args.operator,
            )
        elif args.trading_action == "full-stop":
            # Persist first: the durable row stays authoritative even when a
            # venue cancellation fails. The stored reason is the fixed
            # machine-readable full-stop signal enforced before every POST.
            state = await repo.set_enabled(
                False,
                reason=FULL_STOP_REASON,
                updated_by=args.operator,
            )
            print(_json_state(state))
            try:
                await _apply_full_stop_to_live_venues(config)
            except Exception as exc:
                print(
                    "full stop is durable but resting orders could not all be "
                    f"canceled: {type(exc).__name__}",
                    file=sys.stderr,
                )
                return 1
            return 0
        elif args.trading_action == "resume":
            if args.confirm != "LIVE":
                print("Refusing resume: --confirm must be exactly LIVE")
                return 2
            current = await repo.get()
            if not current.enabled and current.reason == FULL_STOP_REASON:
                # Controller ruling: resume alone must never silently clear a
                # durable money-path full stop; only the explicit dedicated
                # action may, so a routine resume cannot reopen live flow.
                print(
                    "Refusing resume: a durable full stop is active "
                    f"(reason={FULL_STOP_REASON}). Run "
                    "'pmcopy trading clear-full-stop --confirm LIVE "
                    "--reason ...' to clear it explicitly."
                )
                return 2
            state = await repo.set_enabled(
                True,
                reason=args.reason,
                updated_by=args.operator,
            )
        elif args.trading_action == "clear-full-stop":
            if args.confirm != "LIVE":
                print("Refusing clear-full-stop: --confirm must be exactly LIVE")
                return 2
            current = await repo.get()
            if current.enabled or current.reason != FULL_STOP_REASON:
                # No durable full stop is active: read-only no-op so the
                # explicit clear action stays idempotent.
                state = current
            else:
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
