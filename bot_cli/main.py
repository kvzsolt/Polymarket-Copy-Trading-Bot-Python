import argparse
import sys

from bot_cli.commands import (
    register_doctor_command,
    run_doctor,
    register_status_command,
    run_status,
    register_orders_command,
    run_orders,
    register_positions_command,
    run_positions,
    register_config_command,
    run_config,
    register_reconcile_command,
    run_reconcile,
    register_run_command,
    run_run,
)

_COMMANDS = [
    ("doctor", register_doctor_command, run_doctor),
    ("status", register_status_command, run_status),
    ("orders", register_orders_command, run_orders),
    ("positions", register_positions_command, run_positions),
    ("config", register_config_command, run_config),
    ("reconcile", register_reconcile_command, run_reconcile),
    ("run", register_run_command, run_run),
]

_COMMAND_RUNNERS = {name: runner for name, _, runner in _COMMANDS}


def main() -> int:
    parser = argparse.ArgumentParser(description="Polymarket Copy Trader Bot CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for _name, register_fn, _runner in _COMMANDS:
        register_fn(subparsers)

    args = parser.parse_args()

    runner = _COMMAND_RUNNERS.get(args.command)
    if runner is not None:
        return runner(args)

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 1
