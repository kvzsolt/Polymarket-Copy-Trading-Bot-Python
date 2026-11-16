import argparse
import sys

from pmcopy.bot_cli.commands.history_import import (
    register_history_import_command,
    run_history_import,
)

from pmcopy.bot_cli.commands import (
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
    register_reservations_command,
    run_reservations,
    register_stale_locks_command,
    run_stale_locks,
    register_drift_command,
    run_drift,
    register_rotate_traders_command,
    run_rotate_traders,
    register_profile_leader_command,
    run_profile_leader,
    register_trading_command,
    run_trading,
    register_redeem_command,
    run_redeem,
    register_resolve_command,
    run_resolve,
)

_COMMANDS = [
    ("history-import", register_history_import_command, run_history_import),
    ("doctor", register_doctor_command, run_doctor),
    ("status", register_status_command, run_status),
    ("orders", register_orders_command, run_orders),
    ("positions", register_positions_command, run_positions),
    ("config", register_config_command, run_config),
    ("reconcile", register_reconcile_command, run_reconcile),
    ("run", register_run_command, run_run),
    ("reservations", register_reservations_command, run_reservations),
    ("stale-locks", register_stale_locks_command, run_stale_locks),
    ("drift", register_drift_command, run_drift),
    ("rotate-traders", register_rotate_traders_command, run_rotate_traders),
    ("profile-leader", register_profile_leader_command, run_profile_leader),
    ("trading", register_trading_command, run_trading),
    ("redeem", register_redeem_command, run_redeem),
    ("resolve", register_resolve_command, run_resolve),
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


if __name__ == "__main__":
    sys.exit(main())
