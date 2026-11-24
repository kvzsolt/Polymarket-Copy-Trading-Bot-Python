"""CLI command registration functions.

Each command module exposes a ``register_X_command(subparsers)`` function
and a ``run_X(args)`` function matching the pattern established by ``doctor.py``.
"""

from pmcopy.bot_cli.commands.doctor import register_doctor_command, run_doctor
from pmcopy.bot_cli.commands.status import register_status_command, run_status
from pmcopy.bot_cli.commands.orders import register_orders_command, run_orders
from pmcopy.bot_cli.commands.positions import register_positions_command, run_positions
from pmcopy.bot_cli.commands.config import register_config_command, run_config
from pmcopy.bot_cli.commands.reconcile import register_reconcile_command, run_reconcile
from pmcopy.bot_cli.commands.run import register_run_command, run_run
from pmcopy.bot_cli.commands.reservations import (
    register_reservations_command,
    run_reservations,
)
from pmcopy.bot_cli.commands.stale_locks import (
    register_stale_locks_command,
    run_stale_locks,
)
from pmcopy.bot_cli.commands.drift import register_drift_command, run_drift
from pmcopy.bot_cli.commands.rotate_traders import (
    register_rotate_traders_command,
    run_rotate_traders,
)
from pmcopy.bot_cli.commands.profile_leader import (
    register_profile_leader_command,
    run_profile_leader,
)
from pmcopy.bot_cli.commands.trading import register_trading_command, run_trading
from pmcopy.bot_cli.commands.redeem import register_redeem_command, run_redeem
from pmcopy.bot_cli.commands.resolve import register_resolve_command, run_resolve
from pmcopy.bot_cli.commands.tasks import register_tasks_command, run_tasks

__all__ = [
    "register_doctor_command",
    "run_doctor",
    "register_status_command",
    "run_status",
    "register_orders_command",
    "run_orders",
    "register_positions_command",
    "run_positions",
    "register_config_command",
    "run_config",
    "register_reconcile_command",
    "run_reconcile",
    "register_run_command",
    "run_run",
    "register_reservations_command",
    "run_reservations",
    "register_stale_locks_command",
    "run_stale_locks",
    "register_drift_command",
    "run_drift",
    "register_rotate_traders_command",
    "run_rotate_traders",
    "register_profile_leader_command",
    "run_profile_leader",
    "register_trading_command",
    "run_trading",
    "register_redeem_command",
    "run_redeem",
    "register_resolve_command",
    "run_resolve",
    "register_tasks_command",
    "run_tasks",
]
