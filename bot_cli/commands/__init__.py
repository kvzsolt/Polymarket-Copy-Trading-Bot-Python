"""CLI command registration functions.

Each command module exposes a ``register_X_command(subparsers)`` function
and a ``run_X(args)`` function matching the pattern established by ``doctor.py``.
"""

from bot_cli.commands.doctor import register_doctor_command, run_doctor
from bot_cli.commands.status import register_status_command, run_status
from bot_cli.commands.orders import register_orders_command, run_orders
from bot_cli.commands.positions import register_positions_command, run_positions
from bot_cli.commands.config import register_config_command, run_config
from bot_cli.commands.reconcile import register_reconcile_command, run_reconcile
from bot_cli.commands.run import register_run_command, run_run

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
]
