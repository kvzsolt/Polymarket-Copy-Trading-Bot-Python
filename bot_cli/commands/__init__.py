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
from bot_cli.commands.reservations import (
    register_reservations_command,
    run_reservations,
)
from bot_cli.commands.stale_locks import (
    register_stale_locks_command,
    run_stale_locks,
)
from bot_cli.commands.drift import register_drift_command, run_drift
from bot_cli.commands.rotate_traders import (
    register_rotate_traders_command,
    run_rotate_traders,
)
from bot_cli.commands.profile_leader import (
    register_profile_leader_command,
    run_profile_leader,
)
from bot_cli.commands.tune_strategy import (
    register_tune_strategy_command,
    run_tune_strategy,
)
from bot_cli.commands.trading import register_trading_command, run_trading

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
    "register_tune_strategy_command",
    "run_tune_strategy",
    "register_trading_command",
    "run_trading",
]
