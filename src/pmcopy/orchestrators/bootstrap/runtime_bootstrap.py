"""Compose per-user trading dependencies behind venue-neutral ports.

Decomposed into ``user_state``, ``reservation_wiring``, and ``context_builder``
while preserving the canonical ``pmcopy.orchestrators.bootstrap.runtime_bootstrap``
public contract.
"""

from __future__ import annotations

from pmcopy.orchestrators.bootstrap.context_builder import (
    _GammaLookup,  # noqa: F401
    _lookup_gamma_market,  # noqa: F401
    build_context_maps,
    build_trading_context,
    hydrate_resolved_market_state,
    startup_drift_check,
    startup_reconcile_unconfirmed,
)
from pmcopy.orchestrators.bootstrap.reservation_wiring import (
    _bind_reservation_runtime,  # noqa: F401
    _build_ambiguous_recovery_service,  # noqa: F401
    _build_execution_coordinator,  # noqa: F401
    _build_manual_sync_service,  # noqa: F401
    _build_position_drift_service,  # noqa: F401
    _build_reservations_repo,  # noqa: F401
    _log_ambiguous_recovery_results,  # noqa: F401
    _release_expired_reservations_for_context,  # noqa: F401
    ambiguous_recovery_loop,
)
from pmcopy.orchestrators.bootstrap.user_state import (
    UserRuntimeState,
    _ensure_user_state_account_containers,  # noqa: F401
    _user_state_telemetry,  # noqa: F401
    build_user_state,
    build_user_venue_runtime,
    configured_leaders,
    default_follow,
    ensure_runtime_follows,
    fetch_live_positions,
)
from pmcopy.services.execution.execution_service import ExecutionService  # noqa: F401
from pmcopy.services.positions.position_manager import PositionManager  # noqa: F401
from pmcopy.services.risk.risk_monitor import (
    reconcile_unconfirmed_risk_orders,  # noqa: F401
    rehydrate_position_manager,  # noqa: F401
)
from pmcopy.storage.postgres.fills_repo import FillsRepo  # noqa: F401
from pmcopy.storage.postgres.orders_repo import OrdersRepo  # noqa: F401
from pmcopy.utils.fetch_data import _get_http_client, fetch_data  # noqa: F401
from pmcopy.utils.logger import get_logger

logger = get_logger("RuntimeBootstrap")

__all__ = [
    "UserRuntimeState",
    "ambiguous_recovery_loop",
    "build_context_maps",
    "build_trading_context",
    "build_user_state",
    "build_user_venue_runtime",
    "configured_leaders",
    "default_follow",
    "ensure_runtime_follows",
    "fetch_live_positions",
    "hydrate_resolved_market_state",
    "startup_drift_check",
    "startup_reconcile_unconfirmed",
]
