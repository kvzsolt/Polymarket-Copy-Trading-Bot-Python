"""Utility package exports."""

import re as _re
import threading as _threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .fetchData import close_session, fetch_data
    from .getMyBalance import get_my_balance
    from .idempotency import build_trade_key, has_processed, mark_processed

# Polymarket CLOB token IDs are CTF (Conditional Token Framework) uint256
# values represented as decimal digit strings, typically 70-80 characters.
# Anything shorter than 10 digits or containing non-digit characters is
# not a valid on-chain token identifier.
_CLOB_TOKEN_RE = _re.compile(r"^\d{10,}$")


def is_valid_clob_token_id(token_id: str | None) -> bool:
    """Return True when *token_id* looks like a real Polymarket CLOB token."""
    if not token_id:
        return False
    return bool(_CLOB_TOKEN_RE.match(token_id))


# ---------------------------------------------------------------------------
# Lazy imports for modules that depend on Web3 or venue clients.
# These are imported on first access to avoid requiring all optional
# dependencies at module load time.
#
# SECURITY: This is a strict allowlist.  Only symbols explicitly listed
# below can be resolved via lazy import.  Arbitrary attribute access
# will raise AttributeError as normal — no dynamic code execution.
# ---------------------------------------------------------------------------
_LAZY_IMPORTS: dict[str, str] = {
    # symbol_name: module_path
    "fetch_data": "pmcopy.utils.fetchData",
    "close_session": "pmcopy.utils.fetchData",
    "get_my_balance": "pmcopy.utils.getMyBalance",
    "build_trade_key": "pmcopy.utils.idempotency",
    "has_processed": "pmcopy.utils.idempotency",
    "mark_processed": "pmcopy.utils.idempotency",
}
_LAZY_IMPORT_LOCK = _threading.Lock()


def __getattr__(name: str):
    """Lazy-load heavy submodules only when accessed (strict allowlist)."""
    module_path = _LAZY_IMPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    mod = importlib.import_module(module_path)
    value = getattr(mod, name)
    with _LAZY_IMPORT_LOCK:
        # Double-check after acquiring lock (another thread may have set it)
        if name not in globals():
            globals()[name] = value
    return value


# Eagerly import lightweight modules that don't require optional deps
from .logger import (  # noqa: E402 - kept after lazy-export definitions intentionally
    configure_logging,
    get_logger,
    print_startup_banner,
    print_status_checks,
    print_clob_client_status,
    print_clob_error,
    print_service_start,
    print_db_status,
    print_positions,
    print_traders_copying,
    print_monitor_start,
    print_first_run_init,
    print_historical_done,
    print_new_trade_detected,
    print_trade_queued,
    print_capital_info,
    print_executing_strategy,
    print_balance_update,
    print_trade_result,
    print_skip_reason,
    print_error,
    print_warning,
    print_info,
    print_success,
    print_api_error,
    print_api_response,
    print_order_skip,
    monitor_heartbeat_tick,
    reset_heartbeat,
    tx_link,
    event_link,
    shorten_addr,
    get_wallet_type,
    log_trade_execution,
    log_trade_detected,
    log_trade_decision,
    log_trade_skip,
    log_trade_submitted,
    log_risk_exit,
    log_monitor_heartbeat,
    log_api_response,
)

__all__ = [
    # validation
    "is_valid_clob_token_id",
    # fetchData
    "fetch_data",
    "close_session",
    # getMyBalance
    "get_my_balance",
    # idempotency
    "build_trade_key",
    "has_processed",
    "mark_processed",
    # logger — configuration
    "configure_logging",
    "get_logger",
    # logger — rich console printers
    "print_startup_banner",
    "print_status_checks",
    "print_clob_client_status",
    "print_clob_error",
    "print_service_start",
    "print_db_status",
    "print_positions",
    "print_traders_copying",
    "print_monitor_start",
    "print_first_run_init",
    "print_historical_done",
    "print_new_trade_detected",
    "print_trade_queued",
    "print_capital_info",
    "print_executing_strategy",
    "print_balance_update",
    "print_trade_result",
    "print_skip_reason",
    "print_error",
    "print_warning",
    "print_info",
    "print_success",
    "print_api_error",
    "print_api_response",
    "print_order_skip",
    # logger — heartbeat
    "monitor_heartbeat_tick",
    "reset_heartbeat",
    # logger — helpers
    "tx_link",
    "event_link",
    "shorten_addr",
    "get_wallet_type",
    # logger — structured logging
    "log_trade_execution",
    "log_trade_detected",
    "log_trade_decision",
    "log_trade_skip",
    "log_trade_submitted",
    "log_risk_exit",
    "log_monitor_heartbeat",
    "log_api_response",
]
