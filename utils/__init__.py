"""Utility package exports."""

import re as _re

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


from .createClobClient import ClobClientWrapper, MarketMeta, create_clob_client
from .fetchData import fetch_data, close_session
from .getMyBalance import get_my_balance
from .idempotency import build_trade_key, has_processed, mark_processed
from .logger import (
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
    # createClobClient
    "ClobClientWrapper",
    "MarketMeta",
    "create_clob_client",
    # fetchData
    "fetch_data",
    "close_session",
    # getMyBalance
    "get_my_balance",
    # postOrder — REMOVED (replaced by ExecutionService)
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
