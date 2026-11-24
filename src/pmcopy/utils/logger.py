"""Secret-safe structured and operator-facing logging facade.

Decomposed into ``pmcopy.utils.logging`` (``formatters``, ``console_handler``,
``printers``, ``trade_events``).
"""

from __future__ import annotations

from pmcopy.utils.logging.console_handler import (
    _CONFIGURED_LOGGERS,  # noqa: F401
    RichConsoleHandler,
    _ManagedRichConsoleHandler,  # noqa: F401
    _ManagedRotatingFileHandler,  # noqa: F401
    _ManagedStreamHandler,  # noqa: F401
    _remove_managed_handlers,  # noqa: F401
    configure_logging,
    get_logger,
)
from pmcopy.utils.logging.formatters import (
    APP_NAME,
    DEFAULT_BACKUP_COUNT,
    DEFAULT_LOG_SUBDIR,
    DEFAULT_MAX_BYTES,
    MONITOR_HEARTBEAT_INTERVAL,
    JsonFormatter,
    _HTTP_ERROR_CATEGORIES,  # noqa: F401
    _LEVEL_STYLES,  # noqa: F401
    _ManagedHandlerMixin,  # noqa: F401
    _ORDER_LIFECYCLE_LAST,  # noqa: F401
    _ORDER_LIFECYCLE_LAST_MAX,  # noqa: F401
    _ORDER_LIFECYCLE_PANEL,  # noqa: F401
    _POSITION_DRIFT_PANEL,  # noqa: F401
    _RedactedRenderable,  # noqa: F401
    _SecretRedactionFilter,  # noqa: F401
    _STANDARD_RECORD_ATTRS,  # noqa: F401
    _THEME,  # noqa: F401
    _append_extra_fields,  # noqa: F401
    _build_panel_lines,  # noqa: F401
    _default_log_dir,  # noqa: F401
    _extra_value_text,  # noqa: F401
    _lifecycle_dedup_key,  # noqa: F401
    _lifecycle_should_emit,  # noqa: F401
    _record_extra_fields,  # noqa: F401
    _redact_record,  # noqa: F401
    console,
)
from pmcopy.utils.logging.printers import (
    MonitorHeartbeat,
    _first_present_text,  # noqa: F401
    _heartbeat,  # noqa: F401
    _parse_api_error,  # noqa: F401
    _response_shape,  # noqa: F401
    event_link,
    format_usdc,
    get_wallet_type,
    monitor_heartbeat_tick,
    print_api_error,
    print_api_response,
    print_balance_update,
    print_capital_info,
    print_clob_client_status,
    print_clob_error,
    print_db_status,
    print_error,
    print_executing_strategy,
    print_first_run_init,
    print_historical_done,
    print_info,
    print_manual_sync_discrepancy,
    print_manual_trade_detected,
    print_monitor_start,
    print_new_trade_detected,
    print_order_skip,
    print_positions,
    print_service_start,
    print_skip_reason,
    print_startup_banner,
    print_status_checks,
    print_success,
    print_trade_queued,
    print_trade_result,
    print_traders_copying,
    print_warning,
    redact_error_detail,
    reset_heartbeat,
    safe_error_detail,
    shorten_addr,
    tx_link,
)
from pmcopy.utils.logging.trade_events import (
    _log_trade,  # noqa: F401
    _reset_lifecycle_dedup_for_tests,  # noqa: F401
    log_api_error,
    log_api_response,
    log_monitor_heartbeat,
    log_order_lifecycle,
    log_position_drift,
    log_risk_exit,
    log_trade_batch_summary,
    log_trade_decision,
    log_trade_detected,
    log_trade_execution,
    log_trade_skip,
    log_trade_submitted,
)

__all__ = [
    "APP_NAME",
    "DEFAULT_BACKUP_COUNT",
    "DEFAULT_LOG_SUBDIR",
    "DEFAULT_MAX_BYTES",
    "MONITOR_HEARTBEAT_INTERVAL",
    "JsonFormatter",
    "MonitorHeartbeat",
    "RichConsoleHandler",
    "configure_logging",
    "console",
    "event_link",
    "format_usdc",
    "get_logger",
    "get_wallet_type",
    "log_api_error",
    "log_api_response",
    "log_monitor_heartbeat",
    "log_order_lifecycle",
    "log_position_drift",
    "log_risk_exit",
    "log_trade_batch_summary",
    "log_trade_decision",
    "log_trade_detected",
    "log_trade_execution",
    "log_trade_skip",
    "log_trade_submitted",
    "monitor_heartbeat_tick",
    "print_api_error",
    "print_api_response",
    "print_balance_update",
    "print_capital_info",
    "print_clob_client_status",
    "print_clob_error",
    "print_db_status",
    "print_error",
    "print_executing_strategy",
    "print_first_run_init",
    "print_historical_done",
    "print_info",
    "print_manual_sync_discrepancy",
    "print_manual_trade_detected",
    "print_monitor_start",
    "print_new_trade_detected",
    "print_order_skip",
    "print_positions",
    "print_service_start",
    "print_skip_reason",
    "print_startup_banner",
    "print_status_checks",
    "print_success",
    "print_trade_queued",
    "print_trade_result",
    "print_traders_copying",
    "print_warning",
    "redact_error_detail",
    "reset_heartbeat",
    "safe_error_detail",
    "shorten_addr",
    "tx_link",
]
