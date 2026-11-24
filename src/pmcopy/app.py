"""Compose, run, supervise, and close the Polymarket copy-trading runtime.

Decomposed into ``pmcopy.orchestrators.app`` (``builders``, ``startup``, ``runner``)
while preserving the canonical ``pmcopy.app`` entrypoint and facade contract.
"""

from __future__ import annotations

from typing import Any

from pmcopy.bootstrap import build_app  # noqa: F401
from pmcopy.config.app_config import AppConfig, EnvValidationError, load_config  # noqa: F401
from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime  # noqa: F401
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import (
    build_user_state,  # noqa: F401
    fetch_live_positions,  # noqa: F401
)
from pmcopy.orchestrators.lifecycle.startup_health import (
    perform_startup_health_check,  # noqa: F401
)
from pmcopy.services.accounts.account_lease import AccountLeaseManager  # noqa: F401
from pmcopy.utils.wallet_balance import get_my_balance  # noqa: F401
from pmcopy.orchestrators.app.builders import (
    _attach_market_data_runtime_to_contexts,  # noqa: F401
    _build_detection_source_factory,  # noqa: F401
    _build_leader_sell_fraction_resolver,  # noqa: F401
    _build_market_token_subscribe_closure,  # noqa: F401
    _build_onchain_condition_id_resolver,  # noqa: F401
    _build_onchain_websocket_factory,  # noqa: F401
    _build_onchain_ws_health_reporter,  # noqa: F401
    _build_pending_copy_tracker,  # noqa: F401
    _build_pending_tx_config,  # noqa: F401
    _build_reconcile_wake_closure,  # noqa: F401
    _build_risk_cache_evict_closure,  # noqa: F401
    _build_telemetry_emitters,  # noqa: F401
    _cold_start_monitored_after_reconcile,  # noqa: F401
    _confirmed_source_builder,  # noqa: F401
    _copy_task_contexts,  # noqa: F401
    _default_source_state_store_provider,  # noqa: F401
    _detection_confidence_from_config,  # noqa: F401
    _detection_connection_probe,  # noqa: F401
    _discovery_refresh_loop,  # noqa: F401
    _reap_reservations_once,  # noqa: F401
    _refresh_contexts_for_new_leaders,  # noqa: F401
    _refresh_onchain_watched_wallets,  # noqa: F401
    _reject_duplicate_user_state,  # noqa: F401
    _should_wait_for_discovery_roster,  # noqa: F401
    _shutdown_runtime,  # noqa: F401
    _start_market_data_runtimes,  # noqa: F401
    _start_onchain_runtime,  # noqa: F401
    _start_telemetry_runtime,  # noqa: F401
    _start_user_data_runtimes,  # noqa: F401
    _subscribe_context_tokens,  # noqa: F401
    _to_decimal_balance,  # noqa: F401
    _validate_detection_confidence_floor,  # noqa: F401
    _wait_for_active_leaders,  # noqa: F401
    _wallets_blocked_by_reconcile_failures,  # noqa: F401
)
from pmcopy.orchestrators.app.runner import _run_application  # noqa: F401
from pmcopy.orchestrators.lifecycle.app_lifecycle import (
    HealthServerResource as _HealthServerResource,  # noqa: F401
    SupervisedTaskSpec as _Supervised,  # noqa: F401
    close_polymarket_clients,
    empty_user_ws_api_creds as _empty_user_ws_api_creds,  # noqa: F401
    flatten_contexts as _flatten_contexts,  # noqa: F401
    get_clob_wrapper_from_user_state as _get_clob_wrapper_from_user_state,  # noqa: F401
    live_monitor_count as _live_monitor_count,  # noqa: F401
    normalise_user_ws_api_creds as _normalise_user_ws_api_creds,  # noqa: F401
    on_supervised_crash as _on_supervised_crash,  # noqa: F401
    user_ws_api_creds as _user_ws_api_creds,  # noqa: F401
)
from pmcopy.orchestrators.lifecycle.trader_startup_policy import TraderStartupPolicy
from pmcopy.orchestrators.streaming.market_data_runtime import MarketDataRuntime  # noqa: F401
from pmcopy.utils.logger import (
    configure_logging,  # noqa: F401
    get_logger,
    print_error,  # noqa: F401
    redact_error_detail,
)

logger = get_logger("Main")


async def _close_polymarket_clients(clients: list[Any]) -> None:
    """Close each distinct SDK owner without masking the shutdown reason."""
    await close_polymarket_clients(clients, logger=logger)


async def main(*, auto_trader: bool = False, copier: str | None = None) -> int:
    """Run the application while owning every created Polymarket client.

    Args:
        auto_trader: Use durable automatic membership and degradation-driven rotation.
        copier: Optional copier id selector (``--copier``); limits routing and
            runtime ownership to that single configured copier.

    Returns:
        Process exit code.
    """
    clients: list[Any] = []
    try:
        return await _run_application(
            clients,
            startup_policy=TraderStartupPolicy(auto_trader=auto_trader),
            copier=copier,
        )
    finally:
        await _close_polymarket_clients(clients)
        # Startup can return before ``_shutdown_runtime`` exists (doctor,
        # validation, or compliance failures). Always close the process-wide
        # HTTP pool here too; ``close_session`` is intentionally idempotent.
        try:
            from pmcopy.utils.fetch_data import close_session

            await close_session()
        except Exception as exc:
            logger.warning(
                "shared_http_client_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )
        try:
            from pmcopy.storage.postgres.connection import shutdown_pool

            await shutdown_pool()
        except Exception as exc:
            logger.warning(
                "db_pool_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )


if __name__ == "__main__":
    from pmcopy.utils.application_runner import ApplicationRunner
    from pmcopy.utils.event_loop import new_event_loop

    import sys

    exit_code = 0
    try:
        # main() returns 1 on a fatal/fail-closed startup or a supervisor
        # exhaustion; orderly SIGTERM/SIGINT shutdown cancels the task and
        # exits 0.
        exit_code = ApplicationRunner(loop_factory=new_event_loop).run(main()) or 0
    except Exception as exc:
        import traceback

        print_error(f"Fatal startup error: {exc}")
        traceback.print_exc()
        exit_code = 1
    sys.exit(exit_code)
