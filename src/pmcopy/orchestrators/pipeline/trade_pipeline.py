"""Normalize leader signals and execute them through the guarded pipeline.

Decomposed into ``pipeline_market_data``, ``pipeline_sizing_and_caps``,
``pipeline_reconcile``, ``pipeline_buy_stage``, ``pipeline_sell_stage``,
and ``pipeline_executor`` while preserving the canonical
``pmcopy.orchestrators.pipeline.trade_pipeline`` facade contract.
"""

from __future__ import annotations

from pmcopy.orchestrators.pipeline.pipeline_buy_stage import (
    _prepare_buy_execution_request,  # noqa: F401
)
from pmcopy.orchestrators.pipeline.pipeline_executor import (
    _execute_trades_impl,  # noqa: F401
    execute_trades,
)
from pmcopy.orchestrators.pipeline.pipeline_market_data import (
    _COPY_TRADE_IDENTITY,  # noqa: F401
    _DISABLED_TELEMETRY,  # noqa: F401
    _LEADER_SELL_MINIMUM_POLICY,  # noqa: F401
    _TX_HASH_KEYS,  # noqa: F401
    _analysis_semaphore,  # noqa: F401
    _copy_entry_price_plan,  # noqa: F401
    _decimal_or_zero,  # noqa: F401
    _ensure_token_subscribed,  # noqa: F401
    _execution_telemetry,  # noqa: F401
    _fetch_live_position_size,  # noqa: F401
    _market_data_primary_source,  # noqa: F401
    _market_data_subscribe_timeout,  # noqa: F401
    _matched_amount_from_response,  # noqa: F401
    _observe_copy_latency,  # noqa: F401
    _positive_int,  # noqa: F401
    _prefetch_copy_execution_metadata,  # noqa: F401
    _prefetch_market_data_for_signals,  # noqa: F401
    _prefetch_market_data_wss,  # noqa: F401
    _reconcile_semaphore,  # noqa: F401
    _resolve_entry_drift_pct,  # noqa: F401
    _resolve_market_bundle,  # noqa: F401
    _spawn_market_data_warmup,  # noqa: F401
    _submit_copy_through_coordinator,  # noqa: F401
    _sync_sell_position_from_live_holdings,  # noqa: F401
    get_reconciliation_service,
)
from pmcopy.orchestrators.pipeline.pipeline_reconcile import (
    _SignalRef,  # noqa: F401
    _decision_live_price,  # noqa: F401
    _project_pipeline_results,  # noqa: F401
    _reconcile_one,  # noqa: F401
    _signals_by_trade_key,  # noqa: F401
    _trade_telemetry,  # noqa: F401
    reconcile_submitted_unconfirmed,
)
from pmcopy.orchestrators.pipeline.pipeline_sell_stage import (
    _prepare_sell_execution_request,  # noqa: F401
)
from pmcopy.orchestrators.pipeline.pipeline_sizing_and_caps import (
    TradePipelineResult,
    _BuyRiskCap,  # noqa: F401
    _account_cap_policy_enabled,  # noqa: F401
    _buy_fee_bps_or_none,  # noqa: F401
    _buy_required_cash_with_fees,  # noqa: F401
    _derive_sell_tokens,  # noqa: F401
    _explicit_copier_exposure_cap,  # noqa: F401
    _extract_leader_sell_fraction,  # noqa: F401
    _extract_trade_outcome,  # noqa: F401
    _extract_trade_submarket,  # noqa: F401
    _fetch_balance_snapshot,  # noqa: F401
    _first_text,  # noqa: F401
    _funder_wallet_address,  # noqa: F401
    _max_non_terminal_failure_attempts,  # noqa: F401
    _normalize,  # noqa: F401
    _position_pct_cap_balance_usdc,  # noqa: F401
    _position_pct_cap_snapshot_is_fresh,  # noqa: F401
    _resolve_buy_risk_cap,  # noqa: F401
    _resolve_leader_sell_tokens,  # noqa: F401
    _resolve_leader_tier,  # noqa: F401
    _resolve_position_pct_cap,  # noqa: F401
    _retry_attempt_from_status,  # noqa: F401
    _seed_position_pct_cap_snapshot_if_stale,  # noqa: F401
    _to_decimal,  # noqa: F401
    _trade_config,  # noqa: F401
    all_tx_hashes_for_trade,
    build_trade_key,
    copier_open_exposure_headroom,
    normalize_signals,
    seed_position_pct_cap_snapshot,
)
from pmcopy.services.execution.reconciliation_service import ReconciliationService  # noqa: F401
from pmcopy.services.observability.metrics import metrics  # noqa: F401
from pmcopy.utils.logger import (
    get_logger,
    log_trade_decision,  # noqa: F401
    log_trade_execution,  # noqa: F401
    log_trade_skip,  # noqa: F401
    log_trade_submitted,  # noqa: F401
    print_executing_strategy,  # noqa: F401
    print_skip_reason,  # noqa: F401
    print_trade_result,  # noqa: F401
    print_warning,  # noqa: F401
    shorten_addr,  # noqa: F401
)
from pmcopy.utils.wallet_balance import get_my_balance  # noqa: F401

logger = get_logger("TradePipeline")

__all__ = [
    "TradePipelineResult",
    "all_tx_hashes_for_trade",
    "build_trade_key",
    "copier_open_exposure_headroom",
    "execute_trades",
    "get_reconciliation_service",
    "normalize_signals",
    "reconcile_submitted_unconfirmed",
    "seed_position_pct_cap_snapshot",
]
