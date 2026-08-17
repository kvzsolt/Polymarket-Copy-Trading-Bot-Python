"""Immutable application configuration loaded from service-owned YAML files.

Source precedence, from lowest to highest: typed model defaults ->
``config/<service>.yml`` (canonical, one file per owning service) ->
deployment ``config/runtime.local.yml`` (single legacy deprecation shim,
translated) -> ``config/environments/<PMCOPY_ENVIRONMENT>/<service>.yml``
(explicitly selected environment, default ``paper``) -> ``config/leaders/
<wallet>.yml`` (per-leader overrides) -> nested test/composition ``overrides``
-> deployment mode-0600 ``secrets.env`` -> process variables named
``PMCOPY__SECTION__FIELD``.

The environment is selected explicitly (``PMCOPY_ENVIRONMENT``); it is never
inferred from credential presence. Legacy pre-split key names (``network.*``,
``leader.*``, ``polling.*``, ...) keep working through a translation table
applied at the loader boundary only — for the shim, ``secrets.env``, process
environment, and programmatic overrides.

The public object remains a graph of frozen dataclasses/models so the trading
runtime gets an immutable, typed contract with strict unknown-key rejection.
"""

from __future__ import annotations

import errno
import ipaddress
import logging
import os
import re
import stat
import sys as _sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import orjson
import yaml
from dotenv import dotenv_values
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    TypeAdapter,
    ValidationError,
    model_validator,
)
from config.env import EnvValidationError, SignatureType, WalletConfig, secret_value
from config.v1_runtime import CopyTasksRuntimeConfig
from domain.detection import DetectionMode

__all__ = [
    "AppConfig",
    "AppConfigSemanticValidator",
    "AppServiceConfig",
    "CopyTradingConfig",
    "ControlPlaneAuthConfig",
    "ControlPlaneConfig",
    "DatabaseConfig",
    "DetectionConfig",
    "DetectionModeResolver",
    "DiscoveryConfig",
    "EnvValidationError",
    "ExecutionConfig",
    "LeaderCopySettings",
    "LeaderOverrideConfig",
    "LoggingConfig",
    "MarketDataConfig",
    "OnchainDetectionConfig",
    "OrderConfirmationConfig",
    "PolymarketConfig",
    "PositionDriftConfig",
    "ProtectiveExitsConfig",
    "ReconciliationConfig",
    "RiskConfig",
    "SignatureType",
    "SizingConfig",
    "WalletConfig",
    "POLYGON_USDCE_ADDRESS",
    "POLYMARKET_CTF_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_PUSD_ADDRESS",
    "SERVICE_CONFIG_FILES",
    "dump_config_payload",
    "load_config",
]

_ETH_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HEX_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_DETECTION_MODE_VALUES = tuple(mode.value for mode in DetectionMode)
_ONCHAIN_DETECTION_CONFIDENCE_VALUES = {
    "market_unattributed",
    "pending_probable",
    "mined_authoritative",
    "confirmed_final",
}
logger = logging.getLogger("AppConfig")

from config.addresses import (  # noqa: E402
    POLYGON_USDCE_ADDRESS,
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    POLYMARKET_PUSD_ADDRESS,
)


# Frozen config sections.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatabaseConfig:
    """Validate PostgreSQL connection and pool settings."""

    dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 20
    command_timeout_seconds: float = 30.0


@dataclass(frozen=True)
class AppServiceConfig:
    """Application-wide operational settings (service owner: app).

    ``kill_switch`` is the C1 global master switch: when True the bot
    observes but submits nothing; this supersedes every per-engine switch.
    Enforced at act-time in ``ExecutionService.execute`` so flipping it is a
    live hot-stop, not a construction-time no-op.
    """

    kill_switch: bool = False
    # Maximum age (seconds) for retained processed-trade rows before the
    # background purge sweeper deletes them.
    retention_max_age_seconds: float = 30.0 * 86400.0
    enable_metrics_server: bool = False
    metrics_port: int = 9102
    metrics_bind_host: str = "127.0.0.1"
    multi_user_config_file: str = ""


@dataclass(frozen=True)
class ControlPlaneAuthConfig:
    """Secret bearer credential loaded only from deployment secret layers."""

    token: SecretStr = field(default_factory=lambda: SecretStr(""))


@dataclass(frozen=True)
class ControlPlaneConfig:
    """Safe network policy for the optional operator control-plane adapter.

    The nested bearer credential is a :class:`SecretStr` accepted from
    ``secrets.env`` or process environment only in production. Frontend
    settings adapters omit the entire ``auth`` object.
    """

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 9103
    require_authentication: bool = True
    request_max_bytes: int = 262_144
    websocket_queue_capacity: int = 128
    telemetry_snapshot_limit: int = 100
    command_rate_limit: int = 30
    command_rate_window_seconds: float = 60.0
    rate_limit_max_keys: int = 1024
    shutdown_timeout_seconds: float = 5.0
    portfolio_refresh_interval_seconds: float = 10.0
    portfolio_stale_after_seconds: float = 30.0
    cors_allowed_origins: tuple[str, ...] = ()
    auth: ControlPlaneAuthConfig = field(default_factory=ControlPlaneAuthConfig)


@dataclass(frozen=True)
class DetectionConfig:
    """Leader-signal detection cadence and on-chain detection policy.

    ``mode`` selects the single on-chain detection source (``polling`` keeps
    REST polling only; ``on_chain_pending`` adds the provisional mempool
    path; ``on_chain_confirmed`` adds the mempool-observed, receipt-confirmed
    path). REST polling keeps running as a parallel fallback in every mode.
    The legacy ``onchain.enable_onchain_*`` booleans are deprecated and only
    consulted by ``DetectionModeResolver`` when ``mode`` is absent.
    """

    fetch_interval: float = 0.50
    aggregate_signal_window_seconds: float = 4.0
    leader_positions_min_fetch_spacing_ms: int = 250
    leader_activity_min_fetch_spacing_ms: int = 250
    trade_queue_max_batches: int = 64
    mode: str = DetectionMode.POLLING.value
    onchain: "OnchainDetectionConfig" = field(default_factory=lambda: OnchainDetectionConfig())

    @property
    def detection_mode(self) -> DetectionMode:
        """Return the typed detection source selected by ``mode``.

        Raises:
            ValueError: If ``mode`` is not a known ``DetectionMode`` value.
                ``AppConfigSemanticValidator`` rejects such configs first.
        """

        return DetectionMode(str(self.mode or "").strip().lower())


@dataclass(frozen=True)
class SizingConfig:
    """Validate copy-trade sizing settings."""

    mode: str = "proportional"
    percentage: float = 4.0
    fixed_amount: float = 3.0
    trade_multiplier: float = 1.0
    tiered_trade_multipliers: str = ""
    adaptive_basis: str = "trade_size"
    adaptive_min_percent: float = 0.0
    adaptive_max_percent: float = 0.0
    adaptive_threshold_usdc: float = 500.0
    buy_min_amount: float = 5.0
    buy_max_amount: float = 8.0
    position_threshold: int = 0
    max_position_pct: float = 0.10
    enable_trust_size_multipliers: bool = False
    trust_size_multiplier_min: float = 0.5
    trust_size_multiplier_max: float = 1.25
    # V5 pUSD-balance-aware envelope (folded from copy_sizing_v5.yaml; the
    # live consumer is the v1 task sizing path via
    # ``services.copy_sizing_v5.policy.copy_sizing_policy_from_config``).
    copyable_balance_percent: float = 75.0
    proportional_balance_cap_enabled: bool = True
    require_leader_balance_for_proportional: bool = True
    portfolio_weight_multiplier: float = 1.0
    strict_fixed_amount: bool = True
    strict_exact_mirror: bool = True


@dataclass(frozen=True)
class RiskConfig:
    """Account/exposure risk limits. Exit-lifecycle knobs live in
    :class:`ProtectiveExitsConfig` (service owner: protective_exits)."""

    enable_risk_mgmt: bool = True
    max_market_exposure_usdc: float = 300.0
    max_category_exposure_usdc: float = 600.0
    max_leader_exposure_usdc: float = 500.0
    max_open_positions: int = 40
    max_daily_drawdown_pct: float = 20.0
    kill_switch: bool = False
    enable_balance_tiered_policy: bool = False
    balance_policy_reserve_pct: float = 0.12
    balance_policy_reserve_floor: float = 7.0
    balance_policy_hard_cap_pct: float = 0.15
    balance_policy_hard_cap_ceiling: float = 8.0
    balance_policy_max_open_positions: int = 12
    balance_policy_allow_partial_exits: bool = False
    balance_policy_allow_pyramiding: bool = False
    balance_policy_max_adds_per_position: int = 1
    balance_policy_base_min_score: float = 0.22
    balance_policy_min_notional_usdc: float = 0.00001
    balance_policy_dynamic_threshold_raise: bool = False
    day_start_equity_usdc: float = 0.0
    # PR #105 review: bound the in-memory risk price cache via typed config
    # so the production sizing/risk path no longer relies on a raw env read.
    # ``services.risk.risk_monitor.RiskPriceCache`` still honours its env fallback
    # as a legacy adapter (issue #73 allows raw env in legacy/test paths only).
    risk_price_cache_max_size: int = 5000
    # Cadence (seconds) of the risk-monitor loop (was polling.risk_check_interval).
    risk_check_interval: float = 6.0
    # Cooldown (seconds) before re-dispatching a risk exit for a scope whose
    # previous protective SELL ended terminal (rejected/cancelled). Stops the
    # 1 Hz resubmission storm against phantom positions (2026-07-04 incident).
    # 0 disables. Defaulted so existing direct constructors keep working.
    risk_exit_reject_cooldown_seconds: float = 60.0
    risk_pre_reconcile_timeout_seconds: float = 25.0
    risk_pre_reconcile_backoff_seconds: float = 30.0
    # Sanity gate for the best-bid risk mark: reject crossed books outright and
    # reject marks whose bid/ask spread exceeds this cap (mid-relative bps).
    # A rejected mark means "no risk action this tick" — fail closed for the
    # protective-exit feature set. 0 disables the spread check.
    risk_mark_max_spread_bps: int = 2000
    # Phase 2.8 — absolute bid/ask distance cap for the risk mark. The bps-
    # of-mid gate is inert near 1.0 (bid >= 0.82 -> max spread < 2000 bps of
    # mid), exactly where near-max triggers live. This absolute price-point
    # gap rejects wide books near 1.0 that the bps gate misses. 0 disables.
    risk_mark_max_abs_distance: float = 0.0


@dataclass(frozen=True)
class ProtectiveExitsConfig:
    """Protective-exit feature switches and lifecycle thresholds.

    Combines the former ``features.enable_*`` exit flags with the former
    ``risk.*`` exit-lifecycle thresholds under one owning service.
    """

    enable_profit_lock: bool = False
    enable_near_max_exit: bool = False
    enable_trailing_stop: bool = False
    enable_staged_tp: bool = False
    enable_leader_divergence_exit: bool = False
    enable_stop_loss: bool = False
    near_max_price_warn: float = 0.95
    near_max_price_warn_trim_ratio: float = 0.6
    near_max_price_exit: float = 0.99
    # Phase 2.8 — min-hold grace (seconds) for the near-max partial trim. 0
    # disables (legacy instant-trim behavior). A fresh BUY at the ask with
    # the risk mark at the best bid satisfies the trim condition at the
    # instant of the fill; the grace prevents selling back across the spread.
    near_max_grace_period_seconds: float = 0.0
    profit_lock_trigger_pct: float = 70.0
    profit_lock_sell_fraction: float = 0.5
    trailing_activation_pct: float | None = None
    trailing_distance: float = 0.05
    staged_tp_levels: str = "0.30,0.50"
    staged_tp_fractions: str = "0.25,0.35"
    stop_loss_threshold: float = 0.15
    stop_loss_grace_period_seconds: float = 180.0


@dataclass(frozen=True)
class ExecutionConfig:
    """Validate venue-independent execution settings."""

    slippage_tolerance: float = 2.0
    min_share_price: float = 0.03
    max_share_price: float = 0.95
    non_terminal_failure_max_attempts: int = 3
    trading_mode: Literal["paper", "live"] = "paper"
    enable_copy_decision_gate: bool = False
    enable_near_resolution_gate: bool = False
    near_resolution_gate_price: float = 0.95
    near_resolution_max_age_seconds: float = 15.0
    pending_submission_ttl_seconds: int = 120
    # TTL for pre-submit balance/share reservations. Bounds the window in
    # which a pending order blocks the next same-scope copy before it is
    # expired (and reconciled). Mirrors ``pending_submission_ttl_seconds``.
    reservation_ttl_seconds: int = 120
    # Master switch for pre-submit BUY-quote / SELL-share reservations. When
    # False the ReservationService no-ops (returns placeholder reservations)
    # and double-spend protection falls back to the idempotency submission
    # lock + position coordinator. Defaults False on this hardening branch
    # because the reservation blocker was over-firing on legitimate same-scope
    # copies; flip to True once that is retuned. See docs/02-configuration.md.
    reservations_enabled: bool = False
    book_spread_mode: str = "absolute_bps"
    max_book_spread_bps: int = 2500
    max_book_spread_ticks: int = 0
    circuit_failure_threshold: int = 15
    # Phase 4 — balance regime
    max_position_size_usdc: float = 0.0
    max_daily_spend_usdc: float = 0.0
    min_balance_headroom_usdc: float = 0.0
    # Phase 4 — book depth gate
    min_book_depth_usdc: float = 0.0
    book_depth_band_pct: float = 5.0
    # Submission concurrency: typed cap so production paths do not depend on
    # raw env reads inside ExecutionService.
    execution_submit_concurrency: int = 2
    # Per-batch concurrency for trade analysis and unconfirmed-order
    # reconciliation. Typed so the orchestrator no longer reads ``config.raw``
    # (closes the partial-config-migration drift the audit flagged). Defaulted
    # to keep existing direct constructors / fixtures valid.
    trade_analysis_concurrency: int = 6
    reconcile_concurrency: int = 4
    # FAK (fill-and-kill) repost ceilings: how many extra book levels a copy
    # fill vs a forced protective exit may sweep. Promoted off ``config.raw``.
    copy_fak_max_attempts: int = 8
    exit_fak_max_attempts: int = 8
    # What to do with a leader SELL whose sell fraction cannot be determined
    # (leader positions fetch failed / annotation missing). "skip" fails
    # closed: the copy sell is dropped and retried on the next signal.
    # "full_close" preserves the legacy dump-everything behaviour.
    sell_fraction_missing_policy: str = "skip"
    # Phase 2.8 — sub-minimum leader sell policy. "round_up" (legacy) sells
    # the venue minimum when the proportional sell is below it (a leader's
    # small nibble becomes a near-full exit on small positions). "skip"
    # (fail-closed) drops the copy sell rather than overshoot.
    sub_min_sell_policy: str = "round_up"
    # C1 — execution-level kill switch, enforced at act-time in
    # ExecutionService.execute (not just in RiskEngine.evaluate). When True
    # the service refuses BUY entries but still allows protective SELL exits
    # (closing a position is risk-reducing and must not be stranded). The
    # global master (AppConfig.global_kill_switch) supersedes this and blocks
    # every submission including protective exits.
    kill_switch: bool = False

    @property
    def paper_trade_only(self) -> bool:
        """Return whether execution is restricted to paper trading."""
        return self.trading_mode == "paper"


class LeaderOverrideConfig(BaseModel):
    """Per-leader override values from one ``config/leaders/<wallet>.yml``.

    Leader files hold per-leader tunables only; they never join the roster —
    the roster stays explicit in ``copy_trading.wallets`` (or discovery).
    ``None`` means "inherit the layered copy_trading value".
    """

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    wallet: str = ""
    enabled: bool = True
    copy_decision_min_score: float | None = None
    copy_max_price_drift_pct: float | None = None
    copy_max_edge_decay_pct: float | None = None
    copy_max_signal_age_seconds: float | None = None
    copy_liquidity_target_usdc: float | None = None


@dataclass(frozen=True)
class LeaderCopySettings:
    """Effective copy tunables for one leader after override resolution."""

    copy_decision_min_score: float
    copy_max_price_drift_pct: float
    copy_max_edge_decay_pct: float
    copy_max_signal_age_seconds: float
    copy_liquidity_target_usdc: float


class CopyTradingConfig(BaseModel):
    """Copy-trading roster, gates, and the v1 copy-task engine settings."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    wallets: tuple[str, ...] = ()
    copy_decision_min_score: float = 0.15
    copy_max_price_drift_pct: float = 15.0
    copy_max_edge_decay_pct: float = 90.0
    copy_max_signal_age_seconds: float = 100.0
    copy_liquidity_target_usdc: float = 50.0
    # Tier-gate observation flags (formerly features.enable_tier_gate_*).
    enable_tier_gate_watchlist: bool = False
    enable_tier_gate_watchlist_log: bool = False
    # v1 copy-task engine (formerly config/runtime_v1.yml). Paper-safe:
    # ``copy_tasks.enabled`` defaults to False.
    copy_tasks: CopyTasksRuntimeConfig = CopyTasksRuntimeConfig()
    # Per-leader overrides sourced from ``config/leaders/<wallet>.yml``.
    leaders: tuple[LeaderOverrideConfig, ...] = ()

    def leader_override(self, wallet: str) -> LeaderOverrideConfig | None:
        """Return the override entry for ``wallet`` (case-insensitive)."""

        normalized = str(wallet).strip().lower()
        for entry in self.leaders:
            if entry.wallet.strip().lower() == normalized:
                return entry
        return None

    def resolved_for_leader(self, wallet: str) -> LeaderCopySettings:
        """Resolve effective copy tunables for one leader.

        Per-leader file values take precedence over the layered
        ``copy_trading`` values; unset override fields inherit.
        """

        override = self.leader_override(wallet)

        def _pick(name: str) -> float:
            value = getattr(override, name, None) if override else None
            return float(getattr(self, name) if value is None else value)

        return LeaderCopySettings(
            copy_decision_min_score=_pick("copy_decision_min_score"),
            copy_max_price_drift_pct=_pick("copy_max_price_drift_pct"),
            copy_max_edge_decay_pct=_pick("copy_max_edge_decay_pct"),
            copy_max_signal_age_seconds=_pick("copy_max_signal_age_seconds"),
            copy_liquidity_target_usdc=_pick("copy_liquidity_target_usdc"),
        )


@dataclass(frozen=True)
class LoggingConfig:
    """Validate structured logging settings."""

    level: str = "INFO"
    log_dir: str = "log"
    app_log_jsonl_file: str = "log/PolymarketCopyTradingBot.jsonl"
    log_max_bytes: int = 5242880
    log_backup_count: int = 3


@dataclass(frozen=True)
class PolymarketConfig:
    """Polymarket venue/network endpoints and contract addresses."""

    rpc_url: str = ""
    chain_id: int = 137
    usdc_contract_address: str = ""
    enforce_geoblock: bool = True
    # Optional fallback RPC when primary returns internal errors (-32603).
    fallback_rpc_url: str = ""
    # Polymarket-side collateral contract addresses. CLOB v2 defaults to pUSD
    # collateral; ``usdc_contract_address`` is kept as a legacy field name
    # for callers that still expect that attribute.
    collateral_contract_address: str = ""
    pusd_contract_address: str = ""


@dataclass(frozen=True)
class PositionDriftConfig:
    """PR7 — manual / off-bot position drift policy.

    Proactive cross-check between local position state and venue truth.
    """

    check_on_startup: bool = True
    fail_closed: bool = True
    allow_auto_prune_zero: bool = True
    allow_external_position_tracking: bool = False
    pause_scope_on_unresolved: bool = True
    max_size_epsilon: Decimal = Decimal("0.000001")
    prune_holdoff_seconds: float = 60.0


@dataclass(frozen=True)
class OrderConfirmationConfig:
    """Confirmation-delay and ambiguous-timeout policy (PR5).

    Polymarket order submissions legitimately pass through delayed/live/matched
    states before reaching CONFIRMED. The reconciler should defer its first
    poll by ``initial_delay_seconds`` (plus ``delayed_extra_seconds`` if the
    venue acknowledged ``delayed``). After ``max_wait_seconds + auto_release_
    grace_seconds`` with no terminal state, the strict auto-release path may
    run a battery of safety probes; only when every probe is clean is the
    reservation released. Probe failures or inconclusive results keep the
    reservation active.
    """

    initial_delay_seconds: float = 2.0
    poll_interval_seconds: float = 3.0
    max_wait_seconds: float = 90.0
    delayed_extra_seconds: float = 2.0
    token_delay_overrides: dict[str, float] = field(default_factory=dict)
    condition_delay_overrides: dict[str, float] = field(default_factory=dict)
    auto_release_ambiguous: bool = True
    auto_release_grace_seconds: float = 300.0
    auto_release_require_all_probes_clean: bool = True
    auto_release_max_probe_errors: int = 0
    # Per-order pacing for repeated auto-release attempts: the first failed
    # attempt reschedules after ``auto_release_retry_backoff_seconds`` and the
    # interval doubles per failed attempt up to the ``_max`` cap. Keeps a stuck
    # ambiguous order from being probed on every reconcile tick.
    auto_release_retry_backoff_seconds: float = 30.0
    auto_release_retry_backoff_max_seconds: float = 900.0

    def resolve_initial_delay(
        self,
        *,
        token_id: str | None,
        condition_id: str | None,
        venue_status: str | None = None,
    ) -> float:
        """Return the configured startup delay for the active mode."""
        delay = self.initial_delay_seconds
        if token_id and token_id in self.token_delay_overrides:
            delay = self.token_delay_overrides[token_id]
        elif condition_id and condition_id in self.condition_delay_overrides:
            delay = self.condition_delay_overrides[condition_id]
        if str(venue_status or "").lower() == "delayed":
            delay += self.delayed_extra_seconds
        return delay


@dataclass(frozen=True)
class ReconciliationConfig:
    """Reconciliation cadence plus the confirmation-delay and drift policies
    it owns (formerly the top-level ``order_confirmation``/``position_drift``
    sections)."""

    stale_threshold_seconds: float = 3600.0
    auto_cancel_stale: bool = False
    # Per-tick cap on how many already-closed orders the reconciler is
    # allowed to re-probe. Fail-closed default mirrors the previous
    # ``ENV.get("RECONCILE_MAX_CLOSED_PROBES", 10)`` read.
    max_closed_probes: int = 10
    # Cadence (seconds) of the reconciliation loop (was
    # ``polling.reconcile_interval_seconds``).
    reconcile_interval_seconds: int = 300
    order_confirmation: OrderConfirmationConfig = field(default_factory=OrderConfirmationConfig)
    position_drift: PositionDriftConfig = field(default_factory=PositionDriftConfig)


class _FrozenSettingsModel(BaseModel):
    # ``allow_inf_nan=False`` is set here rather than per-field: Pydantic
    # resolves it from the model owning the schema, so a nested BaseModel
    # ignores the shell's setting. Every settings model inherits this base,
    # so NaN/+Inf/-Inf are rejected graph-wide without a field allowlist to
    # drift. Nested frozen dataclasses inherit it from ``_LayeredSettings``.
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class FalconApiConfig(_FrozenSettingsModel):
    """Validate the private Falcon analytics transport settings."""

    base_url: str = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
    token: SecretStr = SecretStr("")
    timeout_seconds: float = Field(20.0, gt=0)
    max_retries: int = Field(6, ge=1, le=10)
    requests_per_second: float = Field(1.0, gt=0)
    burst: float = Field(1.0, ge=1)
    max_connections: int = Field(32, ge=1)
    max_keepalive_connections: int = Field(16, ge=0)
    keepalive_expiry_seconds: float = Field(30.0, gt=0)
    retry_base_delay_seconds: float = Field(1.0, gt=0)
    retry_max_delay_seconds: float = Field(30.0, gt=0)
    retry_jitter_ratio: float = Field(0.50, ge=0, le=1)

    @property
    def has_token(self) -> bool:
        """Return whether the analytics token is configured."""
        return bool(self.token.get_secret_value())


class ShortlistConfig(_FrozenSettingsModel):
    """Validate discovery shortlist thresholds."""

    min_win_rate_15d: float = Field(0.45, ge=0, le=1)
    max_win_rate_15d: float = Field(0.92, ge=0, le=1)
    min_roi_15d: float = 0.0
    min_pnl_15d: float = 100.0
    min_total_trades_15d: int = Field(30, ge=0)
    max_total_trades_15d: int = Field(5000, ge=1)
    sort_modes: tuple[str, ...] = ("h_score", "roi", "win_rate", "pnl")
    page_size: int = Field(100, ge=1, le=200)
    max_pages: int = Field(5, ge=1)
    candidate_pool_size: int = Field(250, ge=1)

    @model_validator(mode="after")
    def _ordered_bounds(self) -> "ShortlistConfig":
        if self.max_win_rate_15d < self.min_win_rate_15d:
            raise ValueError("shortlist max win rate must be >= min win rate")
        if self.max_total_trades_15d < self.min_total_trades_15d:
            raise ValueError("shortlist max trades must be >= min trades")
        if not self.sort_modes:
            raise ValueError("shortlist sort_modes must not be empty")
        return self


class Wallet360Config(_FrozenSettingsModel):
    """Validate Wallet360 evidence-window settings."""

    windows: tuple[Literal["1", "3", "7", "15"], ...] = ("1", "3", "7", "15")
    max_drawdown_pct: float = Field(35.0, ge=0)
    min_market_diversity: int = Field(3, ge=1)
    reject_bot_flagged: bool = True
    max_stake_cv: float = Field(2.5, gt=0)
    min_win_rate_consistency: float = Field(0.60, ge=0, le=1)

    @model_validator(mode="after")
    def _required_windows(self) -> "Wallet360Config":
        if "15" not in self.windows:
            raise ValueError("Wallet360 windows must contain 15")
        if len(set(self.windows)) != len(self.windows):
            raise ValueError("Wallet360 windows must be unique")
        return self


class RecencyConfig(_FrozenSettingsModel):
    """Validate discovery recency thresholds."""

    max_hours_since_last_trade: int = Field(48, ge=1)
    min_trades_lookback: int = Field(3, ge=1)
    lookback_hours: int = Field(168, ge=1)
    page_size: int = Field(100, ge=1, le=200)
    max_pages: int = Field(5, ge=1)


class MarketQualityConfig(_FrozenSettingsModel):
    """Validate market-quality thresholds for discovery."""

    min_volume_24h: float = Field(5000.0, ge=0)
    min_liquidity_percentile: float = Field(40.0, ge=0, le=100)
    volume_trend: Literal["UP", "DOWN", "ALL"] = "ALL"
    max_top1_wallet_pct: float = Field(60.0, ge=0, le=100)
    min_unique_traders_7d: int = Field(25, ge=0)
    min_pass_ratio: float = Field(0.60, ge=0, le=1)
    max_markets_per_candidate: int = Field(8, ge=1)


class SocialConfig(_FrozenSettingsModel):
    """Validate optional social-evidence settings."""

    enabled: bool = False
    keywords: tuple[str, ...] = ()
    hours_back: int = Field(24, ge=1)
    min_acceleration: float = 1.0
    min_author_diversity_pct: float = Field(20.0, ge=0, le=100)

    @model_validator(mode="after")
    def _keyword_wire_format(self) -> "SocialConfig":
        if any(
            not keyword or any(character.isspace() or character in "{}," for character in keyword)
            for keyword in self.keywords
        ):
            raise ValueError(
                "social keywords must be non-empty single tokens without spaces, commas, or braces"
            )
        return self


class ScoringWeights(_FrozenSettingsModel):
    """Define normalized top-level discovery scoring weights."""

    falcon_quality: float = Field(0.30, ge=0)
    copyability: float = Field(0.25, ge=0)
    recent_consistency: float = Field(0.15, ge=0)
    market_quality: float = Field(0.10, ge=0)
    volume_fit: float = Field(0.10, ge=0)
    rank_quality: float = Field(0.05, ge=0)
    risk_inverse: float = Field(0.05, ge=0)

    @model_validator(mode="after")
    def _sum_to_one(self) -> "ScoringWeights":
        if abs(sum(self.model_dump().values()) - 1.0) > 1e-6:
            raise ValueError("discovery scoring weights must sum to 1.0")
        return self


class FalconQualityWeights(_FrozenSettingsModel):
    """Define normalized Falcon evidence weights."""

    h_score: float = Field(0.50, ge=0)
    roi: float = Field(0.20, ge=0)
    win_rate: float = Field(0.15, ge=0)
    sharpe: float = Field(0.10, ge=0)
    baseline: float = Field(0.05, ge=0)

    @model_validator(mode="after")
    def _sum_to_one(self) -> "FalconQualityWeights":
        if abs(sum(self.model_dump().values()) - 1.0) > 1e-6:
            raise ValueError("Falcon-quality weights must sum to 1.0")
        return self


class RuntimeSignalWeights(_FrozenSettingsModel):
    """Blend persisted discovery rank with fresh execution-time evidence."""

    roster_score: float = Field(0.70, ge=0)
    win_rate: float = Field(0.15, ge=0)
    pnl: float = Field(0.10, ge=0)
    activity: float = Field(0.05, ge=0)

    @model_validator(mode="after")
    def _sum_to_one(self) -> "RuntimeSignalWeights":
        if abs(sum(self.model_dump().values()) - 1.0) > 1e-6:
            raise ValueError("runtime signal weights must sum to 1.0")
        return self


class ScoringConfig(_FrozenSettingsModel):
    """Validate discovery scoring policy settings."""

    weights: ScoringWeights = ScoringWeights.model_validate({})
    quality_weights: FalconQualityWeights = FalconQualityWeights.model_validate({})
    runtime_weights: RuntimeSignalWeights = RuntimeSignalWeights.model_validate({})
    h_score_range: tuple[float, float] = (0.0, 100.0)
    roi_15d_range: tuple[float, float] = (-50.0, 200.0)
    win_rate_15d_range: tuple[float, float] = (0.35, 0.75)
    sharpe_15d_range: tuple[float, float] = (-1.0, 5.0)
    volume_target_15d: float = Field(50000.0, gt=0)
    rank_target: int = Field(100, ge=1)
    copyability_trade_target: int = Field(50, ge=1)
    risk_drawdown_max_pct: float = Field(100.0, gt=0)
    runtime_default_roster_score: float = Field(0.50, ge=0, le=1)
    runtime_pnl_target_usdc: float = Field(100.0, gt=0)
    runtime_activity_target: int = Field(8, ge=1)
    runtime_score_floor: float = Field(0.10, ge=0, le=1)

    @model_validator(mode="after")
    def _ranges_increase(self) -> "ScoringConfig":
        for name in (
            "h_score_range",
            "roi_15d_range",
            "win_rate_15d_range",
            "sharpe_15d_range",
        ):
            lower, upper = getattr(self, name)
            if upper <= lower:
                raise ValueError(f"{name} upper bound must exceed lower bound")
        return self


class LongitudinalWeights(_FrozenSettingsModel):
    """Define normalized longitudinal scoring weights."""

    sharpe: float = Field(0.30, ge=0)
    roi_slope: float = Field(0.20, ge=0)
    win_rate_stability: float = Field(0.20, ge=0)
    rank_trajectory: float = Field(0.15, ge=0)
    drawdown: float = Field(0.15, ge=0)

    @model_validator(mode="after")
    def _sum_to_one(self) -> "LongitudinalWeights":
        if abs(sum(self.model_dump().values()) - 1.0) > 1e-6:
            raise ValueError("longitudinal weights must sum to 1.0")
        return self


class LongitudinalConfig(_FrozenSettingsModel):
    """Validate longitudinal discovery settings."""

    enabled: bool = True
    half_life_days: float = Field(7.0, gt=0)
    prior_score: float = Field(0.50, ge=0, le=1)
    shrinkage_trade_count: int = Field(30, ge=1)
    history_days: int = Field(90, ge=1)
    pnl_backfill_days: int = Field(180, ge=1)
    weights: LongitudinalWeights = LongitudinalWeights.model_validate({})
    sharpe_range: tuple[float, float] = (-1.0, 5.0)
    roi_slope_range: tuple[float, float] = (-1.0, 1.0)
    rank_slope_range: tuple[float, float] = (-2.0, 2.0)
    drawdown_max_pct: float = Field(50.0, gt=0)
    stability_window: int = Field(4, ge=2)
    trend_window: int = Field(7, ge=2)
    backfill_candidates_per_cycle: int = Field(10, ge=0)
    backfill_concurrency: int = Field(2, ge=1)
    trade_backfill_max_pages: int = Field(50, ge=1)
    pnl_backfill_max_pages: int = Field(10, ge=1)
    leaderboard_windows: tuple[Literal[1, 3, 7, 30], ...] = (1, 3, 7, 30)
    sizing_multiplier_min: float = Field(0.75, gt=0)
    sizing_multiplier_max: float = Field(1.25, gt=0)
    sizing_multiplier_exponent: float = Field(1.0, gt=0)
    base_score_weight: float = Field(0.70, ge=0, le=1)
    longitudinal_score_weight: float = Field(0.30, ge=0, le=1)

    @model_validator(mode="after")
    def _combined_weights(self) -> "LongitudinalConfig":
        if abs(self.base_score_weight + self.longitudinal_score_weight - 1.0) > 1e-6:
            raise ValueError("base and longitudinal score weights must sum to 1.0")
        if self.sizing_multiplier_max < self.sizing_multiplier_min:
            raise ValueError("longitudinal sizing multiplier max must be >= min")
        for name in ("sharpe_range", "roi_slope_range", "rank_slope_range"):
            lower, upper = getattr(self, name)
            if upper <= lower:
                raise ValueError(f"{name} upper bound must exceed lower bound")
        return self


class HysteresisConfig(_FrozenSettingsModel):
    """Validate roster promotion and demotion thresholds."""

    promote_score: float = Field(0.62, ge=0, le=1)
    demote_score: float = Field(0.50, ge=0, le=1)
    min_tenure_cycles: int = Field(3, ge=1)
    min_dwell_seconds: int = Field(10800, ge=0)

    @model_validator(mode="after")
    def _gap(self) -> "HysteresisConfig":
        if self.promote_score <= self.demote_score:
            raise ValueError("promote_score must be greater than demote_score")
        return self


class DiscoverySchedulerConfig(_FrozenSettingsModel):
    """Validate bounded discovery scheduling settings."""

    enabled: bool = True
    rediscover_seconds: int = Field(1800, ge=30)
    rescore_seconds: int = Field(900, ge=30)
    evaluation_concurrency: int = Field(2, ge=1)
    max_evaluation_error_ratio: float = Field(0.25, ge=0, le=1)
    max_active_leaders: int = Field(2, ge=1)
    start_from_roster: bool = True
    startup_wait_timeout_seconds: int = Field(3900, ge=1)


class FalconBankrollConfig(_FrozenSettingsModel):
    """Bankroll-compatibility thresholds (mirrors BankrollCompatibilityPolicy).

    Semantic validation is enforced by the dataclass ``__post_init__`` when
    ``discovery_policy_from_config`` constructs the policy; the Pydantic
    model only parses and holds the values.
    """

    reserve_fraction: Decimal = Decimal("0.20")
    max_positions: int = Field(12, ge=1)
    max_position_fraction: Decimal = Decimal("0.10")
    target_trade_fraction: Decimal = Decimal("0.08")
    min_order_usdc: Decimal = Decimal("1")
    min_practical_copy_ratio: Decimal = Decimal("0.001")
    max_copy_ratio: Decimal = Decimal("1")
    min_copyable_trade_share: Decimal = Decimal("0.55")
    min_fit_score: Decimal = Decimal("0.55")
    visible_capacity_utilization: Decimal = Decimal("1")


class FalconDiscoveryConfig(_FrozenSettingsModel):
    """Validate Falcon discovery policy and enablement settings.

    The bearer token belongs solely to ``DiscoveryConfig.api``. When this
    policy is disabled or that token is empty, composition fails closed
    rather than substituting a weaker discovery source.
    """

    enabled: bool = False
    candidate_limit: int = Field(50, ge=1)
    candidate_concurrency: int = Field(6, ge=1)
    wallet360_windows: tuple[int, ...] = (1, 3, 7, 15)
    required_wallet360_windows: int = Field(3, ge=1)
    max_wallet360_snapshot_age_hours: int = Field(72, ge=1)
    recent_trade_lookback_hours: int = Field(48, ge=1)
    max_last_trade_age_hours: int = Field(12, ge=1)
    min_today_trades: int = Field(1, ge=0)
    min_recent_trades: int = Field(10, ge=0)
    max_recent_trades: int = Field(500, ge=1)
    min_h_score: Decimal = Decimal("65")
    min_liquidity_percentile: Decimal = Decimal("50")
    max_toxic_market_share: Decimal = Decimal("0.25")
    max_market_concentration: Decimal = Decimal("0.65")
    max_missing_field_ratio: Decimal = Decimal("0.25")
    max_drawdown: Decimal = Decimal("0.35")
    max_sybil_score: Decimal = Decimal("20")
    short_market_max_days: int = Field(30, ge=1)
    min_short_trade_count: int = Field(10, ge=0)
    min_short_trade_share: Decimal = Decimal("0.50")
    leaderboard_min_win_rate_15d: Decimal = Decimal("0.45")
    leaderboard_max_win_rate_15d: Decimal = Decimal("0.95")
    leaderboard_min_roi_15d: Decimal = Decimal("0")
    leaderboard_min_trades_15d: int = Field(25, ge=0)
    leaderboard_max_trades_15d: int = Field(100_000, ge=1)
    leaderboard_min_pnl_15d: Decimal = Decimal("0")
    bankroll: FalconBankrollConfig = FalconBankrollConfig.model_validate({})


class DiscoveryConfig(_FrozenSettingsModel):
    """Validate the complete discovery service configuration."""

    api: FalconApiConfig = FalconApiConfig.model_validate({})
    shortlist: ShortlistConfig = ShortlistConfig.model_validate({})
    wallet360: Wallet360Config = Wallet360Config.model_validate({})
    recency: RecencyConfig = RecencyConfig.model_validate({})
    market: MarketQualityConfig = MarketQualityConfig.model_validate({})
    social: SocialConfig = SocialConfig.model_validate({})
    scoring: ScoringConfig = ScoringConfig.model_validate({})
    longitudinal: LongitudinalConfig = LongitudinalConfig.model_validate({})
    hysteresis: HysteresisConfig = HysteresisConfig.model_validate({})
    scheduler: DiscoverySchedulerConfig = DiscoverySchedulerConfig.model_validate({})
    falcon: FalconDiscoveryConfig = FalconDiscoveryConfig.model_validate({})


@dataclass(frozen=True)
class MarketDataConfig:
    """Market data WSS health monitoring and REST fallback policy.

    PR10.4: REST `/book` snapshot is the primary execution source when
    ``primary_source == "rest_snapshot"``. WSS keeps running for background
    cache, prewarm, diagnostics, and resolved-market events, but copy-trade
    execution no longer waits for WSS subscribe/bootstrap.
    """

    primary_source: str = "rest_snapshot"
    rest_snapshot_max_age_ms: int = 500
    rest_snapshot_request_timeout_ms: int = 750
    rest_snapshot_total_budget_ms: int = 1000
    rest_snapshot_max_spread_bps: int = 300
    enable_rest_bootstrap: bool = True
    rest_bootstrap_max_age_ms: int = 500
    rest_bootstrap_request_timeout_ms: int = 750
    rest_bootstrap_total_budget_ms: int = 1000
    rest_bootstrap_max_spread_bps: int = 300
    rest_bootstrap_require_wss_connected: bool = True
    # Execution-time freshness policy for the WSS-fed market-data cache.
    # quote/book age windows apply to the snapshot's own timestamp; a quiet
    # WSS book past that window is still executable while the WSS connection
    # is proven live (heartbeat/pong within wss_connection_max_silence_seconds).
    executable_quote_max_age_seconds: float = 2.0
    executable_book_max_age_seconds: float = 2.0
    executable_tick_max_age_seconds: float = 300.0
    wss_connection_max_silence_seconds: float = 20.0
    subscribe_timeout_seconds: float = 2.0


@dataclass(frozen=True)
class OnchainDetectionConfig:
    """Early on-chain trader detection policy (PR2).

    The mined-``OrderFilled`` log subscription is always safe to run because
    mined fills are authoritative; it is gated by ``enable_mined_detection``
    (default False) so operators explicitly opt in with a WebSocket URL. The pending-
    transaction subscription is provisional and gated by
    ``enable_onchain_pending_detection`` (default False) — it must not feed
    the copy path until the operator opts in.
    """

    enable_onchain_pending_detection: bool = False
    enable_onchain_mined_detection: bool = False
    pending_tx_provider: str = "alchemy"
    onchain_ws_url: str = ""
    pending_tx_ws_url: str = ""
    pending_tx_hashes_only: bool = False
    pending_tx_max_signal_age_ms: int = 500
    pending_tx_detection_confidence_required: str = "pending_probable"


@dataclass(frozen=True)
class AppConfig:
    """Root configuration object: a composition of per-service typed models.

    Immutable after construction. Each field is owned by exactly one service
    and sourced from ``config/<field>.yml`` plus the layered overrides.
    """

    app: AppServiceConfig = field(default_factory=AppServiceConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    wallet: WalletConfig = field(
        default_factory=lambda: WalletConfig(private_key=SecretStr(""), chain_id=137)
    )
    polymarket: PolymarketConfig = field(default_factory=PolymarketConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    detection: DetectionConfig = field(default_factory=DetectionConfig)
    discovery: DiscoveryConfig = field(default_factory=lambda: DiscoveryConfig.model_validate({}))
    copy_trading: CopyTradingConfig = field(
        default_factory=lambda: CopyTradingConfig.model_validate({})
    )
    sizing: SizingConfig = field(default_factory=SizingConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    protective_exits: ProtectiveExitsConfig = field(default_factory=ProtectiveExitsConfig)
    market_data: MarketDataConfig = field(default_factory=MarketDataConfig)
    reconciliation: ReconciliationConfig = field(default_factory=ReconciliationConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    control_plane: ControlPlaneConfig = field(default_factory=ControlPlaneConfig)
    # Derived loader metadata used by the operator API to select the exact
    # environment override directory. It is not a service-owned YAML section.
    environment: str = "paper"
    # Global safety: when True the bot runs in paper-only / dry-run mode.
    # Derived from ``execution.trading_mode``; never set directly.
    dry_run: bool = False


# Factory & validation.
# ---------------------------------------------------------------------------


# Keep version checks behind a local proxy so tests can monkeypatch this module
# without mutating the real process-wide ``sys.version_info`` object.
sys = SimpleNamespace(version_info=_sys.version_info)


def _runtime_python_supported() -> bool:
    """Return whether the current interpreter matches repository support."""

    return sys.version_info.major == 3 and 11 <= sys.version_info.minor < 13


def _is_loopback_bind_host(host: str) -> bool:
    """Return whether a configured control-plane bind is strictly local."""

    normalized = str(host or "").strip().casefold()
    if normalized == "localhost":
        return True
    if normalized.startswith("[") and normalized.endswith("]"):
        normalized = normalized[1:-1]
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _validate_semantics(cfg: AppConfig, errors: list[str]) -> None:
    """Cross-field validation applied after parsing.

    Appends human-readable messages to *errors* so they can be folded into the
    single aggregated ``EnvValidationError``.
    """

    if not _runtime_python_supported():
        errors.append(
            f"Unsupported Python runtime {sys.version_info.major}.{sys.version_info.minor}. "
            "Use Python 3.11.x or 3.12.x."
        )

    # PostgreSQL is mandatory for idempotency, reconciliation, and roster state.
    if not cfg.database.dsn:
        errors.append("database.dsn is required.")
    elif not cfg.database.dsn.startswith(("postgresql://", "postgres://")):
        errors.append("database.dsn must use postgresql:// or postgres://.")

    if cfg.execution.sell_fraction_missing_policy not in ("skip", "full_close"):
        errors.append("execution.sell_fraction_missing_policy must be 'skip' or 'full_close'.")

    # Wallet essentials
    private_key = secret_value(cfg.wallet.private_key)
    if not cfg.execution.paper_trade_only and not private_key:
        errors.append("PRIVATE_KEY is required.")
    if private_key and not _HEX_PRIVATE_KEY_RE.match(private_key):
        errors.append("PRIVATE_KEY must be a 66-char hex key (0x + 64 hex digits).")
    # Required in paper mode only. Live authenticates, so the SDK derives the
    # Deposit Wallet from the private key and this value is just an optional
    # cross-check — keeping it optional preserves the one-secret auth chain.
    # Paper deliberately never builds a secure client, so nothing resolves an
    # address and this is the bot's only wallet identity: left empty, the
    # balance and live-position probes query "" and fail readiness with no
    # actionable error.
    if cfg.execution.paper_trade_only and not cfg.wallet.funder_address:
        errors.append(
            "FUNDER_ADDRESS is required in paper mode (it is the wallet "
            "identity used by the balance/live-position health probes; live "
            "mode derives it from PRIVATE_KEY instead)."
        )
    elif cfg.wallet.funder_address and not _ETH_ADDR_RE.match(cfg.wallet.funder_address):
        errors.append(
            f'Invalid FUNDER_ADDRESS "{cfg.wallet.funder_address}" '
            f"(expected 42-char Ethereum address)."
        )
    if cfg.wallet.chain_id not in (137, 80002):
        errors.append(f'Unsupported CHAIN_ID "{cfg.wallet.chain_id}". Use 137 or 80002.')
    if not cfg.wallet.polygon_rpc_url:
        errors.append("RPC_URL is required.")
    if not cfg.polymarket.rpc_url:
        errors.append("polymarket.rpc_url is required.")
    for wallet in cfg.copy_trading.wallets:
        if not _ETH_ADDR_RE.match(wallet):
            errors.append(f'Invalid leader wallet "{wallet}" (expected 42-char Ethereum address).')

    # Sizing invariants
    s = cfg.sizing
    if not (0 < s.max_position_pct <= 1):
        errors.append("MAX_POSITION_PCT must be in (0, 1].")
    if s.trade_multiplier < 0:
        errors.append("TRADE_MULTIPLIER must be non-negative.")
    if s.adaptive_basis not in ("trade_size", "performance"):
        errors.append("ADAPTIVE_SIZING_BASIS must be one of: trade_size, performance.")
    if s.adaptive_min_percent < 0:
        errors.append("ADAPTIVE_MIN_PERCENT must be non-negative.")
    if s.adaptive_max_percent < 0:
        errors.append("ADAPTIVE_MAX_PERCENT must be non-negative.")
    if s.adaptive_threshold_usdc <= 0:
        errors.append("ADAPTIVE_THRESHOLD_USDC must be > 0.")
    if (
        s.adaptive_min_percent > 0
        and s.adaptive_max_percent > 0
        and s.adaptive_min_percent > s.adaptive_max_percent
    ):
        errors.append("ADAPTIVE_MIN_PERCENT must be <= ADAPTIVE_MAX_PERCENT.")
    if s.trust_size_multiplier_min <= 0:
        errors.append("TRUST_SIZE_MULTIPLIER_MIN must be > 0.")
    if s.trust_size_multiplier_max <= 0:
        errors.append("TRUST_SIZE_MULTIPLIER_MAX must be > 0.")
    if s.trust_size_multiplier_min > s.trust_size_multiplier_max:
        errors.append("TRUST_SIZE_MULTIPLIER_MIN must be <= TRUST_SIZE_MULTIPLIER_MAX.")

    # Execution
    e = cfg.execution
    if e.slippage_tolerance < 0:
        errors.append("SLIPPAGE_TOLERANCE must be non-negative.")
    if e.min_share_price < 0:
        errors.append("MIN_SHARE_PRICE must be non-negative.")
    if e.min_share_price > 1:
        errors.append("MIN_SHARE_PRICE must be <= 1.0 for binary markets.")
    if e.max_share_price < 0:
        errors.append("MAX_SHARE_PRICE must be non-negative.")
    if e.max_share_price > 1:
        errors.append("MAX_SHARE_PRICE must be <= 1.0 for binary markets.")
    if e.max_share_price > 0 and e.max_share_price < e.min_share_price:
        errors.append("MAX_SHARE_PRICE must be >= MIN_SHARE_PRICE when enabled.")
    if e.pending_submission_ttl_seconds <= 0:
        errors.append("PENDING_SUBMISSION_TTL_SECONDS must be > 0.")
    if e.reservation_ttl_seconds <= 0:
        errors.append("RESERVATION_TTL_SECONDS must be > 0.")
    # Phase 4 — balance regime + book depth gate. ``0`` disables a knob.
    if e.max_position_size_usdc < 0:
        errors.append("MAX_POSITION_SIZE_USDC must be non-negative.")
    if e.max_daily_spend_usdc < 0:
        errors.append("MAX_DAILY_SPEND_USDC must be non-negative.")
    if e.min_balance_headroom_usdc < 0:
        errors.append("MIN_BALANCE_HEADROOM_USDC must be non-negative.")
    if e.min_book_depth_usdc < 0:
        errors.append("MIN_BOOK_DEPTH_USDC must be non-negative.")
    if not (0 < e.book_depth_band_pct <= 100):
        errors.append("BOOK_DEPTH_BAND_PCT must be in (0, 100].")

    # Protective exits (exit-lifecycle thresholds)
    pe = cfg.protective_exits
    if not (0 <= pe.near_max_price_warn <= 1):
        errors.append("NEAR_MAX_PRICE_WARN must be in [0, 1].")
    if not (0 <= pe.near_max_price_exit <= 1):
        errors.append("NEAR_MAX_PRICE_EXIT must be in [0, 1].")
    if not (0 <= pe.near_max_price_warn_trim_ratio <= 1):
        errors.append("NEAR_MAX_PRICE_WARN_TRIM_RATIO must be in [0, 1].")
    if pe.near_max_price_warn > pe.near_max_price_exit:
        errors.append("NEAR_MAX_PRICE_WARN must be <= NEAR_MAX_PRICE_EXIT.")
    if pe.profit_lock_trigger_pct <= 0:
        errors.append("PROFIT_LOCK_TRIGGER_PCT must be > 0.")

    # Risk
    r = cfg.risk
    if r.max_market_exposure_usdc <= 0:
        errors.append("MAX_MARKET_EXPOSURE_USDC must be > 0.")
    if r.max_category_exposure_usdc <= 0:
        errors.append("MAX_CATEGORY_EXPOSURE_USDC must be > 0.")
    if r.max_leader_exposure_usdc <= 0:
        errors.append("MAX_LEADER_EXPOSURE_USDC must be > 0.")
    if r.max_open_positions <= 0:
        errors.append("MAX_OPEN_POSITIONS must be > 0.")
    if r.max_daily_drawdown_pct <= 0:
        errors.append("MAX_DAILY_DRAWDOWN_PCT must be > 0.")
    if r.risk_pre_reconcile_timeout_seconds <= 0:
        errors.append("risk.risk_pre_reconcile_timeout_seconds must be > 0.")
    if r.risk_pre_reconcile_backoff_seconds < r.risk_pre_reconcile_timeout_seconds:
        errors.append("risk.risk_pre_reconcile_backoff_seconds must be >= timeout.")

    # Copy trading
    le = cfg.copy_trading
    if not (0 <= le.copy_decision_min_score <= 1):
        errors.append("COPY_DECISION_MIN_SCORE must be in [0, 1].")
    if le.copy_max_price_drift_pct < 0:
        errors.append("COPY_MAX_PRICE_DRIFT_PCT must be non-negative.")
    if le.copy_max_edge_decay_pct < 0:
        errors.append("COPY_MAX_EDGE_DECAY_PCT must be non-negative.")
    if le.copy_max_signal_age_seconds < 0:
        errors.append("COPY_MAX_SIGNAL_AGE_SECONDS must be non-negative.")
    if le.copy_liquidity_target_usdc <= 0:
        errors.append("COPY_LIQUIDITY_TARGET_USDC must be > 0.")

    # On-chain detection
    oc = cfg.detection.onchain
    confidence_required = str(oc.pending_tx_detection_confidence_required or "").strip().lower()
    if confidence_required not in _ONCHAIN_DETECTION_CONFIDENCE_VALUES:
        allowed = ", ".join(sorted(_ONCHAIN_DETECTION_CONFIDENCE_VALUES))
        errors.append(f"PENDING_TX_DETECTION_CONFIDENCE_REQUIRED must be one of: {allowed}.")
    _validate_detection_mode(cfg, errors)
    # Discovery is field-constrained by its Pydantic models. Keep only
    # invariants that span nested sections here.
    d = cfg.discovery
    if not d.shortlist.sort_modes:
        errors.append("discovery.shortlist.sort_modes must not be empty.")
    if d.scheduler.rescore_seconds > d.longitudinal.history_days * 86400:
        errors.append(
            "discovery.scheduler.rescore_seconds exceeds the longitudinal history window."
        )

    # Market data
    md = cfg.market_data
    if md.primary_source not in ("rest_snapshot", "wss"):
        errors.append('MARKET_DATA_PRIMARY_SOURCE must be one of: "rest_snapshot", "wss".')
    if md.rest_snapshot_max_age_ms <= 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_MAX_AGE_MS must be > 0.")
    if md.rest_snapshot_request_timeout_ms <= 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_REQUEST_TIMEOUT_MS must be > 0.")
    if md.rest_snapshot_total_budget_ms <= 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_TOTAL_BUDGET_MS must be > 0.")
    if md.rest_snapshot_max_spread_bps < 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_MAX_SPREAD_BPS must be non-negative.")
    if md.rest_bootstrap_max_age_ms <= 0:
        errors.append("MARKET_DATA_REST_BOOTSTRAP_MAX_AGE_MS must be > 0.")
    if md.rest_bootstrap_request_timeout_ms <= 0:
        errors.append("MARKET_DATA_REST_BOOTSTRAP_REQUEST_TIMEOUT_MS must be > 0.")
    if md.rest_bootstrap_total_budget_ms <= 0:
        errors.append("MARKET_DATA_REST_BOOTSTRAP_TOTAL_BUDGET_MS must be > 0.")
    if md.rest_bootstrap_max_spread_bps < 0:
        errors.append("MARKET_DATA_REST_BOOTSTRAP_MAX_SPREAD_BPS must be non-negative.")
    if md.executable_quote_max_age_seconds <= 0:
        errors.append("MARKET_DATA_EXECUTABLE_QUOTE_MAX_AGE_SECONDS must be > 0.")
    if md.executable_book_max_age_seconds <= 0:
        errors.append("MARKET_DATA_EXECUTABLE_BOOK_MAX_AGE_SECONDS must be > 0.")
    if md.executable_tick_max_age_seconds <= 0:
        errors.append("MARKET_DATA_EXECUTABLE_TICK_MAX_AGE_SECONDS must be > 0.")
    if md.wss_connection_max_silence_seconds <= 0:
        errors.append("MARKET_DATA_WSS_CONNECTION_MAX_SILENCE_SECONDS must be > 0.")

    control = cfg.control_plane
    control_token = secret_value(control.auth.token)
    if control.enabled and control.require_authentication and not control_token:
        errors.append("control_plane auth token is required when authenticated control is enabled.")
    if len(control_token) > 1024:
        errors.append("control_plane auth token must contain at most 1024 characters.")
    if (
        control.enabled
        and not control.require_authentication
        and not _is_loopback_bind_host(control.host)
    ):
        errors.append("control_plane non-loopback binds require authentication.")
    if control.portfolio_refresh_interval_seconds <= 0:
        errors.append("control_plane portfolio refresh interval must be positive.")
    if control.portfolio_stale_after_seconds <= 0:
        errors.append("control_plane portfolio stale interval must be positive.")


def _validate_detection_mode(cfg: AppConfig, errors: list[str]) -> None:
    """Fail closed when ``detection.mode`` selects an unusable on-chain source.

    Both on-chain runtimes issue ``alchemy_pendingTransactions`` over the
    configured WebSocket, so a non-polling mode requires an Alchemy ``wss://``
    endpoint. The provisional pending path additionally needs full pending
    transaction bodies (calldata) and a positive staleness budget.
    """

    detection = cfg.detection
    oc = detection.onchain
    mode = str(detection.mode or "").strip().lower()
    if mode not in _DETECTION_MODE_VALUES:
        allowed = ", ".join(_DETECTION_MODE_VALUES)
        errors.append(f"detection.mode must be one of: {allowed}.")
        return
    if mode == DetectionMode.POLLING.value:
        return
    ws_url = str(oc.onchain_ws_url or oc.pending_tx_ws_url or "").strip()
    if not ws_url or not ws_url.lower().startswith("wss://"):
        errors.append(
            f"detection.mode={mode} requires a wss:// endpoint in "
            "detection.onchain.onchain_ws_url or detection.onchain.pending_tx_ws_url."
        )
    provider = str(oc.pending_tx_provider or "").strip().lower()
    if provider != "alchemy":
        errors.append(
            f"detection.mode={mode} requires detection.onchain.pending_tx_provider=alchemy."
        )
    if mode == DetectionMode.ON_CHAIN_PENDING.value:
        if int(oc.pending_tx_max_signal_age_ms) <= 0:
            errors.append(
                "detection.onchain.pending_tx_max_signal_age_ms must be > 0 for "
                "detection.mode=on_chain_pending."
            )
        if bool(oc.pending_tx_hashes_only):
            errors.append(
                "detection.onchain.pending_tx_hashes_only must be false for "
                "detection.mode=on_chain_pending (calldata is required)."
            )


class DetectionModeResolver:
    """Resolve the raw ``detection`` payload onto exactly one detection source.

    The resolver runs before typed validation. An explicit ``mode`` always
    wins. Without one, the deprecated ``onchain.enable_onchain_*`` booleans
    derive ``on_chain_confirmed`` (the mempool-observed, receipt-confirmed
    runtime — what enabling either boolean used to wire); provisional copy on
    mempool sightings must be an explicit ``on_chain_pending`` and is never
    derived. Otherwise the source stays ``polling``.
    """

    _LEGACY_FLAGS = ("enable_onchain_pending_detection", "enable_onchain_mined_detection")

    def resolve(self, payload: Mapping[str, Any] | None) -> tuple[str, str | None]:
        """Return ``(mode, warning)`` for one raw ``detection`` section.

        Args:
            payload: Raw ``detection`` mapping as merged from every config
                layer, or ``None`` when the section is absent.

        Returns:
            The resolved mode string (not yet validated against
            ``DetectionMode``) and an operator warning message, or ``None``
            when no deprecated flag influenced or contradicted the result.
        """

        section: Mapping[str, Any] = payload if isinstance(payload, Mapping) else {}
        explicit = str(section.get("mode") or "").strip().lower()
        legacy_enabled = self._legacy_flags_enabled(section)
        if explicit:
            if legacy_enabled:
                return explicit, (
                    "detection_legacy_flags_ignored | mode=%s ignored=%s — "
                    "detection.onchain.enable_onchain_* booleans are deprecated; "
                    "detection.mode selects the source" % (explicit, ",".join(legacy_enabled))
                )
            return explicit, None
        if legacy_enabled:
            derived = DetectionMode.ON_CHAIN_CONFIRMED.value
            return derived, (
                "detection_legacy_flags_deprecated | derived_mode=%s from=%s "
                "replace_with=detection.mode" % (derived, ",".join(legacy_enabled))
            )
        return DetectionMode.POLLING.value, None

    @classmethod
    def _legacy_flags_enabled(cls, section: Mapping[str, Any]) -> tuple[str, ...]:
        onchain = section.get("onchain")
        if not isinstance(onchain, Mapping):
            return ()
        return tuple(flag for flag in cls._LEGACY_FLAGS if cls._truthy(onchain.get(flag)))

    @staticmethod
    def _truthy(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(value, (int, float)):
            return bool(value)
        return False


class AppConfigSemanticValidator:
    """Validate cross-section application invariants without performing I/O."""

    def errors_for(self, config: AppConfig) -> tuple[str, ...]:
        """Return deterministic safe validation messages for ``config``.

        Args:
            config: Fully typed candidate application configuration.

        Returns:
            A tuple of safe messages. Secret values are never interpolated.
        """

        errors: list[str] = []
        _validate_semantics(config, errors)
        return tuple(errors)

    def validate(self, config: AppConfig) -> None:
        """Raise when a typed candidate violates application invariants.

        Args:
            config: Fully typed candidate application configuration.

        Raises:
            EnvValidationError: If any cross-section invariant fails.
        """

        errors = self.errors_for(config)
        if errors:
            raise EnvValidationError("Invalid application configuration:\n- " + "\n- ".join(errors))


# Service-owned layered configuration loader.
#
# ---------------------------------------------------------------------------

_CONFIG_ROOT = Path(__file__).resolve().parent
# Deployment-local layers are resolved relative to a deployment root, not the
# process CWD (Phase 3.11). The root is, in priority order: the
# ``PMCOPY_DEPLOYMENT_ROOT`` env var, or the nearest ancestor directory (walked
# up from CWD) that contains a service-config marker (``config/app.yml``, the
# legacy ``config/runtime.yml``) or a ``secrets.env`` file. Falls back to CWD.
_RUNTIME_LOCAL_YAML = "config/runtime.local.yml"
_SECRETS_ENV = "secrets.env"
_DEFAULT_ENVIRONMENT = "paper"
_ENVIRONMENT_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")

#: One YAML file per owning service, all optional, all strict.
SERVICE_CONFIG_FILES: tuple[str, ...] = (
    "app",
    "database",
    "wallet",
    "polymarket",
    "execution",
    "detection",
    "discovery",
    "copy_trading",
    "sizing",
    "risk",
    "protective_exits",
    "market_data",
    "reconciliation",
    "logging",
    "control_plane",
)


def _resolve_deployment_root() -> Path:
    """Find the deployment root so local secrets/shim are not CWD-relative."""
    explicit = os.getenv("PMCOPY_DEPLOYMENT_ROOT")
    if explicit:
        root = Path(explicit).expanduser().resolve()
        if root.is_dir():
            return root
    cwd = Path.cwd()
    for candidate in [cwd, *cwd.parents]:
        if (
            (candidate / "config" / "app.yml").exists()
            or (candidate / "config" / "runtime.yml").exists()
            or (candidate / "secrets.env").exists()
        ):
            return candidate
    return cwd


class _LayeredModel(BaseModel):
    """Strict validation shell around the immutable runtime sections.

    ``allow_inf_nan=False`` rejects NaN/+Inf/-Inf for every float/Decimal
    reachable from this shell, including the frozen dataclass sections, which
    carry no config of their own. YAML (``.nan``/``.inf``) and env strings
    ("nan"/"inf") both coerce to non-finite floats before this gate. A
    non-finite threshold silently disables the comparison that depends on it,
    so this must fail closed at load time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    app: AppServiceConfig = AppServiceConfig()
    database: DatabaseConfig = DatabaseConfig()
    wallet: WalletConfig = WalletConfig(private_key=SecretStr(""), chain_id=137)
    polymarket: PolymarketConfig = PolymarketConfig()
    execution: ExecutionConfig = ExecutionConfig()
    detection: DetectionConfig = DetectionConfig()
    discovery: DiscoveryConfig = DiscoveryConfig.model_validate({})
    copy_trading: CopyTradingConfig = CopyTradingConfig.model_validate({})
    sizing: SizingConfig = SizingConfig()
    risk: RiskConfig = RiskConfig()
    protective_exits: ProtectiveExitsConfig = ProtectiveExitsConfig()
    market_data: MarketDataConfig = MarketDataConfig()
    reconciliation: ReconciliationConfig = ReconciliationConfig()
    logging: LoggingConfig = LoggingConfig()
    control_plane: ControlPlaneConfig = ControlPlaneConfig()

    @model_validator(mode="before")
    @classmethod
    def _resolve_detection_mode(cls, values: Any) -> Any:
        """Thin glue: pin ``detection.mode`` via ``DetectionModeResolver``."""

        if not isinstance(values, dict):
            return values
        detection = values.get("detection")
        if detection is not None and not isinstance(detection, dict):
            return values
        mode, warning = DetectionModeResolver().resolve(detection)
        if warning:
            logger.warning(warning)
        return {**values, "detection": {**(detection or {}), "mode": mode}}

    @model_validator(mode="before")
    @classmethod
    def _decode_nested_structures(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        structured_paths = (
            ("copy_trading", "wallets"),
            ("reconciliation", "order_confirmation", "token_delay_overrides"),
            ("reconciliation", "order_confirmation", "condition_delay_overrides"),
            ("discovery", "shortlist", "sort_modes"),
            ("discovery", "wallet360", "windows"),
            ("discovery", "social", "keywords"),
            ("discovery", "scoring", "h_score_range"),
            ("discovery", "scoring", "roi_15d_range"),
            ("discovery", "scoring", "win_rate_15d_range"),
            ("discovery", "scoring", "sharpe_15d_range"),
            ("discovery", "longitudinal", "sharpe_range"),
            ("discovery", "longitudinal", "roi_slope_range"),
            ("discovery", "longitudinal", "rank_slope_range"),
            ("discovery", "longitudinal", "leaderboard_windows"),
        )
        for path in structured_paths:
            cursor: Any = values
            for part in path[:-1]:
                if not isinstance(cursor, dict) or part not in cursor:
                    cursor = None
                    break
                cursor = cursor[part]
            if not isinstance(cursor, dict):
                continue
            leaf = path[-1]
            raw = cursor.get(leaf)
            if not isinstance(raw, str):
                continue
            try:
                cursor[leaf] = orjson.loads(raw)
            except orjson.JSONDecodeError as exc:
                raise ValueError(
                    f"{'.'.join(path)} must be valid JSON when set by environment"
                ) from exc
        return values


# Legacy pre-split key translation, applied at the loader boundary only (the
# runtime.local.yml shim, secrets.env, process env, and programmatic
# overrides). ``None`` routes mean the key was retired and is dropped with a
# warning. Canonical service files never pass through this table and stay
# strict.
_LEGACY_SECTION_ROUTES: dict[str, tuple[str, ...]] = {
    "network": ("polymarket",),
    "onchain_detection": ("detection", "onchain"),
    "leader": ("copy_trading",),
    "polling": ("detection",),
    "features": ("protective_exits",),
    "order_confirmation": ("reconciliation", "order_confirmation"),
    "position_drift": ("reconciliation", "position_drift"),
    "global": ("app",),
    "files": ("app",),
}
_LEGACY_KEY_ROUTES: dict[tuple[str, str], tuple[str, ...] | None] = {
    ("network", "enable_metrics_server"): ("app", "enable_metrics_server"),
    ("network", "metrics_port"): ("app", "metrics_port"),
    ("network", "metrics_bind_host"): ("app", "metrics_bind_host"),
    ("network", "ctf_exchange_v2_address"): None,
    ("network", "neg_risk_ctf_exchange_v2_address"): None,
    ("polling", "risk_check_interval"): ("risk", "risk_check_interval"),
    ("polling", "reconcile_interval_seconds"): (
        "reconciliation",
        "reconcile_interval_seconds",
    ),
    ("polling", "retention_max_age_seconds"): ("app", "retention_max_age_seconds"),
    ("polling", "discovery_refresh_interval_seconds"): None,
    ("features", "enable_tier_gate_watchlist"): (
        "copy_trading",
        "enable_tier_gate_watchlist",
    ),
    ("features", "enable_tier_gate_watchlist_log"): (
        "copy_trading",
        "enable_tier_gate_watchlist_log",
    ),
    ("risk", "near_max_price_warn"): ("protective_exits", "near_max_price_warn"),
    ("risk", "near_max_price_warn_trim_ratio"): (
        "protective_exits",
        "near_max_price_warn_trim_ratio",
    ),
    ("risk", "near_max_price_exit"): ("protective_exits", "near_max_price_exit"),
    ("risk", "near_max_grace_period_seconds"): (
        "protective_exits",
        "near_max_grace_period_seconds",
    ),
    ("risk", "profit_lock_trigger_pct"): (
        "protective_exits",
        "profit_lock_trigger_pct",
    ),
    ("risk", "profit_lock_sell_fraction"): (
        "protective_exits",
        "profit_lock_sell_fraction",
    ),
    ("risk", "trailing_activation_pct"): (
        "protective_exits",
        "trailing_activation_pct",
    ),
    ("risk", "trailing_distance"): ("protective_exits", "trailing_distance"),
    ("risk", "staged_tp_levels"): ("protective_exits", "staged_tp_levels"),
    ("risk", "staged_tp_fractions"): ("protective_exits", "staged_tp_fractions"),
    ("risk", "stop_loss_threshold"): ("protective_exits", "stop_loss_threshold"),
    ("risk", "stop_loss_grace_period_seconds"): (
        "protective_exits",
        "stop_loss_grace_period_seconds",
    ),
    ("risk", "trailing_activation_mode"): None,
    ("leader", "tiny_leader_high_confidence_score"): None,
    ("leader", "min_leader_trade_notional_usdc"): None,
    ("logging", "pnl_log_file"): None,
    ("execution", "max_retries"): None,
    ("execution", "retry_backoff_base"): None,
    ("execution", "transaction_fee_bps"): None,
    ("execution", "gas_cost_usdc"): None,
    ("execution", "risk_buffer_pct"): None,
    ("execution", "execution_require_fresh_book"): None,
    ("execution", "circuit_cooldown_seconds"): None,
    ("execution", "meta_cache_ttl_good"): None,
    ("execution", "meta_cache_ttl_thin"): None,
    ("market_data", "rest_snapshot_enable"): None,
    ("market_data", "allow_rest_when_wss_down"): None,
    ("market_data", "wss_down_after_seconds"): None,
    ("market_data", "rest_degraded_max_age_seconds"): None,
    ("market_data", "rest_degraded_max_spread_bps"): None,
    ("market_data", "rest_degraded_log_every_seconds"): None,
}
_OBSOLETE_CLOB_CREDENTIAL_KEYS = ("api_key", "api_secret", "api_passphrase")

# Venue backend selection is gone: CCXT is the only transport. These keys must
# be rejected (not silently dropped) so an operator's stale config cannot
# imply a hybrid/SDK/shadow mode that no longer exists.
_OBSOLETE_VENUE_BACKEND_KEYS = (
    "venue_backend",
    "allow_experimental_ccxt_live",
    "shadow_ccxt",
)


class _RouteSentinel(Enum):
    """Identify a legacy key that retains its section-relative route."""

    KEEP = auto()


_ROUTE_KEEP = _RouteSentinel.KEEP


def _assign_path(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    """Deep-set ``value`` at ``path`` inside ``target``, merging mappings."""
    cursor = target
    for part in path[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            child = {}
            cursor[part] = child
        cursor = child
    leaf = path[-1]
    current = cursor.get(leaf)
    if isinstance(current, dict) and isinstance(value, Mapping):
        cursor[leaf] = _deep_merge(current, value)
    else:
        cursor[leaf] = value


def _translate_legacy_payload(payload: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    """Map legacy pre-split section/key names onto the service-owned layout.

    Only key NAMES are ever logged; values never are. Retired keys are
    dropped with a warning instead of failing so an operator's un-migrated
    ``runtime.local.yml``/``secrets.env`` cannot brick startup.
    """
    output: dict[str, Any] = {}
    for raw_section, content in payload.items():
        section = str(raw_section)
        if section == "clob":
            if isinstance(content, Mapping):
                for key in content:
                    if str(key) in _OBSOLETE_CLOB_CREDENTIAL_KEYS:
                        raise EnvValidationError(
                            f"clob.{key} is obsolete. CLOB credentials are "
                            "derived at runtime from the wallet key and must "
                            "not be configured."
                        )
            logger.warning("config_legacy_key_dropped | source=%s key=clob", source)
            continue
        if not isinstance(content, Mapping):
            _assign_path(output, (section,), content)
            continue
        for raw_key, value in content.items():
            key = str(raw_key)
            if section == "execution" and key in _OBSOLETE_VENUE_BACKEND_KEYS:
                raise EnvValidationError(
                    f"execution.{key} is obsolete. CCXT is the only Polymarket "
                    "venue transport; venue backend selection, the experimental "
                    "CCXT live gate, and shadow mode have been removed. Delete "
                    "the key and rely on trading_mode for paper/live selection."
                )
            route = _LEGACY_KEY_ROUTES.get((section, key), _ROUTE_KEEP)
            if route is None:
                logger.warning(
                    "config_legacy_key_dropped | source=%s key=%s.%s",
                    source,
                    section,
                    key,
                )
                continue
            if route is _ROUTE_KEEP:
                prefix = _LEGACY_SECTION_ROUTES.get(section, (section,))
                route = (*prefix, key)
            _assign_path(output, route, value)
    return output


def _secrets_environment_selector(contents: str | None) -> str | None:
    """Extract ``PMCOPY_ENVIRONMENT`` from the secrets file, if present.

    ``secrets.env`` is the documented carrier for the environment selector in
    compose deployments; explicit arguments and the process environment still
    take precedence in :func:`_resolve_environment`.
    """
    if contents is None:
        return None
    value = dotenv_values(stream=StringIO(contents)).get("PMCOPY_ENVIRONMENT")
    if value is None or not str(value).strip():
        return None
    return str(value)


def _resolve_environment(explicit: str | None, secrets_value: str | None = None) -> str:
    """Return the explicitly selected environment name (default ``paper``).

    Precedence: explicit argument -> process ``PMCOPY_ENVIRONMENT`` ->
    ``PMCOPY_ENVIRONMENT`` in ``secrets.env`` -> default. The environment is
    never inferred from credential presence.
    """
    name = explicit or os.getenv("PMCOPY_ENVIRONMENT") or secrets_value or _DEFAULT_ENVIRONMENT
    name = str(name).strip().lower()
    if not _ENVIRONMENT_NAME_RE.match(name):
        raise EnvValidationError(f"Invalid configuration environment name: {name!r}")
    return name


def _read_service_files(directory: Path) -> dict[str, Any]:
    """Read every present ``<service>.yml`` under ``directory``."""
    payload: dict[str, Any] = {}
    for service in SERVICE_CONFIG_FILES:
        data = _read_yaml(directory / f"{service}.yml", required=False)
        if data:
            payload[service] = data
    return payload


def _read_leader_overrides(directory: Path) -> list[dict[str, Any]]:
    """Read per-leader override files ``config/leaders/<wallet>.yml``."""
    if not directory.is_dir():
        return []
    entries: list[dict[str, Any]] = []
    for file in sorted(directory.glob("*.yml")):
        wallet = file.stem
        if not _ETH_ADDR_RE.match(wallet):
            raise EnvValidationError(
                f"Leader override filename must be a 0x wallet address: {file}"
            )
        data = _read_yaml(file, required=False)
        declared = str(data.get("wallet") or "").strip()
        if declared and declared.lower() != wallet.lower():
            raise EnvValidationError(
                f"Leader override wallet field does not match filename: {file}"
            )
        data["wallet"] = wallet
        entries.append(data)
    return entries


def _read_yaml(path: Path, *, required: bool) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise EnvValidationError(f"Required configuration file is missing: {path}")
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise EnvValidationError(f"Unable to read YAML configuration {path}: {exc}") from exc
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise EnvValidationError(f"Configuration root must be a mapping: {path}")
    return {str(key): value for key, value in payload.items()}


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        # Phase 3.11 — "null means unset": a YAML null in a local override
        # must NOT erase the packaged value. Previously a null
        # ``leader.wallets`` overrode the packaged ``[]`` with ``None`` and
        # bricked startup (Pydantic tuple validation). Skip nulls so the
        # packaged/default value survives.
        if value is None:
            continue
        current = merged.get(str(key))
        if isinstance(current, dict) and isinstance(value, Mapping):
            merged[str(key)] = _deep_merge(current, value)
        else:
            merged[str(key)] = value
    return merged


def _read_secrets_contents(path: Path) -> str | None:
    """Read one validated secrets snapshot without following symlinks."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise EnvValidationError(
                f"Secrets path must be a regular non-symlink file: {path}"
            ) from exc
        raise EnvValidationError(f"Unable to securely open secrets file: {path}") from exc

    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise EnvValidationError(f"Secrets path must be a regular non-symlink file: {path}")
        if os.name != "nt" and stat.S_IMODE(info.st_mode) != 0o600:
            raise EnvValidationError(
                f"Secrets file must have permissions 0600 exactly (chmod 600): {path}"
            )
        stream = os.fdopen(fd, "r", encoding="utf-8")
        fd = -1
        with stream:
            return stream.read()
    except (OSError, UnicodeError) as exc:
        raise EnvValidationError(f"Unable to read secrets file: {path}") from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _read_secret_overrides(contents: str) -> dict[str, Any]:
    """Parse nested application secrets while allowing Compose's PG key."""
    seen: set[str] = set()
    lines = contents.splitlines()
    for line_number, raw_line in enumerate(lines, 1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key.startswith("export "):
            key = key[7:].strip()
        if key in seen:
            raise EnvValidationError(f"Duplicate key in secrets file at line {line_number}: {key}")
        seen.add(key)

    # Parse the same immutable snapshot used for duplicate detection. Reading
    # the path twice left a TOCTOU window where validation and applied values
    # could come from different file contents.
    values = dotenv_values(stream=StringIO(contents))
    output: dict[str, Any] = {}
    for raw_key, value in values.items():
        key = str(raw_key)
        if key in ("POSTGRES_PASSWORD", "PMCOPY_ENVIRONMENT"):
            # Compose consumes POSTGRES_PASSWORD directly; PMCOPY_ENVIRONMENT
            # is the documented environment selector, consumed before this
            # layer by _resolve_environment.
            continue
        if not key.startswith("PMCOPY__"):
            raise EnvValidationError(
                "secrets.env application keys must use PMCOPY__SECTION__FIELD; "
                f"unsupported key: {key}"
            )
        path_parts = tuple(part.strip().lower() for part in key[len("PMCOPY__") :].split("__"))
        if len(path_parts) < 2 or any(not part for part in path_parts):
            raise EnvValidationError(f"Malformed nested secrets key: {key}")
        # dotenv is merged manually, outside BaseSettings' env source, so
        # ``env_ignore_empty`` does not apply automatically. Treat blank
        # template entries as absent instead of allowing them to erase a
        # valid YAML value at higher precedence.
        if value is None or not str(value).strip():
            continue
        cursor = output
        for part in path_parts[:-1]:
            child = cursor.get(part)
            if child is None:
                child = {}
                cursor[part] = child
            if not isinstance(child, dict):
                raise EnvValidationError(f"Conflicting nested secrets key: {key}")
            cursor = child
        cursor[path_parts[-1]] = value
    return output


def _validation_messages(exc: ValidationError) -> list[str]:
    """Render validation failures without echoing secret input values."""
    messages: list[str] = []
    for error in exc.errors(include_url=False, include_input=False):
        location = ".".join(str(part) for part in error.get("loc", ())) or "config"
        messages.append(f"{location}: {error.get('msg', 'invalid value')}")
    return messages


def _read_process_env_overrides() -> dict[str, Any]:
    """Collect nested ``PMCOPY__SECTION__FIELD`` process-environment values.

    Blank values are ignored (parity with the historical
    ``env_ignore_empty`` behavior) so an exported empty variable never
    erases a lower-layer value.
    """
    output: dict[str, Any] = {}
    for key in sorted(os.environ):
        if not key.startswith("PMCOPY__"):
            continue
        value = os.environ[key]
        if value is None or not str(value).strip():
            continue
        path_parts = tuple(part.strip().lower() for part in key[len("PMCOPY__") :].split("__"))
        if len(path_parts) < 2 or any(not part for part in path_parts):
            raise EnvValidationError(f"Malformed nested environment key: {key}")
        cursor = output
        for part in path_parts[:-1]:
            child = cursor.get(part)
            if child is None:
                child = {}
                cursor[part] = child
            if not isinstance(child, dict):
                raise EnvValidationError(f"Conflicting nested environment key: {key}")
            cursor = child
        cursor[path_parts[-1]] = value
    return output


def load_config(
    overrides: Mapping[str, Any] | None = None,
    *,
    config_dir: str | os.PathLike[str] | None = None,
    environment: str | None = None,
    secrets_path: str | os.PathLike[str] | None = None,
) -> AppConfig:
    """Load the immutable config from service-owned YAML, secrets, and env.

    Layer precedence, lowest to highest: typed defaults ->
    ``<config_dir>/<service>.yml`` -> legacy ``runtime.local.yml`` shim
    (translated, deprecated) -> ``<config_dir>/environments/<environment>/
    <service>.yml`` -> ``<config_dir>/leaders/<wallet>.yml`` -> ``overrides``
    -> ``secrets.env`` -> process ``PMCOPY__*`` variables.

    Args:
        overrides: Nested mapping for tests/composition roots. Flat legacy
            UPPERCASE variables are rejected; legacy nested section names are
            translated at this boundary.
        config_dir: Directory holding the canonical service files. Defaults
            to the packaged ``config/`` directory.
        environment: Explicit environment name; beats ``PMCOPY_ENVIRONMENT``.
            Defaults to ``paper``. Never inferred from credential presence.
        secrets_path: Explicit ``secrets.env`` location. Defaults to
            ``<deployment_root>/secrets.env``.

    Raises:
        EnvValidationError: On unknown keys, malformed values, an unknown
            non-default environment, or failed semantic validation.
    """
    config_root = Path(config_dir) if config_dir else _CONFIG_ROOT
    deployment_root = _resolve_deployment_root()
    secrets_file = Path(secrets_path) if secrets_path else deployment_root / _SECRETS_ENV
    secrets_contents = _read_secrets_contents(secrets_file)
    environment_name = _resolve_environment(
        environment, _secrets_environment_selector(secrets_contents)
    )

    payload = _read_service_files(config_root)

    # Single legacy deprecation shim: an operator's untracked
    # config/runtime.local.yml keeps working (translated) so an un-migrated
    # live deployment does not silently revert to paper/no-leaders. Tracked
    # environment files take precedence over it. Planned for removal.
    shim_file = deployment_root / _RUNTIME_LOCAL_YAML
    if shim_file.exists():
        logger.warning(
            "config_legacy_local_shim | path=%s — config/runtime.local.yml is "
            "deprecated; migrate these overrides to "
            "config/environments/<environment>/<service>.yml",
            shim_file,
        )
        payload = _deep_merge(
            payload,
            _translate_legacy_payload(
                _read_yaml(shim_file, required=False), source="runtime.local.yml"
            ),
        )

    environments_dir = config_root / "environments" / environment_name
    if environments_dir.is_dir():
        payload = _deep_merge(payload, _read_service_files(environments_dir))
        logger.info(
            "config_environment_loaded | environment=%s dir=%s",
            environment_name,
            environments_dir,
        )
    elif environment_name != _DEFAULT_ENVIRONMENT:
        raise EnvValidationError(
            f"Unknown configuration environment {environment_name!r}: "
            f"{environments_dir} does not exist."
        )

    leader_entries = _read_leader_overrides(config_root / "leaders")
    if leader_entries:
        payload = _deep_merge(payload, {"copy_trading": {"leaders": leader_entries}})

    if overrides:
        flat_keys = [key for key in overrides if str(key).upper() == str(key)]
        if flat_keys:
            raise EnvValidationError(
                "Flat environment overrides are no longer supported; use nested "
                "settings or PMCOPY__SECTION__FIELD names."
            )
        payload = _deep_merge(payload, _translate_legacy_payload(overrides, source="overrides"))

    if secrets_contents is not None:
        payload = _deep_merge(
            payload,
            _translate_legacy_payload(
                _read_secret_overrides(secrets_contents), source="secrets.env"
            ),
        )

    env_overrides = _read_process_env_overrides()
    if env_overrides:
        payload = _deep_merge(
            payload,
            _translate_legacy_payload(env_overrides, source="process-env"),
        )

    try:
        settings = _LayeredModel(**payload)
    except ValidationError as exc:
        messages = _validation_messages(exc)
        raise EnvValidationError(
            "Invalid application configuration:\n- " + "\n- ".join(messages)
        ) from exc

    validated = settings.model_dump()
    # One RPC setting is enough for operators.  ``WalletConfig`` keeps these
    # compatibility fields because legacy service boundaries still consume
    # them, but their values are sourced from the canonical polymarket section.
    validated["wallet"]["polygon_rpc_url"] = (
        validated["wallet"].get("polygon_rpc_url") or validated["polymarket"]["rpc_url"]
    )
    validated["wallet"]["fallback_rpc_url"] = validated["wallet"].get(
        "fallback_rpc_url"
    ) or validated["polymarket"].get("fallback_rpc_url", "")
    validated["dry_run"] = settings.execution.paper_trade_only
    validated["environment"] = environment_name
    try:
        cfg = TypeAdapter(AppConfig).validate_python(validated)
    except ValidationError as exc:
        messages = _validation_messages(exc)
        raise EnvValidationError(
            "Invalid application configuration:\n- " + "\n- ".join(messages)
        ) from exc

    semantic_errors = AppConfigSemanticValidator().errors_for(cfg)
    if semantic_errors:
        raise EnvValidationError(
            "Invalid application configuration:\n- " + "\n- ".join(semantic_errors)
        )
    return cfg


def dump_config_payload(cfg: AppConfig) -> dict[str, Any]:
    """Serialize a config to YAML-safe primitives, one key per section.

    ``Decimal`` values become strings, enums become their values, and
    ``SecretStr`` fields are masked by Pydantic (empty secrets stay empty),
    so the payload never leaks a secret and survives a
    ``yaml.safe_dump``/``safe_load`` round trip. The derived ``dry_run``
    flag is omitted (it is recomputed from ``execution.trading_mode``).
    """
    payload = TypeAdapter(AppConfig).dump_python(cfg, mode="json")
    payload.pop("dry_run", None)
    payload.pop("environment", None)
    return payload
