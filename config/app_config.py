"""Immutable application configuration loaded from layered YAML settings.

Source precedence, from lowest to highest: packaged ``config/runtime.yml`` ->
optional deployment-CWD ``config/runtime.local.yml`` -> packaged
``config/trader_discovery.yaml`` -> deployment-CWD mode-0600 ``secrets.env``
-> process variables named ``PMCOPY__SECTION__FIELD``.

The public object remains a graph of frozen dataclasses so the trading runtime
gets an immutable, typed contract. Pydantic Settings owns loading, coercion,
nested environment overrides, and error aggregation.
"""

from __future__ import annotations

import logging
import os
import re
import stat
import sys as _sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import orjson
import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, SecretStr, TypeAdapter, ValidationError, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from config.env import EnvValidationError, SignatureType, WalletConfig, secret_value

__all__ = [
    "AppConfig",
    "ClobConfig",
    "DatabaseConfig",
    "DiscoveryConfig",
    "EnvValidationError",
    "ExecutionConfig",
    "FeatureFlagConfig",
    "LeaderConfig",
    "LoggingConfig",
    "MarketDataConfig",
    "NetworkConfig",
    "OnchainDetectionConfig",
    "OrderConfirmationConfig",
    "PositionDriftConfig",
    "PollingConfig",
    "ReconciliationConfig",
    "RiskConfig",
    "SignatureType",
    "SizingConfig",
    "WalletConfig",
    "POLYGON_USDCE_ADDRESS",
    "POLYMARKET_CTF_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_PUSD_ADDRESS",
    "load_config",
]

_ETH_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HEX_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
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
    dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 20
    command_timeout_seconds: float = 30.0


@dataclass(frozen=True)
class ClobConfig:
    http_url: str
    api_key: SecretStr
    api_secret: SecretStr = field(default_factory=lambda: SecretStr(""))
    api_passphrase: SecretStr = field(default_factory=lambda: SecretStr(""))

    def __post_init__(self) -> None:
        for name in ("api_key", "api_secret", "api_passphrase"):
            value = getattr(self, name)
            if not isinstance(value, SecretStr):
                object.__setattr__(self, name, SecretStr(str(value or "")))


@dataclass(frozen=True)
class PollingConfig:
    fetch_interval: float
    aggregate_signal_window_seconds: float
    risk_check_interval: float
    discovery_refresh_interval_seconds: int
    leader_positions_min_fetch_spacing_ms: int
    leader_activity_min_fetch_spacing_ms: int
    reconcile_interval_seconds: int
    # Maximum age (seconds) for retained processed-trade rows before the
    # background purge sweeper deletes them. Previously a raw
    # ``ENV.get("RETENTION_MAX_AGE_SECONDS", 30*86400)`` read from app.py
    # and utils/idempotency.py; promoted here so the purge path is typed.
    retention_max_age_seconds: float = 30.0 * 86400.0
    trade_queue_max_batches: int = 64


@dataclass(frozen=True)
class SizingConfig:
    mode: str
    percentage: float
    fixed_amount: float
    trade_multiplier: float
    tiered_trade_multipliers: str
    adaptive_basis: str
    adaptive_min_percent: float
    adaptive_max_percent: float
    adaptive_threshold_usdc: float
    buy_min_amount: float
    buy_max_amount: float
    position_threshold: int
    max_position_pct: float
    enable_trust_size_multipliers: bool
    trust_size_multiplier_min: float
    trust_size_multiplier_max: float


@dataclass(frozen=True)
class RiskConfig:
    enable_risk_mgmt: bool
    max_market_exposure_usdc: float
    max_category_exposure_usdc: float
    max_leader_exposure_usdc: float
    max_open_positions: int
    max_daily_drawdown_pct: float
    kill_switch: bool
    enable_balance_tiered_policy: bool
    balance_policy_reserve_pct: float
    balance_policy_reserve_floor: float
    balance_policy_hard_cap_pct: float
    balance_policy_hard_cap_ceiling: float
    balance_policy_max_open_positions: int
    balance_policy_allow_partial_exits: bool
    balance_policy_allow_pyramiding: bool
    balance_policy_max_adds_per_position: int
    balance_policy_base_min_score: float
    balance_policy_min_notional_usdc: float
    balance_policy_dynamic_threshold_raise: bool
    day_start_equity_usdc: float
    near_max_price_warn: float
    near_max_price_warn_trim_ratio: float
    near_max_price_exit: float
    profit_lock_trigger_pct: float
    profit_lock_sell_fraction: float
    trailing_activation_pct: float | None
    trailing_distance: float
    staged_tp_levels: str
    staged_tp_fractions: str
    stop_loss_threshold: float
    stop_loss_grace_period_seconds: float
    # PR #105 review: bound the in-memory risk price cache via typed config
    # so the production sizing/risk path no longer relies on a raw env read.
    # ``services.risk.risk_monitor.RiskPriceCache`` still honours its env fallback
    # as a legacy adapter (issue #73 allows raw env in legacy/test paths only).
    risk_price_cache_max_size: int
    # Explicit trailing-activation mode so 'auto' is never smuggled through a
    # float. "disabled" whenever ENABLE_TRAILING_STOP is off (the activation
    # input is then never fatal-parsed); "auto" scales activation from max
    # payout (``trailing_activation_pct`` is None); "fixed" uses the numeric
    # ``trailing_activation_pct``. Defaulted so existing direct constructors
    # and fixtures keep working.
    trailing_activation_mode: Literal["disabled", "fixed", "auto"] = "disabled"
    # Cooldown (seconds) before re-dispatching a risk exit for a scope whose
    # previous protective SELL ended terminal (rejected/cancelled). Stops the
    # 1 Hz resubmission storm against phantom positions (2026-07-04 incident).
    # 0 disables. Defaulted so existing direct constructors keep working.
    risk_exit_reject_cooldown_seconds: float = 60.0
    risk_pre_reconcile_timeout_seconds: float = 25.0
    risk_pre_reconcile_backoff_seconds: float = 30.0


@dataclass(frozen=True)
class ExecutionConfig:
    slippage_tolerance: float
    min_share_price: float
    max_share_price: float
    max_retries: int
    non_terminal_failure_max_attempts: int
    trading_mode: Literal["paper", "live"]
    enable_copy_decision_gate: bool
    enable_near_resolution_gate: bool
    near_resolution_gate_price: float
    near_resolution_max_age_seconds: float
    pending_submission_ttl_seconds: int
    # TTL for pre-submit balance/share reservations. Bounds the window in
    # which a pending order blocks the next same-scope copy before it is
    # expired (and reconciled). Mirrors ``pending_submission_ttl_seconds``.
    reservation_ttl_seconds: int
    # Master switch for pre-submit BUY-quote / SELL-share reservations. When
    # False the ReservationService no-ops (returns placeholder reservations)
    # and double-spend protection falls back to the idempotency submission
    # lock + position coordinator. Defaults False on this hardening branch
    # because the reservation blocker was over-firing on legitimate same-scope
    # copies; flip to True once that is retuned. See docs/02-configuration.md.
    reservations_enabled: bool
    retry_backoff_base: float
    transaction_fee_bps: float
    gas_cost_usdc: float
    risk_buffer_pct: float
    book_spread_mode: str
    max_book_spread_bps: int
    max_book_spread_ticks: int
    execution_require_fresh_book: bool
    circuit_failure_threshold: int
    circuit_cooldown_seconds: float
    meta_cache_ttl_good: float
    meta_cache_ttl_thin: float
    # Phase 4 — balance regime
    max_position_size_usdc: float
    max_daily_spend_usdc: float
    min_balance_headroom_usdc: float
    # Phase 4 — book depth gate
    min_book_depth_usdc: float
    book_depth_band_pct: float
    # Submission concurrency: typed cap so production paths do not depend on
    # raw env reads inside ExecutionService.
    execution_submit_concurrency: int
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

    @property
    def paper_trade_only(self) -> bool:
        return self.trading_mode == "paper"


@dataclass(frozen=True)
class LeaderConfig:
    wallets: tuple[str, ...]
    copy_decision_min_score: float
    tiny_leader_high_confidence_score: float
    copy_max_price_drift_pct: float
    copy_max_edge_decay_pct: float
    copy_max_signal_age_seconds: float
    copy_liquidity_target_usdc: float
    min_leader_trade_notional_usdc: float


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    log_dir: str
    app_log_jsonl_file: str
    log_max_bytes: int
    log_backup_count: int
    pnl_log_file: str


@dataclass(frozen=True)
class NetworkConfig:
    rpc_url: str
    chain_id: int
    usdc_contract_address: str
    enforce_geoblock: bool
    enable_metrics_server: bool
    metrics_port: int
    metrics_bind_host: str
    # Optional fallback RPC when primary returns internal errors (-32603).
    fallback_rpc_url: str = ""
    # Polymarket-side collateral / exchange contract addresses. These used
    # to be read straight out of ``ENV`` by ``utils/createClobClient.py``
    # and ``utils/getMyBalance.py``; promoted to typed config so the
    # balance-check + CLOB-client paths can drop their raw env reads.
    # CLOB v2 defaults to pUSD collateral; ``usdc_contract_address`` is kept
    # as a legacy field name for callers that still expect that attribute.
    collateral_contract_address: str = ""
    pusd_contract_address: str = ""
    ctf_exchange_v2_address: str = ""
    # Neg Risk CTF Exchange V2 — the separate settlement contract for
    # negative-risk markets. Verified against docs.polymarket.com/resources/
    # contracts. Both exchanges must be covered by on-chain detection and
    # collateral-allowance checks.
    neg_risk_ctf_exchange_v2_address: str = ""


@dataclass(frozen=True)
class ReconciliationConfig:
    stale_threshold_seconds: float
    auto_cancel_stale: bool
    # Per-tick cap on how many already-closed orders the reconciler is
    # allowed to re-probe. Fail-closed default mirrors the previous
    # ``ENV.get("RECONCILE_MAX_CLOSED_PROBES", 10)`` read.
    max_closed_probes: int = 10


@dataclass(frozen=True)
class PositionDriftConfig:
    """PR7 — manual / off-bot position drift policy.

    Proactive cross-check between local position state and venue truth.
    """

    check_on_startup: bool
    fail_closed: bool
    allow_auto_prune_zero: bool
    allow_external_position_tracking: bool
    pause_scope_on_unresolved: bool
    max_size_epsilon: Decimal
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

    initial_delay_seconds: float
    poll_interval_seconds: float
    max_wait_seconds: float
    delayed_extra_seconds: float
    token_delay_overrides: dict[str, float]
    condition_delay_overrides: dict[str, float]
    auto_release_ambiguous: bool
    auto_release_grace_seconds: float
    auto_release_require_all_probes_clean: bool
    auto_release_max_probe_errors: int

    def resolve_initial_delay(
        self,
        *,
        token_id: str | None,
        condition_id: str | None,
        venue_status: str | None = None,
    ) -> float:
        delay = self.initial_delay_seconds
        if token_id and token_id in self.token_delay_overrides:
            delay = self.token_delay_overrides[token_id]
        elif condition_id and condition_id in self.condition_delay_overrides:
            delay = self.condition_delay_overrides[condition_id]
        if str(venue_status or "").lower() == "delayed":
            delay += self.delayed_extra_seconds
        return delay


class _FrozenSettingsModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class DiscoveryApiConfig(_FrozenSettingsModel):
    base_url: str = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
    token: SecretStr = SecretStr("")
    timeout_seconds: float = Field(20.0, gt=0)
    max_retries: int = Field(4, ge=1, le=10)
    requests_per_second: float = Field(5.0, gt=0)
    burst: float = Field(5.0, ge=1)
    max_connections: int = Field(32, ge=1)
    max_keepalive_connections: int = Field(16, ge=0)
    keepalive_expiry_seconds: float = Field(30.0, gt=0)
    retry_base_delay_seconds: float = Field(0.25, gt=0)
    retry_max_delay_seconds: float = Field(8.0, gt=0)
    retry_jitter_ratio: float = Field(0.50, ge=0, le=1)

    @property
    def has_token(self) -> bool:
        return bool(self.token.get_secret_value())


class ShortlistConfig(_FrozenSettingsModel):
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
    max_hours_since_last_trade: int = Field(48, ge=1)
    min_trades_lookback: int = Field(3, ge=1)
    lookback_hours: int = Field(168, ge=1)
    page_size: int = Field(100, ge=1, le=200)
    max_pages: int = Field(5, ge=1)


class MarketQualityConfig(_FrozenSettingsModel):
    min_volume_24h: float = Field(5000.0, ge=0)
    min_liquidity_percentile: float = Field(40.0, ge=0, le=100)
    volume_trend: Literal["UP", "DOWN", "ALL"] = "ALL"
    max_top1_wallet_pct: float = Field(60.0, ge=0, le=100)
    min_unique_traders_7d: int = Field(25, ge=0)
    min_pass_ratio: float = Field(0.60, ge=0, le=1)
    max_markets_per_candidate: int = Field(8, ge=1)


class SocialConfig(_FrozenSettingsModel):
    enabled: bool = False
    keywords: tuple[str, ...] = ()
    hours_back: int = Field(24, ge=1)
    min_acceleration: float = 1.0
    min_author_diversity_pct: float = Field(20.0, ge=0, le=100)

    @model_validator(mode="after")
    def _keyword_wire_format(self) -> "SocialConfig":
        if any(
            not keyword
            or any(character.isspace() or character in "{}," for character in keyword)
            for keyword in self.keywords
        ):
            raise ValueError(
                "social keywords must be non-empty single tokens without spaces, commas, or braces"
            )
        return self


class ScoringWeights(_FrozenSettingsModel):
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
            raise ValueError(
                "longitudinal sizing multiplier max must be >= min"
            )
        for name in ("sharpe_range", "roi_slope_range", "rank_slope_range"):
            lower, upper = getattr(self, name)
            if upper <= lower:
                raise ValueError(f"{name} upper bound must exceed lower bound")
        return self


class HysteresisConfig(_FrozenSettingsModel):
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
    enabled: bool = True
    rediscover_seconds: int = Field(1800, ge=30)
    rescore_seconds: int = Field(900, ge=30)
    evaluation_concurrency: int = Field(8, ge=1)
    max_evaluation_error_ratio: float = Field(0.25, ge=0, le=1)
    max_active_leaders: int = Field(2, ge=1)
    start_from_roster: bool = True
    startup_wait_timeout_seconds: int = Field(3900, ge=1)


class DiscoveryConfig(_FrozenSettingsModel):
    api: DiscoveryApiConfig = DiscoveryApiConfig.model_validate({})
    shortlist: ShortlistConfig = ShortlistConfig.model_validate({})
    wallet360: Wallet360Config = Wallet360Config.model_validate({})
    recency: RecencyConfig = RecencyConfig.model_validate({})
    market: MarketQualityConfig = MarketQualityConfig.model_validate({})
    social: SocialConfig = SocialConfig.model_validate({})
    scoring: ScoringConfig = ScoringConfig.model_validate({})
    longitudinal: LongitudinalConfig = LongitudinalConfig.model_validate({})
    hysteresis: HysteresisConfig = HysteresisConfig.model_validate({})
    scheduler: DiscoverySchedulerConfig = DiscoverySchedulerConfig.model_validate({})


@dataclass(frozen=True)
class FeatureFlagConfig:
    enable_profit_lock: bool
    enable_near_max_exit: bool
    enable_trailing_stop: bool
    enable_staged_tp: bool
    enable_leader_divergence_exit: bool
    enable_stop_loss: bool
    enable_tier_gate_watchlist: bool
    enable_tier_gate_watchlist_log: bool


@dataclass(frozen=True)
class MarketDataConfig:
    """Market data WSS health monitoring and REST fallback policy.

    PR10.4: REST `/book` snapshot is the primary execution source when
    ``primary_source == "rest_snapshot"``. WSS keeps running for background
    cache, prewarm, diagnostics, and resolved-market events, but copy-trade
    execution no longer waits for WSS subscribe/bootstrap.
    """

    primary_source: str = "rest_snapshot"
    rest_snapshot_enable: bool = True
    rest_snapshot_max_age_ms: int = 500
    rest_snapshot_request_timeout_ms: int = 750
    rest_snapshot_total_budget_ms: int = 1000
    rest_snapshot_max_spread_bps: int = 300
    allow_rest_when_wss_down: bool = False
    wss_down_after_seconds: float = 15.0
    rest_degraded_max_age_seconds: float = 2.0
    rest_degraded_max_spread_bps: int = 500
    rest_degraded_log_every_seconds: float = 30.0
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
class RuntimeFilesConfig:
    multi_user_config_file: str = ""


@dataclass(frozen=True)
class AppConfig:
    """Root configuration object. Immutable after construction."""

    database: DatabaseConfig
    wallet: WalletConfig
    clob: ClobConfig
    polling: PollingConfig
    sizing: SizingConfig
    risk: RiskConfig
    execution: ExecutionConfig
    leader: LeaderConfig
    logging: LoggingConfig
    network: NetworkConfig
    reconciliation: ReconciliationConfig
    order_confirmation: OrderConfirmationConfig
    position_drift: PositionDriftConfig
    discovery: DiscoveryConfig
    features: FeatureFlagConfig
    market_data: MarketDataConfig = field(default_factory=MarketDataConfig)
    onchain_detection: OnchainDetectionConfig = field(default_factory=OnchainDetectionConfig)
    files: RuntimeFilesConfig = field(default_factory=RuntimeFilesConfig)
    # Global safety: when True the bot runs in paper-only / dry-run mode.
    dry_run: bool = False



# Factory & validation.
# ---------------------------------------------------------------------------


# Keep version checks behind a local proxy so tests can monkeypatch this module
# without mutating the real process-wide ``sys.version_info`` object.
sys = SimpleNamespace(version_info=_sys.version_info)


def _runtime_python_supported() -> bool:
    return sys.version_info.major == 3 and 11 <= sys.version_info.minor < 13


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

    # Wallet essentials
    private_key = secret_value(cfg.wallet.private_key)
    if not cfg.execution.paper_trade_only and not private_key:
        errors.append("PRIVATE_KEY is required.")
    if private_key and not _HEX_PRIVATE_KEY_RE.match(private_key):
        errors.append("PRIVATE_KEY must be a 66-char hex key (0x + 64 hex digits).")
    if not cfg.wallet.funder_address:
        errors.append("FUNDER_ADDRESS is required.")
    elif not _ETH_ADDR_RE.match(cfg.wallet.funder_address):
        errors.append(
            f'Invalid FUNDER_ADDRESS "{cfg.wallet.funder_address}" '
            f"(expected 42-char Ethereum address)."
        )
    if cfg.wallet.chain_id not in (137, 80002):
        errors.append(f'Unsupported CHAIN_ID "{cfg.wallet.chain_id}". Use 137 or 80002.')
    if not cfg.wallet.polygon_rpc_url:
        errors.append("RPC_URL is required.")
    if not cfg.network.rpc_url:
        errors.append("network.rpc_url is required.")
    for wallet in cfg.leader.wallets:
        if not _ETH_ADDR_RE.match(wallet):
            errors.append(
                f'Invalid leader wallet "{wallet}" (expected 42-char Ethereum address).'
            )

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
    if e.transaction_fee_bps < 0:
        errors.append("TRANSACTION_FEE_BPS must be non-negative.")
    if e.gas_cost_usdc < 0:
        errors.append("GAS_COST_USDC must be non-negative.")
    if e.risk_buffer_pct < 0:
        errors.append("RISK_BUFFER_PCT must be non-negative.")
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

    # Risk
    r = cfg.risk
    if not (0 <= r.near_max_price_warn <= 1):
        errors.append("NEAR_MAX_PRICE_WARN must be in [0, 1].")
    if not (0 <= r.near_max_price_exit <= 1):
        errors.append("NEAR_MAX_PRICE_EXIT must be in [0, 1].")
    if not (0 <= r.near_max_price_warn_trim_ratio <= 1):
        errors.append("NEAR_MAX_PRICE_WARN_TRIM_RATIO must be in [0, 1].")
    if r.near_max_price_warn > r.near_max_price_exit:
        errors.append("NEAR_MAX_PRICE_WARN must be <= NEAR_MAX_PRICE_EXIT.")
    if r.profit_lock_trigger_pct <= 0:
        errors.append("PROFIT_LOCK_TRIGGER_PCT must be > 0.")
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
        errors.append(
            "risk.risk_pre_reconcile_backoff_seconds must be >= timeout."
        )

    # Leader
    le = cfg.leader
    if not (0 <= le.copy_decision_min_score <= 1):
        errors.append("COPY_DECISION_MIN_SCORE must be in [0, 1].")
    if not (0 <= le.tiny_leader_high_confidence_score <= 1):
        errors.append("TINY_LEADER_HIGH_CONFIDENCE_SCORE must be in [0, 1].")
    if le.copy_max_price_drift_pct < 0:
        errors.append("COPY_MAX_PRICE_DRIFT_PCT must be non-negative.")
    if le.copy_max_edge_decay_pct < 0:
        errors.append("COPY_MAX_EDGE_DECAY_PCT must be non-negative.")
    if le.copy_max_signal_age_seconds < 0:
        errors.append("COPY_MAX_SIGNAL_AGE_SECONDS must be non-negative.")
    if le.copy_liquidity_target_usdc <= 0:
        errors.append("COPY_LIQUIDITY_TARGET_USDC must be > 0.")

    # On-chain detection
    oc = cfg.onchain_detection
    confidence_required = str(
        oc.pending_tx_detection_confidence_required or ""
    ).strip().lower()
    if confidence_required not in _ONCHAIN_DETECTION_CONFIDENCE_VALUES:
        allowed = ", ".join(sorted(_ONCHAIN_DETECTION_CONFIDENCE_VALUES))
        errors.append(
            "PENDING_TX_DETECTION_CONFIDENCE_REQUIRED must be one of: "
            f"{allowed}."
        )
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
        errors.append(
            'MARKET_DATA_PRIMARY_SOURCE must be one of: "rest_snapshot", "wss".'
        )
    if md.rest_snapshot_max_age_ms <= 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_MAX_AGE_MS must be > 0.")
    if md.rest_snapshot_request_timeout_ms <= 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_REQUEST_TIMEOUT_MS must be > 0.")
    if md.rest_snapshot_total_budget_ms <= 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_TOTAL_BUDGET_MS must be > 0.")
    if md.rest_snapshot_max_spread_bps < 0:
        errors.append("MARKET_DATA_REST_SNAPSHOT_MAX_SPREAD_BPS must be non-negative.")
    if md.wss_down_after_seconds <= 0:
        errors.append("MARKET_DATA_WSS_DOWN_AFTER_SECONDS must be > 0.")
    if md.rest_degraded_max_age_seconds <= 0:
        errors.append("MARKET_DATA_REST_DEGRADED_MAX_AGE_SECONDS must be > 0.")
    if md.rest_degraded_max_spread_bps < 0:
        errors.append("MARKET_DATA_REST_DEGRADED_MAX_SPREAD_BPS must be non-negative.")
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



# Layered YAML settings loader.
#
# ---------------------------------------------------------------------------

_CONFIG_ROOT = Path(__file__).resolve().parent
_RUNTIME_YAML = _CONFIG_ROOT / "runtime.yml"
_RUNTIME_LOCAL_YAML = Path("config/runtime.local.yml")
_TRADER_DISCOVERY_YAML = _CONFIG_ROOT / "trader_discovery.yaml"
_SECRETS_ENV = Path("secrets.env")


class _LayeredSettings(BaseSettings):
    """Pydantic validation shell around the immutable runtime dataclasses."""

    model_config = SettingsConfigDict(
        env_prefix="PMCOPY__",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="forbid",
        frozen=True,
    )

    database: DatabaseConfig
    wallet: WalletConfig
    clob: ClobConfig
    polling: PollingConfig
    sizing: SizingConfig
    risk: RiskConfig
    execution: ExecutionConfig
    leader: LeaderConfig
    logging: LoggingConfig
    network: NetworkConfig
    reconciliation: ReconciliationConfig
    order_confirmation: OrderConfirmationConfig
    position_drift: PositionDriftConfig
    discovery: DiscoveryConfig
    features: FeatureFlagConfig
    market_data: MarketDataConfig
    onchain_detection: OnchainDetectionConfig
    files: RuntimeFilesConfig

    @model_validator(mode="before")
    @classmethod
    def _decode_nested_structures(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        structured_paths = (
            ("leader", "wallets"),
            ("order_confirmation", "token_delay_overrides"),
            ("order_confirmation", "condition_delay_overrides"),
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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Sources earlier in the tuple have higher priority. secrets.env is
        # parsed into the init graph by ``load_config`` so unrelated Compose
        # keys can be allowlisted without weakening extra-field validation.
        del dotenv_settings
        return (
            env_settings,
            init_settings,
            file_secret_settings,
        )


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
        current = merged.get(str(key))
        if isinstance(current, dict) and isinstance(value, Mapping):
            merged[str(key)] = _deep_merge(current, value)
        else:
            merged[str(key)] = value
    return merged


def _validate_secrets_path(path: Path) -> None:
    """Require the local dotenv secret container to be a regular 0600 file."""
    try:
        info = path.lstat()
    except OSError as exc:
        raise EnvValidationError(f"Unable to inspect secrets file: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise EnvValidationError(f"Secrets path must be a regular non-symlink file: {path}")
    if os.name != "nt" and stat.S_IMODE(info.st_mode) != 0o600:
        raise EnvValidationError(
            f"Secrets file must have permissions 0600 exactly (chmod 600): {path}"
        )


def _read_secret_overrides(path: Path) -> dict[str, Any]:
    """Parse nested application secrets while allowing Compose's PG key."""
    seen: set[str] = set()
    try:
        contents = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise EnvValidationError(f"Unable to read secrets file: {path}") from exc
    lines = contents.splitlines()
    for line_number, raw_line in enumerate(lines, 1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key.startswith("export "):
            key = key[7:].strip()
        if key in seen:
            raise EnvValidationError(
                f"Duplicate key in secrets file at line {line_number}: {key}"
            )
        seen.add(key)

    # Parse the same immutable snapshot used for duplicate detection. Reading
    # the path twice left a TOCTOU window where validation and applied values
    # could come from different file contents.
    values = dotenv_values(stream=StringIO(contents))
    output: dict[str, Any] = {}
    for raw_key, value in values.items():
        key = str(raw_key)
        if key == "POSTGRES_PASSWORD":
            continue
        if not key.startswith("PMCOPY__"):
            raise EnvValidationError(
                "secrets.env application keys must use PMCOPY__SECTION__FIELD; "
                f"unsupported key: {key}"
            )
        path_parts = tuple(
            part.strip().lower() for part in key[len("PMCOPY__") :].split("__")
        )
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


def load_config(
    overrides: Mapping[str, Any] | None = None,
    *,
    runtime_path: str | os.PathLike[str] | None = None,
    local_path: str | os.PathLike[str] | None = None,
    discovery_path: str | os.PathLike[str] | None = None,
    secrets_path: str | os.PathLike[str] | None = None,
) -> AppConfig:
    """Load the immutable config from YAML, dotenv secrets, and nested env.

    ``overrides`` is a nested mapping intended for tests/composition roots. Flat
    legacy variables are rejected; operational overrides use names such as
    ``PMCOPY__EXECUTION__TRADING_MODE=live``.
    """
    runtime_file = Path(runtime_path) if runtime_path else _RUNTIME_YAML
    deployment_root = Path.cwd()
    local_file = (
        Path(local_path)
        if local_path
        else deployment_root / _RUNTIME_LOCAL_YAML
    )
    discovery_file = Path(discovery_path) if discovery_path else _TRADER_DISCOVERY_YAML
    secrets_file = (
        Path(secrets_path)
        if secrets_path
        else deployment_root / _SECRETS_ENV
    )

    payload = _read_yaml(runtime_file, required=True)
    payload = _deep_merge(payload, _read_yaml(local_file, required=False))
    payload = _deep_merge(payload, _read_yaml(discovery_file, required=True))
    if overrides:
        flat_keys = [key for key in overrides if str(key).upper() == str(key)]
        if flat_keys:
            raise EnvValidationError(
                "Flat environment overrides are no longer supported; use nested "
                "settings or PMCOPY__SECTION__FIELD names."
            )
        payload = _deep_merge(payload, overrides)

    if secrets_file.exists():
        _validate_secrets_path(secrets_file)
        payload = _deep_merge(payload, _read_secret_overrides(secrets_file))

    try:
        settings = _LayeredSettings(**payload)
    except ValidationError as exc:
        messages = _validation_messages(exc)
        raise EnvValidationError("Invalid application configuration:\n- " + "\n- ".join(messages)) from exc

    validated = settings.model_dump()
    validated["dry_run"] = settings.execution.paper_trade_only
    try:
        cfg = TypeAdapter(AppConfig).validate_python(validated)
    except ValidationError as exc:
        messages = _validation_messages(exc)
        raise EnvValidationError("Invalid application configuration:\n- " + "\n- ".join(messages)) from exc

    semantic_errors: list[str] = []
    _validate_semantics(cfg, semantic_errors)
    if semantic_errors:
        raise EnvValidationError(
            "Invalid application configuration:\n- " + "\n- ".join(semantic_errors)
        )
    return cfg
