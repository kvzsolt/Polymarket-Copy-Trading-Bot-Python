"""Discovery, market-data, on-chain detection, and top-level AppConfig schemas."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from pmcopy.config.auto_follow import AutoFollowConfig
from pmcopy.config.env import WalletConfig
from pmcopy.config.evidence import EvidenceConfig
from pmcopy.config.provider_budget import ProviderBudgetConfig
from pmcopy.config.redemption import RedemptionConfig
from pmcopy.config.schemas_core import (
    AppServiceConfig,
    CopyTradingConfig,
    DatabaseConfig,
    DetectionConfig,
    ExecutionConfig,
    LoggingConfig,
    PolymarketConfig,
    ProtectiveExitsConfig,
    ReconciliationConfig,
    RiskConfig,
    SizingConfig,
)


class _FrozenSettingsModel(BaseModel):
    # ``allow_inf_nan=False`` is set here rather than per-field: Pydantic
    # resolves it from the model owning the schema, so a nested BaseModel
    # ignores the shell's setting. Every settings model inherits this base,
    # so NaN/+Inf/-Inf are rejected graph-wide without a field allowlist to
    # drift. Nested frozen dataclasses inherit it from ``_LayeredSettings``.
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class FalconApiConfig(_FrozenSettingsModel):
    """Validate the private Falcon analytics transport settings."""

    base_url: str = "https://retriever.falconapi.net/api/v2/semantic/retrieve/parameterized"
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

    promote_score: float = Field(0.55, ge=0, le=1)
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
    max_active_leaders: int = Field(3, ge=1)
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
    min_copyable_trade_share: Decimal = Decimal("0.35")
    min_fit_score: Decimal = Decimal("0.45")
    visible_capacity_utilization: Decimal = Decimal("1")


class FalconDiscoveryConfig(_FrozenSettingsModel):
    """Validate Falcon discovery policy and enablement settings.

    The bearer token belongs solely to ``DiscoveryConfig.api``. When this
    policy is disabled or that token is empty, composition fails closed
    rather than substituting a weaker discovery source.
    """

    enabled: bool = False
    candidate_limit: int = Field(500, ge=1)
    candidate_concurrency: int = Field(12, ge=1)
    wallet360_windows: tuple[int, ...] = (1, 3, 7, 15)
    required_wallet360_windows: int = Field(3, ge=1)
    max_wallet360_snapshot_age_hours: int = Field(72, ge=1)
    recent_trade_lookback_hours: int = Field(48, ge=1)
    max_last_trade_age_hours: int = Field(36, ge=1)
    activity_window_hours: int = Field(24, ge=1)
    min_activity_window_trades: int = Field(1, ge=0)
    min_recent_trades: int = Field(10, ge=0)
    max_recent_trades: int = Field(500, ge=1)
    max_recent_markets_per_wallet: int = Field(30, ge=1)
    max_depth_probes_per_wallet: int = Field(8, ge=0)
    max_price_jump_probes_per_wallet: int = Field(3, ge=0)
    price_jump_lookback_hours: int = Field(24, ge=1)
    price_jump_min_change_pct: Decimal = Decimal("10")
    max_recent_price_jumps: int = Field(8, ge=0)
    enable_price_jump_risk: bool = True
    short_market_max_days: int = Field(30, ge=1)
    min_short_trade_count: int = Field(0, ge=0)
    min_short_trade_share: Decimal = Decimal("0")
    min_h_score: Decimal = Decimal("60")
    min_liquidity_percentile: Decimal = Decimal("50")
    toxic_market_flags: tuple[str, ...] = ("liquidity_risk", "volume_collapse", "whale_control")
    max_toxic_market_share: Decimal = Decimal("0.50")
    max_market_concentration: Decimal = Decimal("0.65")
    max_missing_field_ratio: Decimal = Decimal("0.25")
    drawdown_gate_enabled: bool = False
    max_drawdown: Decimal = Decimal("0.35")
    max_sybil_score: Decimal = Decimal("20")
    reject_perfect_timing_flag: bool = True
    reject_suspicious_win_rate_flag: bool = True
    max_similar_wallets: int = Field(2, ge=0)
    max_trades_per_day: Decimal = Decimal("300")
    min_avg_trade_size_usdc: Decimal = Decimal("5")
    excluded_categories: tuple[str, ...] = ("Crypto",)
    max_excluded_category_trade_share: Decimal = Decimal("0.50")
    specialist_bonus_enabled: bool = True
    specialist_min_dominant_share: Decimal = Decimal("0.80")
    specialist_min_win_rate: Decimal = Decimal("0.57")
    specialist_bonus_weight: Decimal = Decimal("0.10")
    leaderboard_min_win_rate_15d: Decimal = Decimal("0.45")
    leaderboard_max_win_rate_15d: Decimal = Decimal("0.95")
    leaderboard_min_roi_15d: Decimal = Decimal("0")
    leaderboard_min_trades_15d: int = Field(15, ge=0)
    leaderboard_max_trades_15d: int = Field(3_000, ge=1)
    leaderboard_min_pnl_15d: Decimal = Decimal("0")
    leaderboard_sort_modes: tuple[str, ...] = (
        "h_score",
        "roi",
        "win_rate",
        "pnl",
        "sharpe",
        "trades",
    )
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
    auto_follow: AutoFollowConfig = AutoFollowConfig.model_validate({})


@dataclass(frozen=True)
class MarketDataConfig:
    """Market data WSS health monitoring and REST fallback policy.

    REST `/book` remains available for cache recovery and non-copy consumers.
    In REST-primary mode, copy FAK execution fetches metadata only and never
    waits for a book or WSS subscribe/bootstrap.
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
    redemption: RedemptionConfig = field(default_factory=RedemptionConfig)
    provider_budget: ProviderBudgetConfig = field(default_factory=ProviderBudgetConfig)
    evidence: EvidenceConfig = field(default_factory=EvidenceConfig)
    # Derived loader metadata used by the operator API to select the exact
    # environment override directory. It is not a service-owned YAML section.
    environment: str = "paper"
    # Global safety: when True the bot runs in paper-only / dry-run mode.
    # Derived from ``execution.trading_mode``; never set directly.
    dry_run: bool = False

    def entry_price_drift_pct_for_leader(self, wallet: str) -> float:
        """Resolve the copy-entry FAK band for one leader.

        Leaders differ in how hard they sweep a book, so the band that keeps a
        copied FAK marketable is a per-leader property rather than one global
        constant. Precedence, highest first: ``config/traders/<name>.yml`` ->
        ``config/copy_trading.yml`` -> ``config/execution.yml``.

        Args:
            wallet: Leader wallet address; matching is case-insensitive and an
                unknown wallet simply resolves to the inherited value.

        Returns:
            The effective drift percentage for this leader.
        """
        override = self.copy_trading.leader_override(wallet)
        leader_value = getattr(override, "max_entry_price_drift_pct", None) if override else None
        if leader_value is not None:
            return float(leader_value)
        roster_value = self.copy_trading.max_entry_price_drift_pct
        if roster_value is not None:
            return float(roster_value)
        return float(self.execution.max_entry_price_drift_pct)


