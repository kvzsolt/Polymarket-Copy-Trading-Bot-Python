"""Strongly-typed application configuration (Phase 1 — Clean Architecture).

This module provides an immutable, frozen-dataclass representation of the entire
runtime configuration and a single factory (:func:`load_config`) that parses and
validates an environment mapping. It is the target type that services,
infrastructure adapters, and the composition root will accept via their
constructors in later phases.

Design notes
------------
* ``AppConfig`` is a ``@dataclass(frozen=True)`` — mutation raises
  ``dataclasses.FrozenInstanceError``.
* Parsing is performed against an injected ``Mapping[str, str]``. When no
  mapping is supplied, ``os.environ`` is used. This makes the factory trivially
  testable with synthetic dicts.
* Validation errors are aggregated and raised together in a single
  :class:`EnvValidationError` whose message lists every problem found. This
  matches the style of the existing ``validate_runtime_env`` in
  ``config/env.py``.
* No module-level side effects. Nothing is parsed or loaded at import time.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Any

# Re-use the existing validation error type so callers can catch either
# ``config.env.EnvValidationError`` or ``config.app_config.EnvValidationError``
# interchangeably.
from config.env import EnvValidationError, SignatureType, WalletConfig

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
    "NetworkConfig",
    "PollingConfig",
    "RedemptionConfig",
    "RiskConfig",
    "SignatureType",
    "SizingConfig",
    "WalletConfig",
    "load_config",
]


_ETH_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HEX_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


# ---------------------------------------------------------------------------
# Parsing primitives (env-mapping parameterised — no os.environ coupling).
# ---------------------------------------------------------------------------


def _has_inline_comment(raw: str) -> bool:
    return (" #" in raw) or ("\t#" in raw)


class _Parser:
    """Stateful parser that collects validation errors instead of raising early.

    Methods return best-effort defaults when a value is malformed so that we
    can accumulate all problems in one pass and surface them together.
    """

    def __init__(self, env: Mapping[str, str]) -> None:
        self.env = env
        self.errors: list[str] = []

    # ---- raw access ----
    def raw(self, key: str, default: str = "") -> str:
        return str(self.env.get(key, default) or "")

    def _strip(self, key: str, raw: str) -> str:
        if _has_inline_comment(raw):
            self.errors.append(
                f"{key} contains an inline comment. Use pure values only; "
                f"place comments on separate lines."
            )
            # Strip the comment defensively so downstream conversion can still proceed.
            raw = raw.split("#", 1)[0]
        return raw.strip()

    # ---- primitive conversions ----
    def string(self, key: str, default: str = "") -> str:
        return self._strip(key, self.raw(key, default))

    def float_(self, key: str, default: str) -> float:
        raw = self._strip(key, self.raw(key, default))
        if raw == "":
            raw = default
        try:
            return float(raw)
        except ValueError:
            self.errors.append(f"{key} must be a valid float, got: {raw!r}")
            try:
                return float(default)
            except ValueError:
                return 0.0

    def int_(self, key: str, default: str) -> int:
        raw = self._strip(key, self.raw(key, default))
        if raw == "":
            raw = default
        try:
            return int(raw)
        except ValueError:
            self.errors.append(f"{key} must be a valid int, got: {raw!r}")
            try:
                return int(default)
            except ValueError:
                return 0

    def bool_(self, key: str, default: bool = False) -> bool:
        raw_default = "true" if default else "false"
        raw = self._strip(key, self.raw(key, raw_default))
        if raw == "":
            raw = raw_default
        return raw.lower() in ("1", "true", "yes")

    # ---- specialised parsers ----
    def signature_type(self) -> SignatureType:
        raw = self._strip("SIGNATURE_TYPE", self.raw("SIGNATURE_TYPE", "0"))
        if raw == "":
            raw = "0"
        try:
            value = int(raw)
        except ValueError:
            self.errors.append(
                "SIGNATURE_TYPE must be one of: 0 (EOA), 1 (POLY_PROXY), 2 (GNOSIS_SAFE)."
            )
            return SignatureType.EOA
        try:
            return SignatureType(value)
        except ValueError:
            self.errors.append(
                "SIGNATURE_TYPE must be one of: 0 (EOA), 1 (POLY_PROXY), 2 (GNOSIS_SAFE)."
            )
            return SignatureType.EOA

    def leader_wallets(self) -> tuple[str, ...]:
        raw = self._strip("LEADER_WALLETS", self.raw("LEADER_WALLETS", ""))
        if raw:
            wallets = [w.strip() for w in raw.split(",") if w.strip()]
        else:
            single = self._strip("USER_ADDRESS", self.raw("USER_ADDRESS", ""))
            wallets = [single] if single else []
        for w in wallets:
            if not _ETH_ADDR_RE.match(w):
                self.errors.append(
                    f'Invalid leader wallet "{w}" (expected 42-char Ethereum address).'
                )
        return tuple(wallets)

    def private_key(self) -> str:
        key_file = self._strip("PRIVATE_KEY_FILE", self.raw("PRIVATE_KEY_FILE", ""))
        if key_file:
            try:
                path = Path(key_file).expanduser()
                if not path.exists():
                    self.errors.append(f"PRIVATE_KEY_FILE path does not exist: {path}")
                    return ""
                if not path.is_file():
                    self.errors.append(f"PRIVATE_KEY_FILE must point to a file: {path}")
                    return ""
                mode = path.stat().st_mode & 0o777
                if mode != 0o600:
                    self.errors.append(
                        f"PRIVATE_KEY_FILE must have permissions 0600 exactly (chmod 600): {path}"
                    )
                    return ""
                key = path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                self.errors.append(f"Unable to read PRIVATE_KEY_FILE: {exc}")
                return ""
        else:
            key = self._strip("PRIVATE_KEY", self.raw("PRIVATE_KEY", ""))

        if key and not _HEX_PRIVATE_KEY_RE.match(key):
            self.errors.append("PRIVATE_KEY must be a 66-char hex key (0x + 64 hex digits).")
            return ""
        return key

    def funder_address(self) -> str:
        funder = self._strip("FUNDER_ADDRESS", self.raw("FUNDER_ADDRESS", ""))
        if funder:
            return funder
        return self._strip("PROXY_WALLET", self.raw("PROXY_WALLET", ""))


# ---------------------------------------------------------------------------
# Frozen config sections.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatabaseConfig:
    db_path: str
    enabled: bool = True


@dataclass(frozen=True)
class ClobConfig:
    http_url: str
    api_key: str


@dataclass(frozen=True)
class PollingConfig:
    fetch_interval: float
    aggregate_signal_window_seconds: float
    risk_check_interval: float
    redeem_check_interval: int
    discovery_refresh_interval_seconds: int


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
    copy_min_shares: float
    copy_min_notional_usdc: float
    copy_size_decimals: int
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
    trailing_activation_pct: float
    trailing_distance: float
    staged_tp_levels: str
    staged_tp_fractions: str
    stop_loss_threshold: float
    stop_loss_grace_period_seconds: float


@dataclass(frozen=True)
class ExecutionConfig:
    slippage_tolerance: float
    min_share_price: float
    max_share_price: float
    max_retries: int
    non_terminal_failure_max_attempts: int
    paper_trade_only: bool
    enable_executable_order_guard: bool
    executable_order_buffer_notional_usdc: float
    enable_copy_decision_gate: bool
    enable_near_resolution_gate: bool
    near_resolution_gate_price: float
    near_resolution_max_age_seconds: float
    pending_submission_ttl_seconds: int
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


@dataclass(frozen=True)
class LeaderConfig:
    wallets: tuple[str, ...]
    leader_auto: bool
    leader_auto_count: int
    auto_discover_leaders: bool
    auto_discover_max_leaders: int
    auto_discover_top_n: int
    auto_discover_min_score: float
    copy_decision_min_score: float
    tiny_leader_high_confidence_score: float
    copy_max_price_drift_pct: float
    copy_max_edge_decay_pct: float
    copy_max_signal_age_seconds: float
    copy_liquidity_target_usdc: float
    min_leader_trade_notional_usdc: float
    leader_score_balance_target_usdc: float
    leader_score_pnl_bonus_cap: float
    leader_score_activity_target: int
    leader_score_weight_balance: float
    leader_score_weight_analytics: float
    leader_score_weight_activity: float
    leader_score_floor: float
    tier_promote_to_paper: float
    tier_promote_to_live: float
    tier_min_paper_seconds: float
    tier_demote_to_paper: float
    tier_demote_to_watchlist: float


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
    min_order_size: float


@dataclass(frozen=True)
class RedemptionConfig:
    enable_auto_redeem: bool


@dataclass(frozen=True)
class DiscoveryConfig:
    enable_trader_discovery: bool
    discovery_interval_seconds: int
    discovery_promote_to_active: bool
    min_leader_pnl: float
    min_leader_win_rate: float
    min_leader_positions: int
    min_active_positions: int
    discovery_max_results: int
    multi_user_config_file: str


@dataclass(frozen=True)
class FeatureFlagConfig:
    enable_neg_risk_arb: bool
    enable_profit_lock: bool
    enable_near_max_exit: bool
    enable_trailing_stop: bool
    enable_staged_tp: bool
    enable_leader_divergence_exit: bool
    enable_stop_loss: bool
    enable_tier_gate_watchlist: bool
    enable_tier_gate_watchlist_log: bool
    arb_min_profit_bps: int
    arb_max_cost_usdc: float
    arb_min_ask_depth: float
    arb_scan_interval: int
    arb_max_concurrent: int


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
    redemption: RedemptionConfig
    discovery: DiscoveryConfig
    features: FeatureFlagConfig
    # The raw mapping is kept for transitional backwards compatibility
    # (Phase 6 will remove any remaining legacy ``ENV`` fallbacks).
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


# ---------------------------------------------------------------------------
# Factory & validation.
# ---------------------------------------------------------------------------


def _runtime_python_supported() -> bool:
    return sys.version_info.major == 3 and 11 <= sys.version_info.minor < 13


def _database_enabled(parser: _Parser) -> bool:
    if parser.raw("BOT_STATE_DB_ENABLED", "") != "":
        return parser.bool_("BOT_STATE_DB_ENABLED", True)
    return parser.bool_("DB_ENABLED", True)


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

    # Wallet essentials
    if not cfg.wallet.private_key:
        errors.append("PRIVATE_KEY is required.")
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
    if s.copy_min_shares < 0:
        errors.append("COPY_MIN_SHARES must be >= 0.")
    if s.copy_min_notional_usdc < 0:
        errors.append("COPY_MIN_NOTIONAL_USDC must be >= 0.")
    if s.copy_size_decimals < 0:
        errors.append("COPY_SIZE_DECIMALS must be >= 0.")
    if s.trust_size_multiplier_min <= 0:
        errors.append("TRUST_SIZE_MULTIPLIER_MIN must be > 0.")
    if s.trust_size_multiplier_max <= 0:
        errors.append("TRUST_SIZE_MULTIPLIER_MAX must be > 0.")
    if s.trust_size_multiplier_min > s.trust_size_multiplier_max:
        errors.append("TRUST_SIZE_MULTIPLIER_MIN must be <= TRUST_SIZE_MULTIPLIER_MAX.")

    # Execution
    e = cfg.execution
    if e.executable_order_buffer_notional_usdc < 0:
        errors.append("EXECUTABLE_ORDER_BUFFER_NOTIONAL_USDC must be >= 0.")
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
    if le.leader_score_balance_target_usdc <= 0:
        errors.append("LEADER_SCORE_BALANCE_TARGET_USDC must be > 0.")
    if le.leader_score_activity_target <= 0:
        errors.append("LEADER_SCORE_ACTIVITY_TARGET must be > 0.")
    if not (0 <= le.leader_score_floor <= 1):
        errors.append("LEADER_SCORE_FLOOR must be in [0, 1].")
    for w_name, w_val in (
        ("LEADER_SCORE_WEIGHT_BALANCE", le.leader_score_weight_balance),
        ("LEADER_SCORE_WEIGHT_ANALYTICS", le.leader_score_weight_analytics),
        ("LEADER_SCORE_WEIGHT_ACTIVITY", le.leader_score_weight_activity),
    ):
        if w_val < 0:
            errors.append(f"{w_name} must be non-negative.")
    if (
        le.leader_score_weight_balance
        + le.leader_score_weight_analytics
        + le.leader_score_weight_activity
    ) <= 0:
        errors.append("LEADER_SCORE_WEIGHT_* values must sum to > 0.")
    if le.auto_discover_top_n <= 0:
        errors.append("AUTO_DISCOVER_TOP_N must be > 0.")

    # Discovery
    d = cfg.discovery
    if d.min_active_positions <= 0:
        errors.append("MIN_ACTIVE_POSITIONS must be > 0.")
    if d.discovery_max_results <= 0:
        errors.append("DISCOVERY_MAX_RESULTS must be > 0.")


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    """Build and validate an :class:`AppConfig` from *env* (defaults to ``os.environ``).

    All parsing and validation problems are aggregated into a single
    :class:`EnvValidationError` whose message lists every issue. A happy path
    call returns a fully typed, fully frozen ``AppConfig``.
    """

    if env is None:
        import os  # local import — the whole point of DI is to not couple at module load

        env = os.environ

    p = _Parser(env)

    # Database
    database = DatabaseConfig(
        db_path=p.string("BOT_STATE_DB", "data/bot_state.db"),
        enabled=_database_enabled(p),
    )

    # Wallet
    wallet = WalletConfig(
        private_key=p.private_key(),
        chain_id=p.int_("CHAIN_ID", "137"),
        signature_type=p.signature_type(),
        funder_address=p.funder_address(),
        polygon_rpc_url=p.string("RPC_URL", "https://polygon-bor-rpc.publicnode.com"),
    )

    # CLOB
    clob = ClobConfig(
        http_url=p.string("CLOB_HTTP_URL", "https://clob.polymarket.com/"),
        api_key=p.string("CLOB_API_KEY", ""),
    )

    # Polling / cadence
    polling = PollingConfig(
        fetch_interval=p.float_("FETCH_INTERVAL", "0.25"),
        aggregate_signal_window_seconds=p.float_("AGGREGATE_SIGNAL_WINDOW_SECONDS", "0.1"),
        risk_check_interval=p.float_("RISK_CHECK_INTERVAL", "15"),
        redeem_check_interval=p.int_("REDEEM_CHECK_INTERVAL", "60"),
        discovery_refresh_interval_seconds=p.int_("DISCOVERY_REFRESH_INTERVAL_SECONDS", "300"),
    )

    # Sizing
    sizing = SizingConfig(
        mode=p.string("SIZING_MODE", "proportional").lower(),
        percentage=p.float_("PERCENTAGE", "2"),
        fixed_amount=p.float_("FIXED_AMOUNT", "3"),
        trade_multiplier=p.float_("TRADE_MULTIPLIER", "1.0"),
        tiered_trade_multipliers=p.string("TIERED_TRADE_MULTIPLIERS", ""),
        adaptive_basis=p.string("ADAPTIVE_SIZING_BASIS", "trade_size").lower(),
        adaptive_min_percent=p.float_("ADAPTIVE_MIN_PERCENT", "0"),
        adaptive_max_percent=p.float_("ADAPTIVE_MAX_PERCENT", "0"),
        adaptive_threshold_usdc=p.float_("ADAPTIVE_THRESHOLD_USDC", "500"),
        buy_min_amount=p.float_("BUY_MIN_AMOUNT", "6"),
        buy_max_amount=p.float_("BUY_MAX_AMOUNT", "15"),
        position_threshold=p.int_("POSITION_THRESHOLD", "0"),
        max_position_pct=p.float_("MAX_POSITION_PCT", "0.45"),
        enable_trust_size_multipliers=p.bool_("ENABLE_TRUST_SIZE_MULTIPLIERS", True),
        copy_min_shares=p.float_("COPY_MIN_SHARES", "5"),
        copy_min_notional_usdc=p.float_("COPY_MIN_NOTIONAL_USDC", "1"),
        copy_size_decimals=p.int_("COPY_SIZE_DECIMALS", "4"),
        trust_size_multiplier_min=p.float_("TRUST_SIZE_MULTIPLIER_MIN", "0.50"),
        trust_size_multiplier_max=p.float_("TRUST_SIZE_MULTIPLIER_MAX", "1.25"),
    )

    # Risk
    risk = RiskConfig(
        enable_risk_mgmt=p.bool_("ENABLE_RISK_MGMT", False),
        max_market_exposure_usdc=p.float_("MAX_MARKET_EXPOSURE_USDC", "300"),
        max_category_exposure_usdc=p.float_("MAX_CATEGORY_EXPOSURE_USDC", "600"),
        max_leader_exposure_usdc=p.float_("MAX_LEADER_EXPOSURE_USDC", "500"),
        max_open_positions=p.int_("MAX_OPEN_POSITIONS", "40"),
        max_daily_drawdown_pct=p.float_("MAX_DAILY_DRAWDOWN_PCT", "20"),
        kill_switch=p.bool_("KILL_SWITCH", False),
        enable_balance_tiered_policy=p.bool_("ENABLE_BALANCE_TIERED_POLICY", False),
        balance_policy_reserve_pct=p.float_("BALANCE_POLICY_RESERVE_PCT", "0.12"),
        balance_policy_reserve_floor=p.float_("BALANCE_POLICY_RESERVE_FLOOR", "7"),
        balance_policy_hard_cap_pct=p.float_("BALANCE_POLICY_HARD_CAP_PCT", "0.15"),
        balance_policy_hard_cap_ceiling=p.float_("BALANCE_POLICY_HARD_CAP_CEILING", "8"),
        balance_policy_max_open_positions=p.int_("BALANCE_POLICY_MAX_OPEN_POSITIONS", "12"),
        balance_policy_allow_partial_exits=p.bool_("BALANCE_POLICY_ALLOW_PARTIAL_EXITS", False),
        balance_policy_allow_pyramiding=p.bool_("BALANCE_POLICY_ALLOW_PYRAMIDING", False),
        balance_policy_max_adds_per_position=p.int_("BALANCE_POLICY_MAX_ADDS_PER_POSITION", "1"),
        balance_policy_base_min_score=p.float_("BALANCE_POLICY_BASE_MIN_SCORE", "0.22"),
        balance_policy_min_notional_usdc=p.float_("BALANCE_POLICY_MIN_NOTIONAL_USDC", "0.00001"),
        balance_policy_dynamic_threshold_raise=p.bool_(
            "BALANCE_POLICY_DYNAMIC_THRESHOLD_RAISE", False
        ),
        day_start_equity_usdc=p.float_("DAY_START_EQUITY_USDC", "50"),
        near_max_price_warn=p.float_("NEAR_MAX_PRICE_WARN", "0.95"),
        near_max_price_warn_trim_ratio=p.float_("NEAR_MAX_PRICE_WARN_TRIM_RATIO", "0.50"),
        near_max_price_exit=p.float_("NEAR_MAX_PRICE_EXIT", "0.95"),
        profit_lock_trigger_pct=p.float_("PROFIT_LOCK_TRIGGER_PCT", "70"),
        profit_lock_sell_fraction=p.float_("PROFIT_LOCK_SELL_FRACTION", "0.50"),
        trailing_activation_pct=p.float_("TRAILING_ACTIVATION_PCT", "20"),
        trailing_distance=p.float_("TRAILING_DISTANCE", "0.05"),
        staged_tp_levels=p.string("STAGED_TP_LEVELS", "30,50"),
        staged_tp_fractions=p.string("STAGED_TP_FRACTIONS", "0.25,0.35"),
        stop_loss_threshold=p.float_("STOP_LOSS_THRESHOLD", "0.15"),
        stop_loss_grace_period_seconds=p.float_("STOP_LOSS_GRACE_PERIOD_SECONDS", "180"),
    )

    # Execution
    execution = ExecutionConfig(
        slippage_tolerance=p.float_("SLIPPAGE_TOLERANCE", "2.0"),
        min_share_price=p.float_("MIN_SHARE_PRICE", "0.03"),
        max_share_price=p.float_("MAX_SHARE_PRICE", "0"),
        max_retries=p.int_("MAX_RETRIES", "3"),
        non_terminal_failure_max_attempts=p.int_("NON_TERMINAL_FAILURE_MAX_ATTEMPTS", "3"),
        paper_trade_only=p.bool_("PAPER_TRADE_ONLY", False),
        enable_executable_order_guard=p.bool_("ENABLE_EXECUTABLE_ORDER_GUARD", True),
        executable_order_buffer_notional_usdc=p.float_("EXECUTABLE_ORDER_BUFFER_NOTIONAL_USDC", "1.05"),
        enable_copy_decision_gate=p.bool_("ENABLE_COPY_DECISION_GATE", False),
        enable_near_resolution_gate=p.bool_("ENABLE_NEAR_RESOLUTION_GATE", False),
        near_resolution_gate_price=p.float_("NEAR_RESOLUTION_GATE_PRICE", "0.95"),
        near_resolution_max_age_seconds=p.float_("NEAR_RESOLUTION_MAX_AGE_SECONDS", "15"),
        pending_submission_ttl_seconds=p.int_("PENDING_SUBMISSION_TTL_SECONDS", "120"),
        retry_backoff_base=p.float_("RETRY_BACKOFF_BASE", "1.0"),
        transaction_fee_bps=p.float_("TRANSACTION_FEE_BPS", "20"),
        gas_cost_usdc=p.float_("GAS_COST_USDC", "0.03"),
        risk_buffer_pct=p.float_("RISK_BUFFER_PCT", "0.10"),
        book_spread_mode=p.string("BOOK_SPREAD_MODE", "absolute_bps").lower(),
        max_book_spread_bps=p.int_("MAX_BOOK_SPREAD_BPS", "2500"),
        max_book_spread_ticks=p.int_("MAX_BOOK_SPREAD_TICKS", "0"),
        execution_require_fresh_book=p.bool_("EXECUTION_REQUIRE_FRESH_BOOK", True),
        circuit_failure_threshold=p.int_("CIRCUIT_FAILURE_THRESHOLD", "15"),
        circuit_cooldown_seconds=p.float_("CIRCUIT_COOLDOWN_SECONDS", "60"),
        meta_cache_ttl_good=p.float_("META_CACHE_TTL_GOOD", "300"),
        meta_cache_ttl_thin=p.float_("META_CACHE_TTL_THIN", "30"),
    )

    # Leader / discovery cascades — reproduce the same cascading defaults the legacy
    # ENV snapshot uses so services relying on these derived flags behave identically.
    configured_auto_discover = p.bool_("AUTO_DISCOVER_LEADERS", False)
    configured_trader_discovery = p.bool_("ENABLE_TRADER_DISCOVERY", False)
    configured_promote_to_active = p.bool_("DISCOVERY_PROMOTE_TO_ACTIVE", False)
    leader_auto_default = configured_auto_discover or configured_promote_to_active
    leader_auto = p.bool_("LEADER_AUTO", leader_auto_default)
    auto_discover_leaders = configured_auto_discover or leader_auto
    enable_trader_discovery = (
        configured_trader_discovery or leader_auto or auto_discover_leaders
    )
    discovery_promote_to_active = configured_promote_to_active or leader_auto

    legacy_auto_count = p.int_("AUTO_DISCOVER_TOP_N", "5")
    leader_auto_count_raw = p._strip(
        "LEADER_AUTO_COUNT", p.raw("LEADER_AUTO_COUNT", "")
    )
    if leader_auto_count_raw:
        try:
            auto_discover_top_n = int(leader_auto_count_raw)
        except ValueError:
            p.errors.append(
                f"LEADER_AUTO_COUNT must be a valid integer, got: {leader_auto_count_raw!r}"
            )
            auto_discover_top_n = legacy_auto_count
    else:
        auto_discover_top_n = legacy_auto_count

    leader = LeaderConfig(
        wallets=p.leader_wallets(),
        leader_auto=leader_auto,
        leader_auto_count=auto_discover_top_n,
        auto_discover_leaders=auto_discover_leaders,
        auto_discover_max_leaders=max(
            p.int_("AUTO_DISCOVER_MAX_LEADERS", "10"), auto_discover_top_n
        ),
        auto_discover_top_n=auto_discover_top_n,
        auto_discover_min_score=p.float_("AUTO_DISCOVER_MIN_SCORE", "0.45"),
        copy_decision_min_score=p.float_("COPY_DECISION_MIN_SCORE", "0.15"),
        tiny_leader_high_confidence_score=p.float_("TINY_LEADER_HIGH_CONFIDENCE_SCORE", "0.75"),
        copy_max_price_drift_pct=p.float_("COPY_MAX_PRICE_DRIFT_PCT", "15"),
        copy_max_edge_decay_pct=p.float_("COPY_MAX_EDGE_DECAY_PCT", "90"),
        copy_max_signal_age_seconds=p.float_("COPY_MAX_SIGNAL_AGE_SECONDS", "100"),
        copy_liquidity_target_usdc=p.float_("COPY_LIQUIDITY_TARGET_USDC", "50"),
        min_leader_trade_notional_usdc=p.float_("MIN_LEADER_TRADE_NOTIONAL_USDC", "1"),
        leader_score_balance_target_usdc=p.float_("LEADER_SCORE_BALANCE_TARGET_USDC", "500"),
        leader_score_pnl_bonus_cap=p.float_("LEADER_SCORE_PNL_BONUS_CAP", "0.10"),
        leader_score_activity_target=p.int_("LEADER_SCORE_ACTIVITY_TARGET", "8"),
        leader_score_weight_balance=p.float_("LEADER_SCORE_WEIGHT_BALANCE", "0.35"),
        leader_score_weight_analytics=p.float_("LEADER_SCORE_WEIGHT_ANALYTICS", "0.45"),
        leader_score_weight_activity=p.float_("LEADER_SCORE_WEIGHT_ACTIVITY", "0.20"),
        leader_score_floor=p.float_("LEADER_SCORE_FLOOR", "0.10"),
        tier_promote_to_paper=p.float_("TIER_PROMOTE_TO_PAPER", "0.40"),
        tier_promote_to_live=p.float_("TIER_PROMOTE_TO_LIVE", "0.55"),
        tier_min_paper_seconds=p.float_("TIER_MIN_PAPER_SECONDS", "86400"),
        tier_demote_to_paper=p.float_("TIER_DEMOTE_TO_PAPER", "0.35"),
        tier_demote_to_watchlist=p.float_("TIER_DEMOTE_TO_WATCHLIST", "0.25"),
    )

    logging_cfg = LoggingConfig(
        level=p.string("POLYMARKET_LOG_LEVEL", "INFO").upper(),
        log_dir=p.string("LOG_DIR", "log"),
        app_log_jsonl_file=p.string("APP_LOG_JSONL_FILE", "log/PolymarketCopyTradingBot.jsonl"),
        log_max_bytes=p.int_("LOG_MAX_BYTES", "5242880"),
        log_backup_count=p.int_("LOG_BACKUP_COUNT", "3"),
        pnl_log_file=p.string("PNL_LOG_FILE", "log/pnl.jsonl"),
    )

    network = NetworkConfig(
        rpc_url=p.string("RPC_URL", ""),
        chain_id=wallet.chain_id,
        usdc_contract_address=(
            p.string("COLLATERAL_CONTRACT_ADDRESS", "")
            or p.string("PUSD_CONTRACT_ADDRESS", "")
            or p.string("USDC_CONTRACT_ADDRESS", "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359")
        ),
        enforce_geoblock=p.bool_("ENFORCE_GEOBLOCK", True),
        enable_metrics_server=p.bool_("ENABLE_METRICS_SERVER", False),
        metrics_port=p.int_("METRICS_PORT", "9102"),
        metrics_bind_host=p.string("METRICS_BIND_HOST", "127.0.0.1"),
        min_order_size=p.float_("MIN_ORDER_SIZE", "0.00001"),
    )

    redemption = RedemptionConfig(
        enable_auto_redeem=p.bool_("ENABLE_AUTO_REDEEM", False),
    )

    discovery = DiscoveryConfig(
        enable_trader_discovery=enable_trader_discovery,
        discovery_interval_seconds=p.int_("DISCOVERY_INTERVAL_SECONDS", "3600"),
        discovery_promote_to_active=discovery_promote_to_active,
        min_leader_pnl=p.float_("MIN_LEADER_PNL", "1000"),
        min_leader_win_rate=p.float_("MIN_LEADER_WIN_RATE", "0.55"),
        min_leader_positions=p.int_("MIN_LEADER_POSITIONS", "20"),
        min_active_positions=p.int_("MIN_ACTIVE_POSITIONS", "1"),
        discovery_max_results=p.int_("DISCOVERY_MAX_RESULTS", "50"),
        multi_user_config_file=p.string("MULTI_USER_CONFIG_FILE", ""),
    )

    features = FeatureFlagConfig(
        enable_neg_risk_arb=p.bool_("ENABLE_NEG_RISK_ARB", False),
        enable_profit_lock=p.bool_("ENABLE_PROFIT_LOCK", False),
        enable_near_max_exit=p.bool_("ENABLE_NEAR_MAX_EXIT", True),
        enable_trailing_stop=p.bool_("ENABLE_TRAILING_STOP", False),
        enable_staged_tp=p.bool_("ENABLE_STAGED_TP", False),
        enable_leader_divergence_exit=p.bool_("ENABLE_LEADER_DIVERGENCE_EXIT", False),
        enable_stop_loss=p.bool_("ENABLE_STOP_LOSS", False),
        enable_tier_gate_watchlist=p.bool_("ENABLE_TIER_GATE_WATCHLIST", False),
        enable_tier_gate_watchlist_log=p.bool_("ENABLE_TIER_GATE_WATCHLIST_LOG", False),
        arb_min_profit_bps=p.int_("ARB_MIN_PROFIT_BPS", "30"),
        arb_max_cost_usdc=p.float_("ARB_MAX_COST_USDC", "10"),
        arb_min_ask_depth=p.float_("ARB_MIN_ASK_DEPTH", "20"),
        arb_scan_interval=p.int_("ARB_SCAN_INTERVAL", "30"),
        arb_max_concurrent=p.int_("ARB_MAX_CONCURRENT", "2"),
    )

    cfg = AppConfig(
        database=database,
        wallet=wallet,
        clob=clob,
        polling=polling,
        sizing=sizing,
        risk=risk,
        execution=execution,
        leader=leader,
        logging=logging_cfg,
        network=network,
        redemption=redemption,
        discovery=discovery,
        features=features,
        raw=dict(env),
    )

    _validate_semantics(cfg, p.errors)

    if p.errors:
        raise EnvValidationError(
            "Invalid runtime environment:\n- " + "\n- ".join(p.errors)
        )

    return cfg
