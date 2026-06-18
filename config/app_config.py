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

import logging
import math
import re
from collections.abc import Mapping
from decimal import Decimal
from types import SimpleNamespace
import sys as _sys
from dataclasses import dataclass, field
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
    "OrderConfirmationConfig",
    "PositionDriftConfig",
    "PollingConfig",
    "RedemptionConfig",
    "ReconciliationConfig",
    "RiskConfig",
    "SignatureType",
    "SizingConfig",
    "WalletConfig",
    "load_config",
]


_ETH_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HEX_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
logger = logging.getLogger("AppConfig")

# Contract addresses live in the dependency-free ``config.addresses`` leaf
# module so the typed config and the legacy ``config.env`` surface share one
# source of truth (env already imports app_config indirectly, so neither can
# own the constants without a cycle). Re-exported here for back-compat with
# existing ``from config.app_config import POLYGON_USDCE_ADDRESS`` call sites.
from config.addresses import (  # noqa: E402
    POLYGON_USDCE_ADDRESS,
    POLYMARKET_CTF_COLLATERAL_ADAPTER_ADDRESS,
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_PUSD_ADDRESS,
)


# ---------------------------------------------------------------------------
# Parsing primitives (env-mapping parameterised — no os.environ coupling).
# ---------------------------------------------------------------------------


def _has_inline_comment(raw: str) -> bool:
    return (" #" in raw) or ("\t#" in raw)


def _optional_int(p: "_Parser", key: str) -> int | None:
    raw = p._strip(key, p.raw(key, ""))
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        p.errors.append(f"{key} must be a valid int, got: {raw!r}")
        return None


def _csv_tuple(p: "_Parser", key: str, default: str) -> tuple[str, ...]:
    raw = p.string(key, default)
    return tuple(item.strip().lower() for item in raw.split(",") if item.strip())


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

    def _finite_or_error(self, key: str, value: float, *, default: str) -> float:
        """Reject NaN / +/-Inf for numeric config (P2-c, fail-closed).

        ``float("nan")`` / ``float("inf")`` parse without error but corrupt
        every downstream comparison gate (slippage tolerance, spread caps),
        so a non-finite value must surface as a config error rather than be
        accepted silently. We inline the check here instead of reusing
        ``config.validation`` because those helpers couple finiteness to a
        sign check and raise eagerly, whereas this parser accumulates errors
        and raises once in ``load_config``.
        """
        if not math.isfinite(value):
            self.errors.append(f"{key} must be a finite number, got: {value!r}")
            try:
                fallback = float(default)
            except ValueError:
                return 0.0
            return fallback if math.isfinite(fallback) else 0.0
        return value

    def float_(self, key: str, default: str) -> float:
        raw = self._strip(key, self.raw(key, default))
        if raw == "":
            raw = default
        try:
            value = float(raw)
        except ValueError:
            self.errors.append(f"{key} must be a valid float, got: {raw!r}")
            try:
                value = float(default)
            except ValueError:
                return 0.0
            return value if math.isfinite(value) else 0.0
        return self._finite_or_error(key, value, default=default)

    def float_or_auto(self, key: str, default: str) -> float | None:
        raw = self._strip(key, self.raw(key, default))
        if raw == "" or raw.lower() == "auto" or raw.startswith("#"):
            return None
        try:
            value = float(raw)
        except ValueError:
            self.errors.append(f"{key} must be a valid float or 'auto', got: {raw!r}")
            try:
                value = float(default)
            except ValueError:
                return 0.0
            return value if math.isfinite(value) else 0.0
        return self._finite_or_error(key, value, default=default)

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

    def json_dict_float(self, key: str, default: str = "{}") -> dict[str, float]:
        """Parse a JSON object mapping to floats.

        Empty / missing input yields an empty dict. Malformed input collects
        an error and returns an empty dict so other config can still load.
        """
        import json as _json

        raw = self._strip(key, self.raw(key, default))
        if not raw:
            return {}
        try:
            parsed = _json.loads(raw)
        except (ValueError, TypeError):
            self.errors.append(f"{key} must be a valid JSON object, got: {raw!r}")
            return {}
        if not isinstance(parsed, dict):
            self.errors.append(f"{key} must be a JSON object (dict), got: {type(parsed).__name__}")
            return {}
        result: dict[str, float] = {}
        for k, v in parsed.items():
            try:
                result[str(k)] = float(v)
            except (TypeError, ValueError):
                self.errors.append(
                    f"{key} entry {k!r}={v!r} is not coercible to float"
                )
        return result

    # ---- specialised parsers ----
    def leader_wallets(self, *, allow_user_address_fallback: bool = True) -> tuple[str, ...]:
        raw = self._strip("LEADER_WALLETS", self.raw("LEADER_WALLETS", ""))
        if raw:
            wallets = [w.strip() for w in raw.split(",") if w.strip()]
        elif allow_user_address_fallback:
            single = self._strip("USER_ADDRESS", self.raw("USER_ADDRESS", ""))
            wallets = [single] if single else []
        else:
            wallets = []
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
                # Resolve symlinks before any permission check so a
                # symlink pointing at /etc/passwd cannot bypass the
                # 0600-on-the-link-itself check.
                raw_path = Path(key_file).expanduser()
                path = raw_path.resolve(strict=False)

                # Confined-deputy guard: only accept paths under the
                # operator's home OR the bot's working dir. An operator
                # who wants a wider location can symlink from one of the
                # allowed roots — the resolve() above means the symlink
                # target must also pass this check.
                allowed_roots = (
                    Path.home().resolve(strict=False),
                    Path.cwd().resolve(strict=False),
                )
                if not any(path == root or _is_under(path, root) for root in allowed_roots):
                    self.errors.append(
                        f"PRIVATE_KEY_FILE must be under $HOME or the working dir; "
                        f"resolved path was {path}"
                    )
                    return ""

                if not path.exists():
                    self.errors.append(f"PRIVATE_KEY_FILE path does not exist: {path}")
                    return ""
                if not path.is_file():
                    self.errors.append(f"PRIVATE_KEY_FILE must point to a file: {path}")
                    return ""

                # Check BOTH the link itself (lstat) and the resolved
                # target (stat). 0600 on a symlink target is meaningless
                # if the link itself is world-readable on systems that
                # honour link mode (some), and vice versa.
                lstat_mode = raw_path.lstat().st_mode & 0o777
                stat_mode = path.stat().st_mode & 0o777
                if stat_mode != 0o600 or (
                    raw_path != path and lstat_mode not in (0o600, 0o777)
                ):
                    # lstat 0777 is the default for symlinks on Linux —
                    # accept that case; otherwise require 0600 on both
                    # the link and the target.
                    if stat_mode != 0o600:
                        self.errors.append(
                            f"PRIVATE_KEY_FILE must have permissions 0600 exactly "
                            f"(chmod 600): {path} (mode 0o{stat_mode:o})"
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
    dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 20
    command_timeout_seconds: float = 30.0


@dataclass(frozen=True)
class ClobConfig:
    http_url: str
    api_key: str
    # CLOB V2 gate — fail-closed default. The bot will not attempt the
    # py-clob-client-v2 surface unless this is explicitly enabled.
    clob_v2_ready: bool = False


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


@dataclass(frozen=True)
class ExecutionConfig:
    slippage_tolerance: float
    min_share_price: float
    max_share_price: float
    max_retries: int
    non_terminal_failure_max_attempts: int
    paper_trade_only: bool
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


@dataclass(frozen=True)
class RedemptionConfig:
    enable_auto_redeem: bool
    check_interval: int
    resolved_high: Decimal
    resolved_low: Decimal
    price_epsilon: Decimal
    zero_threshold: Decimal
    ctf_contract_address: str
    collateral_contract_address: str
    gas_limit: int
    gas_price_buffer_pct: int
    receipt_timeout_seconds: int
    max_conditions_per_tick: int
    require_signer_match: bool


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
    discovery_cache_ttl_seconds: float
    multi_user_config_file: str
    leader_source: str = "manual"
    auto_leader_enabled: bool = False
    auto_leader_count: int = 2
    allow_auto_trader_detection: bool = False
    max_auto_traders_count: int = 2
    falcon_api_url: str = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
    falcon_api_token: str = ""
    falcon_page_size: int = 100
    falcon_max_pages: int = 5
    falcon_sort_modes: tuple[str, ...] = ("h_score", "pnl", "sharpe", "roi", "win_rate")
    falcon_dedupe_candidates: bool = True
    falcon_max_candidates: int = 250
    falcon_min_win_rate_15d: float = 0.45
    falcon_max_win_rate_15d: float = 0.90
    falcon_min_roi_15d: float = 0.0
    falcon_min_pnl_15d: float = 100.0
    falcon_min_trades_15d: int = 30
    falcon_max_trades_15d: int = 750
    polymarket_history_lookback_days: int = 365
    polymarket_history_window_days: int = 7
    polymarket_history_min_window_seconds: int = 21600
    polymarket_activity_max_offset: int = 10000
    discovery_history_trade_limit: int = 50000
    discovery_signal_group_window_seconds: int = 60
    discovery_signal_group_max_gap_seconds: int = 120
    discovery_min_group_notional_usdc: float = 5.0
    discovery_min_copyable_grouped_signal_ratio: float = 0.75
    discovery_min_copyable_notional_ratio: float = 0.60
    discovery_min_auto_sports_esports_ratio: float = 0.75
    discovery_auto_prefer_sports_esports: bool = True
    discovery_evaluation_ttl_seconds: int = 3600
    discovery_reject_stale_evaluations: bool = True
    discovery_require_recent_state_for_live: bool = True
    discovery_require_7d_non_negative: bool = True
    discovery_min_7d_pnl_usdc: float = 0.0
    discovery_allow_3d_slightly_negative: bool = True
    discovery_min_3d_pnl_usdc: float = -50.0
    discovery_min_3d_roi: float = -0.05
    leader_score_version: str = "leader_score_v2"
    discovery_volume_target_15d: float = 10000.0
    discovery_min_volume_15d: float = 0.0
    discovery_rank_target: int = 100
    discovery_risk_penalty_enabled: bool = True
    auto_leader_require_score_version: bool = True
    discovery_crypto_fast_max_resolution_minutes: float = 30.0
    discovery_crypto_fast_reject_signal_ratio: float = 0.70
    discovery_crypto_fast_reject_notional_ratio: float = 0.70
    discovery_crypto_fast_min_non_crypto_signals: int = 5
    discovery_auto_write_leaders: bool = False
    discovery_dynamic_monitoring_enabled: bool = False
    discovery_market_specialists_allowed: bool = True
    discovery_position_volatility_allowed: bool = True
    rotate_traders_env_file: str = "config/runtime.local.env"
    rotate_traders_min_shadow_days: int = 3
    rotate_traders_min_shadow_signals: int = 20
    rotate_traders_replace_only_if_needed: bool = True
    rotate_traders_min_score_delta: float = 0.10
    rotate_keep_live_if_7d_pnl_positive: bool = True
    rotate_reject_live_if_7d_pnl_below: float = 0.0
    rotate_allow_manual_override: bool = True


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
    reconciliation: ReconciliationConfig
    order_confirmation: OrderConfirmationConfig
    position_drift: PositionDriftConfig
    discovery: DiscoveryConfig
    features: FeatureFlagConfig
    market_data: MarketDataConfig = field(default_factory=MarketDataConfig)
    # Global safety: when True the bot runs in paper-only / dry-run mode.
    dry_run: bool = False
    # The raw mapping is kept for transitional backwards compatibility
    # (Phase 6 will remove any remaining legacy ``ENV`` fallbacks).
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


# ---------------------------------------------------------------------------
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

    # Redemption
    red = cfg.redemption
    if red.check_interval <= 0:
        errors.append("REDEEM_CHECK_INTERVAL must be > 0.")
    if not (Decimal("0") <= red.resolved_low <= Decimal("1")):
        errors.append("REDEEM_RESOLVED_LOW must be in [0, 1].")
    if not (Decimal("0") <= red.resolved_high <= Decimal("1")):
        errors.append("REDEEM_RESOLVED_HIGH must be in [0, 1].")
    if red.resolved_low >= red.resolved_high:
        errors.append("REDEEM_RESOLVED_LOW must be < REDEEM_RESOLVED_HIGH.")
    if red.price_epsilon < 0:
        errors.append("REDEEM_PRICE_EPSILON must be non-negative.")
    if red.zero_threshold < 0:
        errors.append("REDEEM_ZERO_THRESHOLD must be non-negative.")
    if red.gas_limit <= 0:
        errors.append("REDEEM_GAS_LIMIT must be > 0.")
    if red.gas_price_buffer_pct < 0:
        errors.append("REDEEM_GAS_PRICE_BUFFER_PCT must be non-negative.")
    if red.receipt_timeout_seconds <= 0:
        errors.append("REDEEM_RECEIPT_TIMEOUT_SECONDS must be > 0.")
    if red.max_conditions_per_tick <= 0:
        errors.append("REDEEM_MAX_CONDITIONS_PER_TICK must be > 0.")
    if red.enable_auto_redeem:
        if not _ETH_ADDR_RE.match(red.ctf_contract_address):
            errors.append("REDEEM_CTF_CONTRACT_ADDRESS must be a valid Ethereum address.")
        if not _ETH_ADDR_RE.match(red.collateral_contract_address):
            errors.append("REDEEM_COLLATERAL_CONTRACT_ADDRESS must be a valid Ethereum address.")

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
    if d.leader_source not in {"manual", "auto"}:
        errors.append('LEADER_SOURCE must be one of: "manual", "auto".')
    if d.min_active_positions <= 0:
        errors.append("MIN_ACTIVE_POSITIONS must be > 0.")
    if d.discovery_max_results <= 0:
        errors.append("DISCOVERY_MAX_RESULTS must be > 0.")
    if d.falcon_page_size <= 0:
        errors.append("FALCON_PAGE_SIZE must be > 0.")
    if d.falcon_max_pages <= 0:
        errors.append("FALCON_MAX_PAGES must be > 0.")
    if not d.falcon_sort_modes:
        errors.append("FALCON_SORT_MODES must include at least one sort mode.")
    valid_falcon_sorts = {"h_score", "roi", "pnl", "win_rate", "trades", "sharpe"}
    invalid_falcon_sorts = [
        mode for mode in d.falcon_sort_modes if mode not in valid_falcon_sorts
    ]
    if invalid_falcon_sorts:
        errors.append(
            "FALCON_SORT_MODES contains unsupported sort mode(s): "
            + ", ".join(invalid_falcon_sorts)
        )
    if d.falcon_max_candidates <= 0:
        errors.append("FALCON_MAX_CANDIDATES must be > 0.")
    if d.leader_score_version not in {"leader_score_v1", "leader_score_v2"}:
        errors.append(
            'LEADER_SCORE_VERSION must be one of: "leader_score_v1", "leader_score_v2".'
        )
    if d.discovery_volume_target_15d <= 0:
        errors.append("DISCOVERY_VOLUME_TARGET_15D must be > 0.")
    if d.discovery_min_volume_15d < 0:
        errors.append("DISCOVERY_MIN_VOLUME_15D must be non-negative.")
    if d.discovery_min_volume_15d > d.discovery_volume_target_15d:
        errors.append(
            "DISCOVERY_MIN_VOLUME_15D must be <= DISCOVERY_VOLUME_TARGET_15D."
        )
    if d.discovery_rank_target <= 0:
        errors.append("DISCOVERY_RANK_TARGET must be > 0.")

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


_RAW_SENSITIVE_KEYS: frozenset[str] = frozenset(
    {
        "PRIVATE_KEY",
        "PRIVATE_KEY_FILE",
        "CLOB_API_KEY",
        "CLOB_SECRET",
        "CLOB_PASSPHRASE",
        "FALCON_API_TOKEN",
        "DATABASE_URL",
    }
)


def _is_under(child: Path, parent: Path) -> bool:
    """Return True when *child* is a descendant of *parent*.

    Both arguments must already be ``.resolve()``-ed by the caller so
    the comparison handles symlinks correctly.
    """
    try:
        return child.is_relative_to(parent)
    except AttributeError:  # pragma: no cover — Python < 3.9
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False


def _strip_sensitive(env: Mapping[str, str]) -> dict[str, str]:
    """Build a copy of *env* with credential keys removed.

    AppConfig.raw is accessed by ``runtime_summary``, ``trade_pipeline``,
    and ``executionService`` for structured logging / debug surfacing. Any
    of those sites that logged ``config.raw`` wholesale used to leak the
    private key. Filter at the boundary so the in-memory AppConfig.raw
    physically does not carry the values, and ``raw.get("PRIVATE_KEY")``
    returns None — typed accessors must be used for those keys instead.
    """
    return {k: v for k, v in env.items() if k not in _RAW_SENSITIVE_KEYS}


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

    # Database — Postgres via asyncpg. Validate pool bounds up front so
    # asyncpg.create_pool does not raise an unhelpful inner-pool error.
    _pool_min = p.int_("DATABASE_POOL_MIN_SIZE", "2")
    _pool_max = p.int_("DATABASE_POOL_MAX_SIZE", "20")
    if _pool_min < 0:
        raise EnvValidationError(
            f"DATABASE_POOL_MIN_SIZE must be >= 0; got {_pool_min}"
        )
    if _pool_max <= 0:
        raise EnvValidationError(
            f"DATABASE_POOL_MAX_SIZE must be > 0; got {_pool_max}"
        )
    if _pool_min > _pool_max:
        raise EnvValidationError(
            f"DATABASE_POOL_MIN_SIZE ({_pool_min}) must be <= "
            f"DATABASE_POOL_MAX_SIZE ({_pool_max})"
        )
    database = DatabaseConfig(
        dsn=p.string(
            "DATABASE_URL",
            "postgresql://bot:bot@localhost:5432/polymarket_copytrading_python",
        ),
        pool_min_size=_pool_min,
        pool_max_size=_pool_max,
        command_timeout_seconds=p.float_("DATABASE_COMMAND_TIMEOUT_SECONDS", "30.0"),
    )

    # Wallet
    wallet = WalletConfig(
        private_key=p.private_key(),
        chain_id=p.int_("CHAIN_ID", "137"),
        funder_address=p.funder_address(),
        polygon_rpc_url=p.string("RPC_URL", "https://polygon-bor-rpc.publicnode.com"),
        signature_type=SignatureType(p.int_("SIGNATURE_TYPE", "0")),
    )

    # CLOB
    clob = ClobConfig(
        http_url=p.string("CLOB_HTTP_URL", "https://clob.polymarket.com/"),
        api_key=p.string("CLOB_API_KEY", ""),
        clob_v2_ready=p.bool_("CLOB_V2_READY", False),
    )

    # Polling / cadence
    polling = PollingConfig(
        fetch_interval=p.float_("FETCH_INTERVAL", "0.25"),
        aggregate_signal_window_seconds=p.float_("AGGREGATE_SIGNAL_WINDOW_SECONDS", "0.1"),
        risk_check_interval=p.float_("RISK_CHECK_INTERVAL", "15"),
        discovery_refresh_interval_seconds=p.int_("DISCOVERY_REFRESH_INTERVAL_SECONDS", "300"),
        leader_positions_min_fetch_spacing_ms=p.int_("LEADER_POSITIONS_MIN_FETCH_SPACING_MS", "250"),
        leader_activity_min_fetch_spacing_ms=p.int_("LEADER_ACTIVITY_MIN_FETCH_SPACING_MS", "250"),
        reconcile_interval_seconds=p.int_("RECONCILE_INTERVAL_SECONDS", "300"),
        retention_max_age_seconds=p.float_("RETENTION_MAX_AGE_SECONDS", str(30 * 86400)),
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
        trailing_activation_pct=p.float_or_auto("TRAILING_ACTIVATION_PCT", "20"),
        trailing_distance=p.float_("TRAILING_DISTANCE", "0.05"),
        staged_tp_levels=p.string("STAGED_TP_LEVELS", "30,50"),
        staged_tp_fractions=p.string("STAGED_TP_FRACTIONS", "0.25,0.35"),
        stop_loss_threshold=p.float_("STOP_LOSS_THRESHOLD", "0.15"),
        stop_loss_grace_period_seconds=p.float_("STOP_LOSS_GRACE_PERIOD_SECONDS", "180"),
        # PR #105 review: typed cap for the per-process risk price cache. The
        # legacy adapter (RiskPriceCache.__init__) still reads the env var
        # directly when no explicit value is passed; this typed value is the
        # production wiring path used when AppConfig is in scope.
        risk_price_cache_max_size=p.int_("RISK_PRICE_CACHE_MAX_SIZE", "5000"),
    )

    # P0-2: live-trading master switch — fail-closed. The bot runs in PAPER mode
    # unless LIVE_TRADING is EXPLICITLY enabled; a missing / unknown / malformed
    # value resolves to paper. The legacy PAPER_TRADE_ONLY flag can additionally
    # force paper but can never enable live. This is the single source for
    # paper_trade_only / dry_run, so live trading requires an affirmative opt-in
    # (LIVE_TRADING=enabled) — there is no path where missing config goes live.
    _live_switch = p._strip("LIVE_TRADING", p.raw("LIVE_TRADING", "")).strip().lower()
    live_trading_enabled = _live_switch in ("enabled", "true", "1", "yes", "on")
    paper_trade_only_effective = (
        p.bool_("PAPER_TRADE_ONLY", False) or not live_trading_enabled
    )

    # Execution
    execution = ExecutionConfig(
        slippage_tolerance=p.float_("SLIPPAGE_TOLERANCE", "2.0"),
        min_share_price=p.float_("MIN_SHARE_PRICE", "0.03"),
        max_share_price=p.float_("MAX_SHARE_PRICE", "0"),
        max_retries=p.int_("MAX_RETRIES", "3"),
        non_terminal_failure_max_attempts=p.int_("NON_TERMINAL_FAILURE_MAX_ATTEMPTS", "3"),
        paper_trade_only=paper_trade_only_effective,
        enable_copy_decision_gate=p.bool_("ENABLE_COPY_DECISION_GATE", False),
        enable_near_resolution_gate=p.bool_("ENABLE_NEAR_RESOLUTION_GATE", False),
        near_resolution_gate_price=p.float_("NEAR_RESOLUTION_GATE_PRICE", "0.95"),
        near_resolution_max_age_seconds=p.float_("NEAR_RESOLUTION_MAX_AGE_SECONDS", "15"),
        pending_submission_ttl_seconds=p.int_("PENDING_SUBMISSION_TTL_SECONDS", "120"),
        reservation_ttl_seconds=p.int_("RESERVATION_TTL_SECONDS", "120"),
        reservations_enabled=p.bool_("RESERVATIONS_ENABLED", False),
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
        # Phase 4 — balance regime. ``0`` disables a knob.
        max_position_size_usdc=p.float_("MAX_POSITION_SIZE_USDC", "0"),
        max_daily_spend_usdc=p.float_("MAX_DAILY_SPEND_USDC", "0"),
        min_balance_headroom_usdc=p.float_("MIN_BALANCE_HEADROOM_USDC", "0"),
        # Phase 4 — book depth gate. ``MIN_BOOK_DEPTH_USDC=0`` disables.
        min_book_depth_usdc=p.float_("MIN_BOOK_DEPTH_USDC", "0"),
        book_depth_band_pct=p.float_("BOOK_DEPTH_BAND_PCT", "5"),
        execution_submit_concurrency=p.int_("EXECUTION_SUBMIT_CONCURRENCY", "2"),
    )

    # Leader / discovery cascades — reproduce the same cascading defaults the legacy
    # ENV snapshot uses so services relying on these derived flags behave identically.
    leader_source = p.string("LEADER_SOURCE", "manual").lower()
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
    auto_discover_max_leaders_raw = p.int_("AUTO_DISCOVER_MAX_LEADERS", "10")
    auto_leader_count_override = _optional_int(p, "AUTO_LEADER_COUNT")
    leader_auto_count_override = _optional_int(p, "LEADER_AUTO_COUNT")
    auto_discover_top_n = (
        auto_leader_count_override
        if auto_leader_count_override is not None
        else leader_auto_count_override
        if leader_auto_count_override is not None
        else legacy_auto_count
    )

    auto_leader_enabled_raw = p.bool_("AUTO_LEADER_ENABLED", False)
    allow_auto_trader_detection_raw = p.bool_("ALLOW_AUTO_TRADER_DETECTION", False)
    allow_auto_trader_detection = (
        leader_source == "auto"
        or auto_leader_enabled_raw
        or allow_auto_trader_detection_raw
    )
    max_auto_traders_override = _optional_int(p, "MAX_AUTO_TRADERS_COUNT")
    auto_leader_count_raw = (
        auto_leader_count_override
        if auto_leader_count_override is not None
        else max_auto_traders_override
        if max_auto_traders_override is not None
        else leader_auto_count_override
        if leader_auto_count_override is not None
        else 2
    )
    auto_traders_cap = max(0, auto_discover_max_leaders_raw)
    max_auto_traders_count = max(0, min(auto_leader_count_raw, auto_traders_cap))

    leader = LeaderConfig(
        wallets=p.leader_wallets(
            allow_user_address_fallback=not allow_auto_trader_detection
        ),
        leader_auto=leader_auto,
        leader_auto_count=auto_discover_top_n,
        auto_discover_leaders=auto_discover_leaders,
        auto_discover_max_leaders=max(
            auto_discover_max_leaders_raw, auto_discover_top_n
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
        fallback_rpc_url=p.string("FALLBACK_RPC_URL", ""),
        chain_id=wallet.chain_id,
        usdc_contract_address=(
            p.string("COLLATERAL_CONTRACT_ADDRESS", "")
            or p.string("PUSD_CONTRACT_ADDRESS", POLYMARKET_PUSD_ADDRESS)
            or p.string("USDC_CONTRACT_ADDRESS", POLYGON_USDCE_ADDRESS)
        ),
        enforce_geoblock=p.bool_("ENFORCE_GEOBLOCK", True),
        enable_metrics_server=p.bool_("ENABLE_METRICS_SERVER", False),
        metrics_port=p.int_("METRICS_PORT", "9102"),
        metrics_bind_host=p.string("METRICS_BIND_HOST", "127.0.0.1"),
        collateral_contract_address=p.string("COLLATERAL_CONTRACT_ADDRESS", ""),
        pusd_contract_address=p.string("PUSD_CONTRACT_ADDRESS", POLYMARKET_PUSD_ADDRESS),
        ctf_exchange_v2_address=p.string(
            "CTF_EXCHANGE_V2_ADDRESS",
            POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
        ),
    )

    reconciliation = ReconciliationConfig(
        stale_threshold_seconds=p.float_("RECONCILE_STALE_SECONDS", "3600"),
        auto_cancel_stale=p.bool_("RECONCILE_AUTO_CANCEL", False),
        max_closed_probes=p.int_("RECONCILE_MAX_CLOSED_PROBES", "10"),
    )

    position_drift = PositionDriftConfig(
        check_on_startup=p.bool_("POSITION_DRIFT_CHECK_ON_STARTUP", True),
        fail_closed=p.bool_("POSITION_DRIFT_FAIL_CLOSED", True),
        allow_auto_prune_zero=p.bool_("POSITION_DRIFT_ALLOW_AUTO_PRUNE_ZERO", True),
        allow_external_position_tracking=p.bool_(
            "POSITION_DRIFT_ALLOW_EXTERNAL_POSITION_TRACKING", False
        ),
        pause_scope_on_unresolved=p.bool_(
            "POSITION_DRIFT_PAUSE_SCOPE_ON_UNRESOLVED", True
        ),
        max_size_epsilon=Decimal(
            p.string("POSITION_DRIFT_MAX_SIZE_EPSILON", "0.000001")
        ),
        prune_holdoff_seconds=p.float_("POSITION_DRIFT_PRUNE_HOLDOFF_SECONDS", "60.0"),
    )

    order_confirmation = OrderConfirmationConfig(
        initial_delay_seconds=p.float_("ORDER_CONFIRMATION_INITIAL_DELAY_SECONDS", "2"),
        poll_interval_seconds=p.float_("ORDER_CONFIRMATION_POLL_INTERVAL_SECONDS", "3"),
        max_wait_seconds=p.float_("ORDER_CONFIRMATION_MAX_WAIT_SECONDS", "90"),
        delayed_extra_seconds=p.float_("ORDER_CONFIRMATION_DELAYED_EXTRA_SECONDS", "2"),
        token_delay_overrides=p.json_dict_float(
            "ORDER_CONFIRMATION_TOKEN_DELAY_OVERRIDES", "{}"
        ),
        condition_delay_overrides=p.json_dict_float(
            "ORDER_CONFIRMATION_CONDITION_DELAY_OVERRIDES", "{}"
        ),
        auto_release_ambiguous=p.bool_("ORDER_CONFIRMATION_AUTO_RELEASE_AMBIGUOUS", True),
        auto_release_grace_seconds=p.float_(
            "ORDER_CONFIRMATION_AUTO_RELEASE_GRACE_SECONDS", "300"
        ),
        auto_release_require_all_probes_clean=p.bool_(
            "ORDER_CONFIRMATION_AUTO_RELEASE_REQUIRE_ALL_PROBES_CLEAN", True
        ),
        auto_release_max_probe_errors=p.int_(
            "ORDER_CONFIRMATION_AUTO_RELEASE_MAX_PROBE_ERRORS", "0"
        ),
    )

    redemption_collateral = (
        p.string("REDEEM_COLLATERAL_CONTRACT_ADDRESS", "")
        or network.pusd_contract_address
        or network.usdc_contract_address
    )
    redemption = RedemptionConfig(
        enable_auto_redeem=p.bool_("ENABLE_AUTO_REDEEM", False),
        check_interval=p.int_("REDEEM_CHECK_INTERVAL", "60"),
        resolved_high=Decimal(p.string("REDEEM_RESOLVED_HIGH", "0.99")),
        resolved_low=Decimal(p.string("REDEEM_RESOLVED_LOW", "0.01")),
        price_epsilon=Decimal(p.string("REDEEM_PRICE_EPSILON", "0.000001")),
        zero_threshold=Decimal(p.string("REDEEM_ZERO_THRESHOLD", "0.0001")),
        ctf_contract_address=p.string(
            "REDEEM_CTF_CONTRACT_ADDRESS",
            p.string(
                "CTF_COLLATERAL_ADAPTER_ADDRESS",
                POLYMARKET_CTF_COLLATERAL_ADAPTER_ADDRESS,
            ),
        ),
        collateral_contract_address=redemption_collateral,
        gas_limit=p.int_("REDEEM_GAS_LIMIT", "500000"),
        gas_price_buffer_pct=p.int_("REDEEM_GAS_PRICE_BUFFER_PCT", "20"),
        receipt_timeout_seconds=p.int_("REDEEM_RECEIPT_TIMEOUT_SECONDS", "120"),
        max_conditions_per_tick=p.int_("REDEEM_MAX_CONDITIONS_PER_TICK", "5"),
        require_signer_match=p.bool_("REDEEM_REQUIRE_SIGNER_MATCH", True),
    )

    if allow_auto_trader_detection and max_auto_traders_count <= 0:
        logger.warning(
            "auto_trader_detection_config_invalid",
            extra={
                "allow_auto_trader_detection": True,
                "max_auto_traders_count": max_auto_traders_count,
                "effect": "auto detection cannot select leaders",
            },
        )

    discovery = DiscoveryConfig(
        enable_trader_discovery=enable_trader_discovery,
        discovery_interval_seconds=p.int_("DISCOVERY_INTERVAL_SECONDS", "3600"),
        discovery_promote_to_active=discovery_promote_to_active,
        leader_source=leader_source,
        auto_leader_enabled=allow_auto_trader_detection,
        auto_leader_count=max_auto_traders_count,
        allow_auto_trader_detection=allow_auto_trader_detection,
        max_auto_traders_count=max_auto_traders_count,
        min_leader_pnl=p.float_("MIN_LEADER_PNL", "1000"),
        min_leader_win_rate=p.float_("MIN_LEADER_WIN_RATE", "0.55"),
        min_leader_positions=p.int_("MIN_LEADER_POSITIONS", "20"),
        min_active_positions=p.int_("MIN_ACTIVE_POSITIONS", "1"),
        discovery_max_results=p.int_("DISCOVERY_MAX_RESULTS", "50"),
        discovery_cache_ttl_seconds=p.float_("DISCOVERY_CACHE_TTL_SECONDS", "3600"),
        multi_user_config_file=p.string("MULTI_USER_CONFIG_FILE", ""),
        falcon_api_url=p.string(
            "FALCON_API_URL",
            "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized",
        ),
        falcon_api_token=p.string("FALCON_API_TOKEN", ""),
        falcon_page_size=p.int_("FALCON_PAGE_SIZE", "100"),
        falcon_max_pages=p.int_("FALCON_MAX_PAGES", "5"),
        falcon_sort_modes=_csv_tuple(
            p, "FALCON_SORT_MODES", "h_score,pnl,sharpe,roi,win_rate"
        ),
        falcon_dedupe_candidates=p.bool_("FALCON_DEDUPE_CANDIDATES", True),
        falcon_max_candidates=p.int_("FALCON_MAX_CANDIDATES", "250"),
        falcon_min_win_rate_15d=p.float_("FALCON_MIN_WIN_RATE_15D", "0.45"),
        falcon_max_win_rate_15d=p.float_("FALCON_MAX_WIN_RATE_15D", "0.90"),
        falcon_min_roi_15d=p.float_("FALCON_MIN_ROI_15D", "0"),
        falcon_min_pnl_15d=p.float_("FALCON_MIN_PNL_15D", "100"),
        falcon_min_trades_15d=p.int_("FALCON_MIN_TRADES_15D", "30"),
        falcon_max_trades_15d=p.int_("FALCON_MAX_TRADES_15D", "750"),
        polymarket_history_lookback_days=p.int_("POLYMARKET_HISTORY_LOOKBACK_DAYS", "365"),
        polymarket_history_window_days=p.int_("POLYMARKET_HISTORY_WINDOW_DAYS", "7"),
        polymarket_history_min_window_seconds=p.int_(
            "POLYMARKET_HISTORY_MIN_WINDOW_SECONDS", "21600"
        ),
        polymarket_activity_max_offset=p.int_("POLYMARKET_ACTIVITY_MAX_OFFSET", "10000"),
        discovery_history_trade_limit=p.int_("DISCOVERY_HISTORY_TRADE_LIMIT", "50000"),
        discovery_signal_group_window_seconds=p.int_("DISCOVERY_SIGNAL_GROUP_WINDOW_SECONDS", "60"),
        discovery_signal_group_max_gap_seconds=p.int_("DISCOVERY_SIGNAL_GROUP_MAX_GAP_SECONDS", "120"),
        discovery_min_group_notional_usdc=p.float_("DISCOVERY_MIN_GROUP_NOTIONAL_USDC", "5"),
        discovery_min_copyable_grouped_signal_ratio=p.float_(
            "DISCOVERY_MIN_COPYABLE_GROUPED_SIGNAL_RATIO", "0.75"
        ),
        discovery_min_copyable_notional_ratio=p.float_(
            "DISCOVERY_MIN_COPYABLE_NOTIONAL_RATIO", "0.60"
        ),
        discovery_min_auto_sports_esports_ratio=p.float_(
            "DISCOVERY_MIN_AUTO_SPORTS_ESPORTS_RATIO", "0.75"
        ),
        discovery_auto_prefer_sports_esports=p.bool_(
            "DISCOVERY_AUTO_PREFER_SPORTS_ESPORTS", True
        ),
        discovery_evaluation_ttl_seconds=p.int_(
            "DISCOVERY_EVALUATION_TTL_SECONDS", "3600"
        ),
        discovery_reject_stale_evaluations=p.bool_(
            "DISCOVERY_REJECT_STALE_EVALUATIONS", True
        ),
        discovery_require_recent_state_for_live=p.bool_(
            "DISCOVERY_REQUIRE_RECENT_STATE_FOR_LIVE", True
        ),
        discovery_require_7d_non_negative=p.bool_("DISCOVERY_REQUIRE_7D_NON_NEGATIVE", True),
        discovery_min_7d_pnl_usdc=p.float_("DISCOVERY_MIN_7D_PNL_USDC", "0"),
        discovery_allow_3d_slightly_negative=p.bool_(
            "DISCOVERY_ALLOW_3D_SLIGHTLY_NEGATIVE", True
        ),
        discovery_min_3d_pnl_usdc=p.float_("DISCOVERY_MIN_3D_PNL_USDC", "-50"),
        discovery_min_3d_roi=p.float_("DISCOVERY_MIN_3D_ROI", "-0.05"),
        leader_score_version=p.string(
            "LEADER_SCORE_VERSION", "leader_score_v2"
        ).lower(),
        discovery_volume_target_15d=p.float_(
            "DISCOVERY_VOLUME_TARGET_15D", "10000"
        ),
        discovery_min_volume_15d=p.float_("DISCOVERY_MIN_VOLUME_15D", "0"),
        discovery_rank_target=p.int_("DISCOVERY_RANK_TARGET", "100"),
        discovery_risk_penalty_enabled=p.bool_(
            "DISCOVERY_RISK_PENALTY_ENABLED", True
        ),
        auto_leader_require_score_version=p.bool_(
            "AUTO_LEADER_REQUIRE_SCORE_VERSION", True
        ),
        discovery_crypto_fast_max_resolution_minutes=p.float_(
            "DISCOVERY_CRYPTO_FAST_MAX_RESOLUTION_MINUTES", "30"
        ),
        discovery_crypto_fast_reject_signal_ratio=p.float_(
            "DISCOVERY_CRYPTO_FAST_REJECT_SIGNAL_RATIO", "0.70"
        ),
        discovery_crypto_fast_reject_notional_ratio=p.float_(
            "DISCOVERY_CRYPTO_FAST_REJECT_NOTIONAL_RATIO", "0.70"
        ),
        discovery_crypto_fast_min_non_crypto_signals=p.int_(
            "DISCOVERY_CRYPTO_FAST_MIN_NON_CRYPTO_SIGNALS", "5"
        ),
        discovery_auto_write_leaders=p.bool_("DISCOVERY_AUTO_WRITE_LEADERS", False),
        discovery_dynamic_monitoring_enabled=p.bool_(
            "DISCOVERY_DYNAMIC_MONITORING_ENABLED", False
        ),
        discovery_market_specialists_allowed=p.bool_(
            "DISCOVERY_MARKET_SPECIALISTS_ALLOWED", True
        ),
        discovery_position_volatility_allowed=p.bool_(
            "DISCOVERY_POSITION_VOLATILITY_ALLOWED", True
        ),
        rotate_traders_env_file=p.string(
            "ROTATE_TRADERS_ENV_FILE", "config/runtime.local.env"
        ),
        rotate_traders_min_shadow_days=p.int_("ROTATE_TRADERS_MIN_SHADOW_DAYS", "3"),
        rotate_traders_min_shadow_signals=p.int_("ROTATE_TRADERS_MIN_SHADOW_SIGNALS", "20"),
        rotate_traders_replace_only_if_needed=p.bool_(
            "ROTATE_TRADERS_REPLACE_ONLY_IF_NEEDED", True
        ),
        rotate_traders_min_score_delta=p.float_("ROTATE_TRADERS_MIN_SCORE_DELTA", "0.10"),
        rotate_keep_live_if_7d_pnl_positive=p.bool_(
            "ROTATE_KEEP_LIVE_IF_7D_PNL_POSITIVE", True
        ),
        rotate_reject_live_if_7d_pnl_below=p.float_(
            "ROTATE_REJECT_LIVE_IF_7D_PNL_BELOW", "0"
        ),
        rotate_allow_manual_override=p.bool_("ROTATE_ALLOW_MANUAL_OVERRIDE", True),
    )

    features = FeatureFlagConfig(
        enable_profit_lock=p.bool_("ENABLE_PROFIT_LOCK", False),
        enable_near_max_exit=p.bool_("ENABLE_NEAR_MAX_EXIT", True),
        enable_trailing_stop=p.bool_("ENABLE_TRAILING_STOP", False),
        enable_staged_tp=p.bool_("ENABLE_STAGED_TP", False),
        enable_leader_divergence_exit=p.bool_("ENABLE_LEADER_DIVERGENCE_EXIT", False),
        enable_stop_loss=p.bool_("ENABLE_STOP_LOSS", False),
        enable_tier_gate_watchlist=p.bool_("ENABLE_TIER_GATE_WATCHLIST", False),
        enable_tier_gate_watchlist_log=p.bool_("ENABLE_TIER_GATE_WATCHLIST_LOG", False),
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
        reconciliation=reconciliation,
        order_confirmation=order_confirmation,
        position_drift=position_drift,
        discovery=discovery,
        features=features,
        market_data=MarketDataConfig(
            primary_source=p.string("MARKET_DATA_PRIMARY_SOURCE", "rest_snapshot").lower(),
            rest_snapshot_enable=p.bool_("MARKET_DATA_REST_SNAPSHOT_ENABLE", True),
            rest_snapshot_max_age_ms=p.int_("MARKET_DATA_REST_SNAPSHOT_MAX_AGE_MS", "500"),
            rest_snapshot_request_timeout_ms=p.int_("MARKET_DATA_REST_SNAPSHOT_REQUEST_TIMEOUT_MS", "750"),
            rest_snapshot_total_budget_ms=p.int_("MARKET_DATA_REST_SNAPSHOT_TOTAL_BUDGET_MS", "1000"),
            rest_snapshot_max_spread_bps=p.int_("MARKET_DATA_REST_SNAPSHOT_MAX_SPREAD_BPS", "300"),
            allow_rest_when_wss_down=p.bool_("MARKET_DATA_ALLOW_REST_WHEN_WSS_DOWN", False),
            wss_down_after_seconds=p.float_("MARKET_DATA_WSS_DOWN_AFTER_SECONDS", 15.0),
            rest_degraded_max_age_seconds=p.float_("MARKET_DATA_REST_DEGRADED_MAX_AGE_SECONDS", 2.0),
            rest_degraded_max_spread_bps=p.int_("MARKET_DATA_REST_DEGRADED_MAX_SPREAD_BPS", 500),
            rest_degraded_log_every_seconds=p.float_("MARKET_DATA_REST_DEGRADED_LOG_EVERY_SECONDS", 30.0),
            enable_rest_bootstrap=p.bool_("MARKET_DATA_ENABLE_REST_BOOTSTRAP", True),
            rest_bootstrap_max_age_ms=p.int_("MARKET_DATA_REST_BOOTSTRAP_MAX_AGE_MS", 500),
            rest_bootstrap_request_timeout_ms=p.int_("MARKET_DATA_REST_BOOTSTRAP_REQUEST_TIMEOUT_MS", 750),
            rest_bootstrap_total_budget_ms=p.int_("MARKET_DATA_REST_BOOTSTRAP_TOTAL_BUDGET_MS", 1000),
            rest_bootstrap_max_spread_bps=p.int_("MARKET_DATA_REST_BOOTSTRAP_MAX_SPREAD_BPS", 300),
            rest_bootstrap_require_wss_connected=p.bool_("MARKET_DATA_REST_BOOTSTRAP_REQUIRE_WSS_CONNECTED", True),
            executable_quote_max_age_seconds=p.float_("MARKET_DATA_EXECUTABLE_QUOTE_MAX_AGE_SECONDS", 2.0),
            executable_book_max_age_seconds=p.float_("MARKET_DATA_EXECUTABLE_BOOK_MAX_AGE_SECONDS", 2.0),
            executable_tick_max_age_seconds=p.float_("MARKET_DATA_EXECUTABLE_TICK_MAX_AGE_SECONDS", 300.0),
            wss_connection_max_silence_seconds=p.float_("MARKET_DATA_WSS_CONNECTION_MAX_SILENCE_SECONDS", 20.0),
        ),
        # P0-2: single source — derived from the LIVE_TRADING master switch.
        dry_run=paper_trade_only_effective,
        raw=_strip_sensitive(env),
    )

    _validate_semantics(cfg, p.errors)

    if p.errors:
        raise EnvValidationError(
            "Invalid runtime environment:\n- " + "\n- ".join(p.errors)
        )

    return cfg
