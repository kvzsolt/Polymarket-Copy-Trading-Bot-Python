"""Core typed frozen configuration dataclasses (database, execution, risk, trading)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, model_validator

from pmcopy.config.native_detection import NativeDetectionConfig
from pmcopy.config.trader_config import TraderConfig, TraderTaskConfig
from pmcopy.config.v1_runtime import CopyTasksRuntimeConfig
from pmcopy.domain.detection import DetectionMode

_ETH_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HEX_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_DETECTION_MODE_VALUES = tuple(mode.value for mode in DetectionMode)
_ONCHAIN_DETECTION_CONFIDENCE_VALUES = {
    "market_unattributed",
    "pending_probable",
    "mined_authoritative",
    "confirmed_final",
}
#: Confidence floors a mempool sighting can actually clear. ``PendingTxDetector``
#: labels every pending signal ``PENDING_PROBABLE`` and
#: ``OnchainDetectionRuntime`` drops anything ranked below the configured floor,
#: so ``mined_authoritative``/``confirmed_final`` in ``detection.mode=
#: on_chain_pending`` is a mode that detects nothing at all.
_PENDING_MODE_CONFIDENCE_VALUES = ("market_unattributed", "pending_probable")
logger = logging.getLogger("AppConfig")

from pmcopy.config.addresses import (  # noqa: E402
    verified_polymarket_exchange_addresses,
)


def _alchemy_key_from_url(raw_url: object, *, expected_scheme: str) -> str | None:
    """Extract the Alchemy API key from a strict Polygon URL, or return ``None``."""
    value = str(raw_url or "").strip()
    if not value or any(ch.isspace() for ch in value):
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if (
        parsed.scheme != expected_scheme
        or parsed.hostname != "polygon-mainnet.g.alchemy.com"
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or bool(parsed.query)
        or bool(parsed.fragment)
        or not parsed.path.startswith("/v2/")
    ):
        return None
    api_key = parsed.path[len("/v2/") :]
    if not api_key or "/" in api_key:
        return None
    if not all(ch.isascii() and (ch.isalnum() or ch in "-_") for ch in api_key):
        return None
    return api_key


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
    # Task 22 — schema v2 copiers roster (YAML). When set it is the
    # AUTHORITATIVE account roster: per-copier ``*_env`` secret references
    # replace the single-account ``wallet`` section, and combining it with
    # ``multi_user_config_file`` is a configuration error. Empty = disabled.
    copiers_config_file: str = ""


@dataclass(frozen=True)
class OnchainDetectionConfig:
    """Alchemy-only on-chain trader detection policy and WSS cost controls."""

    alchemy_wss_url: str = ""
    include_legacy_v1_exchanges: bool = False
    track_direct_user_txs: bool = False
    max_hourly_ws_mb: Decimal = Decimal("250")
    active_exchange_addresses: tuple[str, ...] = ()
    mempool_reservation_ttl_seconds: float = 15.0
    subscription_debounce_seconds: float = 2.0
    # Retained compatibility fields for legacy config/test layers.
    enable_onchain_pending_detection: bool = False
    enable_onchain_mined_detection: bool = False
    confirmed_source: Literal["mempool_reserved", "logs_only"] = "mempool_reserved"
    pending_tx_provider: str = "alchemy"
    onchain_ws_url: str = ""
    pending_tx_ws_url: str = ""
    pending_tx_hashes_only: bool = False
    pending_tx_max_signal_age_ms: int = 500
    pending_tx_detection_confidence_required: str = "pending_probable"
    pending_copy_confirmation_enabled: bool = True
    pending_copy_confirmation_deadline_seconds: float = 60.0
    pending_copy_confirmation_max_tracked: int = 2048
    pending_copy_auto_unwind_enabled: bool = False

    def resolved_exchange_addresses(self) -> tuple[str, ...]:
        """Return lowercase active Polymarket Exchange contract addresses."""
        if self.active_exchange_addresses:
            return tuple(
                dict.fromkeys(
                    str(addr).strip().lower()
                    for addr in self.active_exchange_addresses
                    if str(addr).strip()
                )
            )
        return verified_polymarket_exchange_addresses(
            include_legacy_v1=bool(self.include_legacy_v1_exchanges)
        )


@dataclass(frozen=True)
class DetectionConfig:
    """Alchemy-only leader detection configuration and WSS bandwidth cost controls."""

    fetch_interval: float = 0.50
    aggregate_signal_window_seconds: float = 4.0
    leader_positions_min_fetch_spacing_ms: int = 250
    leader_activity_min_fetch_spacing_ms: int = 250
    trade_queue_max_batches: int = 64
    mode: str = "on_chain_logs_only"
    onchain: "OnchainDetectionConfig" = field(default_factory=lambda: OnchainDetectionConfig())
    native: NativeDetectionConfig = field(default_factory=NativeDetectionConfig)

    def __post_init__(self) -> None:
        normalized = str(self.mode or "").strip().lower()
        if normalized == "on_chain_confirmed+mempool_reserved":
            object.__setattr__(self, "mode", DetectionMode.CONFIRMED_MEMPOOL_RESERVED.value)
        elif normalized and normalized != self.mode:
            object.__setattr__(self, "mode", normalized)

    @property
    def detection_mode(self) -> DetectionMode:
        """Return the typed detection source selected by ``mode``."""

        return DetectionMode.parse_config_mode(self.mode)


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
    # One-shot copy-entry tolerance and protective-exit band, in percent. This
    # is the default floor of a three-layer chain: a leader file or the
    # ``copy_trading`` section may override it, resolved by
    # ``AppConfig.entry_price_drift_pct_for_leader``.
    max_entry_price_drift_pct: float = 5.0
    # Seconds a venue metadata fetch (tick size, min order size, negative risk)
    # stays usable for signing. ``0`` refetches before every signed order, which
    # is the money-path default. Raising it trades a bounded staleness window
    # for the duplicate CLOB round trip that currently sits on the copy
    # critical path; tick size can move on a live market, so raise it knowingly.
    venue_metadata_ttl_seconds: float = 0.0
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
    """Per-leader override values for one followed wallet.

    Canonically these come from ``config/traders/<name>.yml``; the legacy
    ``config/leaders/<wallet>.yml`` shape remains readable for unmigrated
    deployments. Override files hold per-leader tunables only; the roster is
    defined by the trader files (projected into ``copy_trading.wallets``).
    ``None`` means "inherit the layered copy_trading value".
    """

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    wallet: str = ""
    copy_decision_min_score: float | None = None
    copy_max_price_drift_pct: float | None = None
    copy_max_edge_decay_pct: float | None = None
    copy_max_signal_age_seconds: float | None = None
    copy_liquidity_target_usdc: float | None = None
    # One-shot copy-entry FAK band for this leader alone. ``None`` inherits the
    # layered ``copy_trading`` value, which in turn inherits ``execution``.
    # Distinct from ``copy_max_price_drift_pct`` above: that one gates whether a
    # signal is admitted at all, this one widens the limit price we sign.
    max_entry_price_drift_pct: float | None = None


class LeaderWalletConfig(BaseModel):
    """Configure one operator-supplied leader wallet and its startup state.

    Legacy string entries remain readable and are interpreted as enabled. New
    YAML should use the explicit ``wallet``/``enabled`` mapping so operators
    can pause one leader without deleting its address.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    wallet: str
    eoa_address: str = ""
    proxy_address: str = ""
    enabled: bool = True

    @model_validator(mode="before")
    @classmethod
    def _upgrade_legacy_string(cls, value: Any) -> Any:
        """Convert one legacy wallet string into the explicit entry shape."""
        if isinstance(value, str):
            return {"wallet": value, "enabled": True}
        return value


@dataclass(frozen=True)
class LeaderCopySettings:
    """Effective copy tunables for one leader after override resolution."""

    copy_decision_min_score: float
    copy_max_price_drift_pct: float
    copy_max_edge_decay_pct: float
    copy_max_signal_age_seconds: float
    copy_liquidity_target_usdc: float


class CopyTradingConfig(BaseModel):
    """Copy-trading roster, gates, and the v1 copy-task engine settings.

    The roster is normally sourced from ``config/traders/<name>.yml`` files
    (one file per followed leader, fully describing that leader and its
    copy strategy). The legacy ``wallets`` and ``leaders`` fields remain for
    old deployments that have not migrated yet.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    wallets: tuple[LeaderWalletConfig, ...] = ()
    copy_decision_min_score: float = 0.15
    copy_max_price_drift_pct: float = 15.0
    copy_max_edge_decay_pct: float = 90.0
    copy_max_signal_age_seconds: float = 100.0
    copy_liquidity_target_usdc: float = 50.0
    # Roster-wide override for the copy-entry FAK band. ``None`` inherits
    # ``execution.max_entry_price_drift_pct``. Not the same knob as
    # ``copy_max_price_drift_pct`` above, which is a signal-admission gate.
    max_entry_price_drift_pct: float | None = None
    # Tier-gate observation flags (formerly features.enable_tier_gate_*).
    enable_tier_gate_watchlist: bool = False
    enable_tier_gate_watchlist_log: bool = False
    # v1 copy-task engine (formerly config/runtime_v1.yml). Paper-safe:
    # ``copy_tasks.enabled`` defaults to False.
    copy_tasks: CopyTasksRuntimeConfig = CopyTasksRuntimeConfig()
    # Per-leader overrides sourced from ``config/traders/<name>.yml``
    # (canonical) or legacy ``config/leaders/<wallet>.yml``. Trader-file
    # entries are placed first so they win when a wallet appears in both.
    leaders: tuple[LeaderOverrideConfig, ...] = ()
    # Canonical followed-trader definitions from ``config/traders/*.yml``.
    # Each entry fully describes one followed leader and its copy-task
    # strategy. The loader also projects these into ``wallets`` and
    # ``leaders`` so existing consumers keep working unchanged.
    traders: tuple[TraderConfig, ...] = ()

    def enabled_wallets(self) -> tuple[str, ...]:
        """Return trimmed wallet addresses enabled for manual startup.

        Returns:
            Enabled manual leader wallet addresses in configuration order.
        """
        return tuple(
            entry.wallet.strip() for entry in self.wallets if entry.enabled and entry.wallet.strip()
        )

    def leader_override(self, wallet: str) -> LeaderOverrideConfig | None:
        """Return the override entry for ``wallet`` (case-insensitive)."""

        normalized = str(wallet).strip().lower()
        for entry in self.leaders:
            if entry.wallet.strip().lower() == normalized:
                return entry
        return None

    def trader_for_wallet(self, wallet: str) -> TraderConfig | None:
        """Return the canonical trader file entry for ``wallet``."""

        normalized = str(wallet).strip().lower()
        for trader in self.traders:
            if trader.wallet.strip().lower() == normalized:
                return trader
        return None

    def trader_task_for_wallet(self, wallet: str) -> TraderTaskConfig | None:
        """Return the file-defined copy-task strategy for ``wallet``."""

        trader = self.trader_for_wallet(wallet)
        return None if trader is None else trader.task

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
    # Seconds after which an order whose only unclean probe is a
    # ``pre_submit_live_position_size`` that was never captured may still be
    # released. Every venue probe must be clean; this forgives only our own
    # bookkeeping gap. ``0`` disables the escape and holds the reservation
    # indefinitely, which is the historical behaviour.
    auto_release_baseline_missing_after_seconds: float = 0.0
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
class AmbiguousRecoveryConfig:
    """Bounded recovery policy for no-order-id ``submitted_unconfirmed`` rows.

    A lost POST response leaves a row with no venue identity. The bounded
    evidence scan resolves it either by recovering an authoritative venue
    order id or by proving repeated complete absence. Defaults match the
    recovery service thresholds exactly; the loader must never silently widen
    them.
    """

    enabled: bool = True
    # Cadence of the supervised recovery loop (seconds).
    interval_seconds: float = 300.0
    # Grace before the first private-history scan for a fresh row.
    grace_seconds: float = 120.0
    # Consecutive complete-absence scans required before resolution.
    required_absence_scans: int = 3
    batch_size: int = 100
    evidence_history_limit: int = 8
    # How long after ``submitted_at`` a fill/order may still be attributed.
    match_window_seconds: float = 900.0
    # Upper bound on every single venue/account read inside one scan.
    read_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("ambiguous recovery interval_seconds must be positive")
        if self.grace_seconds < 0:
            raise ValueError("ambiguous recovery grace_seconds cannot be negative")
        if not 1 <= self.required_absence_scans <= 100:
            raise ValueError("ambiguous recovery required_absence_scans must be between 1 and 100")
        if not 1 <= self.batch_size <= 1000:
            raise ValueError("ambiguous recovery batch_size must be between 1 and 1000")
        if not 1 <= self.evidence_history_limit <= 100:
            raise ValueError("ambiguous recovery evidence_history_limit must be between 1 and 100")
        if self.match_window_seconds < 0:
            raise ValueError("ambiguous recovery match_window_seconds cannot be negative")
        if self.read_timeout_seconds <= 0:
            raise ValueError("ambiguous recovery read_timeout_seconds must be positive")


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
    # Bounded no-order-id recovery for lost-POST rows.
    ambiguous_recovery: AmbiguousRecoveryConfig = field(default_factory=AmbiguousRecoveryConfig)

