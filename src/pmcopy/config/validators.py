"""Fail-closed semantic validation and detection-mode resolution for AppConfig."""

from __future__ import annotations

import logging
import sys as _sys
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from pmcopy.config.addresses import ALL_VERIFIED_EXCHANGE_ADDRESSES
from pmcopy.config.env import EnvValidationError, secret_value
from pmcopy.config.schemas_core import (
    _DETECTION_MODE_VALUES,
    _ETH_ADDR_RE,
    _HEX_PRIVATE_KEY_RE,
    _ONCHAIN_DETECTION_CONFIDENCE_VALUES,
    _PENDING_MODE_CONFIDENCE_VALUES,
    _alchemy_key_from_url,
)
from pmcopy.config.schemas_discovery import AppConfig
from pmcopy.domain.detection import DetectionMode

# Factory & validation.
# ---------------------------------------------------------------------------


# Keep version checks behind a local proxy so tests can monkeypatch this module
# without mutating the real process-wide ``sys.version_info`` object.
sys = SimpleNamespace(version_info=_sys.version_info)


def _runtime_python_supported() -> bool:
    """Return whether the current interpreter matches repository support."""

    # Matches ``requires-python = ">=3.13,<3.14"`` in pyproject.toml: the
    # project targets Python 3.13 only (3.11-3.12 are unsupported, and 3.14 is
    # deliberately rejected too — the target line is 3.13.x).
    return sys.version_info.major == 3 and sys.version_info.minor == 13


_MAX_ENTRY_PRICE_DRIFT_CEILING = 100.0


def _validate_entry_price_drift(value: float, label: str, errors: list[str]) -> None:
    """Validate one copy-entry FAK band, at any configuration layer.

    The band is operator-tunable per leader, so this is deliberately a wide
    sanity bound rather than the former hard 5-10 window: a leader that sweeps
    the book needs a wider band than one that posts passively. It still fails
    closed on a non-positive band (which could never cross) and on a value
    beyond ``_MAX_ENTRY_PRICE_DRIFT_CEILING`` (almost certainly a typo, and on
    a binary market a 100% band is already unbounded).

    Args:
        value: The configured drift percentage.
        label: Operator-facing name of the layer being validated.
        errors: Accumulator the caller raises from.
    """
    if value <= 0:
        errors.append(f"{label} must be > 0.")
    elif value > _MAX_ENTRY_PRICE_DRIFT_CEILING:
        errors.append(f"{label} must be <= {_MAX_ENTRY_PRICE_DRIFT_CEILING:g}.")


def _validate_semantics(cfg: AppConfig, errors: list[str]) -> None:
    """Cross-field validation applied after parsing.

    Appends human-readable messages to *errors* so they can be folded into the
    single aggregated ``EnvValidationError``.
    """

    if not _runtime_python_supported():
        errors.append(
            f"Unsupported Python runtime {sys.version_info.major}.{sys.version_info.minor}. "
            "Use Python 3.13.x."
        )

    # PostgreSQL is mandatory for idempotency, reconciliation, and roster state.
    if not cfg.database.dsn:
        errors.append("database.dsn is required.")
    elif not cfg.database.dsn.startswith(("postgresql://", "postgres://")):
        errors.append("database.dsn must use postgresql:// or postgres://.")

    if cfg.execution.sell_fraction_missing_policy not in ("skip", "full_close"):
        errors.append("execution.sell_fraction_missing_policy must be 'skip' or 'full_close'.")

    # Wallet essentials
    #
    # Task 22 — with a schema v2 copiers roster the per-copier
    # ``wallet.*_env`` references are the only wallet credential source, so
    # the SINGLE-account requirements below are waived (the roster loader
    # enforces its own per-copier rules, including live secret presence).
    # The two roster files are mutually exclusive: a v1 JSON users file and
    # a v2 copiers YAML together is an ambiguous configuration, never a
    # merge.
    copiers_file = bool((getattr(cfg.app, "copiers_config_file", "") or "").strip())
    if copiers_file and bool((cfg.app.multi_user_config_file or "").strip()):
        errors.append(
            "app.copiers_config_file and app.multi_user_config_file are both "
            "set; exactly one account roster may be configured (the schema "
            "v2 copiers file is authoritative on its own)."
        )
    private_key = secret_value(cfg.wallet.private_key)
    if not cfg.execution.paper_trade_only and not private_key and not copiers_file:
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
    if cfg.execution.paper_trade_only and not cfg.wallet.funder_address and not copiers_file:
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
    elif _alchemy_key_from_url(cfg.polymarket.rpc_url, expected_scheme="https") is None:
        errors.append(
            "polymarket.rpc_url must be a valid Alchemy Polygon HTTPS URL "
            "(https://polygon-mainnet.g.alchemy.com/v2/<key>); non-Alchemy RPC providers "
            "are not supported."
        )
    if (
        cfg.polymarket.fallback_rpc_url
        and _alchemy_key_from_url(cfg.polymarket.fallback_rpc_url, expected_scheme="https") is None
    ):
        errors.append(
            "polymarket.fallback_rpc_url must be a valid Alchemy Polygon HTTPS URL when set."
        )
    for entry in cfg.copy_trading.wallets:
        wallet = entry.wallet
        if not _ETH_ADDR_RE.match(wallet):
            errors.append(f'Invalid leader wallet "{wallet}" (expected 42-char Ethereum address).')
        if entry.eoa_address and not _ETH_ADDR_RE.match(entry.eoa_address):
            errors.append(
                f'Invalid leader eoa_address "{entry.eoa_address}" '
                "(expected 42-char Ethereum address)."
            )
        if entry.proxy_address and not _ETH_ADDR_RE.match(entry.proxy_address):
            errors.append(
                f'Invalid leader proxy_address "{entry.proxy_address}" '
                "(expected 42-char Ethereum address)."
            )
    for trader in cfg.copy_trading.traders:
        label = f"trader {trader.name} ({trader.wallet})"
        if trader.copy_decision_min_score is not None and not (
            0 <= trader.copy_decision_min_score <= 1
        ):
            errors.append(f"{label}: copy_decision_min_score must be in [0, 1].")
        if trader.copy_max_price_drift_pct is not None and trader.copy_max_price_drift_pct < 0:
            errors.append(f"{label}: copy_max_price_drift_pct must be non-negative.")
        if trader.copy_max_edge_decay_pct is not None and trader.copy_max_edge_decay_pct < 0:
            errors.append(f"{label}: copy_max_edge_decay_pct must be non-negative.")
        if (
            trader.copy_max_signal_age_seconds is not None
            and trader.copy_max_signal_age_seconds < 0
        ):
            errors.append(f"{label}: copy_max_signal_age_seconds must be non-negative.")
        if trader.copy_liquidity_target_usdc is not None and trader.copy_liquidity_target_usdc <= 0:
            errors.append(f"{label}: copy_liquidity_target_usdc must be > 0.")

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
    _validate_entry_price_drift(e.max_entry_price_drift_pct, "MAX_ENTRY_PRICE_DRIFT_PCT", errors)
    if cfg.copy_trading.max_entry_price_drift_pct is not None:
        _validate_entry_price_drift(
            cfg.copy_trading.max_entry_price_drift_pct,
            "COPY_TRADING.MAX_ENTRY_PRICE_DRIFT_PCT",
            errors,
        )
    for leader in cfg.copy_trading.leaders:
        if leader.max_entry_price_drift_pct is not None:
            _validate_entry_price_drift(
                leader.max_entry_price_drift_pct,
                f"LEADERS.{leader.wallet}.MAX_ENTRY_PRICE_DRIFT_PCT",
                errors,
            )
    if cfg.reconciliation.order_confirmation.auto_release_baseline_missing_after_seconds < 0:
        errors.append("AUTO_RELEASE_BASELINE_MISSING_AFTER_SECONDS must be >= 0.")
    ambiguous_recovery = cfg.reconciliation.ambiguous_recovery
    if ambiguous_recovery.interval_seconds <= 0:
        errors.append("AMBIGUOUS_RECOVERY.INTERVAL_SECONDS must be > 0.")
    if ambiguous_recovery.grace_seconds < 0:
        errors.append("AMBIGUOUS_RECOVERY.GRACE_SECONDS must be >= 0.")
    if not 1 <= ambiguous_recovery.required_absence_scans <= 100:
        errors.append("AMBIGUOUS_RECOVERY.REQUIRED_ABSENCE_SCANS must be between 1 and 100.")
    if not 1 <= ambiguous_recovery.batch_size <= 1000:
        errors.append("AMBIGUOUS_RECOVERY.BATCH_SIZE must be between 1 and 1000.")
    if not 1 <= ambiguous_recovery.evidence_history_limit <= 100:
        errors.append("AMBIGUOUS_RECOVERY.EVIDENCE_HISTORY_LIMIT must be between 1 and 100.")
    if e.venue_metadata_ttl_seconds < 0:
        errors.append("VENUE_METADATA_TTL_SECONDS must be >= 0.")
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


def _validate_detection_mode(cfg: AppConfig, errors: list[str]) -> None:
    """Fail closed unless ``detection.mode`` and Alchemy WSS settings are strictly valid."""

    detection = cfg.detection
    oc = detection.onchain
    try:
        parsed_mode = DetectionMode.parse_config_mode(detection.mode)
    except ValueError:
        allowed = ", ".join(_DETECTION_MODE_VALUES)
        errors.append(
            f"detection.mode must be one of: {allowed} "
            "(or alias on_chain_confirmed+mempool_reserved)."
        )
        return

    if _alchemy_key_from_url(oc.alchemy_wss_url, expected_scheme="wss") is None:
        errors.append(
            "detection.onchain.alchemy_wss_url (PMCOPY__ALCHEMY_WSS_URL) must be set to a "
            "valid Alchemy Polygon WSS URL (wss://polygon-mainnet.g.alchemy.com/v2/<key>)."
        )

    exchanges = oc.resolved_exchange_addresses()
    if (
        not exchanges
        or len(exchanges) > 4
        or any(addr.lower() not in ALL_VERIFIED_EXCHANGE_ADDRESSES for addr in exchanges)
    ):
        errors.append(
            "detection.onchain.active_exchange_addresses must be non-empty and contain at most "
            "4 verified Polymarket Exchange addresses."
        )

    max_hourly_mb = Decimal(str(oc.max_hourly_ws_mb))
    if not max_hourly_mb.is_finite() or max_hourly_mb <= Decimal("0"):
        errors.append("detection.onchain.max_hourly_ws_mb (PMCOPY__MAX_HOURLY_WS_MB) must be > 0.")

    if float(oc.mempool_reservation_ttl_seconds) <= 0:
        errors.append("detection.onchain.mempool_reservation_ttl_seconds must be > 0.")
    if float(oc.subscription_debounce_seconds) < 0:
        errors.append("detection.onchain.subscription_debounce_seconds must be >= 0.")

    provider = str(oc.pending_tx_provider or "alchemy").strip().lower()
    if provider != "alchemy":
        errors.append("detection.onchain.pending_tx_provider must be alchemy.")

    if parsed_mode in {DetectionMode.ON_CHAIN_PENDING, DetectionMode.CONFIRMED_MEMPOOL_RESERVED}:
        if bool(oc.pending_tx_hashes_only):
            errors.append(
                f"detection.onchain.pending_tx_hashes_only must be false for "
                f"detection.mode={parsed_mode.value} (calldata is required)."
            )

    if parsed_mode is DetectionMode.ON_CHAIN_PENDING:
        if int(oc.pending_tx_max_signal_age_ms) <= 0:
            errors.append(
                "detection.onchain.pending_tx_max_signal_age_ms must be > 0 for "
                "detection.mode=on_chain_pending."
            )
        confidence = str(oc.pending_tx_detection_confidence_required or "").strip().lower()
        if (
            confidence in _ONCHAIN_DETECTION_CONFIDENCE_VALUES
            and confidence not in _PENDING_MODE_CONFIDENCE_VALUES
        ):
            allowed = " or ".join(_PENDING_MODE_CONFIDENCE_VALUES)
            errors.append(
                f"detection.onchain.pending_tx_detection_confidence_required must be {allowed} "
                "when detection.mode=on_chain_pending: a mempool sighting only ever reaches "
                "pending_probable, so a higher floor drops every pending signal."
            )
        if float(oc.pending_copy_confirmation_deadline_seconds) <= 0:
            errors.append(
                "detection.onchain.pending_copy_confirmation_deadline_seconds must be > 0 for "
                "detection.mode=on_chain_pending."
            )
        if int(oc.pending_copy_confirmation_max_tracked) <= 0:
            errors.append(
                "detection.onchain.pending_copy_confirmation_max_tracked must be > 0 for "
                "detection.mode=on_chain_pending."
            )


@dataclass(frozen=True)
class DetectionModeWarning:
    """Operator warning produced by ``DetectionModeResolver``.

    Kept as a logging template plus its ``%s`` arguments so it is rendered
    lazily by the logging framework (repo convention) and can be
    de-duplicated per process by ``DetectionModeWarningEmitter``.

    Attributes:
        template: ``%s``-style logging template.
        args: Positional arguments for ``template``.
    """

    template: str
    args: tuple[str, ...]

    def render(self) -> str:
        """Return the fully interpolated message (tests / diagnostics)."""

        return self.template % self.args


class DetectionModeWarningEmitter:
    """Emit each distinct ``DetectionModeWarning`` at most once per process.

    The resolver runs on every ``_LayeredModel`` construction (config reloads
    included); without this guard the same deprecation line would repeat on
    each reload.
    """

    def __init__(self) -> None:
        """Start with no emitted warnings."""

        self._emitted: set[tuple[str, tuple[str, ...]]] = set()

    def emit(self, log: logging.Logger, warning: DetectionModeWarning) -> None:
        """Log ``warning`` through ``log`` unless an identical one was logged.

        Args:
            log: Logger receiving the lazily formatted WARN record.
            warning: The resolver warning to emit.
        """

        key = (warning.template, warning.args)
        if key in self._emitted:
            return
        self._emitted.add(key)
        log.warning(warning.template, *warning.args)


_DETECTION_MODE_WARNINGS = DetectionModeWarningEmitter()


class DetectionModeResolver:
    """Resolve the raw ``detection`` payload onto one canonical detection mode."""

    _LEGACY_FLAGS = ("enable_onchain_pending_detection", "enable_onchain_mined_detection")

    def resolve(self, payload: Mapping[str, Any] | None) -> tuple[str, DetectionModeWarning | None]:
        """Return ``(mode, warning)`` for one raw ``detection`` section."""

        section: Mapping[str, Any] = payload if isinstance(payload, Mapping) else {}
        explicit = str(section.get("mode") or "").strip().lower()
        if explicit == "on_chain_confirmed+mempool_reserved":
            explicit = DetectionMode.CONFIRMED_MEMPOOL_RESERVED.value
        legacy_enabled = self._legacy_flags_enabled(section)
        if explicit:
            if legacy_enabled:
                return explicit, DetectionModeWarning(
                    template=(
                        "detection_legacy_flags_ignored | mode=%s ignored=%s — "
                        "detection.onchain.enable_onchain_* booleans are deprecated; "
                        "detection.mode selects the source"
                    ),
                    args=(explicit, ",".join(legacy_enabled)),
                )
            return explicit, None
        if legacy_enabled:
            derived = (
                DetectionMode.ON_CHAIN_PENDING.value
                if "enable_onchain_pending_detection" in legacy_enabled
                else DetectionMode.ON_CHAIN_LOGS_ONLY.value
            )
            return derived, DetectionModeWarning(
                template=(
                    "detection_legacy_flags_deprecated | derived_mode=%s from=%s "
                    "replace_with=detection.mode"
                ),
                args=(derived, ",".join(legacy_enabled)),
            )
        return DetectionMode.ON_CHAIN_LOGS_ONLY.value, None

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


