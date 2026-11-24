"""Layered YAML, environment overlay, trader file, secrets.env, and PMCOPY__* config loader."""

from __future__ import annotations

import errno
import os
import re
import stat
import sys
from collections.abc import Mapping
from enum import Enum, auto
from io import StringIO
from pathlib import Path
from typing import Any

import orjson
import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, SecretStr, TypeAdapter, ValidationError, model_validator

from pmcopy.config.addresses import verified_polymarket_exchange_addresses
from pmcopy.config.env import EnvValidationError, WalletConfig
from pmcopy.config.evidence import EvidenceConfig, EvidenceSecretBoundary
from pmcopy.config.provider_budget import ProviderBudgetConfig
from pmcopy.config.redemption import RedemptionConfig, RedemptionSecretBoundary
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
    _ETH_ADDR_RE,
    _alchemy_key_from_url,
    logger,
)
from pmcopy.config.schemas_discovery import (
    AppConfig,
    DiscoveryConfig,
    MarketDataConfig,
)
from pmcopy.config.trader_config import (
    TraderConfig,
    validate_trader_filename_stem,
)
from pmcopy.config.validators import (
    _DETECTION_MODE_WARNINGS,
    AppConfigSemanticValidator,
    DetectionModeResolver,
)
from pmcopy.utils.packaged_resources import deployment_root, packaged_dir

# Service-owned layered configuration loader.
#
# ---------------------------------------------------------------------------


def packaged_config_dir() -> Path:
    """Return the packaged readonly ``config/`` tree via ``importlib.resources``.

    The shipped YAML defaults are readonly inputs: they are read from the
    installed distribution (wheel/sdist → site-packages, possibly read-only)
    and never written to. An explicit ``config_dir`` argument or a test
    monkeypatch of ``_CONFIG_ROOT`` overrides this directory.
    """
    return packaged_dir("pmcopy.config")


_CONFIG_ROOT = packaged_config_dir()
# Deployment-local layers are resolved against a deployment root, not the
# process CWD. The root is exactly: the ``PMCOPY_DEPLOYMENT_ROOT`` env var
# (when it names an existing directory), else the CWD. Parent directories
# are never walked — no credential discovery from arbitrary ancestors
# (Task 19 packaging hardening).
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
    "redemption",
    "provider_budget",
    "evidence",
)


def _resolve_deployment_root() -> Path:
    """Find the deployment root so local secrets/shim are not CWD-relative.

    Explicit and bounded: ``PMCOPY_DEPLOYMENT_ROOT`` (an existing directory)
    or the CWD. Ancestor directories are never searched — a leftover
    ``secrets.env`` in a parent must not leak into an unrelated process.
    """
    return deployment_root()


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
    redemption: RedemptionConfig = RedemptionConfig()
    provider_budget: ProviderBudgetConfig = ProviderBudgetConfig()
    evidence: EvidenceConfig = EvidenceConfig()

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
        if warning is not None:
            facade = sys.modules.get("pmcopy.config.app_config")
            emitter = getattr(facade, "_DETECTION_MODE_WARNINGS", _DETECTION_MODE_WARNINGS)
            emitter.emit(logger, warning)
        return {**values, "detection": {**(detection or {}), "mode": mode}}

    @model_validator(mode="before")
    @classmethod
    def _decode_nested_structures(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        structured_paths = (
            ("detection", "onchain", "active_exchange_addresses"),
            ("detection", "native", "traders"),
            ("copy_trading", "wallets"),
            ("copy_trading", "traders"),
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
    "ambiguous_recovery": ("reconciliation", "ambiguous_recovery"),
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

# The operator control plane is gone: the standalone bot is controlled through
# the local CLI (``bot trading`` / ``bot_cli``) and the health/metrics server.
# A stale ``control_plane`` section, ``control_plane.yml`` payload carried by a
# legacy shim, or ``PMCOPY__CONTROL_PLANE__*`` secret/environment key must be
# rejected (not silently dropped) so an operator cannot believe a remote
# control surface is still armed.
_OBSOLETE_CONFIG_SECTIONS = ("control_plane",)


def _reject_obsolete_sections(payload: Mapping[str, Any]) -> None:
    """Fail closed when an obsolete whole-section config surface appears.

    Covers every payload layer (overrides, legacy shim, secrets.env, process
    environment) after they are merged, because any of them can still carry a
    section that no longer has an owner.
    """

    for section in _OBSOLETE_CONFIG_SECTIONS:
        if section in payload:
            raise EnvValidationError(
                f"{section} configuration is obsolete. The operator control "
                "plane has been removed; the standalone bot is controlled "
                "through the local CLI and the health/metrics server. Delete "
                f"the {section} keys, control_plane.yml files, and "
                f"PMCOPY__{section.upper()}__* credentials."
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
    if source == "runtime.local.yml":
        RedemptionSecretBoundary.reject_yaml(payload)
        EvidenceSecretBoundary.reject_yaml(payload)
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
    RedemptionSecretBoundary.reject_yaml(payload)
    EvidenceSecretBoundary.reject_yaml(payload)
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


def _read_trader_configs(directory: Path) -> list[TraderConfig]:
    """Read canonical followed-trader files ``config/traders/<name>.yml``.

    Each file fully describes one followed leader: wallet identity, enabled
    state, per-leader copy gates, and the copy-task strategy. The file stem
    is the trader fantasy name and supplies ``name`` (and the default task id
    used by the v1 bootstrap) when the YAML omits it.
    """
    if not directory.is_dir():
        return []
    entries: list[TraderConfig] = []
    seen_wallets: dict[str, str] = {}
    seen_names: dict[str, str] = {}
    for file in sorted(directory.glob("*.yml")):
        stem = validate_trader_filename_stem(file.stem)
        data = _read_yaml(file, required=False)
        declared = str(data.get("name") or "").strip()
        data["name"] = declared or stem
        wallet = str(
            data.get("wallet") or data.get("wallet_id") or data.get("leader_wallet") or ""
        ).strip()
        # A trader file is normally named by fantasy name and declares its
        # wallet explicitly. A legacy address-named file may omit the wallet
        # field; the file stem is then the wallet address.
        if not wallet and _ETH_ADDR_RE.match(stem):
            wallet = stem
        data["wallet"] = wallet
        data.pop("wallet_id", None)
        data.pop("leader_wallet", None)
        if not _ETH_ADDR_RE.match(wallet):
            raise EnvValidationError(f"Trader file must declare a valid wallet address: {file}")
        try:
            trader = TraderConfig.model_validate(data)
        except ValidationError as exc:
            messages = _validation_messages(exc)
            raise EnvValidationError(
                f"Invalid trader configuration {file}:\n- " + "\n- ".join(messages)
            ) from exc

        name_key = trader.name.casefold()
        previous_name = seen_names.get(name_key)
        if previous_name is not None:
            raise EnvValidationError(
                f"Trader name {trader.name!r} is used by both "
                f"{previous_name!r} and {file.name!r}; trader names must be "
                "unique because they are the default copy-task ids."
            )
        seen_names[name_key] = file.name
        previous = seen_wallets.get(trader.wallet.lower())
        if previous is not None:
            raise EnvValidationError(
                f"Trader wallet {trader.wallet} is defined in both "
                f"{previous!r} and {file.name!r}; a followed leader may only "
                "be defined once."
            )
        seen_wallets[trader.wallet.lower()] = file.name
        entries.append(trader)
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


_FLAT_PMCOPY_ENV_ROUTES: dict[str, tuple[str, ...]] = {
    "PMCOPY__ALCHEMY_WSS_URL": ("detection", "onchain", "alchemy_wss_url"),
    "PMCOPY__INCLUDE_LEGACY_V1_EXCHANGES": (
        "detection",
        "onchain",
        "include_legacy_v1_exchanges",
    ),
    "PMCOPY__TRACK_DIRECT_USER_TXS": ("detection", "onchain", "track_direct_user_txs"),
    "PMCOPY__MAX_HOURLY_WS_MB": ("detection", "onchain", "max_hourly_ws_mb"),
}


def _read_secret_overrides(contents: str) -> dict[str, Any]:
    """Parse nested application secrets, rejecting the legacy PG key.

    ``PMCOPY_ENVIRONMENT`` is allowed through (it is consumed earlier by
    ``_resolve_environment``); the pre-split ``POSTGRES_PASSWORD`` is
    rejected with a migration hint pointing at db.env and docs/docker.md.
    """
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
        if key == "POSTGRES_PASSWORD":
            # T15 split: the database superuser password lives ONLY in
            # db.env (read by the compose postgres service; see
            # docs/docker.md). Toleration kept a stale copy here silently
            # diverging from the real one, so reject with the migration
            # path instead of skipping. Never echo the supplied value.
            raise EnvValidationError(
                "secrets.env no longer accepts POSTGRES_PASSWORD: the "
                "database password moved to db.env (postgres-only file); "
                "see docs/docker.md"
            )
        if key == "PMCOPY_ENVIRONMENT":
            # The documented environment selector, consumed before this
            # layer by _resolve_environment.
            continue
        if not key.startswith("PMCOPY__"):
            raise EnvValidationError(
                "secrets.env application keys must use PMCOPY__SECTION__FIELD; "
                f"unsupported key: {key}"
            )
        flat_route = _FLAT_PMCOPY_ENV_ROUTES.get(key)
        if flat_route is not None:
            path_parts = flat_route
        else:
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
        flat_route = _FLAT_PMCOPY_ENV_ROUTES.get(key)
        if flat_route is not None:
            path_parts = flat_route
        else:
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
    <service>.yml`` -> ``<config_dir>/traders/<name>.yml`` (canonical
    followed-leader definitions) -> legacy ``<config_dir>/leaders/
    <wallet>.yml`` -> ``overrides`` -> ``secrets.env`` -> process
    ``PMCOPY__*`` variables.

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
    facade = sys.modules.get("pmcopy.config.app_config")
    default_root = getattr(facade, "_CONFIG_ROOT", None) or _CONFIG_ROOT
    config_root = Path(config_dir) if config_dir else default_root
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

    traders = _read_trader_configs(config_root / "traders")
    leader_entries = _read_leader_overrides(config_root / "leaders")
    if traders:
        # Canonical config/traders/<name>.yml is the single source of truth
        # for followed leaders. Project its entries into the legacy
        # ``wallets`` and ``leaders`` shapes so existing consumers keep
        # working unchanged. Legacy leader files are kept only for wallets
        # that have not been migrated.
        trader_wallets = {trader.wallet.strip().lower() for trader in traders}
        legacy_leaders = [
            entry
            for entry in leader_entries
            if str(entry.get("wallet", "")).strip().lower() not in trader_wallets
        ]
        trader_leader_entries: list[dict[str, Any]] = []
        for trader in traders:
            trader_leader_entries.append(
                {
                    key: getattr(trader, key)
                    for key in (
                        "wallet",
                        "copy_decision_min_score",
                        "copy_max_price_drift_pct",
                        "copy_max_edge_decay_pct",
                        "copy_max_signal_age_seconds",
                        "copy_liquidity_target_usdc",
                        "max_entry_price_drift_pct",
                    )
                    if getattr(trader, key) is not None
                }
            )
        payload = _deep_merge(
            payload,
            {
                "copy_trading": {
                    "wallets": [
                        {
                            "wallet": trader.wallet,
                            "eoa_address": trader.eoa_address,
                            "proxy_address": trader.proxy_address,
                            "enabled": trader.enabled,
                        }
                        for trader in traders
                    ],
                    "leaders": trader_leader_entries + legacy_leaders,
                    "traders": [trader.model_dump(mode="python") for trader in traders],
                }
            },
        )
        logger.info(
            "config_traders_loaded | traders=%d legacy_leader_files=%d",
            len(traders),
            len(legacy_leaders),
        )
    elif leader_entries:
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
        _reject_obsolete_sections(payload)
        settings = _LayeredModel(**payload)
    except ValidationError as exc:
        messages = _validation_messages(exc)
        raise EnvValidationError(
            "Invalid application configuration:\n- " + "\n- ".join(messages)
        ) from exc

    validated = settings.model_dump()
    alchemy_wss = str(validated["detection"]["onchain"].get("alchemy_wss_url") or "").strip()
    alchemy_key = _alchemy_key_from_url(alchemy_wss, expected_scheme="wss")
    if alchemy_key is not None:
        derived_http_rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}"
        current_rpc = str(validated["polymarket"].get("rpc_url") or "").strip()
        if current_rpc in {"", "https://polygon-rpc.com"}:
            validated["polymarket"]["rpc_url"] = derived_http_rpc
    if not validated["detection"]["onchain"].get("active_exchange_addresses"):
        validated["detection"]["onchain"]["active_exchange_addresses"] = (
            verified_polymarket_exchange_addresses(
                include_legacy_v1=bool(
                    validated["detection"]["onchain"].get("include_legacy_v1_exchanges", False)
                )
            )
        )
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
