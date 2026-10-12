"""Schema v2 copiers roster — multi-copier configuration boundary (Task 22).

This module parses the operator-supplied ``app.copiers_config_file`` YAML
(addon §3 target shape) into the historical
``RuntimeUser``/``UserFollowConfig`` objects the runtime already consumes, so
multi-copier configuration is an option of the existing pipeline — not a
second one. Validation reuses the Task 14 strict primitives from
``services.multiUserConfig`` (``_parse_strict_bool``, ``_parse_money``,
``_validate_eth_address``, ``_reject_unknown_keys``,
``_reject_duplicate_users``): one set of strict rules, no forked copies.

Final exact syntax (``config/copiers.example.yml`` ships a runnable
secret-free template)::

    schema_version: 2
    copiers:
      - id: primary                  # [A-Za-z0-9][A-Za-z0-9._-]{0,63}; NOT "default"
        enabled: true                # strict bool ("true"/"false" text ok); absent = true
        mode: paper                  # paper|live; ALL copiers identical; == execution.trading_mode
        wallet:                      # env-var REFERENCES ONLY — never values
          funder_address_env: PRIMARY_FUNDER_ADDRESS    # required in paper; optional in live
          private_key_env: PRIMARY_PRIVATE_KEY          # optional in paper; required in live
        limits:                      # required; decimal text; finite; > 0
          max_order_quote: "5.00"            # per-order quote cap (per follow default)
          max_open_exposure_quote: "50.00"   # per-market exposure cap
        follows:                     # required (may be []); explicit whitelist
          - leader_address: "0x1111111111111111111111111111111111111111"
            enabled: true            # strict bool; absent = true
            sizing:                  # required; mode + exactly its amount field
              mode: fixed            # fixed|proportional
              quote_amount: "2.00"   # fixed only; must be <= limits.max_order_quote
              # mode: proportional
              # leader_share: "0.07" # REQUIRED for proportional; fraction of leader notional; 0 < x <= 1

Rules on top of the Task 14 contracts:

* **YAML discipline** — the document is parsed with PyYAML's ``BaseLoader``
  so EVERY scalar arrives as text and this module performs every conversion.
  No YAML float/bool resolution can smuggle a binary float or a truthy
  ``yes``/``on`` past the strict parsers; quoting scalars is therefore
  optional (``5.00`` and ``"5.00"`` are identical exact decimal text).
* **Mode** — one process-wide paper/live mode: every copier declares
  ``mode``; mixed modes across copiers are rejected, and a uniform mode
  disagreeing with the authoritative ``execution.trading_mode`` is
  rejected. Live is never inferred from credential presence.
* **Secrets** — wallet fields are environment variable NAMES validated
  against ``[A-Za-z_][A-Za-z0-9_]*`` (a pasted key or a value with spaces is
  a configuration error). References resolve at composition time via
  ``os.environ``; errors name the VARIABLE, never the value. Paper must not
  demand signing secrets: ``private_key_env`` is optional there and a
  declared-but-unset reference resolves empty instead of failing startup
  (paper also never parses key material). Live requires the private-key
  reference to resolve to a ``0x`` + 64-hex key. Resolved private keys are
  registered with the Task 5 redaction registry; nothing is ever logged.
* **Sizing semantics** — ``fixed`` maps to ``allocation_pct = 1`` (mirror
  the leader notional 1:1) with the per-order cap ``max_trade_usdc =
  quote_amount`` (which must not exceed ``limits.max_order_quote``);
  ``proportional`` maps ``leader_share`` (a bounded FRACTION, ``0 < x <= 1``)
  to ``allocation_pct`` with the copier ``max_order_quote`` as the per-order
  cap. Both keep ``max_open_exposure_quote`` as the per-market exposure cap.
  Per-follow sizing still passes the shared account-level reservation and
  risk checks; adding a leader never multiplies spendable balance.
* **Identity** — the copier ``id`` IS the persisted ``user_id``/ledger
  identity: renames create a new account, so ids must be stable.
  ``default`` is reserved for the legacy single-account fallback (Deposit
  Wallet binding re-keys it at runtime; a configured copier must never use
  it). Duplicate copier ids, duplicate resolved wallet identities
  (case-insensitive funder compare across DIFFERENT env names, non-empty
  private-key compare), and duplicate follow leaders per copier are hard
  errors via the shared Task 14 checks.
* **Disabled copiers** — ``enabled: false`` copiers are fully validated but
  not constructed (no lease, no runtime, no routing) and their secret
  references are not required to resolve. An operator who still needs
  protective exits and reconciliation for a wallet keeps it ``enabled``
  with an empty ``follows`` list instead.
"""

from __future__ import annotations

import os
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import SecretStr

from pmcopy.config.env import SignatureType, WalletConfig
from pmcopy.services.multiUserConfig import (
    RuntimeUser,
    UserFollowConfig,
    _parse_money,
    _parse_strict_bool,
    _reject_duplicate_users,
    _reject_unknown_keys,
    _resolve_confined_roster_path,
    _validate_eth_address,
)
from pmcopy.utils.log_redaction import register_secret_values

if TYPE_CHECKING:
    from pmcopy.config.app_config import AppConfig

_SCHEMA_VERSION = "2"

# Strict schema: anything outside these key sets is a typo or a future
# field this loader version does not understand — reject fail-closed.
_TOP_LEVEL_KEYS = frozenset({"schema_version", "copiers"})
_COPIER_KEYS = frozenset({"id", "enabled", "mode", "wallet", "limits", "follows"})
_WALLET_KEYS = frozenset({"funder_address_env", "private_key_env"})
_LIMIT_KEYS = frozenset({"max_order_quote", "max_open_exposure_quote"})
_FOLLOW_KEYS = frozenset({"leader_address", "enabled", "sizing"})
_SIZING_KEYS = frozenset({"mode", "quote_amount", "leader_share"})

_MODES = frozenset({"paper", "live"})

# Persisted account identity constraints: the id keys ledgers, leases, and
# log file names (log/pnl_<user_id>.jsonl), so it is a short safe token —
# never a path, never the reserved legacy fallback id "default".
_COPIER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_RESERVED_COPIER_IDS = frozenset({"default"})

# Environment variable NAME pattern (references only — never values).
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_HEX_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")

_SIZING_MODES = frozenset({"fixed", "proportional"})


def select_runtime_copiers(
    runtime_users: list[RuntimeUser], copier_id: str | None
) -> list[RuntimeUser]:
    """Filter the loaded roster to one copier for ``--copier`` selection.

    Selection limits BOTH routing and runtime ownership: the returned list
    is the complete set of accounts the process constructs, leases, and
    routes signals for. An unknown id is a hard error naming the available
    ids (never wallet data).
    """
    if copier_id is None:
        return runtime_users
    copier_id = copier_id.strip()
    matches = [user for user in runtime_users if user.user_id == copier_id]
    if not matches:
        available = sorted(user.user_id for user in runtime_users)
        raise ValueError(
            f"unknown --copier {copier_id!r}; configured copiers are "
            f"{available} (selection limits routing and runtime ownership, "
            "so it must match a configured copier id exactly)"
        )
    return matches


def _require_str(container: dict, key: str, *, where: str) -> str:
    value = container.get(key)
    # BaseLoader scalars are always strings; a missing/empty value arrives
    # as None/"" and is rejected as required.
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: field {key!r} is required")
    return value


def _validate_env_reference(value: Any, *, where: str, field_name: str) -> str:
    """Validate an env-var NAME (a reference — the value never passes here)."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: field {field_name!r} must be an environment variable name")
    name = value.strip()
    if not _ENV_NAME_RE.match(name):
        raise ValueError(
            f"{where}: field {field_name!r} must be an environment variable "
            f"NAME ([A-Za-z_][A-Za-z0-9_]*), not a value; got {name!r}"
        )
    return name


def _resolve_env_reference(
    name: str,
    *,
    where: str,
    field_name: str,
    required: bool,
    environ: dict[str, str],
) -> str:
    """Resolve one env reference at composition time.

    Errors name the VARIABLE, never the resolved value. An optional
    reference that is unset resolves empty (paper must not demand signing
    secrets).
    """
    raw = environ.get(name)
    value = str(raw).strip() if raw is not None else ""
    if required and not value:
        raise ValueError(
            f"{where}: environment variable {name!r} (from {field_name!r}) "
            "is not set; it is required by this copier's mode"
        )
    return value


def _parse_limits(payload: Any, *, where: str) -> tuple[Decimal, Decimal]:
    if not isinstance(payload, dict):
        raise ValueError(
            f"{where}: field 'limits' must be a mapping of decimal-text "
            f"quote caps, got {type(payload).__name__}"
        )
    _reject_unknown_keys(payload, _LIMIT_KEYS, where=f"{where}: limits")
    max_order = _parse_money(
        _require_str(payload, "max_order_quote", where=f"{where}: limits"),
        where=f"{where}: limits",
        field_name="max_order_quote",
    )
    max_exposure = _parse_money(
        _require_str(payload, "max_open_exposure_quote", where=f"{where}: limits"),
        where=f"{where}: limits",
        field_name="max_open_exposure_quote",
    )
    return max_order, max_exposure


def _parse_sizing(
    payload: Any,
    *,
    where: str,
    max_order_quote: Decimal,
) -> tuple[Decimal, Decimal]:
    """Return ``(allocation_pct, max_trade_usdc)`` for one follow.

    ``fixed`` mirrors the leader notional 1:1 capped at ``quote_amount``
    (which may not exceed the copier per-order cap); ``proportional`` copies
    a bounded fraction of leader notional with the copier cap as the order
    ceiling.
    """
    if not isinstance(payload, dict):
        raise ValueError(
            f"{where}: field 'sizing' must be a mapping with a 'mode', "
            f"got {type(payload).__name__}"
        )
    _reject_unknown_keys(payload, _SIZING_KEYS, where=f"{where}: sizing")
    mode_value = _require_str(payload, "mode", where=f"{where}: sizing")
    mode = mode_value.strip().lower()
    if mode not in _SIZING_MODES:
        raise ValueError(
            f"{where}: sizing field 'mode' must be one of "
            f"{sorted(_SIZING_MODES)}, got {mode_value!r}"
        )
    if mode == "fixed":
        if "leader_share" in payload:
            raise ValueError(
                f"{where}: sizing field 'leader_share' is only valid with "
                "mode 'proportional'; fixed sizing uses 'quote_amount'"
            )
        quote_amount = _parse_money(
            _require_str(payload, "quote_amount", where=f"{where}: sizing"),
            where=f"{where}: sizing",
            field_name="quote_amount",
        )
        if quote_amount > max_order_quote:
            raise ValueError(
                f"{where}: sizing field 'quote_amount' must be <= the "
                f"copier limits.max_order_quote ({max_order_quote}); got "
                f"{quote_amount} — a per-follow order budget may not exceed "
                "the account-level per-order cap"
            )
        return Decimal("1"), quote_amount
    # proportional
    if "quote_amount" in payload:
        raise ValueError(
            f"{where}: sizing field 'quote_amount' is only valid with mode "
            "'fixed'; proportional sizing uses 'leader_share' (a fraction of "
            "leader notional)"
        )
    # Schema v2 keeps sizing fully explicit: a proportional follow must
    # state its fraction — no implicit global default (bounded: a FRACTION
    # of leader notional, 0 < x <= 1, so "0.07" = 7%).
    leader_share = _parse_money(
        _require_str(payload, "leader_share", where=f"{where}: sizing"),
        where=f"{where}: sizing",
        field_name="leader_share",
        maximum=Decimal("1"),
    )
    return leader_share, max_order_quote


def _parse_follows(
    payload: Any,
    *,
    user_id: str,
    where: str,
    max_order_quote: Decimal,
    max_open_exposure_quote: Decimal,
) -> dict[str, UserFollowConfig]:
    if not isinstance(payload, list):
        raise ValueError(
            f"{where}: field 'follows' must be a list of follow objects "
            f"(an explicitly empty list is the documented empty whitelist), "
            f"got {type(payload).__name__}"
        )
    follows: dict[str, UserFollowConfig] = {}
    seen_leaders: dict[str, int] = {}
    for index, item in enumerate(payload):
        entry_where = f"{where}: follows[{index}]"
        if not isinstance(item, dict):
            raise ValueError(
                f"{entry_where}: each follow must be a mapping, "
                f"got {type(item).__name__}"
            )
        _reject_unknown_keys(item, _FOLLOW_KEYS, where=entry_where)
        leader_address = _validate_eth_address(
            item.get("leader_address"),
            where=entry_where,
            field_name="leader_address",
            required=True,
        )
        leader_key = leader_address.lower()
        if leader_key in seen_leaders:
            raise ValueError(
                f"{where}: duplicate leader_address {leader_address!r} in "
                f"follows (entries {seen_leaders[leader_key]} and {index}); "
                "a leader may appear at most once per copier"
            )
        seen_leaders[leader_key] = index
        allocation_pct, max_trade_usdc = _parse_sizing(
            item.get("sizing"),
            where=entry_where,
            max_order_quote=max_order_quote,
        )
        follows[leader_address] = UserFollowConfig(
            user_id=user_id,
            leader_id=leader_address,
            enabled=_parse_strict_bool(
                item.get("enabled", True), where=entry_where, field_name="enabled"
            ),
            allocation_pct=allocation_pct,
            max_trade_usdc=max_trade_usdc,
            max_market_exposure_usdc=max_open_exposure_quote,
        )
    return follows


def _copier_runtime_user(
    payload: Any,
    *,
    config: "AppConfig",
    index: int,
    trading_mode: str,
    environ: dict[str, str],
) -> RuntimeUser | None:
    """Parse one copier entry; ``None`` for a validated-but-disabled copier."""
    if not isinstance(payload, dict):
        raise ValueError(
            f"copiers[{index}]: each copier must be a mapping, "
            f"got {type(payload).__name__}"
        )
    raw_id = payload.get("id")
    provisional = raw_id.strip() if isinstance(raw_id, str) else f"copiers[{index}]"
    label = f"copiers[{index}] ({provisional})"
    _reject_unknown_keys(payload, _COPIER_KEYS, where=label)

    copier_id = _require_str(payload, "id", where=label)
    if not _COPIER_ID_RE.match(copier_id):
        raise ValueError(
            f"{label}: field 'id' must match [A-Za-z0-9][A-Za-z0-9._-]{{0,63}} "
            f"(it is the persisted account identity and appears in account "
            f"file names), got {copier_id!r}"
        )
    if copier_id in _RESERVED_COPIER_IDS:
        raise ValueError(
            f"{label}: copier id {copier_id!r} is reserved for the legacy "
            "single-account fallback (Deposit Wallet binding re-keys it at "
            "runtime); choose a different stable id"
        )
    label = f"copiers[{index}] ({copier_id})"

    enabled = _parse_strict_bool(
        payload.get("enabled", True), where=label, field_name="enabled"
    )

    mode_value = _require_str(payload, "mode", where=label)
    mode = mode_value.strip().lower()
    if mode not in _MODES:
        raise ValueError(
            f"{label}: field 'mode' must be one of {sorted(_MODES)}, "
            f"got {mode_value!r}"
        )
    if mode != trading_mode:
        raise ValueError(
            f"{label}: field 'mode' is {mode!r} but the process-wide "
            f"trading mode is {trading_mode!r}; mixed paper/live copiers are "
            "rejected — every copier must declare the same mode as "
            "execution.trading_mode"
        )

    wallet_payload = payload.get("wallet")
    if not isinstance(wallet_payload, dict):
        raise ValueError(
            f"{label}: field 'wallet' must be a mapping of env-var "
            f"references, got {type(wallet_payload).__name__}"
        )
    _reject_unknown_keys(wallet_payload, _WALLET_KEYS, where=f"{label}: wallet")

    live = mode == "live"
    funder_env = ""
    if str(wallet_payload.get("funder_address_env", "") or "").strip():
        funder_env = _validate_env_reference(
            wallet_payload.get("funder_address_env"),
            where=f"{label}: wallet",
            field_name="funder_address_env",
        )
    if not live and not funder_env:
        raise ValueError(
            f"{label}: wallet field 'funder_address_env' is required in "
            "paper mode (the resolved Deposit Wallet address is the "
            "account identity for balance/position probes; live mode "
            "derives it from the private key instead)"
        )
    key_env = ""
    if str(wallet_payload.get("private_key_env", "") or "").strip():
        key_env = _validate_env_reference(
            wallet_payload.get("private_key_env"),
            where=f"{label}: wallet",
            field_name="private_key_env",
        )
    if live and not key_env:
        raise ValueError(
            f"{label}: wallet field 'private_key_env' is required in live "
            "mode (an env-var NAME referencing the signing key)"
        )

    max_order_quote, max_open_exposure_quote = _parse_limits(
        payload.get("limits"), where=label
    )
    follows = _parse_follows(
        payload.get("follows"),
        user_id=copier_id,
        where=label,
        max_order_quote=max_order_quote,
        max_open_exposure_quote=max_open_exposure_quote,
    )

    # A disabled copier is fully validated above but NOT constructed: no
    # lease, no runtime, no routing — and its secret references are not
    # demanded. An operator who still needs protective exits/reconciliation
    # for the wallet keeps it enabled with an empty follows list instead.
    if not enabled:
        return None

    funder_address = ""
    if funder_env:
        funder_address = _resolve_env_reference(
            funder_env,
            where=label,
            field_name="funder_address_env",
            required=True,
            environ=environ,
        )
        funder_address = _validate_eth_address(
            funder_address,
            where=label,
            field_name=f"funder_address_env value (from {funder_env!r})",
            required=True,
        )
    private_key_value = ""
    if key_env:
        private_key_value = _resolve_env_reference(
            key_env,
            where=label,
            field_name="private_key_env",
            # Paper must not demand signing secrets: a declared optional
            # reference that is unset resolves empty instead of failing.
            required=live,
            environ=environ,
        )
    if live and not _HEX_PRIVATE_KEY_RE.match(private_key_value):
        # Names the variable and the expected shape — never the value.
        raise ValueError(
            f"{label}: environment variable {key_env!r} (from "
            "'private_key_env') must resolve to a 66-char hex key "
            "(0x + 64 hex digits) for live mode"
        )
    if private_key_value:
        # Composition-time registration for value-based redaction (Task 5).
        # Never logged here; the registry only records the value for masking.
        register_secret_values(private_key_value)

    wallet = WalletConfig(
        private_key=SecretStr(private_key_value),
        chain_id=config.wallet.chain_id,
        signature_type=SignatureType.DEPOSIT_WALLET,
        funder_address=funder_address,
        polygon_rpc_url=config.polymarket.rpc_url,
        fallback_rpc_url=config.polymarket.fallback_rpc_url,
    )
    return RuntimeUser(
        user_id=copier_id,
        wallet=wallet,
        follows=follows,
        # A schema v2 follows list is explicit operator policy — even when
        # empty. Whitelist semantics apply; no legacy union.
        follows_explicit=True,
    )


def load_runtime_users_from_copiers(
    copiers_file: str, config: "AppConfig"
) -> list[RuntimeUser]:
    """Load the schema v2 copiers roster as ``RuntimeUser`` rows.

    Raises ``ValueError`` (fail-closed, secret-safe messages) on any schema,
    identity, mode, or secret-reference violation.
    """
    resolved = _resolve_confined_roster_path(
        copiers_file,
        setting="app.copiers_config_file",
        suffixes=(".yml", ".yaml"),
    )
    try:
        # BaseLoader: every scalar arrives as text so this module performs
        # every conversion through the strict Task 14 parsers. No YAML
        # float/bool resolution can bypass decimal-text or strict-bool rules.
        payload = yaml.load(
            resolved.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
        )
    except yaml.YAMLError as exc:
        raise ValueError(
            f"app.copiers_config_file {resolved.name!r} is not valid YAML: "
            f"{exc}"
        ) from exc
    except OSError as exc:
        raise ValueError(
            f"app.copiers_config_file could not be read: {resolved} "
            f"({exc.__class__.__name__})"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            "app.copiers_config_file payload must be a YAML mapping with a "
            f"'schema_version' and a 'copiers' list; got {type(payload).__name__}"
        )
    _reject_unknown_keys(payload, _TOP_LEVEL_KEYS, where="copiers config payload")
    version = _require_str(payload, "schema_version", where="copiers config payload")
    if version.strip() != _SCHEMA_VERSION:
        raise ValueError(
            "copiers config payload: field 'schema_version' must be exactly "
            f"{_SCHEMA_VERSION}, got {version.strip()!r}"
        )
    if "copiers" not in payload:
        raise ValueError(
            "copiers config payload must contain a top-level 'copiers' list "
            "(an explicitly empty list is valid; a missing one is not)"
        )
    copiers_payload = payload["copiers"]
    if not isinstance(copiers_payload, list):
        raise ValueError(
            "copiers config payload 'copiers' must be a list, "
            f"got {type(copiers_payload).__name__}"
        )

    # Process-wide single mode: the authoritative control stays
    # execution.trading_mode; every copier must declare exactly it.
    trading_mode = str(config.execution.trading_mode or "paper").strip().lower()
    environ = dict(os.environ)

    users = [
        user
        for user in (
            _copier_runtime_user(
                entry,
                config=config,
                index=index,
                trading_mode=trading_mode,
                environ=environ,
            )
            for index, entry in enumerate(copiers_payload)
        )
        if user is not None
    ]
    # Shared Task 14 identity checks: duplicate ids, duplicate resolved
    # funders (case-insensitive), duplicate non-empty private keys.
    _reject_duplicate_users(users)
    return users


__all__ = [
    "load_runtime_users_from_copiers",
    "select_runtime_copiers",
]
