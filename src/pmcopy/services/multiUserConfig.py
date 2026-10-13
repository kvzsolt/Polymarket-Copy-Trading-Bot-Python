"""Strict multi-user copier configuration boundary (A06).

This module parses the operator-supplied ``MULTI_USER_CONFIG_FILE`` JSON
payload and is the seed schema for the copier config v2 (Task 22). Inputs are
validated fail-closed; outputs keep the historical
``RuntimeUser``/``UserFollowConfig`` shape for consumers, with money carried
as exact ``Decimal`` end-to-end per the repository money rules.

Input contracts enforced here:

* **Booleans** — only JSON ``true``/``false`` or the exact strings
  ``"true"``/``"false"`` (case-insensitive, for env-templated files).
  Anything else (``1``/``0``, ``"yes"``, ``"1"``, ``null``) is rejected.
  ``enabled`` absent means enabled (documented legacy default).
* **Money** — ``allocation_pct`` / ``max_trade_usdc`` /
  ``max_market_exposure_usdc`` must be decimal TEXT (a JSON string like
  ``"12.5"``) or a ``Decimal``. JSON numbers are rejected: ``json`` parses
  them into binary floats (``int``-typed numbers are rejected too so the
  canonical format is one thing). Values must be finite, strictly positive,
  and bounded: ``allocation_pct`` is a FRACTION of leader notional
  (``0.07`` = 7%), so ``0 < allocation_pct <= 1``; the USDC caps must be
  ``> 0``. When a field is absent the trusted typed ``AppConfig`` default is
  used, converted to ``Decimal`` at that internal boundary.
* **Identity** — duplicate user ids, duplicate wallet identities
  (case-insensitive funder-address compare; private-key compare for live
  entries whose funder is derived later — the key identity compare strips
  surrounding whitespace so a padded copy-paste cannot pose as a second
  wallet, while the stored ``SecretStr`` keeps the value verbatim), and
  duplicate leader ids inside one user's follows are hard errors. Leader
  ids and funders are validated as Ethereum addresses (``0x`` + 40 hex),
  stripped; identity comparisons are case-insensitive while the stored
  spelling is preserved as written so whitelist matching against
  configured/discovered leader ids stays byte-identical.
* **Schema** — unknown keys at the top level, in user objects, and in
  follow objects are rejected (catching ``max_trade_usd``-style typos); a
  payload without a ``users`` list is rejected instead of silently loading
  zero users.
* **Secrets** — the sanctioned ``private_key`` user field aside (existing
  transport, held as ``SecretStr``), credential-shaped keys are rejected and
  no error message ever echoes a supplied secret value.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import SecretStr

from pmcopy.config.env import SignatureType, WalletConfig

if TYPE_CHECKING:
    from pmcopy.config.app_config import AppConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UserFollowConfig:
    user_id: str
    leader_id: str
    enabled: bool
    # A06 — exact-decimal money end-to-end. The strict loader parses decimal
    # text into Decimal; AppConfig-derived defaults are converted through
    # ``Decimal(str(...))`` at that trusted typed-config boundary.
    allocation_pct: Decimal
    max_trade_usdc: Decimal
    max_market_exposure_usdc: Decimal
    # Task 24 fix — True only when the COPIER-level open-exposure cap was
    # EXPLICITLY configured by the operator (schema v2
    # ``limits.max_open_exposure_quote``, or the v1 follow field present in
    # the payload). The trade pipeline enforces the cap only when this is
    # set; legacy/default-derived values keep their historical inert
    # behavior so no existing deployment sees new rejections.
    market_exposure_cap_explicit: bool = False


@dataclass
class RuntimeUser:
    user_id: str
    wallet: WalletConfig
    follows: dict[str, UserFollowConfig] = field(default_factory=dict)
    # A05 — True iff ``follows`` came from explicit operator configuration
    # (a MULTI_USER_CONFIG_FILE payload, even an explicitly empty follows
    # list). The config-less single-account fallback synthesizes follows
    # from the global leader list and leaves this False:
    # ``ensure_runtime_follows`` keys the legacy single-account default on
    # this flag — never on in-memory follows emptiness — so the config-less
    # lone account keeps auto-following newly shown leaders at every
    # refresh, exactly like the pre-whitelist behavior.
    follows_explicit: bool = False


_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Strict schema: anything outside these key sets is a typo or a future field
# that this loader version does not understand — reject instead of silently
# ignoring operator policy.
_TOP_LEVEL_KEYS = frozenset({"users"})
_USER_KEYS = frozenset(
    {"user_id", "private_key", "funder_address", "signature_type", "chain_id", "follows"}
)
_FOLLOW_KEYS = frozenset(
    {"leader_id", "enabled", "allocation_pct", "max_trade_usdc", "max_market_exposure_usdc"}
)

# Credential-shaped key detector (field-NAME fragments only; no secret
# values live in this module). ``private_key`` at the user level is the one
# sanctioned credential field — the existing wallet transport — and is held
# as ``SecretStr``. Any other credential-shaped key is rejected: inline
# secrets beyond the wallet key are not part of this configuration surface.
_CREDENTIAL_KEY_FRAGMENTS = (
    "private",
    "secret",
    "api",
    "token",
    "password",
    "credential",
    "auth",
    "key",
)


def _reject_credential_shaped_keys(keys: Any, *, where: str) -> None:
    """Fail closed on credential-shaped keys, never echoing any value."""
    for name in keys:
        lowered = str(name).lower()
        if any(fragment in lowered for fragment in _CREDENTIAL_KEY_FRAGMENTS):
            raise ValueError(
                f"{where}: key {name!r} looks credential-shaped; inline "
                "secret values are not accepted in this configuration "
                "surface (only the sanctioned 'private_key' user field "
                "carries wallet credentials, as a SecretStr)"
            )


def _reject_unknown_keys(payload: dict, allowed: frozenset[str], *, where: str) -> None:
    unknown = sorted(str(key) for key in payload if key not in allowed)
    if not unknown:
        return
    _reject_credential_shaped_keys(unknown, where=where)
    raise ValueError(
        f"{where}: unknown key(s) {unknown}; allowed keys are "
        f"{sorted(allowed)} (strict schema — typos are rejected, not ignored)"
    )


def _parse_strict_bool(value: Any, *, where: str, field_name: str) -> bool:
    """Only real bools or the exact strings true/false (case-insensitive)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise ValueError(
        f"{where}: field {field_name!r} must be a JSON boolean or the "
        f'string "true"/"false" (case-insensitive); got '
        f"{type(value).__name__}"
    )


def _parse_money(
    value: Any,
    *,
    where: str,
    field_name: str,
    maximum: Decimal | None = None,
) -> Decimal:
    """Parse a money value from decimal text (or Decimal) with bounds."""
    if isinstance(value, bool) or isinstance(value, (int, float)):
        raise ValueError(
            f"{where}: field {field_name!r} must be decimal TEXT (a JSON "
            f'string like "12.5"), not a JSON number ({type(value).__name__}); '
            "binary JSON numbers lose exact money semantics"
        )
    if not isinstance(value, (str, Decimal)):
        raise ValueError(
            f"{where}: field {field_name!r} must be decimal TEXT (a JSON "
            f'string like "12.5"); got {type(value).__name__}'
        )
    try:
        amount = Decimal(value.strip()) if isinstance(value, str) else value
    except (InvalidOperation, ValueError):
        raise ValueError(
            f"{where}: field {field_name!r} is not valid decimal text: {value!r}"
        ) from None
    if not amount.is_finite():
        raise ValueError(f"{where}: field {field_name!r} must be a finite decimal, got {value!r}")
    if amount <= 0:
        raise ValueError(f"{where}: field {field_name!r} must be > 0, got {value!r}")
    if maximum is not None and amount > maximum:
        raise ValueError(
            f"{where}: field {field_name!r} must be <= {maximum} "
            f'(it is a fraction of leader notional; "0.07" = 7%), got {value!r}'
        )
    return amount


def _decimal_from_config(value: Any) -> Decimal:
    """Convert a trusted typed-AppConfig number to exact Decimal.

    The AppConfig surface is float-typed and validated by its own layer;
    ``str()`` of a float is its shortest exact decimal text, so this is the
    lossless bridge for defaults. Operator-supplied payload values never
    pass through here — they must arrive as decimal text (see
    ``_parse_money``).
    """
    return Decimal(str(value))


def _validate_eth_address(value: Any, *, where: str, field_name: str, required: bool) -> str:
    text = str(value or "").strip()
    if not text:
        if required:
            raise ValueError(f"{where}: field {field_name!r} is required")
        return ""
    if not _ETH_ADDRESS_RE.match(text):
        # Final-review fix: rejections carry a length/shape hint ONLY. The
        # supplied value can be a pasted secret (an operator who puts a
        # private key into a funder-address env var reaches this guard with
        # the full key), and the error text flows to startup output, doctor
        # details, and ``pmcopy config validate`` stdout. Mirror
        # ``copierConfig._validate_env_reference``'s never-echoed contract.
        raise ValueError(
            f"{where}: field {field_name!r} must be an Ethereum address "
            f"(0x + 40 hex characters), got a {len(text)}-character value — "
            "supplied content never echoed"
        )
    return text


def _parse_signature_type(value) -> SignatureType:
    return SignatureType(int(value))


def _parse_follows(
    items: Any, *, user_id: str, label: str, config: "AppConfig"
) -> dict[str, UserFollowConfig]:
    """Parse one user's follows list with strict per-entry validation."""
    if items is None or not isinstance(items, list):
        raise ValueError(
            f"{label}: field 'follows' must be a list of follow objects, got {type(items).__name__}"
        )
    follows: dict[str, UserFollowConfig] = {}
    seen_leaders: dict[str, int] = {}
    for index, item in enumerate(items):
        where = f"{label}: follows[{index}]"
        if not isinstance(item, dict):
            raise ValueError(
                f"{where}: each follow must be a JSON object, got {type(item).__name__}"
            )
        _reject_unknown_keys(item, _FOLLOW_KEYS, where=where)
        leader_id = _validate_eth_address(
            item.get("leader_id"), where=where, field_name="leader_id", required=True
        )
        # Case-insensitive identity: the same wallet written twice (any
        # casing) is one leader — the second entry used to silently
        # overwrite the first.
        leader_key = leader_id.lower()
        if leader_key in seen_leaders:
            raise ValueError(
                f"{label}: duplicate leader_id {leader_id!r} in follows "
                f"(entries {seen_leaders[leader_key]} and {index}); a "
                "leader may appear at most once per user"
            )
        seen_leaders[leader_key] = index
        cfg = UserFollowConfig(
            user_id=user_id,
            leader_id=leader_id,
            # Absent → enabled (documented legacy default); present →
            # strictly bool or "true"/"false" text.
            enabled=_parse_strict_bool(
                item.get("enabled", True), where=where, field_name="enabled"
            ),
            allocation_pct=(
                _parse_money(
                    item["allocation_pct"],
                    where=where,
                    field_name="allocation_pct",
                    maximum=Decimal("1"),
                )
                if "allocation_pct" in item
                # Trusted typed-config default: PERCENTAGE (percent) →
                # fraction, exact decimal division.
                else _decimal_from_config(config.sizing.percentage) / Decimal("100")
            ),
            max_trade_usdc=(
                _parse_money(
                    item["max_trade_usdc"],
                    where=where,
                    field_name="max_trade_usdc",
                )
                if "max_trade_usdc" in item
                else _decimal_from_config(config.sizing.buy_max_amount)
            ),
            max_market_exposure_usdc=(
                _parse_money(
                    item["max_market_exposure_usdc"],
                    where=where,
                    field_name="max_market_exposure_usdc",
                )
                if "max_market_exposure_usdc" in item
                else _decimal_from_config(config.risk.max_market_exposure_usdc)
            ),
            # An operator-supplied v1 exposure field is explicit policy —
            # enforce it. The config-default branch stays inert (legacy).
            market_exposure_cap_explicit="max_market_exposure_usdc" in item,
        )
        follows[leader_id] = cfg
    return follows


def _runtime_user_from_payload(payload: Any, config: "AppConfig", *, index: int) -> RuntimeUser:
    if not isinstance(payload, dict):
        raise ValueError(
            f"multi-user config users[{index}]: each user must be a JSON "
            f"object, got {type(payload).__name__}"
        )
    user_id_raw = payload.get("user_id")
    provisional = user_id_raw.strip() if isinstance(user_id_raw, str) else f"users[{index}]"
    label = f"multi-user config users[{index}] ({provisional or f'users[{index}]'})"
    _reject_unknown_keys(payload, _USER_KEYS, where=label)
    if not isinstance(user_id_raw, str) or not user_id_raw.strip():
        raise ValueError(f"{label}: field 'user_id' is required and must be a non-empty string")
    user_id = user_id_raw.strip()
    label = f"multi-user config users[{index}] ({user_id})"

    private_key = payload.get("private_key")
    if not isinstance(private_key, str) or not private_key.strip():
        raise ValueError(
            f"{label}: field 'private_key' is required (the account's "
            "wallet key, held as a SecretStr)"
        )
    try:
        signature_value = int(payload.get("signature_type", 3))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label}: invalid signature_type: {exc}") from exc
    if signature_value != int(SignatureType.DEPOSIT_WALLET):
        raise ValueError(
            f"{label}: multi-user signature_type is no longer configurable; "
            "only Deposit Wallet/POLY_1271 type 3 is supported"
        )
    funder_address = _validate_eth_address(
        payload.get("funder_address", ""),
        where=label,
        field_name="funder_address",
        required=False,
    )
    # Mirrors the single-user AppConfig invariant. Paper never authenticates,
    # so no secure client resolves a Deposit Wallet and
    # ``bind_resolved_deposit_wallet`` never runs: the configured funder is
    # this entry's only wallet identity. Left empty, the balance and
    # live-position probes query "" and fail readiness with no actionable
    # error. Live may omit it — the authenticated SDK derives the address from
    # this entry's own private_key and binds it back.
    if config.execution.paper_trade_only and not funder_address:
        raise ValueError(
            f"{label}: entry {user_id!r} is missing funder_address, which is "
            "required in paper mode (it is the wallet identity used by the "
            "balance/live-position health probes; live mode derives it from "
            "the entry's private_key instead)"
        )
    chain_id_raw = payload.get("chain_id", config.wallet.chain_id)
    # bool is an int subclass: int(True) == 1 would silently select network 1
    # from a templated JSON true. Reject booleans explicitly before the
    # numeric coercion.
    if isinstance(chain_id_raw, bool):
        raise ValueError(
            f"{label}: field 'chain_id' must be a JSON number or decimal text, "
            "not a JSON boolean (true would coerce to chain id 1); got bool"
        )
    try:
        chain_id = int(chain_id_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label}: invalid chain_id: {exc}") from exc
    wallet = WalletConfig(
        private_key=SecretStr(private_key),
        chain_id=chain_id,
        signature_type=SignatureType.DEPOSIT_WALLET,
        funder_address=funder_address,
        polygon_rpc_url=config.polymarket.rpc_url,
        fallback_rpc_url=config.polymarket.fallback_rpc_url,
    )
    follows = _parse_follows(
        payload.get("follows", []), user_id=user_id, label=label, config=config
    )

    return RuntimeUser(
        user_id=user_id,
        wallet=wallet,
        follows=follows,
        # A follows list in the config payload is explicit operator policy —
        # even when empty. Whitelist semantics apply; no legacy union.
        follows_explicit=True,
    )


def _reject_duplicate_users(users: list[RuntimeUser]) -> None:
    """Reject duplicate copier ids and duplicate wallet identities."""
    seen_user_ids: dict[str, int] = {}
    seen_funders: dict[str, str] = {}
    seen_private_keys: dict[str, str] = {}
    for index, user in enumerate(users):
        previous_index = seen_user_ids.get(user.user_id)
        if previous_index is not None:
            raise ValueError(
                f"duplicate multi-user config user_id {user.user_id!r} "
                f"(entries {previous_index} and {index}); copier ids must "
                "be unique"
            )
        seen_user_ids[user.user_id] = index
        funder = user.wallet.funder_address
        if funder:
            funder_key = funder.lower()
            previous_funder = seen_funders.get(funder_key)
            if previous_funder is not None:
                raise ValueError(
                    f"multi-user config entries {previous_funder!r} and "
                    f"{user.user_id!r} share funder_address {funder}; one "
                    "wallet may back exactly one copier entry"
                )
            seen_funders[funder_key] = user.user_id
        # Live entries may omit funder_address (derived later); the private
        # key is then the wallet identity. Compare it internally and name
        # only the users in the error — never the key material. Only the
        # COMPARISON strips surrounding whitespace: two keys differing by
        # padding are the same wallet, while the stored SecretStr keeps the
        # value verbatim so the wallet transport receives exactly what the
        # operator wrote. Empty keys carry no identity (schema v2 paper
        # copiers may legitimately have no signing secret) and are skipped:
        # two empty secrets are not a shared wallet.
        private_key = user.wallet.private_key.get_secret_value()
        if private_key.strip():
            identity_key = private_key.strip()
            previous_key_user = seen_private_keys.get(identity_key)
            if previous_key_user is not None:
                raise ValueError(
                    f"multi-user config entries {previous_key_user!r} and "
                    f"{user.user_id!r} share the same wallet private_key; one "
                    "wallet may back exactly one copier entry"
                )
            seen_private_keys[identity_key] = user.user_id


def _is_descendant(child: Path, parent: Path) -> bool:
    """Path.is_relative_to back-port for 3.8 + readability."""
    try:
        return child.is_relative_to(parent)
    except AttributeError:  # pragma: no cover — Python < 3.9
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False


def _resolve_confined_roster_path(
    config_file: str, *, setting: str, suffixes: tuple[str, ...]
) -> Path:
    """Resolve an operator-supplied roster path with a confined-deputy guard.

    ``Path(x).resolve()`` follows symlinks, then the resolved path must live
    under cwd or $HOME — a misconfigured roster pointing at an arbitrary
    system file must fail closed before any read fills memory. The suffix
    check pins the expected document format per roster schema.
    """
    resolved = Path(config_file).expanduser().resolve(strict=False)
    safe_roots = (
        Path.home().resolve(strict=False),
        Path.cwd().resolve(strict=False),
    )
    if not any(resolved == root or _is_descendant(resolved, root) for root in safe_roots):
        raise ValueError(
            f"{setting} must point under $HOME or the working directory; "
            f"resolved path was {resolved}"
        )
    if resolved.suffix.lower() not in suffixes:
        expected = " or ".join(suffixes)
        raise ValueError(f"{setting} must be a {expected} file; got {resolved.suffix!r}")
    return resolved


def load_runtime_users(leader_wallets: list[str], config: AppConfig) -> list[RuntimeUser]:
    """Load per-user follow configuration.

    Roster precedence (Task 22, documented migration rule):

    1. ``app.copiers_config_file`` (schema v2 YAML) — AUTHORITATIVE when
       set. Per-copier ``wallet.*_env`` references are then the only wallet
       credential source; legacy ``PMCOPY__WALLET__*`` values are never
       merged in. Combining it with ``app.multi_user_config_file`` is an
       error (also enforced by the AppConfig semantic validator).
    2. ``app.multi_user_config_file`` (v1 JSON) — unchanged legacy roster.
    3. Neither — the legacy single-account env/config maps deterministically
       to exactly ONE copier whose id is the EXISTING persisted identity
       (the wallet funder address, or ``default`` until Deposit Wallet
       binding re-keys it) so no ledger splits; a migration notice is
       logged. It is never combined with a copiers file.

    Fallback behavior keeps the existing single-user flow but sources defaults
    from the strongly typed AppConfig instead of the legacy ENV snapshot.
    """
    copiers_file = (getattr(config.app, "copiers_config_file", "") or "").strip()
    if copiers_file:
        if (config.app.multi_user_config_file or "").strip():
            raise ValueError(
                "app.copiers_config_file and app.multi_user_config_file are "
                "both set; exactly one account roster may be configured (the "
                "schema v2 copiers file is authoritative on its own)."
            )
        # Local import: copierConfig reuses this module's strict primitives,
        # so the dependency must stay one-directional at module scope.
        from pmcopy.services.copierConfig import load_runtime_users_from_copiers

        return load_runtime_users_from_copiers(copiers_file, config)

    config_file = (config.app.multi_user_config_file or "").strip()
    if not config_file:
        # Pre-fix: signature_type=None left a hole that crashed
        # downstream callers doing ``int(wallet.signature_type)``.
        # Fall back to the typed AppConfig wallet's signature_type
        # so the single-user path produces a fully-populated
        # WalletConfig identical in shape to the multi-user path.
        base_wallet = WalletConfig(
            private_key=config.wallet.private_key,
            chain_id=config.wallet.chain_id,
            signature_type=SignatureType.DEPOSIT_WALLET,
            funder_address=config.wallet.funder_address,
            polygon_rpc_url=config.wallet.polygon_rpc_url,
            fallback_rpc_url=config.wallet.fallback_rpc_url,
        )
        # One identity for the user and its follows. Deriving them separately
        # left every funder-configured deployment with a RuntimeUser keyed on
        # the funder while its follows still said "default".
        user_id = config.wallet.funder_address or "default"
        # Task 22 migration notice: without a roster file the legacy
        # single-account configuration maps deterministically to exactly ONE
        # copier (this fallback) whose id is the existing persisted identity.
        # It is never combined with a schema v2 copiers file — that file is
        # authoritative whenever configured.
        logger.info(
            "legacy single-account configuration mapped to one default "
            "copier (id=%s); configure app.copiers_config_file to declare "
            "multiple copiers explicitly",
            user_id,
        )
        follows = {
            leader: UserFollowConfig(
                user_id=user_id,
                leader_id=leader,
                enabled=True,
                # Trusted typed-config defaults, converted to exact Decimal
                # at this internal boundary (see module docstring).
                allocation_pct=_decimal_from_config(config.sizing.percentage) / Decimal("100"),
                max_trade_usdc=_decimal_from_config(config.sizing.buy_max_amount),
                max_market_exposure_usdc=_decimal_from_config(config.risk.max_market_exposure_usdc),
            )
            for leader in leader_wallets
        }
        # The fallback account has no explicit follows configuration: the
        # follows above are synthesized defaults, so the documented legacy
        # single-account default (dynamic auto-follow) stays available.
        return [
            RuntimeUser(
                user_id=user_id, wallet=base_wallet, follows=follows, follows_explicit=False
            )
        ]

    # Confined-deputy guard (shared with the schema v2 roster loader):
    # ``MULTI_USER_CONFIG_FILE=/etc/passwd`` must fail closed before the
    # read. ``json.loads`` cannot parse JSONC (comments/trailing commas);
    # only .json is accepted. Operators wanting comments must strip them
    # upstream.
    resolved = _resolve_confined_roster_path(
        config_file, setting="MULTI_USER_CONFIG_FILE", suffixes=(".json",)
    )

    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            "MULTI_USER_CONFIG_FILE payload must be a JSON object with a "
            f'"users" list; got {type(payload).__name__}'
        )
    # Fail closed on a missing users list: a file that names no copiers used
    # to load as zero users, silently disabling every account.
    if "users" not in payload:
        raise ValueError(
            'MULTI_USER_CONFIG_FILE payload must contain a top-level "users" '
            "list (an explicitly empty list is valid; a missing one is not)"
        )
    _reject_unknown_keys(payload, _TOP_LEVEL_KEYS, where="MULTI_USER_CONFIG_FILE payload")
    users_payload = payload["users"]
    if not isinstance(users_payload, list):
        raise ValueError(
            f'MULTI_USER_CONFIG_FILE "users" must be a list, got {type(users_payload).__name__}'
        )
    users = [
        _runtime_user_from_payload(p, config, index=index) for index, p in enumerate(users_payload)
    ]
    _reject_duplicate_users(users)
    return users


def bind_resolved_deposit_wallet(
    runtime_user: RuntimeUser,
    wallet_address: str | None,
) -> None:
    """Bind the type-3 Deposit Wallet before runtime services wire.

    ``wallet_address`` is the verified Deposit Wallet funder address. Callers
    pass it through as-is: for a live user that is an unresolved wallet, and
    rejecting it here is what stops an empty funder from reaching the runtime.
    """
    resolved = str(wallet_address or "").strip()
    if not resolved:
        raise ValueError("no Deposit Wallet address was provided to bind")
    runtime_user.wallet = replace(
        runtime_user.wallet,
        funder_address=resolved,
        signature_type=SignatureType.DEPOSIT_WALLET,
    )
    if runtime_user.user_id in {"", "default"}:
        runtime_user.user_id = resolved
        runtime_user.follows = {
            leader_id: replace(follow, user_id=resolved)
            for leader_id, follow in runtime_user.follows.items()
        }


def _is_descendant(child: Path, parent: Path) -> bool:
    """Path.is_relative_to back-port for 3.8 + readability."""
    try:
        return child.is_relative_to(parent)
    except AttributeError:  # pragma: no cover — Python < 3.9
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False
