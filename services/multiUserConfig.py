from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import SecretStr

from config.env import SignatureType, WalletConfig

if TYPE_CHECKING:
    from config.app_config import AppConfig


@dataclass(frozen=True)
class UserFollowConfig:
    user_id: str
    leader_id: str
    enabled: bool
    allocation_pct: float
    max_trade_usdc: float
    max_market_exposure_usdc: float


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


def _parse_signature_type(value) -> SignatureType:
    return SignatureType(int(value))


def _runtime_user_from_payload(payload: dict, config: AppConfig) -> RuntimeUser:
    legacy_signature_type = payload.get("signature_type", 3)
    if int(legacy_signature_type) != int(SignatureType.DEPOSIT_WALLET):
        raise ValueError(
            "multi-user signature_type is no longer configurable; "
            "only Deposit Wallet/POLY_1271 type 3 is supported"
        )
    funder_address = str(payload.get("funder_address", "")).strip()
    # Mirrors the single-user AppConfig invariant. Paper never authenticates,
    # so no secure client resolves a Deposit Wallet and
    # ``bind_resolved_deposit_wallet`` never runs: the configured funder is
    # this entry's only wallet identity. Left empty, the balance and
    # live-position probes query "" and fail readiness with no actionable
    # error. Live may omit it — the authenticated SDK derives the address from
    # this entry's own private_key and binds it back.
    if config.execution.paper_trade_only and not funder_address:
        entry = str(payload.get("user_id", "")).strip() or "<unknown>"
        raise ValueError(
            f"multi-user entry {entry!r} is missing funder_address, which is "
            "required in paper mode (it is the wallet identity used by the "
            "balance/live-position health probes; live mode derives it from "
            "the entry's private_key instead)"
        )
    wallet = WalletConfig(
        private_key=SecretStr(str(payload["private_key"])),
        chain_id=int(payload.get("chain_id", config.wallet.chain_id)),
        signature_type=SignatureType.DEPOSIT_WALLET,
        funder_address=funder_address,
        polygon_rpc_url=config.polymarket.rpc_url,
        fallback_rpc_url=config.polymarket.fallback_rpc_url,
    )
    follows: dict[str, UserFollowConfig] = {}
    for item in payload.get("follows", []):
        cfg = UserFollowConfig(
            user_id=str(payload["user_id"]),
            leader_id=str(item["leader_id"]),
            enabled=bool(item.get("enabled", True)),
            allocation_pct=float(item.get("allocation_pct", config.sizing.percentage / 100.0)),
            max_trade_usdc=float(item.get("max_trade_usdc", config.sizing.buy_max_amount)),
            max_market_exposure_usdc=float(
                item.get("max_market_exposure_usdc", config.risk.max_market_exposure_usdc)
            ),
        )
        follows[cfg.leader_id] = cfg

    return RuntimeUser(
        user_id=str(payload["user_id"]),
        wallet=wallet,
        follows=follows,
        # A follows list in the config payload is explicit operator policy —
        # even when empty. Whitelist semantics apply; no legacy union.
        follows_explicit=True,
    )


def load_runtime_users(leader_wallets: list[str], config: AppConfig) -> list[RuntimeUser]:
    """Load per-user follow configuration.

    Fallback behavior keeps the existing single-user flow but sources defaults
    from the strongly typed AppConfig instead of the legacy ENV snapshot.
    """
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
        follows = {
            leader: UserFollowConfig(
                user_id=user_id,
                leader_id=leader,
                enabled=True,
                allocation_pct=config.sizing.percentage / 100.0,
                max_trade_usdc=config.sizing.buy_max_amount,
                max_market_exposure_usdc=config.risk.max_market_exposure_usdc,
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

    # Confined-deputy guard: an operator-supplied path is read as JSON
    # into the process. ``Path(x).resolve()`` so symlinks are followed,
    # then require the resolved path under cwd or $HOME — a misconfigured
    # ``MULTI_USER_CONFIG_FILE=/etc/passwd`` should NOT silently parse
    # an arbitrary file. JSON parsing would normally reject it, but the
    # read happens first and fills memory.
    resolved = Path(config_file).expanduser().resolve(strict=False)
    safe_roots = (
        Path.home().resolve(strict=False),
        Path.cwd().resolve(strict=False),
    )
    if not any(resolved == root or _is_descendant(resolved, root) for root in safe_roots):
        raise ValueError(
            f"MULTI_USER_CONFIG_FILE must point under $HOME or the working "
            f"directory; resolved path was {resolved}"
        )
    # ``json.loads`` cannot parse JSONC (comments/trailing commas); only
    # accept .json. Operators wanting comments must strip them upstream.
    if resolved.suffix.lower() != ".json":
        raise ValueError(f"MULTI_USER_CONFIG_FILE must be a .json file; got {resolved.suffix!r}")

    payload = json.loads(resolved.read_text(encoding="utf-8"))
    users = [_runtime_user_from_payload(p, config) for p in payload.get("users", [])]
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
