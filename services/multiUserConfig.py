from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from config.env import (
    ENV,
    SignatureType,
    WalletConfig,
    build_wallet_config,
    get_funder_address,
)


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


def _parse_signature_type(value) -> SignatureType:
    return SignatureType(int(value))


def _runtime_user_from_payload(payload: dict) -> RuntimeUser:
    wallet = WalletConfig(
        private_key=str(payload["private_key"]),
        chain_id=int(payload.get("chain_id", 137)),
        signature_type=_parse_signature_type(payload.get("signature_type", 0)),
        funder_address=str(payload["funder_address"]),
        polygon_rpc_url=str(payload.get("polygon_rpc_url", ENV.get("RPC_URL"))),
    )
    follows: dict[str, UserFollowConfig] = {}
    for item in payload.get("follows", []):
        cfg = UserFollowConfig(
            user_id=str(payload["user_id"]),
            leader_id=str(item["leader_id"]),
            enabled=bool(item.get("enabled", True)),
            allocation_pct=float(item.get("allocation_pct", 0.05)),
            max_trade_usdc=float(item.get("max_trade_usdc", ENV.get("BUY_MAX_AMOUNT", 100))),
            max_market_exposure_usdc=float(
                item.get("max_market_exposure_usdc", ENV.get("MAX_MARKET_EXPOSURE_USDC", 300))
            ),
        )
        follows[cfg.leader_id] = cfg

    return RuntimeUser(user_id=str(payload["user_id"]), wallet=wallet, follows=follows)


def load_runtime_users(leader_wallets: list[str]) -> list[RuntimeUser]:
    """Load per-user follow configuration.

    Fallback behavior keeps existing single-user flow.
    """
    config_file = str(ENV.get("MULTI_USER_CONFIG_FILE") or "").strip()
    if not config_file:
        base_wallet = build_wallet_config()
        follows = {
            leader: UserFollowConfig(
                user_id="default",
                leader_id=leader,
                enabled=True,
                allocation_pct=float(ENV.get("PERCENTAGE", 5.0)) / 100.0,
                max_trade_usdc=float(ENV.get("BUY_MAX_AMOUNT", 100)),
                max_market_exposure_usdc=float(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300)),
            )
            for leader in leader_wallets
        }
        return [RuntimeUser(user_id=get_funder_address() or "default", wallet=base_wallet, follows=follows)]

    payload = json.loads(Path(config_file).read_text(encoding="utf-8"))
    users = [_runtime_user_from_payload(p) for p in payload.get("users", [])]
    return users
