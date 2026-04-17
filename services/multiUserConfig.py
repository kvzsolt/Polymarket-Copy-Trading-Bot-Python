from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

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


def _parse_signature_type(value) -> SignatureType:
    return SignatureType(int(value))


def _runtime_user_from_payload(payload: dict, config: AppConfig) -> RuntimeUser:
    wallet = WalletConfig(
        private_key=str(payload["private_key"]),
        chain_id=int(payload.get("chain_id", config.wallet.chain_id)),
        signature_type=_parse_signature_type(payload.get("signature_type", int(config.wallet.signature_type))),
        funder_address=str(payload["funder_address"]),
        polygon_rpc_url=str(payload.get("polygon_rpc_url", config.network.rpc_url)),
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

    return RuntimeUser(user_id=str(payload["user_id"]), wallet=wallet, follows=follows)


def load_runtime_users(leader_wallets: list[str], config: AppConfig) -> list[RuntimeUser]:
    """Load per-user follow configuration.

    Fallback behavior keeps the existing single-user flow but sources defaults
    from the strongly typed AppConfig instead of the legacy ENV snapshot.
    """
    config_file = config.discovery.multi_user_config_file.strip()
    if not config_file:
        base_wallet = WalletConfig(
            private_key=config.wallet.private_key,
            chain_id=config.wallet.chain_id,
            signature_type=config.wallet.signature_type,
            funder_address=config.wallet.funder_address,
            polygon_rpc_url=config.wallet.polygon_rpc_url,
        )
        follows = {
            leader: UserFollowConfig(
                user_id="default",
                leader_id=leader,
                enabled=True,
                allocation_pct=config.sizing.percentage / 100.0,
                max_trade_usdc=config.sizing.buy_max_amount,
                max_market_exposure_usdc=config.risk.max_market_exposure_usdc,
            )
            for leader in leader_wallets
        }
        return [RuntimeUser(user_id=config.wallet.funder_address or "default", wallet=base_wallet, follows=follows)]

    payload = json.loads(Path(config_file).read_text(encoding="utf-8"))
    users = [_runtime_user_from_payload(p, config) for p in payload.get("users", [])]
    return users
