"""Small shared config value types; runtime settings live in app_config."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import IntEnum

_ETH_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EnvValidationError(ValueError):
    """Raised when layered YAML/settings validation fails."""


class SignatureType(IntEnum):
    EOA = 0
    POLY_PROXY = 1
    GNOSIS_SAFE = 2


@dataclass(frozen=True)
class WalletConfig:
    private_key: str
    chain_id: int
    funder_address: str
    polygon_rpc_url: str
    signature_type: SignatureType | int
    fallback_rpc_url: str = ""


def is_valid_eth_address(address: str) -> bool:
    return bool(_ETH_ADDRESS.fullmatch(str(address)))


def validate_wallet_config(config: WalletConfig) -> None:
    if not config.private_key:
        raise EnvValidationError("PRIVATE_KEY is required.")
    if not is_valid_eth_address(config.funder_address):
        raise EnvValidationError("FUNDER_ADDRESS must be a 42-character hex address.")
    if config.chain_id not in {137, 80002}:
        raise EnvValidationError("CHAIN_ID must be 137 or 80002.")
    if not config.polygon_rpc_url:
        raise EnvValidationError("RPC_URL is required.")


__all__ = [
    "EnvValidationError",
    "SignatureType",
    "WalletConfig",
    "is_valid_eth_address",
    "validate_wallet_config",
]
