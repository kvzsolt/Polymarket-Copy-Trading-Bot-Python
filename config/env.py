"""Small shared config value types; runtime settings live in app_config."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import IntEnum

from pydantic import SecretStr

_ETH_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EnvValidationError(ValueError):
    """Raised when layered YAML/settings validation fails."""


class SignatureType(IntEnum):
    EOA = 0
    POLY_PROXY = 1
    GNOSIS_SAFE = 2


@dataclass(frozen=True)
class WalletConfig:
    private_key: SecretStr
    chain_id: int
    funder_address: str
    polygon_rpc_url: str
    signature_type: SignatureType | int
    fallback_rpc_url: str = ""

    def __post_init__(self) -> None:
        # Direct construction is still used by the multi-user adapter and a
        # few integration boundaries. Keep the immutable public type honest
        # even when those callers provide a plain string.
        if not isinstance(self.private_key, SecretStr):
            object.__setattr__(
                self,
                "private_key",
                SecretStr(str(self.private_key or "")),
            )


def secret_value(value: SecretStr | str | None) -> str:
    """Reveal a secret only at an explicit consumption boundary."""
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    return str(value or "")


def is_valid_eth_address(address: str) -> bool:
    return bool(_ETH_ADDRESS.fullmatch(str(address)))


def validate_wallet_config(config: WalletConfig) -> None:
    if not secret_value(config.private_key):
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
    "secret_value",
    "validate_wallet_config",
]
