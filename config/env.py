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
    DEPOSIT_WALLET = 3


_SIGNATURE_TYPE_BY_WALLET_TYPE: dict[str, SignatureType] = {
    "EOA": SignatureType.EOA,
    "POLY_PROXY": SignatureType.POLY_PROXY,
    "GNOSIS_SAFE": SignatureType.GNOSIS_SAFE,
    "DEPOSIT_WALLET": SignatureType.DEPOSIT_WALLET,
}


def signature_type_from_wallet_type(wallet_type: object) -> SignatureType | None:
    """Map the SDK public wallet type diagnostic to the compatibility enum."""
    normalized = str(wallet_type or "").strip().upper()
    return _SIGNATURE_TYPE_BY_WALLET_TYPE.get(normalized)


@dataclass(frozen=True)
class WalletConfig:
    private_key: SecretStr
    chain_id: int
    # The unified SDK derives the default Deposit Wallet from ``private_key``
    # and exposes the resolved address after authenticated startup.  This
    # optional value is retained only for paper-mode identity and as a live
    # consistency check; it is never sent to the SDK as wallet authority.
    funder_address: str = ""
    polygon_rpc_url: str = ""
    # Runtime compatibility value.  Operator configuration is intentionally
    # fixed to the new Deposit Wallet / POLY_1271 flow.
    signature_type: SignatureType | int = SignatureType.DEPOSIT_WALLET
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
    if config.funder_address and not is_valid_eth_address(config.funder_address):
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
    "signature_type_from_wallet_type",
    "validate_wallet_config",
]
