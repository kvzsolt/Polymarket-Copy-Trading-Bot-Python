"""Signer/funder wallet validation for redemption safety.

Ensures the redemption wallet configuration is internally consistent
BEFORE any on-chain redemption transaction is attempted.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from config.env import SignatureType, WalletConfig

logger = logging.getLogger(__name__)

# --- Proxy signature types that need specialized redemption handling ---
SUPPORTED_PROXY_SIGNATURE_TYPES: set[SignatureType] = {
    SignatureType.POLY_PROXY,
    SignatureType.GNOSIS_SAFE,
}


@dataclass(frozen=True)
class WalletValidationResult:
    """Immutable result of wallet redemption validation."""

    allowed: bool
    reason: str
    signer_address: str
    funder_address: str
    signature_type: SignatureType


def derive_signer_address(private_key_hex: str) -> str:
    """Derive the Ethereum address from a hex-encoded private key using Web3.

    Returns a checksummed address string, or an empty string on failure.
    """
    if not private_key_hex:
        return ""

    try:
        from eth_account import Account

        acct = Account.from_key(private_key_hex)
        return str(acct.address)
    except Exception as exc:
        logger.error("derive_signer_failed error=%r", exc)
        return ""


def validate_redemption_wallet_mode(
    wallet_config: WalletConfig,
) -> WalletValidationResult:
    """Validate wallet is safe for redemption operations.

    Rules (rationale: capital safety):
    - EOA: signer address MUST equal funder address
    - POLY_PROXY / GNOSIS_SAFE: currently NOT supported for redemption,
      because proxy redemption requires additional signature flows that
      are not yet implemented.
    - If private_key cannot derive an address, fail closed.
    - If signer and funder are clear empty strings, fail closed.
    """
    signer_address = derive_signer_address(wallet_config.private_key)

    funder = wallet_config.funder_address or ""
    sig_type = wallet_config.signature_type

    if not signer_address:
        return WalletValidationResult(
            allowed=False,
            reason="signer_address_derivation_failed",
            signer_address="",
            funder_address=funder,
            signature_type=sig_type,
        )

    if not funder:
        return WalletValidationResult(
            allowed=False,
            reason="funder_address_missing",
            signer_address=signer_address,
            funder_address="",
            signature_type=sig_type,
        )

    if sig_type == SignatureType.EOA:
        # EOA: signer and funder must be the same address
        if signer_address.lower() == funder.lower():
            return WalletValidationResult(
                allowed=True,
                reason="eoa_signer_matches_funder",
                signer_address=signer_address,
                funder_address=funder,
                signature_type=sig_type,
            )
        return WalletValidationResult(
            allowed=False,
            reason="eoa_signer_funder_mismatch",
            signer_address=signer_address,
            funder_address=funder,
            signature_type=sig_type,
        )

    # Proxy signature types: not yet supported for redemption
    if sig_type in SUPPORTED_PROXY_SIGNATURE_TYPES:
        return WalletValidationResult(
            allowed=False,
            reason=f"proxy_redemption_not_yet_supported_{sig_type.name.lower()}",
            signer_address=signer_address,
            funder_address=funder,
            signature_type=sig_type,
        )

    return WalletValidationResult(
        allowed=False,
        reason=f"unknown_signature_type_{sig_type}",
        signer_address=signer_address,
        funder_address=funder,
        signature_type=sig_type,
    )
