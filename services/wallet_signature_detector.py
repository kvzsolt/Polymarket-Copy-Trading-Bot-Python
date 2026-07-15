"""Shared wallet signature type detection (PR9.2).

Extracted from ``utils.createClobClient`` so that both the runtime
CLOB client and the CLI doctor can rely on the same detection logic
instead of diverging.

Public API
---------
detect_wallet_signature_type(wallet_config, *, fail_closed_on_rpc_error=False)
    Returns ``(SignatureType, wallet_type_label, signer_address)``.

    *fail_closed_on_rpc_error*:
        When True and the RPC contract-code probe fails while the signer
        differs from the funder, a ``RuntimeError`` is raised instead of
        silently falling back to ``SignatureType.POLY_PROXY``.  Use this
        in the doctor where a stale fallback is more dangerous than a clear
        error.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from config.env import SignatureType, secret_value
from utils.logger import get_logger, redact_error_detail

if TYPE_CHECKING:
    from config.env import WalletConfig

logger = get_logger("WalletSigDetector")


def _derive_signer_address(private_key: str) -> str | None:
    try:
        from web3 import Web3

        return str(Web3().eth.account.from_key(private_key).address)
    except Exception as exc:
        logger.warning("Failed to derive signer address (%s)", redact_error_detail(exc))
        return None


def _fetch_contract_code_hex(rpc_url: str, address: str, fallback_rpc_url: str = "") -> str:
    from clients.network.rpc_fallback import FallbackWeb3Provider
    from web3 import Web3

    provider = FallbackWeb3Provider(rpc_url, fallback_rpc_url)
    w3 = provider.get_web3()
    checksum = Web3.to_checksum_address(address)
    return str(w3.eth.get_code(checksum).hex())


def _classify_wallet_signature_type(
    *,
    signer_address: str | None,
    funder_address: str,
    funder_has_code: bool,
    configured_signature_type: SignatureType | int | None = None,
) -> tuple[SignatureType, str]:
    """Return a pre-SDK diagnostic classification.

    Contract bytecode alone cannot distinguish a Safe from a deposit wallet,
    so an explicit type-3 compatibility hint is used only to label that
    diagnostic case. The unified SDK's public ``wallet_type`` remains
    authoritative when the authenticated client is constructed.
    """
    signer = str(signer_address or "").strip().lower()
    funder = str(funder_address or "").strip().lower()

    if signer and funder and signer == funder:
        return (SignatureType.EOA, "EOA")
    if funder_has_code:
        try:
            configured = SignatureType(configured_signature_type)
        except (TypeError, ValueError):
            configured = None
        if configured == SignatureType.DEPOSIT_WALLET:
            return (SignatureType.DEPOSIT_WALLET, "DEPOSIT_WALLET")
        return (SignatureType.GNOSIS_SAFE, "GNOSIS_SAFE")
    if signer and funder and signer != funder:
        return (SignatureType.POLY_PROXY, "POLY_PROXY")
    return (SignatureType.EOA, "EOA")


async def detect_wallet_signature_type(
    wallet_config: WalletConfig,
    *,
    fail_closed_on_rpc_error: bool = False,
) -> tuple[SignatureType, str, str | None]:
    """Diagnose wallet signature type via key derivation and an RPC probe.

    Returns a 3-tuple ``(signature_type, wallet_type_label, signer_address)``.

    *fail_closed_on_rpc_error*:
        When True, if the RPC code probe fails while the signer differs
        from the funder, a ``RuntimeError`` is raised instead of falling
        back silently to ``POLY_PROXY``. This prevents the doctor from
        silently accepting a stale or wrong preflight diagnosis. The SDK's
        public ``wallet_type`` is authoritative once a client is created.
    """
    signer_address = _derive_signer_address(secret_value(wallet_config.private_key))
    funder_address = str(wallet_config.funder_address or "").strip()
    funder_has_code = False

    if signer_address and funder_address and signer_address.lower() != funder_address.lower():
        try:
            # Try to get fallback from the wallet config if available
            fallback_rpc = getattr(wallet_config, 'fallback_rpc_url', '') or ''
            code_hex = await asyncio.to_thread(
                _fetch_contract_code_hex,
                wallet_config.polygon_rpc_url,
                funder_address,
                fallback_rpc,
            )
            funder_has_code = bool(code_hex and code_hex != "0x")
        except Exception as exc:
            logger.warning(
                "Wallet type RPC probe failed (%s)",
                redact_error_detail(exc),
            )
            if fail_closed_on_rpc_error:
                raise RuntimeError(
                    f"Could not probe wallet type via RPC: {redact_error_detail(exc)}. "
                    "Verify RPC_URL connectivity; SIGNATURE_TYPE is only a "
                    "compatibility/diagnostic hint."
                ) from exc

    signature_type, wallet_type = _classify_wallet_signature_type(
        signer_address=signer_address,
        funder_address=funder_address,
        funder_has_code=funder_has_code,
        configured_signature_type=getattr(wallet_config, "signature_type", None),
    )
    return (signature_type, wallet_type, signer_address)
