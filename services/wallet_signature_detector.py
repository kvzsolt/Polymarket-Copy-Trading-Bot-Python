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

from config.env import SignatureType
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


def _fetch_contract_code_hex(rpc_url: str, address: str) -> str:
    from web3 import Web3

    provider = Web3.HTTPProvider(rpc_url)
    w3 = Web3(provider)
    checksum = Web3.to_checksum_address(address)
    return str(w3.eth.get_code(checksum).hex())


def _classify_wallet_signature_type(
    *,
    signer_address: str | None,
    funder_address: str,
    funder_has_code: bool,
) -> tuple[SignatureType, str]:
    signer = str(signer_address or "").strip().lower()
    funder = str(funder_address or "").strip().lower()

    if signer and funder and signer == funder:
        return (SignatureType.EOA, "EOA")
    if funder_has_code:
        return (SignatureType.GNOSIS_SAFE, "GNOSIS_SAFE")
    if signer and funder and signer != funder:
        return (SignatureType.POLY_PROXY, "POLY_PROXY")
    return (SignatureType.EOA, "EOA")


async def detect_wallet_signature_type(
    wallet_config: WalletConfig,
    *,
    fail_closed_on_rpc_error: bool = False,
) -> tuple[SignatureType, str, str | None]:
    """Detect wallet signature type via private-key derivation and RPC probe.

    Returns a 3-tuple ``(signature_type, wallet_type_label, signer_address)``.

    *fail_closed_on_rpc_error*:
        When True, if the RPC code probe fails while the signer differs
        from the funder, a ``RuntimeError`` is raised instead of falling
        back silently to ``POLY_PROXY``.  This prevents the doctor from
        silently accepting a stale or wrong type.
    """
    signer_address = _derive_signer_address(wallet_config.private_key)
    funder_address = str(wallet_config.funder_address or "").strip()
    funder_has_code = False

    if signer_address and funder_address and signer_address.lower() != funder_address.lower():
        try:
            code_hex = await asyncio.to_thread(
                _fetch_contract_code_hex,
                wallet_config.polygon_rpc_url,
                funder_address,
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
                    "Set SIGNATURE_TYPE explicitly or verify RPC_URL connectivity."
                ) from exc

    signature_type, wallet_type = _classify_wallet_signature_type(
        signer_address=signer_address,
        funder_address=funder_address,
        funder_has_code=funder_has_code,
    )
    return (signature_type, wallet_type, signer_address)