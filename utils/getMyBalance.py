from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from web3 import Web3

from clients.network.rpc_fallback import FallbackWeb3Provider
from clients.network.web3_adapter import AsyncWeb3Adapter
from utils.logger import get_logger

logger = get_logger("Balance")

# USDC has 6 decimals — wei-style raw int → human balance.
_USDC_SCALE = Decimal(10**6)

# Module-level RPC + contract-address state. Production overrides via
# ``configure_from_appconfig(config)`` from ``orchestrators/runtime_
# bootstrap.py`` once at startup. When no typed config is injected, the
# module exposes empty defaults and balance lookups fail-closed with a
# clear error message.
_RPC_URL: str = ""
_COLLATERAL_CONTRACT_ADDRESS: str = ""
_PUSD_CONTRACT_ADDRESS: str = ""
_USDC_CONTRACT_ADDRESS: str = ""


def configure_from_appconfig(config: Any) -> None:
    """Override module-level RPC URL + collateral contract addresses.

    Production bootstrap calls this once at startup so the balance lookup
    path runs entirely off typed AppConfig.
    """
    global _RPC_URL, _COLLATERAL_CONTRACT_ADDRESS, _PUSD_CONTRACT_ADDRESS, _USDC_CONTRACT_ADDRESS
    _RPC_URL = str(getattr(config.network, "rpc_url", "") or "")
    _COLLATERAL_CONTRACT_ADDRESS = str(
        getattr(config.network, "collateral_contract_address", "") or ""
    )
    _PUSD_CONTRACT_ADDRESS = str(
        getattr(config.network, "pusd_contract_address", "") or ""
    )
    _USDC_CONTRACT_ADDRESS = str(
        getattr(config.network, "usdc_contract_address", "") or ""
    )

USDC_ABI = [{
    "constant": True,
    "inputs": [{"name": "owner", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "type": "function"
}]


def _masked_address(address: str | None) -> str:
    if not address:
        return "<missing>"
    if len(address) <= 10:
        return address
    return f"{address[:10]}..."



def _get_balance_sync(address: str, fallback_rpc_url: str = "") -> Decimal | None:
    rpc_url = _RPC_URL

    if not rpc_url:
        logger.error("Balance lookup failed: RPC_URL is not configured")
        return None

    raw_candidates = [
        _COLLATERAL_CONTRACT_ADDRESS,
        _PUSD_CONTRACT_ADDRESS,
        _USDC_CONTRACT_ADDRESS,
    ]
    # Preserve order but drop blanks/duplicates.
    seen: set[str] = set()
    candidates: list[str] = []
    for raw in raw_candidates:
        s = str(raw or '').strip()
        if s and s not in seen:
            seen.add(s)
            candidates.append(s)

    if not candidates:
        logger.error("Balance lookup failed: collateral contract address is not configured")
        return None

    try:
        checksum_address = Web3.to_checksum_address(address)
    except (TypeError, ValueError) as exc:
        logger.error(
            f"Balance lookup failed: invalid wallet address for wallet={_masked_address(address)} | error={exc}"
        )
        return None

    try:
        provider = FallbackWeb3Provider(rpc_url, fallback_rpc_url)
        w3 = provider.get_web3()
    except Exception as exc:
        logger.error("Balance lookup failed: could not create Web3 provider: %s", exc)
        return None
    failures: list[str] = []

    for contract_address in candidates:
        try:
            checksum_contract = Web3.to_checksum_address(contract_address)
        except (TypeError, ValueError) as exc:
            failures.append(f"invalid_contract:{_masked_address(contract_address)}:{exc}")
            continue

        try:
            code = w3.eth.get_code(checksum_contract)
            if not code:
                failures.append(f"no_code:{checksum_contract}")
                continue

            token_contract = w3.eth.contract(address=checksum_contract, abi=USDC_ABI)
            raw_balance = token_contract.functions.balanceOf(checksum_address).call()
            # ``raw_balance`` is uint256 microUSDC. ``Decimal(int) / Decimal(int)``
            # stays exact; the previous ``float(raw / 10**6)`` lost cent precision
            # above 2^53 microUSDC and corrupted downstream risk-cap math.
            bal = Decimal(int(raw_balance)) / _USDC_SCALE
            logger.debug(
                "Collateral balance for %s=%s via %s",
                _masked_address(checksum_address),
                bal,
                _masked_address(checksum_contract),
            )
            return bal
        except Exception as exc:
            # Typical misconfig/chain mismatch path; keep trying fallbacks.
            failures.append(f"call_failed:{_masked_address(checksum_contract)}:{exc}")
            continue

    logger.error(
        "Balance lookup failed for wallet=%s | no usable collateral contract. tried=%s",
        _masked_address(checksum_address),
        "; ".join(failures[:5]),
    )
    return None


async def get_my_balance(address: str) -> Decimal | None:
    if not address:
        logger.error("Balance lookup failed: wallet address is missing")
        return None

    try:
        # Run all Web3 HTTP calls off the event loop.
        return await asyncio.to_thread(_get_balance_sync, address)
    except Exception as exc:
        logger.error(
            f"Balance lookup failed in async wrapper for wallet={_masked_address(address)} | error={exc}",
            exc_info=True,
        )
        return None


async def get_native_balance_wei(address: str, fallback_rpc_url: str = "") -> int | None:
    rpc_url = _RPC_URL
    if not rpc_url:
        return None
    try:
        provider = FallbackWeb3Provider(rpc_url, fallback_rpc_url)
        w3 = provider.get_web3()
        adapter = AsyncWeb3Adapter(w3, logger=logger)
        checksum_address = Web3.to_checksum_address(address)
        return int(await adapter.get_native_balance(checksum_address))
    except Exception as exc:
        logger.warning(f"Native balance lookup failed for {_masked_address(address)}: {exc}")
        return None
