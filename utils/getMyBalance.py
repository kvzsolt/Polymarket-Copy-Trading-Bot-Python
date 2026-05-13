from __future__ import annotations

import asyncio

from web3 import Web3

from config.env import ENV
from clients.network.web3_adapter import AsyncWeb3Adapter
from utils.logger import get_logger

logger = get_logger("Balance")

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



def _get_balance_sync(address: str) -> float | None:
    rpc_url = ENV.get('RPC_URL')

    if not rpc_url:
        logger.error("Balance lookup failed: RPC_URL is not configured")
        return None

    raw_candidates = [
        ENV.get('COLLATERAL_CONTRACT_ADDRESS'),
        ENV.get('PUSD_CONTRACT_ADDRESS'),
        ENV.get('USDC_CONTRACT_ADDRESS'),
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
        w3 = Web3(Web3.HTTPProvider(rpc_url))
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
            bal = float(raw_balance / 10**6)
            logger.debug(
                "Collateral balance for %s=%0.2f via %s",
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


async def get_my_balance(address: str) -> float | None:
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


async def get_native_balance_wei(address: str) -> int | None:
    rpc_url = ENV.get('RPC_URL')
    if not rpc_url:
        return None
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        adapter = AsyncWeb3Adapter(w3, logger=logger)
        checksum_address = Web3.to_checksum_address(address)
        return int(await adapter.get_native_balance(checksum_address))
    except Exception as exc:
        logger.warning(f"Native balance lookup failed for {_masked_address(address)}: {exc}")
        return None
