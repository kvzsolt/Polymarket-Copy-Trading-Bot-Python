from __future__ import annotations

import asyncio

from web3 import Web3

from config.env import ENV
from network.web3_adapter import AsyncWeb3Adapter
from utils.logger import get_logger

logger = get_logger("Balance")

RPC_URL = ENV.get('RPC_URL')
USDC_CONTRACT_ADDRESS = ENV.get('USDC_CONTRACT_ADDRESS')

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
    if not RPC_URL:
        logger.error("Balance lookup failed: RPC_URL is not configured")
        return None
    if not USDC_CONTRACT_ADDRESS:
        logger.error("Balance lookup failed: USDC_CONTRACT_ADDRESS is not configured")
        return None

    try:
        checksum_contract = Web3.to_checksum_address(USDC_CONTRACT_ADDRESS)
        checksum_address = Web3.to_checksum_address(address)
    except (TypeError, ValueError) as exc:
        logger.error(
            f"Balance lookup failed: invalid address config for wallet={_masked_address(address)} | error={exc}"
        )
        return None

    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL))
        usdc_contract = w3.eth.contract(address=checksum_contract, abi=USDC_ABI)
        balance_usdc = usdc_contract.functions.balanceOf(checksum_address).call()
        bal = float(balance_usdc / 10**6)
        logger.debug(f"USDC balance for {_masked_address(checksum_address)}={bal:.2f}")
        return bal
    except Exception as exc:
        logger.error(
            f"Balance lookup failed for wallet={_masked_address(checksum_address)} | error={exc}",
            exc_info=True,
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
    if not RPC_URL:
        return None
    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL))
        adapter = AsyncWeb3Adapter(w3, logger=logger)
        checksum_address = Web3.to_checksum_address(address)
        return int(await adapter.get_native_balance(checksum_address))
    except Exception as exc:
        logger.warning(f"Native balance lookup failed for {_masked_address(address)}: {exc}")
        return None
