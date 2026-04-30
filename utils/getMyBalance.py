from __future__ import annotations

import asyncio

from web3 import Web3

from config.env import ENV
from network.web3_adapter import AsyncWeb3Adapter
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
    collateral_contract_address = (
        ENV.get('COLLATERAL_CONTRACT_ADDRESS')
        or ENV.get('PUSD_CONTRACT_ADDRESS')
        or ENV.get('USDC_CONTRACT_ADDRESS')
    )

    if not rpc_url:
        logger.error("Balance lookup failed: RPC_URL is not configured")
        return None
    if not collateral_contract_address:
        logger.error("Balance lookup failed: collateral contract address is not configured")
        return None

    try:
        checksum_contract = Web3.to_checksum_address(collateral_contract_address)
        checksum_address = Web3.to_checksum_address(address)
    except (TypeError, ValueError) as exc:
        logger.error(
            f"Balance lookup failed: invalid address config for wallet={_masked_address(address)} | error={exc}"
        )
        return None

    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        usdc_contract = w3.eth.contract(address=checksum_contract, abi=USDC_ABI)
        balance_usdc = usdc_contract.functions.balanceOf(checksum_address).call()
        bal = float(balance_usdc / 10**6)
        logger.debug(f"Collateral balance for {_masked_address(checksum_address)}={bal:.2f}")
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
