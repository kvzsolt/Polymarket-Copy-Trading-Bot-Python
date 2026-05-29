"""Wallet balance adapter — ``AbstractBalanceClient`` implementation.

Accepts the RPC URL + USDC contract address explicitly (from ``AppConfig``),
avoiding the global ``ENV`` reads in the legacy ``utils.getMyBalance``
module. Phase 6 removes the legacy module; until then it remains as a
transitional shim.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

from web3 import Web3

from domain.ports.clients import AbstractBalanceClient
from clients.network.web3_adapter import AsyncWeb3Adapter

_USDC_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    }
]

# USDC has 6 decimals — exact Decimal scale, no float.
_USDC_SCALE = Decimal(10**6)


class BalanceClient(AbstractBalanceClient):
    def __init__(
        self,
        *,
        rpc_url: str,
        usdc_contract_address: str,
        logger: logging.Logger | None = None,
    ):
        self._rpc_url = rpc_url
        self._usdc_contract_address = usdc_contract_address
        self._logger = logger or logging.getLogger("BalanceClient")

    def _get_usdc_balance_sync(self, address: str) -> Decimal | None:
        try:
            checksum_contract = Web3.to_checksum_address(self._usdc_contract_address)
            checksum_address = Web3.to_checksum_address(address)
        except (TypeError, ValueError) as exc:
            self._logger.error("Invalid address config: %s", exc)
            return None

        try:
            w3 = Web3(Web3.HTTPProvider(self._rpc_url))
            contract = w3.eth.contract(address=checksum_contract, abi=_USDC_ABI)
            raw = contract.functions.balanceOf(checksum_address).call()
            # Decimal(int) / Decimal(int) is exact across the full uint256
            # range. The previous ``float(raw / 10**6)`` lost cent precision
            # above ~2^53 microUSDC and corrupted downstream risk-cap math.
            return Decimal(int(raw)) / _USDC_SCALE
        except Exception as exc:
            self._logger.error("USDC balance lookup failed: %s", exc)
            return None

    async def get_usdc_balance(self, address: str) -> Decimal | None:
        if not address:
            return None
        return await asyncio.to_thread(self._get_usdc_balance_sync, address)

    async def get_native_balance_wei(self, address: str) -> int | None:
        if not address:
            return None
        try:
            w3 = Web3(Web3.HTTPProvider(self._rpc_url))
            adapter = AsyncWeb3Adapter(w3, logger=self._logger)
            checksum_address = Web3.to_checksum_address(address)
            return int(await adapter.get_native_balance(checksum_address))
        except Exception as exc:
            self._logger.warning("Native balance lookup failed: %s", exc)
            return None
