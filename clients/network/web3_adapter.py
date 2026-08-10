from __future__ import annotations

import asyncio
from typing import Any


class AsyncWeb3Adapter:
    def __init__(self, web3, logger=None):
        self.web3 = web3
        self.logger = logger

    async def get_native_balance(self, address: str) -> int:
        return await asyncio.to_thread(self.web3.eth.get_balance, address)

    async def call_contract(self, contract_call) -> Any:
        return await asyncio.to_thread(contract_call.call)

    async def get_transaction_count(self, address: str) -> int:
        return await asyncio.to_thread(self.web3.eth.get_transaction_count, address)

    async def get_gas_price(self) -> int:
        return await asyncio.to_thread(lambda: self.web3.eth.gas_price)

    async def send_raw_transaction(self, raw_tx: bytes) -> str:
        tx_hash = await asyncio.to_thread(self.web3.eth.send_raw_transaction, raw_tx)
        try:
            return tx_hash.hex()
        except Exception:
            return str(tx_hash)

    # ASYNC109 suppressed: the timeout parameter is forwarded to web3's
    # wait_for_transaction_receipt and mirrored in asyncio.wait_for; removing
    # it would change this adapter's public contract.
    async def wait_for_receipt(self, tx_hash: str, timeout: int = 120) -> Any:  # noqa: ASYNC109
        return await asyncio.wait_for(
            asyncio.to_thread(self.web3.eth.wait_for_transaction_receipt, tx_hash, timeout),
            timeout=timeout + 5,
        )
