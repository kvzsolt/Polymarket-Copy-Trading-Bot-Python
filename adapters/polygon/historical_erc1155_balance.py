"""Read historical Polymarket outcome-token balances over Polygon RPC.

This adapter is read-only. It performs ERC-1155 ``balanceOf`` calls and never
constructs, signs, submits, or cancels a venue order.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Protocol

from web3 import Web3

from clients.network.rpc_fallback import FallbackWeb3Provider
from config.addresses import POLYMARKET_CONDITIONAL_TOKENS_ADDRESS

_TOKEN_SCALE = Decimal("1000000")
_BALANCE_OF_ABI = (
    {
        "inputs": (
            {"internalType": "address", "name": "account", "type": "address"},
            {"internalType": "uint256", "name": "id", "type": "uint256"},
        ),
        "name": "balanceOf",
        "outputs": ({"internalType": "uint256", "name": "", "type": "uint256"},),
        "stateMutability": "view",
        "type": "function",
    },
)


class _HistoricalContractCall(Protocol):
    """Minimal web3 call surface used by the adapter."""

    def call(self, *, block_identifier: int) -> int:
        """Return a raw ERC-1155 balance at one block."""


class _HistoricalContractFunctions(Protocol):
    """Minimal generated contract-functions surface."""

    def balanceOf(self, wallet: str, token_id: int) -> _HistoricalContractCall:  # noqa: N802
        """Build one historical ``balanceOf`` call."""


class HistoricalErc1155Contract(Protocol):
    """Minimal ERC-1155 contract surface required by the reader."""

    functions: _HistoricalContractFunctions


class Web3HistoricalErc1155BalanceReader:
    """Read exact six-decimal outcome-token balances at historical blocks."""

    def __init__(self, *, contract: HistoricalErc1155Contract) -> None:
        """Bind a read-only ERC-1155 contract instance.

        Args:
            contract: Web3-compatible Conditional Tokens contract.
        """

        self._contract = contract

    @classmethod
    def from_rpc_url(
        cls,
        primary: str,
        fallback: str = "",
        *,
        request_timeout_seconds: float = 2.5,
    ) -> "Web3HistoricalErc1155BalanceReader":
        """Build the reader over the existing primary/fallback RPC provider.

        Args:
            primary: Primary Polygon HTTP RPC endpoint.
            fallback: Optional secondary Polygon HTTP RPC endpoint.
            request_timeout_seconds: Per-provider request timeout.

        Returns:
            A reader bound to the canonical Conditional Tokens contract.

        Raises:
            ValueError: If ``primary`` is blank or timeout is non-positive.
        """

        if not str(primary or "").strip():
            raise ValueError("a primary RPC endpoint is required for historical balances")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        web3 = FallbackWeb3Provider(
            str(primary).strip(),
            str(fallback or "").strip(),
            request_timeout_seconds=request_timeout_seconds,
        ).get_web3()
        contract = web3.eth.contract(
            address=Web3.to_checksum_address(POLYMARKET_CONDITIONAL_TOKENS_ADDRESS),
            abi=list(_BALANCE_OF_ABI),
        )
        return cls(contract=contract)

    async def balance_at_block(
        self,
        leader_wallet: str,
        token_id: str,
        block_number: int,
    ) -> Decimal | None:
        """Return the leader's outcome-token balance at a historical block.

        Args:
            leader_wallet: Leader account or proxy-wallet address.
            token_id: Raw Polymarket outcome-token identifier.
            block_number: Polygon block whose end-state should be queried.

        Returns:
            Exact human-readable shares, or ``None`` only when the contract
            returns no value.

        Raises:
            ValueError: If an identifier or block is invalid.
            Exception: Read-only Polygon RPC failures.
        """

        if block_number < 0:
            raise ValueError("block_number must be non-negative")
        try:
            numeric_token_id = int(str(token_id))
        except (TypeError, ValueError) as exc:
            raise ValueError("token_id must be an unsigned integer") from exc
        if numeric_token_id < 0:
            raise ValueError("token_id must be an unsigned integer")
        wallet = Web3.to_checksum_address(str(leader_wallet))
        contract_call = self._contract.functions.balanceOf(wallet, numeric_token_id)
        raw_balance = await asyncio.to_thread(
            contract_call.call,
            block_identifier=block_number,
        )
        if raw_balance is None:
            return None
        value = int(raw_balance)
        if value < 0:
            raise ValueError("ERC-1155 balance cannot be negative")
        return Decimal(value) / _TOKEN_SCALE


__all__ = ["HistoricalErc1155Contract", "Web3HistoricalErc1155BalanceReader"]
