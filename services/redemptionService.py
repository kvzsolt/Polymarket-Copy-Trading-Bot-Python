from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from config.app_config import AppConfig

from web3 import Web3

from network.web3_adapter import AsyncWeb3Adapter
from utils.fetchData import fetch_data
from utils.logger import get_logger

logger = get_logger("RedemptionService")

CTF_REDEEM_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "type": "function",
    }
]
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PARENT_COLLECTION_ID = b"\x00" * 32
RESOLVED_HIGH = 0.99
RESOLVED_LOW = 0.01
ZERO_THRESHOLD = 0.0001


@dataclass(frozen=True)
class RedeemablePosition:
    user_id: str
    token_id: str
    condition_id: str
    outcome_index: int
    size: Decimal
    cur_price: float
    current_value: float
    title: str
    outcome: str


@dataclass
class RedemptionResult:
    submitted: bool
    tx_hash: str | None
    status: str
    redeemed_amount: Decimal
    error: str | None = None


class RedemptionService:
    def __init__(self, clob_client: Any, config: AppConfig | None = None, wallet_config=None):
        self.clob_client = clob_client
        if config is None:
            from config.app_config import load_config

            config = load_config()
        self._config = config
        self.wallet_config = wallet_config or config.wallet
        
        # Pacing and concurrency control per condition
        self._condition_locks: dict[str, asyncio.Lock] = {}
        self._last_redeem_attempt_by_condition: dict[str, float] = {}

    def _get_condition_lock(self, condition_id: str) -> asyncio.Lock:
        if condition_id not in self._condition_locks:
            self._condition_locks[condition_id] = asyncio.Lock()
        return self._condition_locks[condition_id]

    async def scan_redeemables(self, user_id: str) -> list[RedeemablePosition]:
        url = f"https://data-api.polymarket.com/positions?user={user_id}"
        positions = await fetch_data(url, silent=True)
        result: list[RedeemablePosition] = []
        for p in positions or []:
            size_val = float(p.get("size", 0) or 0)
            if size_val <= ZERO_THRESHOLD:
                continue
                
            cur_price = float(p.get("curPrice", 0) or 0)
            redeemable = bool(p.get("redeemable", False))

            if (cur_price >= RESOLVED_HIGH or cur_price <= RESOLVED_LOW) and redeemable:
                result.append(
                    RedeemablePosition(
                        user_id=user_id,
                        token_id=str(p.get("asset") or p.get("tokenId") or ""),
                        condition_id=str(p.get("conditionId") or ""),
                        outcome_index=int(p.get("outcomeIndex", 0) or 0),
                        size=Decimal(str(p.get("size", 0) or 0)),
                        cur_price=cur_price,
                        current_value=float(p.get("currentValue", 0) or 0),
                        title=str(p.get("title", "")),
                        outcome=str(p.get("outcome", "")),
                    )
                )
        return result

    async def redeem_condition(self, condition_id: str) -> RedemptionResult:
        lock = self._get_condition_lock(condition_id)
        async with lock:
            now = time.monotonic()
            last_attempt = self._last_redeem_attempt_by_condition.get(condition_id, 0.0)
            elapsed = now - last_attempt
            if elapsed < 1.0:
                return RedemptionResult(False, None, "skipped", Decimal("0"), "throttled_per_condition")
            
            self._last_redeem_attempt_by_condition[condition_id] = time.monotonic()

            rpc_url = self._config.network.rpc_url.strip()
            if not rpc_url:
                return RedemptionResult(False, None, "not_submitted", Decimal("0"), "rpc_url_missing")

            w3 = Web3(Web3.HTTPProvider(rpc_url))
            adapter = AsyncWeb3Adapter(w3, logger=logger)
            ctf_contract = w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_REDEEM_ABI
            )

            try:
                if condition_id.startswith("0x"):
                    cid_bytes = bytes.fromhex(condition_id[2:])
                else:
                    cid_bytes = bytes.fromhex(condition_id)
                cid_bytes32 = cid_bytes.ljust(32, b"\x00")[:32]
            except Exception as exc:
                return RedemptionResult(
                    False, None, "failed", Decimal("0"), f"invalid_condition_id:{exc}"
                )

            sender = Web3.to_checksum_address(self.wallet_config.funder_address)
            nonce = await adapter.get_transaction_count(sender)

            # Get fee data and add a 20% buffer to gasPrice
            gas_price = await adapter.get_gas_price()
            adjusted_gas_price = int(gas_price * 1.20)

            usdc_address = self._config.network.usdc_contract_address
            try:
                tx = ctf_contract.functions.redeemPositions(
                    Web3.to_checksum_address(usdc_address),
                    PARENT_COLLECTION_ID,
                    cid_bytes32,
                    [1, 2],  # Redeem both outcome collections
                ).build_transaction(
                    {
                        "from": sender,
                        "nonce": nonce,
                        "gas": 500_000,
                        "gasPrice": adjusted_gas_price,
                    }
                )

                signed = await asyncio.to_thread(
                    w3.eth.account.sign_transaction,
                    tx,
                    self.wallet_config.private_key,
                )
                tx_hash = await adapter.send_raw_transaction(signed.raw_transaction)
                
                logger.info(f"Redemption submitted: {tx_hash} | Condition: {condition_id}")
                
                receipt = await adapter.wait_for_receipt(tx_hash, timeout=120)
                
                if int(receipt.get("status", 0)) != 1:
                    return RedemptionResult(
                        False, tx_hash, "reverted", Decimal("0"), "redeem_tx_reverted"
                    )

                logger.info(f"Redemption confirmed! Gas used: {receipt.get('gasUsed')}")
                return RedemptionResult(True, tx_hash, "confirmed", Decimal("0"), None)

            except Exception as exc:
                return RedemptionResult(False, None, "failed", Decimal("0"), str(exc))

    # Compatibility methods for tests / older callers (like negRiskArb)
    async def redeem(self, pos: RedeemablePosition) -> RedemptionResult:
        return await self.redeem_condition(pos.condition_id)

    async def reconcile_pending(self, user_id: str) -> list[RedemptionResult]:
        return []
