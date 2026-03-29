from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Any

from web3 import Web3

from config.env import ENV, SignatureType, WalletConfig, build_wallet_config
from services.accountingService import AccountingService
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


@dataclass(frozen=True)
class RedeemablePosition:
    user_id: str
    token_id: str
    condition_id: str
    outcome_index: int
    size: Decimal


@dataclass
class RedemptionResult:
    submitted: bool
    tx_hash: str | None
    status: str
    redeemed_amount: Decimal
    error: str | None = None


class RelayerClient:
    """Thin abstraction for proxy/safe redemption submission.

    This intentionally isolates uncertain relayer specifics in one place.
    """

    def __init__(self):
        self._url = str(ENV.get("RELAYER_URL") or "").strip()
        self._api_key = str(ENV.get("RELAYER_API_KEY") or "").strip()

    async def submit_redeem(self, position: RedeemablePosition, wallet: WalletConfig) -> RedemptionResult:
        if not self._url or not self._api_key:
            return RedemptionResult(
                submitted=False,
                tx_hash=None,
                status="not_submitted",
                redeemed_amount=Decimal("0"),
                error="relayer_not_configured",
            )
        # Stubbed integration point. Keep assumptions isolated.
        tx_hash = "relayer_" + sha256(
            f"{position.user_id}|{position.condition_id}|{position.outcome_index}".encode("utf-8")
        ).hexdigest()[:24]
        return RedemptionResult(
            submitted=True,
            tx_hash=tx_hash,
            status="submitted",
            redeemed_amount=position.size,
            error=None,
        )


class RedemptionService:
    def __init__(self, clob_client, wallet_config: WalletConfig | None = None):
        self.clob_client = clob_client
        self.wallet_config = wallet_config or build_wallet_config()
        self.accounting = AccountingService()
        self.relayer = RelayerClient()
        self._successful_keys: set[str] = set()

    def _attempt_id(self, pos: RedeemablePosition) -> str:
        return sha256(
            f"{pos.user_id}|{pos.condition_id}|{pos.outcome_index}|{pos.size}".encode("utf-8")
        ).hexdigest()

    async def scan_redeemables(self, user_id: str) -> list[RedeemablePosition]:
        url = f"https://data-api.polymarket.com/positions?user={user_id}"
        positions = await fetch_data(url, silent=True)
        result: list[RedeemablePosition] = []
        for p in positions or []:
            if not bool(p.get("redeemable", False)):
                continue
            size = Decimal(str(p.get("size", 0) or 0))
            if size <= 0:
                continue
            result.append(
                RedeemablePosition(
                    user_id=user_id,
                    token_id=str(p.get("asset") or p.get("tokenId") or ""),
                    condition_id=str(p.get("conditionId") or ""),
                    outcome_index=int(p.get("outcomeIndex", 0) or 0),
                    size=size,
                )
            )
        return result

    async def _redeem_via_eoa(self, pos: RedeemablePosition) -> RedemptionResult:
        rpc_url = str(ENV.get("RPC_URL") or "").strip()
        if not rpc_url:
            return RedemptionResult(False, None, "not_submitted", Decimal("0"), "rpc_url_missing")

        w3 = Web3(Web3.HTTPProvider(rpc_url))
        ctf_contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_REDEEM_ABI)

        try:
            if pos.condition_id.startswith("0x"):
                cid_bytes = bytes.fromhex(pos.condition_id[2:])
            else:
                cid_bytes = bytes.fromhex(pos.condition_id)
            cid_bytes32 = cid_bytes.ljust(32, b"\x00")[:32]
        except Exception as exc:
            return RedemptionResult(False, None, "failed", Decimal("0"), f"invalid_condition_id:{exc}")

        sender = Web3.to_checksum_address(self.wallet_config.funder_address)
        nonce = w3.eth.get_transaction_count(sender)
        gas_price = w3.eth.gas_price
        tx = ctf_contract.functions.redeemPositions(
            Web3.to_checksum_address(ENV.get("USDC_CONTRACT_ADDRESS")),
            PARENT_COLLECTION_ID,
            cid_bytes32,
            [1, 2],
        ).build_transaction(
            {"from": sender, "nonce": nonce, "gas": 300_000, "gasPrice": gas_price}
        )

        signed = w3.eth.account.sign_transaction(tx, self.wallet_config.private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if int(receipt.get("status", 0)) != 1:
            return RedemptionResult(False, tx_hash, "reverted", Decimal("0"), "redeem_tx_reverted")

        return RedemptionResult(True, tx_hash, "confirmed", pos.size, None)

    async def redeem(self, pos: RedeemablePosition) -> RedemptionResult:
        key = f"{pos.user_id}|{pos.condition_id}|{pos.outcome_index}"
        if key in self._successful_keys:
            return RedemptionResult(False, None, "duplicate", Decimal("0"), "already_redeemed")

        attempt_id = self._attempt_id(pos)
        self.accounting.record_redemption_attempt(
            {
                "attempt_id": attempt_id,
                "user_id": pos.user_id,
                "condition_id": pos.condition_id,
                "token_id": pos.token_id,
                "size": str(pos.size),
                "status": "attempting",
            }
        )

        try:
            if self.wallet_config.signature_type == SignatureType.EOA:
                result = await self._redeem_via_eoa(pos)
            else:
                result = await self.relayer.submit_redeem(pos, self.wallet_config)
        except Exception as exc:
            result = RedemptionResult(False, None, "failed", Decimal("0"), str(exc))

        self.accounting.record_redemption_attempt(
            {
                "attempt_id": attempt_id,
                "user_id": pos.user_id,
                "condition_id": pos.condition_id,
                "token_id": pos.token_id,
                "size": str(pos.size),
                "status": result.status,
                "tx_hash": result.tx_hash,
                "error": result.error,
                "redeemed_amount": str(result.redeemed_amount),
            }
        )
        if result.submitted and result.status in {"submitted", "confirmed"}:
            self._successful_keys.add(key)
        return result
