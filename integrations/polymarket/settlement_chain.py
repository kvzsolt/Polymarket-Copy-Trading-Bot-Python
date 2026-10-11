"""Read-only Polygon settlement evidence; this adapter cannot broadcast transactions."""

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import re

from web3 import Web3

from domain.redemption import (
    RedemptionMarket,
    RedemptionSnapshot,
    SettlementAuthError,
    SettlementMarketError,
    SettlementUnavailable,
)
from integrations.polymarket.settlement_wallet import DepositWalletCodec, SettlementContracts
from integrations.polymarket.settlement_rpc import SettlementRpcClient, SettlementRpcTransport
from interfaces.settlement import SettlementClock


@dataclass(frozen=True, slots=True)
class SettlementBlock:
    """Anchor balance reads or finality checks to one canonical chain block."""

    number: int
    timestamp: int
    block_hash: str


class PolygonSettlementReader:
    """Validate resolution, balances, deployment, finality, burns, and collateral receipts."""

    def __init__(
        self,
        http: SettlementRpcTransport,
        rpc_url: str,
        fallback_rpc_url: str = "",
        *,
        clock: SettlementClock,
        jitter: Callable[[float, float], float],
    ) -> None:
        """Inject a bounded read transport; endpoint secrets never enter errors."""
        if not rpc_url:
            raise ValueError("A Polygon RPC endpoint is required for redemption")
        self._rpc_client = SettlementRpcClient(
            http, tuple(url for url in (rpc_url, fallback_rpc_url) if url), clock, jitter
        )

    async def require_wallet(self, wallet: str) -> None:
        """Require Polygon mainnet and a deployed contract at the explicit funder."""
        if re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet) is None:
            raise SettlementAuthError("invalid_deposit_wallet")
        chain = self._hex_integer(await self._rpc("eth_chainId", []))
        if chain != SettlementContracts.CHAIN_ID:
            raise SettlementAuthError("redemption_requires_polygon_mainnet")
        code = await self._rpc("eth_getCode", [wallet, "latest"])
        if not isinstance(code, str) or re.fullmatch(r"0x[0-9a-fA-F]+", code) is None:
            raise SettlementAuthError("deposit_wallet_not_deployed")

    async def finalized_timestamp(self) -> int:
        """Return finalized chain time, never a local-clock expiry guess."""
        return (await self._block("finalized")).timestamp

    async def latest_timestamp(self) -> int:
        """Return chain time when choosing a fresh wallet-batch deadline."""
        return (await self._block("latest")).timestamp

    async def snapshot(self, wallet: str, market: RedemptionMarket) -> RedemptionSnapshot:
        """Read both share balances and all payout evidence at one block height."""
        block = await self._block("latest")
        tag = hex(block.number)
        condition = bytes.fromhex(market.condition_id[2:])
        adapter = (
            SettlementContracts.NEGATIVE_RISK
            if market.negative_risk
            else SettlementContracts.STANDARD
        )
        slots, denominator, balances, approved = await asyncio.gather(
            self._call("getOutcomeSlotCount(bytes32)", ["bytes32"], [condition], ["uint256"], tag),
            self._call("payoutDenominator(bytes32)", ["bytes32"], [condition], ["uint256"], tag),
            self._call(
                "balanceOfBatch(address[],uint256[])",
                ["address[]", "uint256[]"],
                [[wallet, wallet], [int(token) for token in market.token_ids]],
                ["uint256[]"],
                tag,
            ),
            self._call(
                "isApprovedForAll(address,address)",
                ["address", "address"],
                [wallet, adapter],
                ["bool"],
                tag,
            ),
        )
        if slots != (2,):
            raise SettlementMarketError("unsupported_or_unknown_condition")
        numerators = await asyncio.gather(
            *(
                self._call(
                    "payoutNumerators(bytes32,uint256)",
                    ["bytes32", "uint256"],
                    [condition, index],
                    ["uint256"],
                    tag,
                )
                for index in (0, 1)
            )
        )
        try:
            return RedemptionSnapshot(
                market,
                balances[0],
                (numerators[0][0], numerators[1][0]),
                denominator[0],
                not approved[0],
            )
        except (ValueError, TypeError, IndexError):
            raise SettlementUnavailable("invalid_chain_settlement_evidence") from None

    async def receipt_payout(
        self, wallet: str, snapshot: RedemptionSnapshot, transaction_hash: str
    ) -> int | None:
        """Return exact pUSD credit only after canonical finalized burn evidence.

        Args:
            wallet: Deposit Wallet that held the redeemed tokens.
            snapshot: Persisted balances and identities from the signed attempt.
            transaction_hash: Relayer-provided chain reference.

        Returns:
            Integer pUSD micro-units, or None when the receipt is not finalized.

        Raises:
            SettlementMarketError: When a mined transaction definitively reverted.
            SettlementUnavailable: When receipt identity or burn evidence is invalid.
        """
        if re.fullmatch(r"0x[0-9a-fA-F]{64}", transaction_hash) is None:
            raise SettlementUnavailable("invalid_settlement_transaction_hash")
        raw = await self._rpc("eth_getTransactionReceipt", [transaction_hash])
        if raw is None:
            return None
        receipt = self._mapping(raw)
        if str(receipt.get("transactionHash", "")).lower() != transaction_hash.lower():
            raise SettlementUnavailable("settlement_receipt_identity_mismatch")
        number = self._hex_integer(receipt.get("blockNumber"))
        if (await self._block("finalized")).number < number:
            return None
        canonical = await self._block(hex(number))
        if canonical.block_hash.lower() != str(receipt.get("blockHash", "")).lower():
            raise SettlementUnavailable("settlement_receipt_not_canonical")
        status = self._hex_integer(receipt.get("status"))
        if status == 0:
            raise SettlementMarketError("settlement_transaction_reverted")
        if status != 1 or str(receipt.get("to", "")).lower() != SettlementContracts.FACTORY.lower():
            raise SettlementUnavailable("settlement_receipt_target_mismatch")
        return self._payout_from_logs(wallet, snapshot, receipt.get("logs"))

    async def _call(
        self, signature: str, types: list[str], values: list[object], returns: list[str], tag: str
    ) -> tuple:
        data = DepositWalletCodec.encode(signature, types, values)
        raw = await self._rpc("eth_call", [{"to": SettlementContracts.CTF, "data": data}, tag])
        try:
            if not isinstance(raw, str) or not raw.startswith("0x"):
                raise ValueError
            return Web3().codec.decode(returns, bytes.fromhex(raw[2:]))
        except Exception:
            raise SettlementUnavailable("invalid_settlement_contract_response") from None

    async def _block(self, tag: str) -> SettlementBlock:
        block = self._mapping(await self._rpc("eth_getBlockByNumber", [tag, False]))
        block_hash = block.get("hash")
        if (
            not isinstance(block_hash, str)
            or re.fullmatch(r"0x[0-9a-fA-F]{64}", block_hash) is None
        ):
            raise SettlementUnavailable("invalid_settlement_block")
        return SettlementBlock(
            self._hex_integer(block.get("number")),
            self._hex_integer(block.get("timestamp")),
            block_hash,
        )

    async def _rpc(self, method: str, params: list[object]) -> object:
        return await self._rpc_client.read(method, params)

    @staticmethod
    def _mapping(value: object) -> Mapping[str, object]:
        if not isinstance(value, dict):
            raise SettlementUnavailable("invalid_settlement_response")
        return value

    @staticmethod
    def _hex_integer(value: object) -> int:
        if not isinstance(value, str) or re.fullmatch(r"0x[0-9a-fA-F]+", value) is None:
            raise SettlementUnavailable("invalid_settlement_chain_integer")
        return int(value, 16)

    @classmethod
    def _payout_from_logs(cls, wallet: str, snapshot: RedemptionSnapshot, raw: object) -> int:
        if not isinstance(raw, list) or len(raw) > 10000:
            raise SettlementUnavailable("invalid_settlement_receipt_logs")
        adapter = (
            SettlementContracts.NEGATIVE_RISK
            if snapshot.market.negative_risk
            else SettlementContracts.STANDARD
        ).lower()
        owner = wallet.lower()
        zero = "0x" + "0" * 40
        outflows: dict[int, int] = {}
        burns: dict[int, int] = {}
        forwarded: dict[int, int] = {}
        legacy_burns: dict[int, int] = {}
        legacy = SettlementContracts.LEGACY_NEGATIVE_RISK.lower()
        credit = 0
        single = (
            "0x" + Web3.keccak(text="TransferSingle(address,address,address,uint256,uint256)").hex()
        )
        batch = (
            "0x"
            + Web3.keccak(text="TransferBatch(address,address,address,uint256[],uint256[])").hex()
        )
        fungible = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()
        for raw_log in raw:
            log = cls._mapping(raw_log)
            address = str(log.get("address", "")).lower()
            if address not in (SettlementContracts.CTF.lower(), SettlementContracts.PUSD.lower()):
                continue
            topics = log.get("topics")
            data = log.get("data")
            if not isinstance(topics, list) or not topics or not isinstance(data, str):
                raise SettlementUnavailable("invalid_settlement_event")
            try:
                if address == SettlementContracts.PUSD.lower() and topics[0] == fungible:
                    if len(topics) != 3:
                        raise ValueError
                    if cls._topic_address(topics[2]) == owner:
                        credit += Web3().codec.decode(["uint256"], bytes.fromhex(data[2:]))[0]
                elif address == SettlementContracts.CTF.lower() and topics[0] in (single, batch):
                    if len(topics) != 4:
                        raise ValueError
                    source, target = cls._topic_address(topics[2]), cls._topic_address(topics[3])
                    if topics[0] == single:
                        token, size = Web3().codec.decode(
                            ["uint256", "uint256"], bytes.fromhex(data[2:])
                        )
                        transfers = ((token, size),)
                    else:
                        tokens, sizes = Web3().codec.decode(
                            ["uint256[]", "uint256[]"], bytes.fromhex(data[2:])
                        )
                        transfers = tuple(zip(tokens, sizes, strict=True))
                    for token, size in transfers:
                        if source == owner and target in (adapter, zero):
                            outflows[token] = outflows.get(token, 0) + size
                        if source in (owner, adapter) and target == zero:
                            burns[token] = burns.get(token, 0) + size
                        if snapshot.market.negative_risk:
                            if source == adapter and target == legacy:
                                forwarded[token] = forwarded.get(token, 0) + size
                            if source == legacy and target == zero:
                                legacy_burns[token] = legacy_burns.get(token, 0) + size
            except Exception:
                raise SettlementUnavailable("invalid_settlement_event") from None
        for token, size in zip(snapshot.market.token_ids, snapshot.balances, strict=True):
            token_id = int(token)
            burned = burns.get(token_id, 0) + min(
                forwarded.get(token_id, 0), legacy_burns.get(token_id, 0)
            )
            if outflows.get(token_id, 0) < size or burned < size:
                raise SettlementUnavailable("settlement_burn_evidence_incomplete")
        expected = sum(
            size * payout // snapshot.payout_denominator
            for size, payout in zip(snapshot.balances, snapshot.payout_numerators, strict=True)
        )
        if credit < expected:
            raise SettlementUnavailable("settlement_payout_evidence_incomplete")
        return credit

    @staticmethod
    def _topic_address(value: object) -> str:
        if not isinstance(value, str) or re.fullmatch(r"0x0{24}[0-9a-fA-F]{40}", value) is None:
            raise ValueError("Invalid address topic")
        return "0x" + value[-40:].lower()
