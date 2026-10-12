"""Read-only Polygon evidence and receipt attribution for settlement."""

from decimal import Decimal

import pytest
from web3 import Web3

from pmcopy.domain.redemption import (
    RedemptionMarket,
    SettlementAuthError,
    SettlementMarketError,
    SettlementUnavailable,
)
from pmcopy.integrations.polymarket.settlement_chain import PolygonSettlementReader
from pmcopy.integrations.polymarket.settlement_wallet import SettlementContracts
from tests.test_redemption_service import FakeClock

WALLET = "0x" + "1" * 40
CONDITION = "0x" + "ab" * 32
HASH = "0x" + "f" * 64
BLOCK_HASH = "0x" + "e" * 64


class FakeRpcHttp:
    """Answer actual JSON-RPC and ABI requests without duplicating reader logic."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.chain_id = "0x89"
        self.code = "0x1234"
        self.denominator = 1
        self.balances = (0, 5)
        self.approved = False
        self.receipt: dict | None = None
        self.finalized_number = 100

    async def post_json(self, url, *, json_body, **kwargs):
        """Dispatch known read methods and reject any accidental chain write."""
        self.requests.append(json_body)
        method = json_body["method"]
        params = json_body["params"]
        if method == "eth_chainId":
            result = self.chain_id
        elif method == "eth_getCode":
            result = self.code
        elif method == "eth_getBlockByNumber":
            result = {
                "number": hex(self.finalized_number),
                "timestamp": hex(1700000000),
                "hash": BLOCK_HASH,
            }
        elif method == "eth_getTransactionReceipt":
            result = self.receipt
        elif method == "eth_call":
            data = params[0]["data"]
            selector = data[:10]
            specifications = {
                "getOutcomeSlotCount(bytes32)": (["uint256"], [2]),
                "payoutDenominator(bytes32)": (["uint256"], [self.denominator]),
                "balanceOfBatch(address[],uint256[])": (["uint256[]"], [self.balances]),
                "isApprovedForAll(address,address)": (["bool"], [self.approved]),
            }
            if selector == "0x" + Web3.keccak(text="payoutNumerators(bytes32,uint256)")[:4].hex():
                index = Web3().codec.decode(["bytes32", "uint256"], bytes.fromhex(data[10:]))[1]
                types, values = ["uint256"], [1 if index == 0 and self.denominator else 0]
            else:
                matches = [
                    spec
                    for signature, spec in specifications.items()
                    if selector == "0x" + Web3.keccak(text=signature)[:4].hex()
                ]
                assert len(matches) == 1, "unexpected contract read"
                types, values = matches[0]
            result = "0x" + Web3().codec.encode(types, values).hex()
        else:
            pytest.fail(f"Unexpected RPC method: {method}")
        return {"jsonrpc": "2.0", "id": json_body["id"], "result": result}


class TestPolygonSettlementReader:
    """Require chain evidence rather than price marks or stale Data API balances."""

    @staticmethod
    def market() -> RedemptionMarket:
        """Return a standard binary condition."""
        return RedemptionMarket(CONDITION, ("123", "456"), False)

    @staticmethod
    def transfer(source: str, target: str, token: int, amount: int) -> dict:
        """Encode a CTF TransferSingle log independently of the reader."""
        return {
            "address": SettlementContracts.CTF,
            "topics": [
                "0x"
                + Web3.keccak(text="TransferSingle(address,address,address,uint256,uint256)").hex(),
                "0x" + "0" * 64,
                "0x" + source[2:].rjust(64, "0"),
                "0x" + target[2:].rjust(64, "0"),
            ],
            "data": "0x" + Web3().codec.encode(["uint256", "uint256"], [token, amount]).hex(),
        }

    async def test_reads_resolved_losing_dust_at_one_block(self) -> None:
        """Verify reads resolved losing dust at one block."""
        http = FakeRpcHttp()
        reader = PolygonSettlementReader(
            http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
        )
        await reader.require_wallet(WALLET)
        snapshot = await reader.snapshot(WALLET, self.market())
        assert snapshot.balances == (0, 5)
        assert snapshot.eligible and snapshot.expected_payout == Decimal(0)
        assert snapshot.approval_required
        assert all(
            request["params"][1] == "0x64"
            for request in http.requests
            if request["method"] == "eth_call"
        )

    @pytest.mark.parametrize("wrong_chain,empty_code", [(True, False), (False, True)])
    async def test_wrong_chain_or_missing_wallet_contract_blocks_execution(
        self, wrong_chain: bool, empty_code: bool
    ) -> None:
        """Verify wrong chain or missing wallet contract blocks execution."""
        http = FakeRpcHttp()
        http.chain_id = "0x1" if wrong_chain else "0x89"
        http.code = "0x" if empty_code else "0x1234"
        with pytest.raises(SettlementAuthError):
            await PolygonSettlementReader(
                http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
            ).require_wallet(WALLET)

    async def test_receipt_requires_wallet_outflow_and_matching_token_burn(self) -> None:
        """Verify receipt requires wallet outflow and matching token burn."""
        http = FakeRpcHttp()
        reader = PolygonSettlementReader(
            http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
        )
        snapshot = await reader.snapshot(WALLET, self.market())
        zero = "0x" + "0" * 40
        http.receipt = {
            "transactionHash": HASH,
            "blockHash": BLOCK_HASH,
            "blockNumber": "0x64",
            "status": "0x1",
            "to": SettlementContracts.FACTORY,
            "logs": [
                self.transfer(WALLET, SettlementContracts.STANDARD, 456, 5),
                self.transfer(SettlementContracts.STANDARD, zero, 456, 5),
            ],
        }
        assert await reader.receipt_payout(WALLET, snapshot, HASH) == 0
        http.receipt["logs"].pop()
        with pytest.raises(SettlementUnavailable):
            await reader.receipt_payout(WALLET, snapshot, HASH)

    async def test_other_wallet_burn_and_wrong_token_do_not_confirm_our_redemption(self) -> None:
        """Verify other wallet burn and wrong token do not confirm our redemption."""
        http = FakeRpcHttp()
        reader = PolygonSettlementReader(
            http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
        )
        snapshot = await reader.snapshot(WALLET, self.market())
        http.receipt = {
            "transactionHash": HASH,
            "blockHash": BLOCK_HASH,
            "blockNumber": "0x64",
            "status": "0x1",
            "to": SettlementContracts.FACTORY,
            "logs": [
                self.transfer("0x" + "2" * 40, "0x" + "0" * 40, 123, 100),
            ],
        }
        with pytest.raises(SettlementUnavailable):
            await reader.receipt_payout(WALLET, snapshot, HASH)

    async def test_unmined_receipt_remains_pending(self) -> None:
        """Verify unmined receipt remains pending."""
        http = FakeRpcHttp()
        reader = PolygonSettlementReader(
            http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
        )
        snapshot = await reader.snapshot(WALLET, self.market())
        assert await reader.receipt_payout(WALLET, snapshot, HASH) is None

    async def test_winning_batch_burn_requires_exact_pusd_credit_and_finality(self) -> None:
        """A winning claim must actually credit the follower, not just burn shares."""
        http = FakeRpcHttp()
        http.balances = (2_000_001, 5)
        reader = PolygonSettlementReader(
            http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
        )
        snapshot = await reader.snapshot(WALLET, self.market())
        batch = self.transfer(WALLET, SettlementContracts.STANDARD, 123, 2_000_001)
        batch["topics"][0] = (
            "0x"
            + Web3.keccak(text="TransferBatch(address,address,address,uint256[],uint256[])").hex()
        )
        batch["data"] = (
            "0x"
            + Web3().codec.encode(["uint256[]", "uint256[]"], [[123, 456], [2_000_001, 5]]).hex()
        )
        credit = {
            "address": SettlementContracts.PUSD,
            "topics": [
                "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex(),
                "0x" + "0" * 64,
                "0x" + WALLET[2:].rjust(64, "0"),
            ],
            "data": "0x" + Web3().codec.encode(["uint256"], [2_000_001]).hex(),
        }
        http.receipt = {
            "transactionHash": HASH,
            "blockHash": BLOCK_HASH,
            "blockNumber": "0x64",
            "status": "0x1",
            "to": SettlementContracts.FACTORY,
            "logs": [
                batch,
                self.transfer(SettlementContracts.STANDARD, "0x" + "0" * 40, 123, 2_000_001),
                self.transfer(SettlementContracts.STANDARD, "0x" + "0" * 40, 456, 5),
                credit,
            ],
        }
        assert await reader.receipt_payout(WALLET, snapshot, HASH) == 2_000_001
        http.finalized_number = 99
        assert await reader.receipt_payout(WALLET, snapshot, HASH) is None
        http.finalized_number = 100
        http.receipt["blockHash"] = "0x" + "a" * 64
        with pytest.raises(SettlementUnavailable, match="canonical"):
            await reader.receipt_payout(WALLET, snapshot, HASH)
        http.receipt["blockHash"] = BLOCK_HASH
        http.receipt["status"] = "0x0"
        with pytest.raises(SettlementMarketError, match="reverted"):
            await reader.receipt_payout(WALLET, snapshot, HASH)
        http.receipt["status"] = "0x1"
        credit["data"] = "0x" + Web3().codec.encode(["uint256"], [2_000_000]).hex()
        with pytest.raises(SettlementUnavailable, match="payout_evidence"):
            await reader.receipt_payout(WALLET, snapshot, HASH)

    async def test_negative_risk_receipt_requires_the_full_legacy_adapter_transfer_path(
        self,
    ) -> None:
        """Negative-risk tokens burn at the legacy adapter after two transfers."""
        http = FakeRpcHttp()
        reader = PolygonSettlementReader(
            http, "https://rpc.example", clock=FakeClock(), jitter=lambda low, high: low
        )
        snapshot = await reader.snapshot(WALLET, RedemptionMarket(CONDITION, ("123", "456"), True))
        legacy = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
        http.receipt = {
            "transactionHash": HASH,
            "blockHash": BLOCK_HASH,
            "blockNumber": "0x64",
            "status": "0x1",
            "to": SettlementContracts.FACTORY,
            "logs": [
                self.transfer(WALLET, SettlementContracts.NEGATIVE_RISK, 456, 5),
                self.transfer(SettlementContracts.NEGATIVE_RISK, legacy, 456, 5),
                self.transfer(legacy, "0x" + "0" * 40, 456, 5),
            ],
        }
        assert await reader.receipt_payout(WALLET, snapshot, HASH) == 0
        http.receipt["logs"].pop(1)
        with pytest.raises(SettlementUnavailable):
            await reader.receipt_payout(WALLET, snapshot, HASH)

    async def test_rpc_errors_never_echo_endpoint_or_body(self) -> None:
        """Verify rpc errors never echo endpoint or body."""

        class BrokenRpc:
            """Return an untrusted provider error body."""

            async def post_json(self, *args, **kwargs):
                """Simulate a provider exception containing a credential."""
                raise RuntimeError("SYNTHETIC_RPC_SECRET")

        with pytest.raises(SettlementUnavailable) as error:
            await PolygonSettlementReader(
                BrokenRpc(),
                "https://rpc.example/secret",
                clock=FakeClock(),
                jitter=lambda low, high: low,
            ).finalized_timestamp()
        assert "SYNTHETIC_RPC_SECRET" not in str(error.value)
