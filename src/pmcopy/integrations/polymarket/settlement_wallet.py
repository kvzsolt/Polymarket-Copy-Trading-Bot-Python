"""Deposit Wallet settlement encoding, isolated from CCXT order execution.

Contract identities and Solady clone layouts are pinned to Polymarket's
September 2026 documentation and py-sdk revision
579bb2e56be9cc5d152546985870ee6ad795ec52. No SDK code is imported.
"""

from dataclasses import dataclass, field
from typing import ClassVar

from eth_account import Account
from eth_account.messages import encode_typed_data
from pydantic import SecretStr
from web3 import Web3

from pmcopy.domain.redemption import RedemptionAuthorization, RedemptionSnapshot


class SettlementContracts:
    """Pin Polygon settlement targets independently of any trading client."""

    CHAIN_ID: ClassVar[int] = 137
    CTF: ClassVar[str] = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    PUSD: ClassVar[str] = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
    STANDARD: ClassVar[str] = "0xAdA100Db00Ca00073811820692005400218FcE1f"
    NEGATIVE_RISK: ClassVar[str] = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"
    LEGACY_NEGATIVE_RISK: ClassVar[str] = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
    FACTORY: ClassVar[str] = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
    IMPLEMENTATION: ClassVar[str] = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB"
    BEACON: ClassVar[str] = "0x7A18EDfe055488A3128f01F563e5B479D92ffc3a"


@dataclass(frozen=True, slots=True)
class WalletCall:
    """Represent a zero-value contract call inside an atomic settlement batch."""

    target: str
    data: str
    value: int = 0


@dataclass(frozen=True, slots=True)
class SignedSettlementBatch:
    """Keep the submission signature private while exposing its journal identity."""

    authorization: RedemptionAuthorization
    payload: dict[str, object] = field(repr=False)


class DepositWalletCodec:
    """Encode redemption calls and sign only verified owner Deposit Wallet batches."""

    def __init__(self, private_key: SecretStr) -> None:
        """Create an owner signer after the command has passed its live gate."""
        try:
            self._account = Account.from_key(private_key.get_secret_value())
        except (ValueError, TypeError):
            raise ValueError("A valid wallet private key is required for redemption") from None

    @property
    def signer_address(self) -> str:
        """Return the EOA owner address, never the funder identity."""
        return self._account.address

    @staticmethod
    def encode(signature: str, types: list[str], values: list[object]) -> str:
        """Encode a reviewed ABI call using the existing Web3 codec."""
        selector = Web3.keccak(text=signature)[:4]
        return "0x" + (selector + Web3().codec.encode(types, values)).hex()

    @classmethod
    def wallet_addresses(cls, signer: str) -> tuple[str, str]:
        """Return UUPS and beacon factory derivations solely to verify a funder.

        Args:
            signer: EOA owner whose known configured funder must be verified.

        Returns:
            Both officially deployed Deposit Wallet generations for that owner.
        """
        address = Web3.to_checksum_address(signer)
        factory = bytes.fromhex(SettlementContracts.FACTORY[2:])
        args = Web3().codec.encode(
            ["address", "bytes32"],
            [SettlementContracts.FACTORY, bytes.fromhex(address[2:]).rjust(32, b"\x00")],
        )
        uups = (
            (0x61003D3D8160233D3973 + (len(args) << 56)).to_bytes(10, "big")
            + bytes.fromhex(SettlementContracts.IMPLEMENTATION[2:])
            + bytes.fromhex(
                "60095155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076"
                "cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3"
            )
            + args
        )
        beacon = (
            (0x6100523D8160233D3973 + (len(args) << 56)).to_bytes(10, "big")
            + bytes.fromhex(SettlementContracts.BEACON[2:])
            + bytes.fromhex(
                "60195155f3363d3d373d3d363d602036600436635c60da"
                "1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c"
                "b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3"
            )
            + args
        )
        prefix = b"\xff" + factory + Web3.keccak(args)
        return (
            Web3.to_checksum_address(Web3.keccak(prefix + Web3.keccak(uups))[-20:]),
            Web3.to_checksum_address(Web3.keccak(prefix + Web3.keccak(beacon))[-20:]),
        )

    def require_owner(self, wallet: str) -> None:
        """Reject foreign funders, legacy wallets, and unsupported session signers."""
        if wallet.lower() not in tuple(
            value.lower() for value in self.wallet_addresses(self.signer_address)
        ):
            raise ValueError("Configured Deposit Wallet does not match the signing owner")

    @classmethod
    def calls(cls, snapshot: RedemptionSnapshot) -> tuple[WalletCall, ...]:
        """Redeem both outcomes and restore any temporarily granted approval."""
        if not snapshot.eligible:
            raise ValueError("Redemption snapshot is not eligible")
        adapter = (
            SettlementContracts.NEGATIVE_RISK
            if snapshot.market.negative_risk
            else SettlementContracts.STANDARD
        )
        redemption = WalletCall(
            adapter,
            cls.encode(
                "redeemPositions(address,bytes32,bytes32,uint256[])",
                ["address", "bytes32", "bytes32", "uint256[]"],
                [
                    SettlementContracts.PUSD,
                    bytes(32),
                    bytes.fromhex(snapshot.market.condition_id[2:]),
                    [1, 2],
                ],
            ),
        )
        if not snapshot.approval_required:
            return (redemption,)
        return (
            WalletCall(
                SettlementContracts.CTF,
                cls.encode("setApprovalForAll(address,bool)", ["address", "bool"], [adapter, True]),
            ),
            redemption,
            WalletCall(
                SettlementContracts.CTF,
                cls.encode(
                    "setApprovalForAll(address,bool)", ["address", "bool"], [adapter, False]
                ),
            ),
        )

    def sign(
        self, wallet: str, calls: tuple[WalletCall, ...], *, nonce: int, deadline: int
    ) -> SignedSettlementBatch:
        """Sign one chain-bound EIP-712 batch without any HTTP or RPC write.

        Args:
            wallet: Explicit, owner-verified Deposit Wallet funder.
            calls: Reviewed settlement calls, all with zero native value.
            nonce: Fresh Relayer wallet nonce.
            deadline: Future authorization expiry in Unix seconds.

        Returns:
            Private submission envelope and non-secret durable identity.
        """
        self.require_owner(wallet)
        typed_calls = [
            {"target": call.target, "value": call.value, "data": call.data} for call in calls
        ]
        typed = {
            "domain": {
                "name": "DepositWallet",
                "version": "1",
                "chainId": 137,
                "verifyingContract": wallet,
            },
            "primaryType": "Batch",
            "types": {
                "Call": [
                    {"name": "target", "type": "address"},
                    {"name": "value", "type": "uint256"},
                    {"name": "data", "type": "bytes"},
                ],
                "Batch": [
                    {"name": "wallet", "type": "address"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "deadline", "type": "uint256"},
                    {"name": "calls", "type": "Call[]"},
                ],
            },
            "message": {
                "wallet": wallet,
                "nonce": nonce,
                "deadline": deadline,
                "calls": typed_calls,
            },
        }
        signed = self._account.sign_message(encode_typed_data(full_message=typed))
        authorization = RedemptionAuthorization(nonce, deadline, "0x" + signed.message_hash.hex())
        return SignedSettlementBatch(
            authorization,
            {
                "type": "WALLET",
                "from": self.signer_address,
                "to": SettlementContracts.FACTORY,
                "nonce": str(nonce),
                "signature": "0x" + signed.signature.hex(),
                "metadata": "pmcopy redeem " + authorization.digest,
                "depositWalletParams": {
                    "depositWallet": wallet,
                    "deadline": str(deadline),
                    "calls": [
                        {"target": call.target, "value": str(call.value), "data": call.data}
                        for call in calls
                    ],
                },
            },
        )
