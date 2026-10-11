"""Polymarket Deposit Wallet and collateral-adapter wire contracts.

Wallet derivation golden vectors come from Polymarket/py-sdk revision
579bb2e56be9cc5d152546985870ee6ad795ec52, test_wallet_derivations.py.
The SDK is a reference only and is never imported or installed.
"""

from dataclasses import replace

from eth_account import Account
from eth_account.messages import encode_typed_data
from pydantic import SecretStr
import pytest
from web3 import Web3

from domain.redemption import RedemptionMarket, RedemptionSnapshot
from integrations.polymarket.settlement_wallet import DepositWalletCodec, SettlementContracts


class TestDepositWalletCodec:
    """Verify identity, collateral calls, and signatures independently of HTTP."""

    @staticmethod
    def snapshot(*, negative_risk: bool = False, approval: bool = True) -> RedemptionSnapshot:
        """Build a losing-only holding to prevent payout-based filtering."""
        market = RedemptionMarket("0x" + "ab" * 32, ("123", "456"), negative_risk)
        return RedemptionSnapshot(market, (0, 5), (1, 0), 1, approval)

    def test_both_deposit_wallet_generations_match_upstream_vectors(self) -> None:
        """Do not infer an EOA or Safe address as the type-3 funder."""
        assert tuple(
            address.lower()
            for address in DepositWalletCodec.wallet_addresses(
                "0x0000000000000000000000000000000000000001"
            )
        ) == (
            "0x57ffbc34de23124faeb8387fcd689d314e57accd",
            "0x94bf330955a0b957662feaf878de77bf25f76cd9",
        )

    @pytest.mark.parametrize("negative_risk", [False, True])
    def test_missing_approval_is_granted_redeemed_and_restored(self, negative_risk: bool) -> None:
        """Only the matching collateral adapter gets temporary ERC1155 authority."""
        calls = DepositWalletCodec.calls(self.snapshot(negative_risk=negative_risk))
        adapter = (
            "0xadA2005600Dec949baf300f4C6120000bDB6eAab"
            if negative_risk
            else "0xAdA100Db00Ca00073811820692005400218FcE1f"
        )
        assert [call.target.lower() for call in calls] == [
            SettlementContracts.CTF.lower(),
            adapter.lower(),
            SettlementContracts.CTF.lower(),
        ]
        codec = Web3().codec
        for call, approved in ((calls[0], True), (calls[2], False)):
            assert call.data[:10] == "0xa22cb465"
            assert codec.decode(["address", "bool"], bytes.fromhex(call.data[10:])) == (
                adapter.lower(),
                approved,
            )
        assert calls[1].data[:10] == "0x01b7037c"
        collateral, parent, condition, index_sets = codec.decode(
            ["address", "bytes32", "bytes32", "uint256[]"], bytes.fromhex(calls[1].data[10:])
        )
        assert collateral == "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
        assert parent == bytes(32)
        assert condition == bytes.fromhex("ab" * 32)
        assert index_sets == (1, 2)
        assert all(call.value == 0 for call in calls)

    def test_existing_approval_is_left_unchanged(self) -> None:
        """Existing wallet permissions are neither replaced nor revoked."""
        assert len(DepositWalletCodec.calls(self.snapshot(approval=False))) == 1

    def test_unresolved_market_never_produces_calls(self) -> None:
        """A locally constructed unresolved snapshot is also rejected."""
        unresolved = replace(self.snapshot(), payout_numerators=(0, 0), payout_denominator=0)
        with pytest.raises(ValueError, match="eligible"):
            DepositWalletCodec.calls(unresolved)

    def test_batch_signature_binds_wallet_calls_nonce_deadline_and_chain(self) -> None:
        """Recover the signature against independently constructed EIP-712 data."""
        signer = DepositWalletCodec(SecretStr("0x" + "a" * 64))
        wallet = signer.wallet_addresses(signer.signer_address)[1]
        calls = signer.calls(self.snapshot(approval=False))
        batch = signer.sign(wallet, calls, nonce=7, deadline=1700000600)
        typed = {
            "domain": {
                "name": "DepositWallet",
                "version": "1",
                "chainId": 137,
                "verifyingContract": wallet,
            },
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
            "primaryType": "Batch",
            "message": {
                "wallet": wallet,
                "nonce": 7,
                "deadline": 1700000600,
                "calls": [{"target": calls[0].target, "value": 0, "data": calls[0].data}],
            },
        }
        assert (
            Account.recover_message(
                encode_typed_data(full_message=typed), signature=batch.payload["signature"]
            )
            == signer.signer_address
        )
        assert batch.authorization.nonce == 7
        assert batch.payload["type"] == "WALLET"
        assert batch.payload["to"] == "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
        assert batch.payload["depositWalletParams"]["depositWallet"] == wallet
        assert batch.payload["signature"] not in repr(batch)
        assert signer.sign(wallet, calls, nonce=8, deadline=1700000600).authorization.digest != (
            batch.authorization.digest
        )

    def test_foreign_funder_and_eoa_fail_before_signing(self) -> None:
        """Ownership validation is explicit even if a private key exists."""
        signer = DepositWalletCodec(SecretStr("0x" + "a" * 64))
        for wallet in ("0x" + "1" * 40, signer.signer_address):
            with pytest.raises(ValueError, match="owner"):
                signer.require_owner(wallet)

    def test_invalid_key_error_does_not_echo_secret(self) -> None:
        """Signing errors expose actionable categories rather than key material."""
        with pytest.raises(ValueError) as error:
            DepositWalletCodec(SecretStr("SYNTHETIC_SECRET_MARKER"))
        assert "SYNTHETIC_SECRET_MARKER" not in str(error.value)
