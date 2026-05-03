"""Tests for services/redemption/wallet_validator.py"""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from config.env import SignatureType, WalletConfig
from services.redemption.wallet_validator import (
    SUPPORTED_PROXY_SIGNATURE_TYPES,
    WalletValidationResult,
    derive_signer_address,
    validate_redemption_wallet_mode,
)


# --- Helpers ---

def _make_wallet(
    private_key: str = "0x" + "ab" * 32,
    funder_address: str = "0xABCDEF0000000000000000000000000000000000",
    signature_type: SignatureType = SignatureType.EOA,
) -> WalletConfig:
    return WalletConfig(
        private_key=private_key,
        funder_address=funder_address,
        signature_type=signature_type,
        chain_id=137,
        polygon_rpc_url="https://polygon-rpc.example.com",
    )


# --- Tests: derive_signer_address ---


class TestDeriveSignerAddress:
    def test_empty_return_when_empty_key(self):
        assert derive_signer_address("") == ""

    def test_empty_return_when_derivation_raises(self):
        with patch("eth_account.Account.from_key", side_effect=Exception("crypto boom")):
            result = derive_signer_address("0x" + "cd" * 32)
            assert result == ""


# --- Tests: validate_redemption_wallet_mode ---


class TestEoaValidation:
    def test_eoa_matching_addresses_passes(self):
        addr = "0xAbCdEf0000000000000000000000000000000000"
        fake_key = "0x" + "ae" * 32

        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value=addr,
        ):
            cfg = _make_wallet(
                private_key=fake_key,
                funder_address=addr.upper(),
                signature_type=SignatureType.EOA,
            )
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is True
            assert result.reason == "eoa_signer_matches_funder"
            assert result.signature_type == SignatureType.EOA

    def test_eoa_mismatch_fails(self):
        signer = "0xAbCdEf0000000000000000000000000000000001"
        funder = "0xAbCdEf0000000000000000000000000000000002"

        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value=signer,
        ):
            cfg = _make_wallet(
                private_key="0x" + "be" * 32,
                funder_address=funder,
                signature_type=SignatureType.EOA,
            )
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is False
            assert "mismatch" in result.reason


class TestProxyValidation:
    def test_poly_proxy_rejected_gracefully(self):
        addr = "0xDeAdBeEf00000000000000000000000000000000"

        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value=addr,
        ):
            cfg = _make_wallet(
                private_key="0x" + "fa" * 32,
                funder_address=addr,
                signature_type=SignatureType.POLY_PROXY,
            )
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is False
            assert "not_yet_supported" in result.reason

    def test_gnosis_safe_rejected_gracefully(self):
        addr = "0xDeAdBeEf00000000000000000000000000000001"

        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value=addr,
        ):
            cfg = _make_wallet(
                private_key="0x" + "fb" * 32,
                funder_address=addr,
                signature_type=SignatureType.GNOSIS_SAFE,
            )
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is False
            assert "not_yet_supported" in result.reason


class TestEdgeCases:
    def test_signer_derivation_failure_fails_closed(self):
        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value="",
        ):
            cfg = _make_wallet()
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is False
            assert "derivation" in result.reason

    def test_missing_funder_fails(self):
        addr = "0xAbCdEf0000000000000000000000000000000000"

        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value=addr,
        ):
            cfg = _make_wallet(funder_address="")
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is False
            assert "funder" in result.reason

    def test_unknown_signature_type_fails(self):
        addr = "0xAbCdEf0000000000000000000000000000000000"

        with patch(
            "services.redemption.wallet_validator.derive_signer_address",
            return_value=addr,
        ):
            cfg = _make_wallet(signature_type=99)  # type: ignore[arg-type]
            result = validate_redemption_wallet_mode(cfg)
            assert result.allowed is False
            assert "unknown" in result.reason


class TestResultImmutability:
    def test_result_is_frozen(self):
        r = WalletValidationResult(
            allowed=True,
            reason="ok",
            signer_address="0xAb",
            funder_address="0xAb",
            signature_type=SignatureType.EOA,
        )
        with pytest.raises(Exception):
            r.allowed = False  # type: ignore[misc]
