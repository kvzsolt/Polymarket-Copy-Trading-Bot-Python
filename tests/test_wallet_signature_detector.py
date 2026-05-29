"""PR9.2: tests for services/wallet_signature_detector.py."""
from __future__ import annotations

import pytest

from config.env import SignatureType, WalletConfig
from services.wallet_signature_detector import (
    _classify_wallet_signature_type,
    detect_wallet_signature_type,
)


_PRIVATE_KEY = "0x" + "1" * 64
_SIGNER_ADDRESS = "0x19E7E376E7C213B7E7E7E46CC70A5DD086DAFF2A"
_FUNDER_EOA = _SIGNER_ADDRESS
_FUNDER_PROXY = "0x2222222222222222222222222222222222222222"
_FUNDER_SAFE = "0x3333333333333333333333333333333333333333"


def _wallet_config(funder_address: str) -> WalletConfig:
    return WalletConfig(
        private_key=_PRIVATE_KEY,
        chain_id=137,
        signature_type=SignatureType.EOA,
        funder_address=funder_address,
        polygon_rpc_url="https://polygon-rpc.example.com",
    )


def test_classify_wallet_signature_type_eoa():
    signature_type, wallet_type = _classify_wallet_signature_type(
        signer_address=_SIGNER_ADDRESS,
        funder_address=_FUNDER_EOA,
        funder_has_code=False,
    )
    assert signature_type == SignatureType.EOA
    assert wallet_type == "EOA"


def test_classify_wallet_signature_type_poly_proxy():
    signature_type, wallet_type = _classify_wallet_signature_type(
        signer_address=_SIGNER_ADDRESS,
        funder_address=_FUNDER_PROXY,
        funder_has_code=False,
    )
    assert signature_type == SignatureType.POLY_PROXY
    assert wallet_type == "POLY_PROXY"


def test_classify_wallet_signature_type_gnosis_safe():
    signature_type, wallet_type = _classify_wallet_signature_type(
        signer_address=_SIGNER_ADDRESS,
        funder_address=_FUNDER_SAFE,
        funder_has_code=True,
    )
    assert signature_type == SignatureType.GNOSIS_SAFE
    assert wallet_type == "GNOSIS_SAFE"


@pytest.mark.asyncio
async def test_detect_wallet_signature_type_eoa_skips_rpc_probe(monkeypatch):
    monkeypatch.setattr(
        "services.wallet_signature_detector._derive_signer_address",
        lambda _pk: _SIGNER_ADDRESS,
    )

    def _boom(*_args, **_kwargs):
        raise AssertionError("RPC probe should not run for signer=funder EOA")

    monkeypatch.setattr("services.wallet_signature_detector._fetch_contract_code_hex", _boom)

    signature_type, wallet_type, signer_address = await detect_wallet_signature_type(
        _wallet_config(_FUNDER_EOA)
    )

    assert signature_type == SignatureType.EOA
    assert wallet_type == "EOA"
    assert signer_address == _SIGNER_ADDRESS


@pytest.mark.asyncio
async def test_detect_wallet_signature_type_gnosis_safe_from_contract_code(monkeypatch):
    monkeypatch.setattr(
        "services.wallet_signature_detector._derive_signer_address",
        lambda _pk: _SIGNER_ADDRESS,
    )
    monkeypatch.setattr(
        "services.wallet_signature_detector._fetch_contract_code_hex",
        lambda _rpc_url, _address: "0x1234",
    )

    signature_type, wallet_type, signer_address = await detect_wallet_signature_type(
        _wallet_config(_FUNDER_SAFE)
    )

    assert signature_type == SignatureType.GNOSIS_SAFE
    assert wallet_type == "GNOSIS_SAFE"
    assert signer_address == _SIGNER_ADDRESS


@pytest.mark.asyncio
async def test_detect_wallet_signature_type_poly_proxy_when_funder_differs_and_has_no_code(
    monkeypatch,
):
    monkeypatch.setattr(
        "services.wallet_signature_detector._derive_signer_address",
        lambda _pk: _SIGNER_ADDRESS,
    )
    monkeypatch.setattr(
        "services.wallet_signature_detector._fetch_contract_code_hex",
        lambda _rpc_url, _address: "0x",
    )

    signature_type, wallet_type, signer_address = await detect_wallet_signature_type(
        _wallet_config(_FUNDER_PROXY)
    )

    assert signature_type == SignatureType.POLY_PROXY
    assert wallet_type == "POLY_PROXY"
    assert signer_address == _SIGNER_ADDRESS


@pytest.mark.asyncio
async def test_detect_wallet_signature_type_fail_closed_on_rpc_error(monkeypatch):
    monkeypatch.setattr(
        "services.wallet_signature_detector._derive_signer_address",
        lambda _pk: _SIGNER_ADDRESS,
    )

    def _rpc_fail(*_args, **_kwargs):
        raise ConnectionError("RPC unreachable")

    monkeypatch.setattr(
        "services.wallet_signature_detector._fetch_contract_code_hex",
        _rpc_fail,
    )

    with pytest.raises(RuntimeError, match="Could not probe wallet type via RPC"):
        await detect_wallet_signature_type(
            _wallet_config(_FUNDER_PROXY),
            fail_closed_on_rpc_error=True,
        )


@pytest.mark.asyncio
async def test_detect_wallet_signature_type_eoa_with_config_mismatch(monkeypatch):
    """Config says POLY_PROXY but runtime detects EOA — runtime wins."""
    monkeypatch.setattr(
        "services.wallet_signature_detector._derive_signer_address",
        lambda _pk: _SIGNER_ADDRESS,
    )
    monkeypatch.setattr(
        "services.wallet_signature_detector._fetch_contract_code_hex",
        lambda *_a, **_k: "0x",
    )
    config = WalletConfig(
        private_key=_PRIVATE_KEY,
        chain_id=137,
        signature_type=SignatureType.POLY_PROXY,
        funder_address=_FUNDER_EOA,
        polygon_rpc_url="https://polygon-rpc.example.com",
    )
    sig_type, wallet_type, signer = await detect_wallet_signature_type(config)
    assert sig_type == SignatureType.EOA
    assert wallet_type == "EOA"