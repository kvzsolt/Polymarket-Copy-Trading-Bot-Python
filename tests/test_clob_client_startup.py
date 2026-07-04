from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from py_clob_client_v2.exceptions import PolyApiException

from config.env import SignatureType, WalletConfig
from services.collateral_readiness import CollateralStatus
import utils.createClobClient as create_clob_client_module
from utils.createClobClient import _create_or_derive_api_key_with_retry


@pytest.mark.asyncio
async def test_create_or_derive_api_key_retries_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    creds = object()
    client.create_or_derive_api_key.side_effect = [
        PolyApiException(error_msg="Request exception!"),
        creds,
    ]
    reconfigured: list[bool] = []
    monkeypatch.setattr(
        "utils.createClobClient._configure_sdk_http_transport",
        lambda *, force=False: reconfigured.append(force),
    )

    result = await _create_or_derive_api_key_with_retry(
        client,
        max_attempts=2,
        base_delay_seconds=0,
    )

    assert result is creds
    assert client.create_or_derive_api_key.call_count == 2
    assert reconfigured == [True]


@pytest.mark.asyncio
async def test_create_or_derive_api_key_survives_reset_burst(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    creds = object()
    client.create_or_derive_api_key.side_effect = [
        PolyApiException(error_msg="Request exception!"),
        PolyApiException(error_msg="Request exception!"),
        PolyApiException(error_msg="Request exception!"),
        PolyApiException(error_msg="Request exception!"),
        creds,
    ]
    reconfigured: list[bool] = []
    monkeypatch.setattr(
        "utils.createClobClient._configure_sdk_http_transport",
        lambda *, force=False: reconfigured.append(force),
    )

    result = await _create_or_derive_api_key_with_retry(
        client,
        max_attempts=5,
        base_delay_seconds=0,
    )

    assert result is creds
    assert client.create_or_derive_api_key.call_count == 5
    assert reconfigured == [True, True, True, True]


@pytest.mark.asyncio
async def test_create_or_derive_api_key_does_not_retry_auth_failure() -> None:
    class AuthError(Exception):
        status_code = 401

    client = MagicMock()
    client.create_or_derive_api_key.side_effect = AuthError("unauthorized")

    with pytest.raises(AuthError):
        await _create_or_derive_api_key_with_retry(
            client,
            max_attempts=4,
            base_delay_seconds=0,
        )

    client.create_or_derive_api_key.assert_called_once()


@pytest.mark.asyncio
async def test_create_clob_client_uses_configured_api_creds_without_deriving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = MagicMock()
    signer_address = "0x" + "a" * 40
    configured_creds = {
        "api_key": "configured-key",
        "api_secret": "configured-value",
        "api_passphrase": "configured-phrase",
    }
    mock_cfg = SimpleNamespace(
        clob=SimpleNamespace(
            clob_v2_ready=True,
            **configured_creds,
        ),
        network=SimpleNamespace(
            pusd_contract_address="",
            collateral_contract_address="",
            ctf_exchange_v2_address="",
            neg_risk_ctf_exchange_v2_address="",
        ),
    )
    wallet_cfg = WalletConfig(
        private_key="0x" + "a" * 64,
        chain_id=137,
        signature_type=SignatureType.EOA,
        funder_address=signer_address,
        polygon_rpc_url="http://localhost",
    )

    async def fake_detect_wallet_signature_type(_cfg: WalletConfig):
        return (SignatureType.EOA, "EOA", signer_address)

    async def fake_check_collateral_readiness(**_kwargs):
        return CollateralStatus(
            pusd_balance=Decimal("100"),
            pusd_allowance_ctf=Decimal("1000"),
            wallet_address=signer_address,
            rpc_url="http://localhost",
            ready=True,
            failures=(),
        )

    monkeypatch.setattr(create_clob_client_module, "_configure_sdk_http_transport", lambda: None)
    monkeypatch.setattr(create_clob_client_module, "build_wallet_config", lambda: wallet_cfg)
    monkeypatch.setattr(create_clob_client_module, "validate_wallet_config", lambda _cfg: None)
    monkeypatch.setattr(
        create_clob_client_module,
        "detect_wallet_signature_type",
        fake_detect_wallet_signature_type,
    )
    monkeypatch.setattr(create_clob_client_module, "ClobClient", MagicMock(return_value=mock_client))
    monkeypatch.setattr(
        "services.collateral_readiness.check_collateral_readiness",
        fake_check_collateral_readiness,
    )

    wrapper = await create_clob_client_module.create_clob_client(config=mock_cfg)

    mock_client.create_or_derive_api_key.assert_not_called()
    mock_client.set_api_creds.assert_called_once()
    api_creds = mock_client.set_api_creds.call_args.args[0]
    assert api_creds.api_key == configured_creds["api_key"]
    assert api_creds.api_secret == configured_creds["api_secret"]
    assert api_creds.api_passphrase == configured_creds["api_passphrase"]
    assert wrapper.api_creds is api_creds
