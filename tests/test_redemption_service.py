from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.redemptionService import RedemptionService


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        network=SimpleNamespace(
            rpc_url="https://polygon-rpc.example.com",
            usdc_contract_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        ),
        polling=SimpleNamespace(redeem_check_interval=0),
        wallet=SimpleNamespace(
            funder_address="0x1111111111111111111111111111111111111111",
            private_key="0x" + "1" * 64,
        ),
    )


@pytest.mark.asyncio
async def test_redeem_condition_rejects_non_32_byte_condition_id() -> None:
    svc = RedemptionService(clob_client=MagicMock(), config=_config())

    result = await svc.redeem_condition("0x" + "ab" * 31)

    assert result.submitted is False
    assert result.tx_hash is None
    assert result.status == "failed"
    assert result.error == "invalid_condition_id_length:31"


@pytest.mark.asyncio
async def test_redeem_condition_returns_not_submitted_on_reverted_receipt() -> None:
    cfg = _config()
    svc = RedemptionService(clob_client=MagicMock(), config=cfg)

    signed = SimpleNamespace(raw_transaction=b"signed")
    contract_fn = MagicMock()
    contract_fn.build_transaction.return_value = {"gas": 500_000}
    functions = MagicMock(redeemPositions=MagicMock(return_value=contract_fn))
    contract = MagicMock(functions=functions)
    eth = MagicMock(
        contract=MagicMock(return_value=contract),
        account=MagicMock(sign_transaction=MagicMock(return_value=signed)),
    )
    web3_instance = MagicMock(eth=eth)

    adapter = MagicMock()
    adapter.get_transaction_count = AsyncMock(return_value=7)
    adapter.get_gas_price = AsyncMock(return_value=100)
    adapter.send_raw_transaction = AsyncMock(return_value="0xtx")
    adapter.wait_for_receipt = AsyncMock(return_value={"status": 0, "gasUsed": 12345})

    with patch("services.redemptionService.Web3") as web3_cls, patch(
        "services.redemptionService.AsyncWeb3Adapter", return_value=adapter
    ):
        web3_cls.HTTPProvider.return_value = object()
        web3_cls.return_value = web3_instance
        web3_cls.to_checksum_address.side_effect = lambda value: value

        result = await svc.redeem_condition("0x" + "ab" * 32)

    assert result.submitted is False
    assert result.tx_hash == "0xtx"
    assert result.status == "reverted"
    assert result.error == "redeem_tx_reverted"
