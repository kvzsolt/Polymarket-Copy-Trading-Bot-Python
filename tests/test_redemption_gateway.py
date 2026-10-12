"""Public inventory and single-attempt gasless submission adapter contracts."""

from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import aiohttp
from pydantic import SecretStr
import pytest

from pmcopy.clients.network.http_client import HttpRetryPolicy
from pmcopy.config.redemption import RedemptionConfig
from pmcopy.domain.redemption import (
    RedemptionAttempt,
    RedemptionMarket,
    RedemptionSnapshot,
    RedemptionState,
    SettlementAmbiguous,
    SettlementAuthError,
    SettlementMarketError,
    SettlementUnavailable,
)
from pmcopy.integrations.polymarket.settlement_gateway import PolymarketSettlementGateway
from pmcopy.integrations.polymarket.settlement_wallet import DepositWalletCodec
from tests.test_redemption_service import FakeClock

CONDITION = "0x" + "ab" * 32


class GatewayScenario:
    """Fake only external I/O while exercising the actual adapter and signer."""

    def __init__(self, *, authenticated: bool = True) -> None:
        self.signer = DepositWalletCodec(SecretStr("0x" + "a" * 64))
        self.wallet = self.signer.wallet_addresses(self.signer.signer_address)[1]
        self.read = AsyncMock()
        self.write = AsyncMock()
        self.write.retry_policy = HttpRetryPolicy(attempts=1)
        self.write.post_json.return_value = {"transactionID": "tx-1", "state": "STATE_NEW"}
        self.chain = AsyncMock()
        self.market = RedemptionMarket(CONDITION, ("123", "456"), False, "Resolved test")
        self.snapshot = RedemptionSnapshot(self.market, (0, 5), (1, 0), 1, True)
        self.chain.snapshot.return_value = self.snapshot
        self.chain.latest_timestamp.return_value = 1700000000
        self.chain.receipt_payout.return_value = 0
        self.positions = [
            {
                "conditionId": CONDITION,
                "asset": "456",
                "size": Decimal("0.000005"),
                "oppositeAsset": "123",
                "outcomeIndex": 1,
                "negativeRisk": False,
                "redeemable": True,
                "curPrice": Decimal(0),
                "title": "Resolved test",
            }
        ]
        self.metadata = [
            {"conditionId": CONDITION, "clobTokenIds": '["123", "456"]', "negRisk": False}
        ]
        self.status = {
            "transaction_id": "tx-1",
            "state": "STATE_CONFIRMED",
            "transaction_hash": "0x" + "f" * 64,
        }
        self.nonce = {"address": self.signer.signer_address, "nonce": "7"}
        self.read.get_json.side_effect = self.get_json
        credentials = RedemptionConfig(
            SecretStr("SYNTHETIC_RELAYER_KEY"), self.signer.signer_address
        )
        self.gateway = PolymarketSettlementGateway(
            self.read,
            self.write,
            self.chain,
            FakeClock(),
            credentials=credentials if authenticated else None,
            signer=self.signer if authenticated else None,
        )

    async def get_json(self, url, **kwargs):
        """Dispatch official endpoint shapes and guard credential separation."""
        if url.startswith("https://data-api.polymarket.com"):
            assert "headers" not in kwargs
            assert kwargs["params"]["sizeThreshold"] == 0
            return self.positions
        if url.startswith("https://gamma-api.polymarket.com"):
            assert not kwargs.get("headers")
            return self.metadata
        assert kwargs["headers"]["RELAYER_API_KEY"] == "SYNTHETIC_RELAYER_KEY"
        if url.endswith("/params"):
            return self.nonce
        return self.status


class TestPolymarketSettlementGateway:
    """Exercise account boundaries and the failure window around the actual POST."""

    async def test_execute_params_relayer_address_is_not_the_wallet_signer(self) -> None:
        """The WALLET nonce is requested for the owner; the response address is independent."""
        scenario = GatewayScenario()
        scenario.nonce["address"] = "0x" + "b" * 40
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        assert authorization.nonce == 7
        request = scenario.read.get_json.await_args
        assert request.kwargs["params"] == {
            "address": scenario.signer.signer_address,
            "type": "WALLET",
        }
        scenario.write.post_json.assert_not_awaited()

    @pytest.mark.parametrize("address", [None, 1, "", "0x123", "0x" + "g" * 40])
    async def test_malformed_relayer_address_never_reaches_signing(self, address) -> None:
        """Removing the identity comparison must retain response schema validation."""
        scenario = GatewayScenario()
        scenario.nonce["address"] = address
        scenario.signer.sign = Mock(side_effect=AssertionError("unexpected signing"))
        with pytest.raises(SettlementUnavailable, match="invalid_relayer_address"):
            await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        scenario.signer.sign.assert_not_called()
        scenario.write.post_json.assert_not_awaited()

    async def test_preview_reads_losing_dust_without_authentication(self) -> None:
        """Verify preview reads losing dust without authentication."""
        scenario = GatewayScenario(authenticated=False)
        markets = await scenario.gateway.inventory(scenario.wallet)
        assert markets == (scenario.market,)
        assert (await scenario.gateway.inspect(scenario.wallet, markets[0])).eligible
        scenario.write.post_json.assert_not_awaited()
        with pytest.raises(SettlementAuthError):
            await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)

    async def test_two_outcomes_are_grouped_into_one_condition(self) -> None:
        """Verify two outcomes are grouped into one condition."""
        scenario = GatewayScenario()
        scenario.positions.append(
            {
                **scenario.positions[0],
                "asset": "123",
                "oppositeAsset": "456",
                "outcomeIndex": 0,
                "size": Decimal("1"),
            }
        )
        assert len(await scenario.gateway.inventory(scenario.wallet)) == 1

    @pytest.mark.parametrize("bad", [None, "false", 0])
    async def test_missing_or_coerced_negative_risk_fails_closed(self, bad) -> None:
        """Verify missing or coerced negative risk fails closed."""
        scenario = GatewayScenario()
        scenario.positions[0]["negativeRisk"] = bad
        with pytest.raises(SettlementUnavailable):
            await scenario.gateway.inventory(scenario.wallet)

    async def test_conflicting_market_metadata_never_reaches_signing(self) -> None:
        """Verify conflicting market metadata never reaches signing."""
        scenario = GatewayScenario()
        scenario.metadata[0]["negRisk"] = True
        with pytest.raises(SettlementMarketError):
            await scenario.gateway.inspect(scenario.wallet, scenario.market)
        scenario.write.post_json.assert_not_awaited()

    async def test_submit_returns_reference_and_cannot_be_called_twice(self) -> None:
        """Verify submit returns reference and cannot be called twice."""
        scenario = GatewayScenario()
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        assert authorization.deadline == 1700000600
        assert await scenario.gateway.submit(authorization) == "tx-1"
        with pytest.raises(SettlementAuthError):
            await scenario.gateway.submit(authorization)
        assert scenario.write.post_json.await_count == 1

    @pytest.mark.parametrize("failure", [TimeoutError(), ValueError("SYNTHETIC_PROVIDER_SECRET")])
    async def test_post_transport_failure_is_ambiguous_and_redacted(self, failure) -> None:
        """Verify post transport failure is ambiguous and redacted."""
        scenario = GatewayScenario()
        scenario.write.post_json.side_effect = failure
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        with pytest.raises(SettlementAmbiguous) as error:
            await scenario.gateway.submit(authorization)
        assert "SYNTHETIC_PROVIDER_SECRET" not in str(error.value)
        assert scenario.write.post_json.await_count == 1

    async def test_success_without_reference_is_ambiguous(self) -> None:
        """Verify success without reference is ambiguous."""
        scenario = GatewayScenario()
        scenario.write.post_json.return_value = {"state": "STATE_NEW"}
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        with pytest.raises(SettlementAmbiguous):
            await scenario.gateway.submit(authorization)

    @pytest.mark.parametrize(
        "status,error_type",
        [
            (401, SettlementAuthError),
            (403, SettlementAuthError),
            (400, SettlementMarketError),
            (500, SettlementAmbiguous),
        ],
    )
    async def test_http_failures_keep_auth_rejection_and_ambiguity_distinct(
        self, status, error_type
    ) -> None:
        """Verify http failures keep auth rejection and ambiguity distinct."""
        scenario = GatewayScenario()
        scenario.write.post_json.side_effect = aiohttp.ClientResponseError(
            None, (), status=status, message="SYNTHETIC_SECRET"
        )
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        with pytest.raises(error_type):
            await scenario.gateway.submit(authorization)
        assert scenario.write.post_json.await_count == 1

    async def test_final_relayer_state_requires_chain_receipt_evidence(self) -> None:
        """Verify final relayer state requires chain receipt evidence."""
        scenario = GatewayScenario()
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        attempt = RedemptionAttempt(
            scenario.wallet, scenario.snapshot, authorization, transaction_id="tx-1"
        )
        evidence = await scenario.gateway.confirm(scenario.wallet, attempt)
        assert evidence.state is RedemptionState.CONFIRMED
        assert evidence.received_payout_units == 0
        scenario.chain.receipt_payout.return_value = None
        assert (
            await scenario.gateway.confirm(scenario.wallet, attempt)
        ).state is RedemptionState.PENDING

    async def test_status_reference_mismatch_is_not_accepted(self) -> None:
        """Verify status reference mismatch is not accepted."""
        scenario = GatewayScenario()
        authorization = await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        scenario.status["transaction_id"] = "different-tx"
        with pytest.raises(SettlementUnavailable):
            await scenario.gateway.confirm(
                scenario.wallet,
                RedemptionAttempt(
                    scenario.wallet, scenario.snapshot, authorization, transaction_id="tx-1"
                ),
            )

    def test_retrying_post_transport_is_rejected(self) -> None:
        """Verify retrying post transport is rejected."""
        scenario = GatewayScenario()
        scenario.write.retry_policy.attempts = 4
        with pytest.raises(ValueError, match="single-attempt"):
            PolymarketSettlementGateway(scenario.read, scenario.write, scenario.chain, FakeClock())

    @pytest.mark.parametrize("nonce", [None, 7, "-1", str(2**256), "7.0"])
    async def test_bad_nonce_cannot_reach_signing_or_submission(self, nonce) -> None:
        """Verify bad nonce cannot reach signing or submission."""
        scenario = GatewayScenario()
        scenario.nonce["nonce"] = nonce
        with pytest.raises(SettlementUnavailable):
            await scenario.gateway.prepare(scenario.wallet, scenario.snapshot)
        scenario.write.post_json.assert_not_awaited()

    async def test_foreign_funder_cannot_reach_relayer(self) -> None:
        """Verify foreign funder cannot reach relayer."""
        scenario = GatewayScenario()
        with pytest.raises(SettlementAuthError, match="owner_mismatch"):
            await scenario.gateway.prepare("0x" + "1" * 40, scenario.snapshot)
        scenario.read.get_json.assert_not_awaited()
        scenario.write.post_json.assert_not_awaited()

    async def test_real_http_writer_attempts_ambiguous_request_once(self) -> None:
        """Verify real http writer attempts ambiguous request once."""
        from pmcopy.clients.network.http_client import AsyncHttpClient

        class FailedSession:
            """Fail at the transport boundary before any response, without sleeping."""

            def __init__(self) -> None:
                self.requests = 0

            def request(self, *args, **kwargs):
                """Count the actual HTTP layer attempt and simulate a timeout."""
                self.requests += 1
                raise TimeoutError("SYNTHETIC_TRANSPORT_SECRET")

        scenario = GatewayScenario()
        session = FailedSession()
        writer = AsyncHttpClient(session, retry_policy=HttpRetryPolicy(attempts=1))
        gateway = PolymarketSettlementGateway(
            scenario.read,
            writer,
            scenario.chain,
            FakeClock(),
            credentials=RedemptionConfig(
                SecretStr("SYNTHETIC_RELAYER_KEY"), scenario.signer.signer_address
            ),
            signer=scenario.signer,
        )
        authorization = await gateway.prepare(scenario.wallet, scenario.snapshot)
        with pytest.raises(SettlementAmbiguous) as error:
            await gateway.submit(authorization)
        assert "SYNTHETIC" not in str(error.value)
        assert session.requests == 1
