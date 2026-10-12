"""Closed-market Gamma filtering and fail-closed settlement metadata contracts."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import Mock

import aiohttp
import pytest

from pmcopy.domain.redemption import RedemptionState
from pmcopy.services.settlement import RedemptionService
from tests.test_redemption_gateway import CONDITION, GatewayScenario
from tests.test_redemption_service import FakeClock, FakeJournal


class FilteredGammaScenario(GatewayScenario):
    """Model Gamma's default open-only lookup without any network access."""

    def __init__(self) -> None:
        """Supply a closed market and record each requested status filter."""
        super().__init__()
        self.closed_metadata: object = self.metadata
        self.open_metadata: object = []
        self.filters: list[str | None] = []
        self.signer.sign = Mock(wraps=self.signer.sign)

    async def get_json(self, url, **kwargs):
        """Return closed metadata only when explicitly requested."""
        if not url.startswith("https://gamma-api.polymarket.com"):
            return await super().get_json(url, **kwargs)
        assert not kwargs.get("headers")
        assert kwargs["silent"] is True
        assert kwargs["params"]["condition_ids"] == CONDITION
        assert kwargs["params"]["limit"] == 2
        closed = kwargs["params"].get("closed")
        self.filters.append(closed)
        response = self.closed_metadata if closed == "true" else self.open_metadata
        if isinstance(response, Exception):
            raise response
        return response


class TestRedemptionMetadata:
    """Exercise the real service and adapter before the signing boundary."""

    @pytest.mark.parametrize("execute", [False, True])
    @pytest.mark.parametrize("winning", [False, True])
    async def test_closed_winners_and_losing_dust_reach_settlement(
        self, execute: bool, winning: bool
    ) -> None:
        """Require explicit closed lookup for both positive and zero payouts."""
        scenario = FilteredGammaScenario()
        numerators = (0, 1) if winning else (1, 0)
        scenario.chain.snapshot.return_value = replace(
            scenario.snapshot, payout_numerators=numerators
        )
        scenario.chain.receipt_payout.return_value = 5 if winning else 0
        journal = FakeJournal()
        service = RedemptionService(scenario.gateway, journal, FakeClock())
        report = await service.run(scenario.wallet, execute=execute)
        assert report.success
        assert scenario.filters == ["true"]
        result = report.results[0]
        expected = Decimal("0.000005") if winning else Decimal(0)
        assert result.snapshot.expected_payout == expected
        if execute:
            assert result.state is RedemptionState.CONFIRMED
            assert result.received_payout == expected
            assert scenario.write.post_json.await_count == 1
            assert len(journal.attempts) == 1
        else:
            assert result.state is RedemptionState.PLANNED
            scenario.signer.sign.assert_not_called()
            scenario.write.post_json.assert_not_awaited()
            assert not journal.attempts

    @pytest.mark.parametrize("closed", [False, True])
    async def test_unresolved_open_and_closed_markets_are_skipped(self, closed: bool) -> None:
        """Market status must never substitute for on-chain resolution."""
        scenario = FilteredGammaScenario()
        if not closed:
            scenario.closed_metadata = []
            scenario.open_metadata = scenario.metadata
        scenario.chain.snapshot.return_value = replace(
            scenario.snapshot, payout_numerators=(0, 0), payout_denominator=0
        )
        journal = FakeJournal()
        report = await RedemptionService(scenario.gateway, journal, FakeClock()).run(
            scenario.wallet, execute=True
        )
        assert report.success
        assert scenario.filters == (["true"] if closed else ["true", "false"])
        assert report.results[0].state is RedemptionState.SKIPPED
        assert report.results[0].reason == "unresolved"
        scenario.signer.sign.assert_not_called()
        scenario.write.post_json.assert_not_awaited()
        assert not journal.attempts

    @pytest.mark.parametrize("fallback", [False, True])
    @pytest.mark.parametrize(
        "failure,reason",
        [
            (None, "invalid"),
            ({}, "invalid"),
            ([None], "invalid"),
            ([{}, {}], "invalid"),
            ([{}], "invalid"),
            ("bad_tokens", "invalid"),
            ("missing_risk", "invalid"),
            ("string_risk", "invalid"),
            ("wrong_condition", "conflicting"),
            ("reversed_tokens", "conflicting"),
            ("wrong_risk", "conflicting"),
            (TimeoutError("SYNTHETIC_SECRET"), "unavailable"),
            (aiohttp.ClientConnectionError("SYNTHETIC_SECRET"), "unavailable"),
            (
                aiohttp.ClientResponseError(None, (), status=403, message="SYNTHETIC_SECRET"),
                "unavailable",
            ),
        ],
    )
    async def test_bad_metadata_fails_without_signing_or_additional_fallback(
        self, failure, reason: str, fallback: bool
    ) -> None:
        """Keep missing data, transport failures, and identity conflicts distinct."""
        scenario = FilteredGammaScenario()
        row = dict(scenario.metadata[0])
        if isinstance(failure, str):
            if failure == "bad_tokens":
                row["clobTokenIds"] = "SYNTHETIC_SECRET"
            elif failure == "missing_risk":
                del row["negRisk"]
            elif failure == "string_risk":
                row["negRisk"] = "false"
            elif failure == "wrong_condition":
                row["conditionId"] = "0x" + "cd" * 32
            elif failure == "reversed_tokens":
                row["clobTokenIds"] = '["456", "123"]'
            elif failure == "wrong_risk":
                row["negRisk"] = True
            failure = [row]
        scenario.closed_metadata = [] if fallback else failure
        scenario.open_metadata = failure if fallback else scenario.metadata
        journal = FakeJournal()
        report = await RedemptionService(scenario.gateway, journal, FakeClock()).run(
            scenario.wallet, execute=True
        )
        assert not report.success
        assert report.results[0].state is RedemptionState.FAILED
        assert report.results[0].reason == "redemption_market_metadata_" + reason
        assert scenario.filters == (["true", "false"] if fallback else ["true"])
        scenario.chain.snapshot.assert_not_awaited()
        scenario.signer.sign.assert_not_called()
        scenario.write.post_json.assert_not_awaited()
        assert not journal.attempts

    async def test_market_absent_in_both_filters_reports_not_found(self) -> None:
        """Bound absence checks and report the failure before signing."""
        scenario = FilteredGammaScenario()
        scenario.closed_metadata = []
        journal = FakeJournal()
        report = await RedemptionService(scenario.gateway, journal, FakeClock()).run(
            scenario.wallet, execute=True
        )
        assert not report.success
        assert report.results[0].reason == "redemption_market_metadata_not_found"
        assert scenario.filters == ["true", "false"]
        scenario.chain.snapshot.assert_not_awaited()
        scenario.signer.sign.assert_not_called()
        scenario.write.post_json.assert_not_awaited()
        assert not journal.attempts
