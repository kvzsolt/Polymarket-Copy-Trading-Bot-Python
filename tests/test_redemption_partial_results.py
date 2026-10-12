"""Mixed settlement outcomes and complete accounting for interrupted inventory scans."""

from dataclasses import replace
import json
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.redemption import (
    RedemptionConfirmation,
    RedemptionMarket,
    RedemptionState,
    SettlementAmbiguous,
    SettlementAuthError,
    SettlementMarketError,
    SettlementUnavailable,
)
from tests.test_cli_redeem import TestRedeemCommand as CommandFixture
from tests.test_redemption_service import TestRedemptionService as ServiceFixture, WALLET


class TestPartialRedemptionResults:
    """Keep successes visible and stop only when safe continuation is impossible."""

    @staticmethod
    def scenario():
        """Build three independently identified conditions using the real service."""
        service, gateway, journal, clock = ServiceFixture.scenario()
        base = gateway.snapshots[0]
        gateway.snapshots = [
            replace(base, market=RedemptionMarket("0x" + f"{index:064x}", ("123", "456"), False))
            for index in (1, 2, 3)
        ]
        return service, gateway, journal, clock

    @pytest.mark.parametrize("failure", ["metadata", "rejected", "reverted"])
    async def test_success_failure_success_continues_and_summarizes(self, failure) -> None:
        """A definite condition failure must not prevent the next redemption."""
        service, gateway, _, _ = self.scenario()
        if failure == "metadata":
            gateway.inspect_errors.add(gateway.snapshots[1].market.condition_id)
        elif failure == "rejected":
            gateway.submit = AsyncMock(
                side_effect=["tx-1", SettlementMarketError("rejected"), "tx-3"]
            )
        else:
            gateway.confirm = AsyncMock(
                side_effect=[
                    gateway.confirmation,
                    RedemptionConfirmation(RedemptionState.FAILED),
                    gateway.confirmation,
                ]
            )
        report = await service.run(WALLET, execute=True)
        assert [result.state for result in report.results] == [
            RedemptionState.CONFIRMED,
            RedemptionState.FAILED,
            RedemptionState.CONFIRMED,
        ]
        assert not report.success
        assert report.scan_complete
        assert report.summary["confirmed"] == 2
        assert report.summary["failed"] == 1
        assert report.summary["discovered"] == report.summary["inspected"] == 3
        assert report.uninspected_condition_ids == ()

    async def test_rpc_failure_identifies_current_and_remaining_conditions(self) -> None:
        """An interrupted preview must retain the completed result and missing IDs."""
        service, gateway, _, _ = self.scenario()
        gateway.inspect = AsyncMock(
            side_effect=[
                gateway.snapshots[0],
                SettlementUnavailable("settlement_rpc_unavailable"),
            ]
        )
        report = await service.run(WALLET, execute=False)
        assert report.error == "settlement_rpc_unavailable"
        assert [r.state for r in report.results] == [
            RedemptionState.PLANNED,
            RedemptionState.FAILED,
        ]
        assert report.results[1].market == gateway.snapshots[1].market
        assert report.results[1].reason == report.error
        assert not report.scan_complete
        assert report.summary["discovered"] == 3
        assert report.summary["inspected"] == 2
        assert report.uninspected_condition_ids == (gateway.snapshots[2].market.condition_id,)

    @pytest.mark.parametrize(
        "error",
        [
            SettlementAmbiguous("submission_unconfirmed"),
            SettlementAuthError("relayer_authentication_failed"),
        ],
    )
    async def test_wallet_stop_retains_success_and_uninspected_ids(self, error) -> None:
        """Ambiguity and account authentication errors stop further writes."""
        service, gateway, _, _ = self.scenario()
        gateway.submit = AsyncMock(side_effect=["tx-1", error])
        report = await service.run(WALLET, execute=True)
        assert gateway.submit.await_count == 2
        assert report.results[0].state is RedemptionState.CONFIRMED
        assert report.summary["confirmed"] == 1
        assert report.summary["uninspected"] == 1
        assert not report.scan_complete and not report.success
        if isinstance(error, SettlementAmbiguous):
            again = await service.run(WALLET, execute=True)
            assert not again.success and gateway.submit.await_count == 2
            assert again.summary["discovered"] is None

    async def test_fresh_empty_balances_skip_stale_api_inventory(self) -> None:
        """A successful rerun does not redeem a now empty holding."""
        service, gateway, _, _ = self.scenario()
        gateway.snapshots = [replace(gateway.snapshots[0], balances=(0, 0))]
        report = await service.run(WALLET, execute=True)
        assert report.success and report.scan_complete
        assert report.results[0].reason == "already_empty"
        assert report.summary["skipped"] == 1
        assert gateway.submitted == gateway.prepared == 0

    async def test_recovered_expired_attempt_counts_latest_condition_result_once(self) -> None:
        """Recovery history must not inflate the summary's number of conditions."""
        service, gateway, _, _ = ServiceFixture.scenario()
        gateway.submit_error = SettlementAmbiguous("submission_unconfirmed")
        await service.run(WALLET, execute=True)
        gateway.finalized += 601
        gateway.submit_error = None
        report = await service.run(WALLET, execute=True)
        assert report.success
        assert report.summary["confirmed"] == 1
        assert report.summary["skipped"] == 0
        assert report.summary["discovered"] == report.summary["inspected"] == 1

    @pytest.mark.parametrize("as_json", [False, True])
    async def test_cli_explains_partial_scan_and_zero_payout_cleanup(self, as_json, capsys) -> None:
        """Human and machine reports expose successful work and inventory left to inspect."""
        service, gateway, _, _ = self.scenario()
        gateway.inspect = AsyncMock(
            side_effect=[
                gateway.snapshots[0],
                SettlementUnavailable("settlement_rpc_unavailable"),
            ]
        )
        command, runtime = CommandFixture.command()
        runtime.report = await service.run(WALLET, execute=False)
        args = CommandFixture.parse(*(["--json"] if as_json else []))
        assert await command.run(args) == 1
        output = capsys.readouterr().out
        if as_json:
            data = json.loads(output)
            assert data["scan_complete"] is False
            assert data["summary"]["uninspected"] == 1
            assert data["uninspected_condition_ids"] == [gateway.snapshots[2].market.condition_id]
            assert data["results"][0]["purpose"] == "zero_payout_cleanup"
        else:
            assert "Scan incomplete" in output
            assert "discovered=3" in output and "inspected=2" in output
            assert "zero-payout cleanup" in output
            assert gateway.snapshots[2].market.condition_id in output
