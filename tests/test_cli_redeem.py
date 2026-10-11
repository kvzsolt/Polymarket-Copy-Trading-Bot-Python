"""Standalone redemption CLI gating and operator output without live I/O."""

import argparse
from dataclasses import replace
import json

from pydantic import SecretStr
import pytest

from bot_cli.commands.redeem import RedemptionCommand, register_redeem_command
from config.app_config import AppConfig
from config.env import WalletConfig
from config.redemption import RedemptionConfig
from domain.redemption import (
    RedemptionMarket,
    RedemptionReport,
    RedemptionResult,
    RedemptionSnapshot,
    RedemptionState,
)
from services.multiUserConfig import RuntimeUser

WALLET = "0x" + "1" * 40


class FakeRuntime:
    """Record whether the CLI allowed any I/O construction at all."""

    def __init__(self) -> None:
        self.calls: list[bool] = []
        market = RedemptionMarket("0x" + "ab" * 32, ("123", "456"), False, "Example")
        snapshot = RedemptionSnapshot(market, (0, 5), (1, 0), 1, False)
        self.report = RedemptionReport(
            WALLET,
            False,
            (RedemptionResult(market, RedemptionState.PLANNED, snapshot),),
            discovered_condition_ids=(market.condition_id,),
            inspected_condition_ids=(market.condition_id,),
        )
        self.error: Exception | None = None

    async def run(self, config, user, *, execute: bool) -> RedemptionReport:
        """Return a deterministic report without starting the bot."""
        self.calls.append(execute)
        if self.error:
            raise self.error
        return replace(self.report, execute=execute)


class TestRedeemCommand:
    """Command permission is explicit and independent of available credentials."""

    @staticmethod
    def parse(*arguments: str) -> argparse.Namespace:
        """Use the actual command registration."""
        parser = argparse.ArgumentParser()
        register_redeem_command(parser.add_subparsers(dest="command", required=True))
        return parser.parse_args(["redeem", *arguments])

    @staticmethod
    def config(live: bool = False, credentials: bool = False) -> AppConfig:
        """Build typed settings with synthetic data only."""
        config = AppConfig()
        return replace(
            config,
            wallet=WalletConfig(
                private_key=SecretStr("11" * 32),
                chain_id=137,
                funder_address=WALLET,
                polygon_rpc_url="https://rpc.example",
            ),
            execution=replace(config.execution, trading_mode="live" if live else "paper"),
            redemption=RedemptionConfig(
                SecretStr("synthetic-key") if credentials else SecretStr(""),
                "0x" + "2" * 40 if credentials else "",
            ),
        )

    @classmethod
    def command(cls, config=None, users=None):
        """Inject both loaders and the runtime to prohibit ambient configuration."""
        config = config or cls.config()
        users = users if users is not None else [RuntimeUser("follower", config.wallet)]
        runtime = FakeRuntime()
        return RedemptionCommand(lambda: config, lambda leaders, cfg: users, runtime), runtime

    async def test_default_is_preview_even_when_live_and_credentials_exist(self, capsys) -> None:
        """Verify default is preview even when live and credentials exist."""
        command, runtime = self.command(self.config(True, True))
        assert await command.run(self.parse("--json")) == 0
        assert runtime.calls == [False]
        output = json.loads(capsys.readouterr().out)
        assert output["execute"] is False
        assert output["results"][0]["expected_payout"] == "0.000000"
        assert output["results"][0]["shares"] == ["0.000000", "0.000005"]

    async def test_paper_execute_is_blocked_before_runtime_construction(self, capsys) -> None:
        """Verify paper execute is blocked before runtime construction."""
        command, runtime = self.command(self.config(False, True))
        assert await command.run(self.parse("--execute", "--json")) == 1
        assert not runtime.calls
        assert "live" in json.loads(capsys.readouterr().out)["error"]

    async def test_missing_relayer_credentials_are_actionable_and_block_execution(
        self, capsys
    ) -> None:
        """Verify missing relayer credentials are actionable and block execution."""
        command, runtime = self.command(self.config(True))
        assert await command.run(self.parse("--execute", "--json")) == 1
        assert not runtime.calls
        assert "PMCOPY__REDEMPTION__RELAYER_API_KEY" in capsys.readouterr().out

    async def test_live_execute_is_forwarded_once(self, capsys) -> None:
        """Verify live execute is forwarded once."""
        command, runtime = self.command(self.config(True, True))
        assert await command.run(self.parse("--execute")) == 0
        assert runtime.calls == [True]
        assert "Example" in capsys.readouterr().out

    async def test_multiple_followers_require_explicit_selection(self, capsys) -> None:
        """Verify multiple followers require explicit selection."""
        config = self.config()
        command, runtime = self.command(
            config, [RuntimeUser("a", config.wallet), RuntimeUser("b", config.wallet)]
        )
        assert await command.run(self.parse("--json")) == 1
        assert "--user-id" in capsys.readouterr().out
        assert not runtime.calls
        assert await command.run(self.parse("--user-id", "b", "--json")) == 0

    @pytest.mark.parametrize("user_id", ["missing", ""])
    async def test_missing_or_empty_user_selection_fails_closed(self, user_id, capsys) -> None:
        """Verify missing or empty user selection fails closed."""
        command, runtime = self.command(users=[])
        assert await command.run(self.parse("--user-id", user_id, "--json")) == 1
        assert not runtime.calls

    async def test_missing_explicit_funder_is_not_replaced_with_signer(self, capsys) -> None:
        """Verify missing explicit funder is not replaced with signer."""
        config = self.config(True, True)
        command, runtime = self.command(
            replace(config, wallet=replace(config.wallet, funder_address=""))
        )
        assert await command.run(self.parse("--execute", "--json")) == 1
        assert "funder" in capsys.readouterr().out
        assert not runtime.calls

    async def test_pending_report_exits_nonzero_and_keeps_transaction_reference(
        self, capsys
    ) -> None:
        """Verify pending report exits nonzero and keeps transaction reference."""
        command, runtime = self.command()
        runtime.report = replace(
            runtime.report,
            results=(
                replace(
                    runtime.report.results[0], state=RedemptionState.PENDING, transaction_id="tx-1"
                ),
            ),
        )
        assert await command.run(self.parse("--json")) == 1
        assert json.loads(capsys.readouterr().out)["results"][0]["transaction_id"] == "tx-1"

    async def test_untrusted_exceptions_never_print_secrets(self, capsys) -> None:
        """Verify untrusted exceptions never print secrets."""
        command, runtime = self.command()
        runtime.error = RuntimeError("SYNTHETIC_SECRET_MUST_NOT_LEAK")
        assert await command.run(self.parse("--json")) == 1
        output = capsys.readouterr()
        assert "SYNTHETIC_SECRET" not in output.out + output.err
        assert json.loads(output.out)["error"] == "redemption_command_failed"

    @pytest.mark.parametrize("pending", [False, True])
    async def test_reconciliation_guidance_requires_a_pending_attempt(
        self, pending: bool, capsys
    ) -> None:
        """Metadata failures must not imply that a transaction was submitted."""
        command, runtime = self.command(self.config(True, True))
        result = replace(
            runtime.report.results[0],
            state=RedemptionState.FAILED,
            snapshot=None,
            reason="redemption_market_metadata_not_found",
        )
        results = (result,)
        if pending:
            results += (
                replace(result, state=RedemptionState.PENDING, reason="submission_unconfirmed"),
            )
        runtime.report = replace(runtime.report, results=results)
        assert await command.run(self.parse("--execute")) == 1
        output = capsys.readouterr().out
        assert "redemption_market_metadata_not_found" in output
        assert ("reconcile pending attempts" in output) is pending
        if not pending:
            assert "Review the reported errors" in output

    async def test_runtime_error_does_not_imply_pending_submission(self, capsys) -> None:
        """Failures before an inventory report have no known pending attempt."""
        command, runtime = self.command(self.config(True, True))
        runtime.report = replace(
            runtime.report, results=(), error="redemption_inventory_unavailable"
        )
        assert await command.run(self.parse("--execute")) == 1
        output = capsys.readouterr().out
        assert "reconcile pending attempts" not in output
        assert "Review the reported errors" in output

    def test_execute_and_dry_run_are_mutually_exclusive(self) -> None:
        """Verify execute and dry run are mutually exclusive."""
        with pytest.raises(SystemExit) as error:
            self.parse("--execute", "--dry-run")
        assert error.value.code == 2

    def test_root_cli_registers_redeem(self, monkeypatch, capsys) -> None:
        """Verify root cli registers redeem."""
        from bot_cli.main import main

        monkeypatch.setattr("sys.argv", ["bot.py", "redeem", "--help"])
        with pytest.raises(SystemExit) as error:
            main()
        assert error.value.code == 0
        assert "--execute" in capsys.readouterr().out
