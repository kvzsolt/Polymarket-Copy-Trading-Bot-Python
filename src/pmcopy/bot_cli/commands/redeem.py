"""Preview or explicitly redeem resolved winning and losing Deposit Wallet holdings."""

import argparse
import asyncio
from collections.abc import Callable
import json
import logging
import re
from typing import Protocol

from pmcopy.config.app_config import AppConfig, EnvValidationError, load_config
from pmcopy.domain.redemption import RedemptionReport, RedemptionState, SettlementError
from pmcopy.orchestration.redemption_runtime import RedemptionRuntime
from pmcopy.orchestrators.provider_runtime import ProviderRuntime
from pmcopy.services.multiUserConfig import RuntimeUser, load_runtime_users


class RedemptionRunner(Protocol):
    """Expose only the standalone runtime operation needed by the command."""

    async def run(self, config: AppConfig, user: RuntimeUser, *, execute: bool) -> RedemptionReport:
        """Run the selected follower's settlement operation."""
        ...


class RedemptionCommand:
    """Validate operator intent, select one follower, and render safe results."""

    def __init__(
        self,
        config_loader: Callable[[], AppConfig],
        user_loader: Callable[[list[str], AppConfig], list[RuntimeUser]],
        runtime: RedemptionRunner,
    ) -> None:
        """Inject settings, follower lookup, and runtime independently for tests."""
        self._config_loader = config_loader
        self._user_loader = user_loader
        self._runtime = runtime

    async def run(self, args: argparse.Namespace) -> int:
        """Return zero only when the complete preview or execution succeeded."""
        execute = args.execute
        wallet = ""
        try:
            config = self._config_loader()
            users = self._user_loader([], config)
            if args.user_id is not None:
                users = [user for user in users if user.user_id == args.user_id]
            if len(users) != 1:
                raise SettlementError("select_exactly_one_follower_with_--user-id")
            user = users[0]
            wallet = user.wallet.funder_address
            if re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet) is None:
                raise SettlementError("explicit_deposit_wallet_funder_address_required")
            if user.wallet.chain_id != 137 or user.wallet.signature_type != 3:
                raise SettlementError("redemption_requires_polygon_deposit_wallet_type_3")
            if execute:
                if config.execution.trading_mode != "live":
                    raise SettlementError("redemption_requires_execution.trading_mode_live")
                config.redemption.require_credentials()
                logging.getLogger("Redemption").info("redemption_execute_opt_in")
            report = await self._runtime.run(config, user, execute=execute)
        except SettlementError as error:
            report = RedemptionReport(wallet, execute, error=str(error))
        except EnvValidationError:
            report = RedemptionReport(
                wallet,
                execute,
                error=(
                    "redemption_configuration_invalid: check application settings and secrets.env; "
                    "--execute requires PMCOPY__REDEMPTION__RELAYER_API_KEY and "
                    "PMCOPY__REDEMPTION__RELAYER_API_KEY_ADDRESS"
                ),
            )
        except Exception:
            report = RedemptionReport(wallet, execute, error="redemption_command_failed")
        self._render(report, json_output=args.json)
        return 0 if report.success else 1

    @staticmethod
    def _render(report: RedemptionReport, *, json_output: bool) -> None:
        rows = [
            {
                "condition_id": result.market.condition_id,
                "title": result.market.title,
                "negative_risk": result.market.negative_risk,
                "state": result.state.value,
                "reason": result.reason,
                "purpose": (
                    "payout" if result.snapshot.expected_payout > 0 else "zero_payout_cleanup"
                )
                if result.snapshot and result.snapshot.eligible
                else None,
                "token_ids": result.market.token_ids,
                "shares": [str(value) for value in result.snapshot.shares]
                if result.snapshot
                else None,
                "expected_payout": str(result.snapshot.expected_payout)
                if result.snapshot
                else None,
                "received_payout": str(result.received_payout)
                if result.received_payout is not None
                else None,
                "transaction_id": result.transaction_id,
                "transaction_hash": result.transaction_hash,
            }
            for result in report.results
        ]
        if json_output:
            print(
                json.dumps(
                    {
                        "wallet": report.wallet,
                        "execute": report.execute,
                        "success": report.success,
                        "error": report.error,
                        "scan_complete": report.scan_complete,
                        "summary": report.summary,
                        "uninspected_condition_ids": report.uninspected_condition_ids,
                        "results": rows,
                    },
                    indent=2,
                )
            )
            return
        print(f"Redemption {'execution' if report.execute else 'preview'}: {report.wallet}")
        if not rows and not report.error:
            print("No positions found.")
        for row in rows:
            print(f"{row['state']}: {row['title'] or row['condition_id']} ({row['condition_id']})")
            if row["shares"] is not None:
                print(f"  shares={row['shares']} expected payout={row['expected_payout']} pUSD")
            if row["received_payout"] is not None:
                print(f"  confirmed payout={row['received_payout']} pUSD")
            if row["purpose"] == "zero_payout_cleanup":
                print("  zero-payout cleanup (resolved losing shares)")
            if row["reason"]:
                print(f"  {row['reason']}")
            if row["transaction_id"] or row["transaction_hash"]:
                print(f"  relayer={row['transaction_id']} transaction={row['transaction_hash']}")
        if report.error:
            print(f"Error: {report.error}")
        print(
            "Summary: "
            + " | ".join(
                f"{key}={value if value is not None else 'unknown'}"
                for key, value in report.summary.items()
            )
        )
        print("Scan complete." if report.scan_complete else "Scan incomplete.")
        for condition_id in report.uninspected_condition_ids:
            print(f"  uninspected: {condition_id}")
        if not report.execute:
            print(
                "Preview only. Redeem resolved winners and losers with: python bot.py redeem --execute"
            )
        elif any(result.state is RedemptionState.PENDING for result in report.results):
            print("Rerun the same command to reconcile pending attempts before any new submission.")
        elif not report.success:
            print("Review the reported errors, then rerun the preview: python bot.py redeem")


def register_redeem_command(subparsers: argparse._SubParsersAction) -> None:
    """Register explicit preview and live execution options."""
    parser = subparsers.add_parser(
        "redeem", help="Redeem all resolved winning and losing positions"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute", action="store_true", help="Submit gasless redemptions (requires live mode)"
    )
    mode.add_argument("--dry-run", action="store_true", help="Preview only (the default)")
    parser.add_argument("--user-id", help="Required when more than one follower is configured")
    parser.add_argument("--json", action="store_true", help="Emit a machine-readable report")


def run_redeem(args: argparse.Namespace) -> int:
    """Delegate framework glue to the command and its standalone runtime."""
    try:
        return asyncio.run(
            RedemptionCommand(
                load_config,
                load_runtime_users,
                RedemptionRuntime(provider_factory=ProviderRuntime.from_config),
            ).run(args)
        )
    except KeyboardInterrupt:
        return 130
