"""Operator CLI helpers for bankroll-aware Falcon wallet ranking.

This module deliberately contains no live-trading capability.  It accepts a
follower bankroll, builds the discovery evidence graph, and prints a ranked
report.  Private keys are neither accepted nor read.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence, TextIO

from config.app_config import FalconApiConfig
from integrations.falcon.client import FalconClient
from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionClient,
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)
from integrations.polymarket.ccxt_prediction_market_data import CcxtPredictionMarketData
from services.discovery.ccxt_capacity_probe import CcxtCapacityProbe
from services.discovery.falcon_candidate_service import (
    FalconCandidateService,
    FalconDiscoveryPolicy,
)
from services.discovery.ranking_report import build_ranking_report, render_ranking_report


class DiscoveryCliError(RuntimeError):
    """Safe operator-facing CLI failure."""


@dataclass(slots=True)
class DiscoveryCliResources:
    """Own the read-only analytics and optional market-depth clients."""

    service: FalconCandidateService
    falcon_client: FalconClient
    ccxt_client: CcxtPredictionClient | None = None

    async def close(self) -> None:
        """Close every owned client and surface the first cleanup failure."""
        failures: list[BaseException] = []
        if self.ccxt_client is not None:
            try:
                await self.ccxt_client.close()
            except BaseException as exc:  # pragma: no cover - defensive close path
                failures.append(exc)
        try:
            await self.falcon_client.close()
        except BaseException as exc:  # pragma: no cover - defensive close path
            failures.append(exc)
        if failures:
            raise failures[0]


def _positive_decimal(value: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("funds must be a decimal number") from exc
    if not result.is_finite() or result <= 0:
        raise argparse.ArgumentTypeError("funds must be positive and finite")
    return result


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return result


def parse_cli_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse and validate the read-only discovery ranking arguments."""
    parser = argparse.ArgumentParser(
        prog="rank-copy-wallets",
        description=(
            "Rank Polymarket wallets for a specific follower bankroll using "
            "Falcon H-Score, Wallet360, trades, PnL, market quality and "
            "optional CCXT visible-depth evidence."
        ),
    )
    parser.add_argument(
        "--funds",
        required=True,
        type=_positive_decimal,
        help="Follower bankroll in USDC.",
    )
    parser.add_argument(
        "--top",
        type=_positive_int,
        default=10,
        help="Maximum number of passing wallets to print (default: 10).",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Render machine-readable JSON.",
    )
    parser.add_argument(
        "--ccxt-depth",
        choices=("auto", "on", "off"),
        default="auto",
        help=(
            "Use public CCXT order-book depth in copyability scoring. 'auto' "
            "uses it when the optional dependency is installed; 'on' fails if "
            "unavailable; 'off' disables it."
        ),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Per-request Falcon timeout (default: 10).",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    return args


def resolve_api_key(environ: Mapping[str, str] | None = None) -> str:
    """Return the first configured Falcon CLI key without logging it."""
    env = os.environ if environ is None else environ
    value = str(env.get("PMCOPY__DISCOVERY__API__TOKEN", "") or "").strip()
    if value:
        return value
    raise DiscoveryCliError("Falcon API key is required in PMCOPY__DISCOVERY__API__TOKEN")


async def discover_and_render(
    service: Any,
    *,
    funds: Decimal,
    top: int,
    as_json: bool,
):
    """Run one injected discovery cycle and render its bounded report."""
    normalized_funds = Decimal(str(funds))
    result = await service.discover(normalized_funds)
    report = build_ranking_report(result, funds=normalized_funds, top=top)
    return result, render_ranking_report(report, as_json=as_json)


async def rank_with_service(
    service: Any,
    *,
    funds: Decimal,
    top: int,
    as_json: bool,
) -> str:
    """Return a rendered report from an injected discovery service."""
    _result, rendered = await discover_and_render(
        service,
        funds=funds,
        top=top,
        as_json=as_json,
    )
    return rendered


async def build_cli_resources(
    *,
    api_key: str,
    ccxt_depth: str,
    timeout_seconds: float,
    base_url: str = FalconApiConfig.model_validate({}).base_url,
    policy: FalconDiscoveryPolicy | None = None,
    stderr: TextIO | None = None,
) -> DiscoveryCliResources:
    """Compose read-only CLI resources behind injected configuration."""
    error_stream = stderr or sys.stderr
    falcon = FalconClient(
        api_key=api_key,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )
    ccxt_client: CcxtPredictionClient | None = None
    capacity_probe: CcxtCapacityProbe | None = None
    try:
        if ccxt_depth != "off":
            try:
                ccxt_client = await create_ccxt_prediction_client(authenticated=False)
                capacity_probe = CcxtCapacityProbe(CcxtPredictionMarketData(ccxt_client))
            except CcxtPredictionUnavailable as exc:
                if ccxt_depth == "on":
                    raise DiscoveryCliError(str(exc)) from exc
                print(
                    "warning: CCXT depth evidence unavailable; continuing with "
                    "Falcon liquidity evidence only",
                    file=error_stream,
                )
        service = FalconCandidateService(
            falcon,
            policy=policy or FalconDiscoveryPolicy(),
            capacity_probe=capacity_probe,
        )
        return DiscoveryCliResources(
            service=service,
            falcon_client=falcon,
            ccxt_client=ccxt_client,
        )
    except BaseException:
        if ccxt_client is not None:
            await ccxt_client.close()
        await falcon.close()
        raise


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run the operator CLI and return its stable process status code."""
    output = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    args = parse_cli_args(argv)
    env = os.environ if environ is None else environ
    resources: DiscoveryCliResources | None = None
    try:
        resources = await build_cli_resources(
            api_key=resolve_api_key(environ),
            base_url=env.get(
                "PMCOPY__DISCOVERY__API__BASE_URL",
                FalconApiConfig.model_validate({}).base_url,
            ),
            ccxt_depth=args.ccxt_depth,
            timeout_seconds=args.timeout_seconds,
            stderr=error_stream,
        )
        result, rendered = await discover_and_render(
            resources.service,
            funds=args.funds,
            top=args.top,
            as_json=args.as_json,
        )
        print(rendered, end="", file=output)
        # A completed, all-rejected ranking is a policy result, not a transport
        # failure.  Give shell automation a distinct status without falling back
        # to a weaker provider.
        return 0 if result.best is not None else 3
    except DiscoveryCliError as exc:
        print(f"error: {exc}", file=error_stream)
        return 2
    finally:
        if resources is not None:
            await resources.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the async CLI through the synchronous console entry point."""
    try:
        return asyncio.run(run_cli(argv))
    except KeyboardInterrupt:
        return 130


__all__ = [
    "DiscoveryCliError",
    "DiscoveryCliResources",
    "build_cli_resources",
    "discover_and_render",
    "main",
    "parse_cli_args",
    "rank_with_service",
    "resolve_api_key",
    "run_cli",
]
