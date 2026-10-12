"""Reviewed historical evidence preview and explicitly requested finite RPC reads."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from dataclasses import asdict
import json
from pathlib import Path
from typing import Protocol

from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.domain.history_import import HistoryImportError, HistoryImportPlan
from pmcopy.services.history_import import HistoryGateway, HistoryImportCollector, HistoryImportOutcome
from pmcopy.storage.history_import import HistoryImportJournal
from pmcopy.storage.provider_budget import SqliteProviderBudget


class HistoryProviderRuntime(Protocol):
    """Only an admitted read gateway and local cleanup are available to this command."""

    @property
    def gateway(self) -> HistoryGateway:
        """Return the existing shared-ledger observed RPC adapter."""
        ...

    def close(self) -> None:
        """Release local admission storage; never reset usage."""
        ...


class HistoryImportCommand:
    """Construct provider state only inside explicit read-only execution."""

    def __init__(
        self,
        config_loader: Callable[[], object],
        provider_factory: Callable[[object], HistoryProviderRuntime],
    ) -> None:
        """Inject construction seams so preview and tests need no credentials."""
        self._config_loader, self._provider_factory = config_loader, provider_factory

    async def execute(self, plan: HistoryImportPlan, journal_path: Path) -> HistoryImportOutcome:
        """Collect into the job spool; the bot's existing publisher drains it later."""
        journal = HistoryImportJournal(journal_path, plan)
        try:
            provider = self._provider_factory(self._config_loader())
            try:
                return await HistoryImportCollector(
                    plan,
                    journal,
                    provider.gateway,
                    rpc_cost=SqliteProviderBudget.rpc_cost,
                    clock=SystemReceiptClock(),
                ).collect()
            finally:
                provider.close()
        finally:
            journal.close()


def register_history_import_command(subparsers: argparse._SubParsersAction) -> None:
    """Register a preview by default, with one explicit finite execution flag."""
    parser = subparsers.add_parser(
        "history-import", help="Preview or collect reviewed historical protocol evidence"
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--execute-read-only", action="store_true")


def run_history_import(args: argparse.Namespace) -> int:
    """Print only public planning data or bounded codes, never provider exceptions."""
    try:
        with args.plan.open("rb") as source:
            payload = source.read(262145)
        if len(payload) > 262144:
            raise HistoryImportError("manifest_limit")
        plan = HistoryImportPlan.from_dict(json.loads(payload))
        if not args.execute_read_only:
            print(json.dumps(plan.preview(), sort_keys=True))
            return 0
        if args.journal is None:
            raise HistoryImportError("history_journal_required")
        # Approved loader/runtime are deliberately imported after pure preview.
        from pmcopy.config.app_config import load_config
        from pmcopy.orchestrators.provider_runtime import ProviderRuntime

        outcome = asyncio.run(
            HistoryImportCommand(load_config, ProviderRuntime.from_config).execute(
                plan, args.journal
            )
        )
        print(json.dumps(asdict(outcome), sort_keys=True))
        return 0 if outcome.complete else 2
    except Exception:
        # Configuration and transport failures can contain credentials; never echo them.
        print(json.dumps({"complete": False, "reason": "history_import_unavailable"}))
        return 2
