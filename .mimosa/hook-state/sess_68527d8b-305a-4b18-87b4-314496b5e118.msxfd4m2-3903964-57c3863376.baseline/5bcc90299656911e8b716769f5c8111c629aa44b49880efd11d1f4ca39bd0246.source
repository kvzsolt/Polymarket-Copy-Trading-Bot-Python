"""Composition contracts for CLI reconciliation venue selection."""

from __future__ import annotations

from pathlib import Path


def test_reconcile_commands_use_the_sdk_default_runtime_builder() -> None:
    """Manual reconciliation must not bypass the configured venue backend."""

    root = Path(__file__).resolve().parents[1]
    for relative in ("bot_cli/commands/reconcile.py", "bot_cli/commands/orders.py"):
        source = (root / relative).read_text(encoding="utf-8")
        assert "ReconciliationCommandService" in source
        assert "CcxtRuntimeGatewayFactory" not in source
