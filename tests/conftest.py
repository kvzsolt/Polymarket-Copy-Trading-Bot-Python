"""Shared test fixtures (Issue #75).

Promotes the ad-hoc ``_make_exec_config`` / ``_make_gateway`` / ``_StubClob``
helpers that lived inline in ``tests/test_issue66_gateway_isolation.py`` to
the project-wide fixture surface so the release-gate suite (and any future
integration tests) can construct a fail-closed-compatible configuration and
a mock CLOB V2 gateway without duplicating boilerplate.

The fixtures here are deliberately minimal: they expose just enough surface
for ``ExecutionService``, ``ReconciliationService``, ``ReservationService``,
and the WSS clients to function under a paper-trade harness.
"""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from integrations.polymarket.models import (
    VenueCancelResult,
    VenueOrderState,
    VenueSubmitResult,
)
from interfaces.clob_gateway import ClobGateway
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


def make_exec_config(
    *,
    min_share_price: float = 0.0,
    paper_trade_only: bool = False,
    require_fresh_book: bool = False,
) -> SimpleNamespace:
    """Build a SimpleNamespace that mimics ``AppConfig.execution``."""
    execution = SimpleNamespace(
        min_share_price=min_share_price,
        max_share_price=0.0,
        pending_submission_ttl_seconds=120,
        enable_executable_order_guard=True,
        circuit_failure_threshold=5,
        circuit_cooldown_seconds=60,
        execution_require_fresh_book=require_fresh_book,
        book_spread_mode="absolute_bps",
        max_book_spread_bps=5000,
        max_book_spread_ticks=0,
        slippage_tolerance=2.0,
        max_retries=3,
        retry_backoff_base=1.0,
        non_terminal_failure_max_attempts=3,
        transaction_fee_bps=20,
        gas_cost_usdc=0.03,
        risk_buffer_pct=0.10,
        paper_trade_only=paper_trade_only,
        enable_copy_decision_gate=False,
        enable_near_resolution_gate=False,
        near_resolution_gate_price=0.95,
        near_resolution_max_age_seconds=15,
        meta_cache_ttl_good=300,
        meta_cache_ttl_thin=30,
    )
    return SimpleNamespace(
        execution=execution,
        raw={"EXECUTION_SUBMIT_CONCURRENCY": 2},
    )


@pytest.fixture
def exec_config() -> SimpleNamespace:
    return make_exec_config()


def make_clob_gateway(
    submit_result: VenueSubmitResult | Exception | None = None,
) -> MagicMock:
    """Build a ``MagicMock(spec=ClobGateway)`` preset with the same awaitable
    surface used in ``tests/test_issue66_gateway_isolation.py``."""
    if submit_result is None:
        submit_result = VenueSubmitResult(
            success=True, order_id="ord-mock", status="submitted"
        )
    gateway = MagicMock(spec=ClobGateway)
    if isinstance(submit_result, Exception):
        gateway.submit_marketable_order = AsyncMock(side_effect=submit_result)
    else:
        gateway.submit_marketable_order = AsyncMock(return_value=submit_result)
    gateway.submit_limit_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True, order_id="ord-limit", status="submitted"
        )
    )
    gateway.get_order = AsyncMock(
        return_value=VenueOrderState(
            order_id="ord-mock",
            status="filled",
            size=Decimal("5"),
            filled_size=Decimal("5"),
        )
    )
    gateway.get_fills = AsyncMock(return_value=[])
    gateway.get_market_info = AsyncMock(
        return_value={"tick_size": "0.01", "neg_risk": False, "min_order_size": "1"}
    )
    gateway.get_orderbook = AsyncMock()
    gateway.cancel_order = AsyncMock(
        return_value=VenueCancelResult(success=True, order_id="ord-mock")
    )
    gateway.check_compatibility = AsyncMock(
        return_value={"sdk_available": True}
    )
    return gateway


@pytest.fixture
def mock_clob_gateway() -> MagicMock:
    return make_clob_gateway()


class StubClobClient:
    """Minimal venue stub for the legacy ``clob_client`` arg used by
    ExecutionService's slippage / price-lookup path. Submission goes through
    the injected gateway, not through this stub."""

    def __init__(self) -> None:
        self.get_price = AsyncMock(return_value={"price": "0.50"})
        self.client = SimpleNamespace(
            get_order=lambda _oid: {
                "status": "filled",
                "size_matched": "5",
                "avg_price": "0.50",
            }
        )


@pytest.fixture
def stub_clob_client() -> StubClobClient:
    return StubClobClient()


@pytest.fixture
def fresh_db_path(tmp_path) -> str:
    """Path to a per-test SQLite file with migrations pre-applied."""
    path = tmp_path / "release_gate.db"
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()
    return str(path)


class ScriptedWss:
    """Async iterator over scripted WSS frames.

    Tests preload it with the dicts they expect to receive and then iterate
    it like a real ``websockets`` connection. ``recv()`` raises
    ``StopAsyncIteration`` once exhausted so reconnect loops can be exercised.
    """

    def __init__(self, frames: list[dict] | None = None) -> None:
        self._frames: list[dict] = list(frames or [])
        self._sent: list[str] = []
        self.closed: bool = False

    def push(self, frame: dict) -> None:
        self._frames.append(frame)

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("WSS closed")
        self._sent.append(message)

    async def recv(self) -> str:
        import json as _json

        if not self._frames:
            self.closed = True
            raise StopAsyncIteration
        return _json.dumps(self._frames.pop(0))

    @property
    def sent_messages(self) -> list[str]:
        return list(self._sent)


@pytest.fixture
def scripted_wss() -> ScriptedWss:
    return ScriptedWss()
