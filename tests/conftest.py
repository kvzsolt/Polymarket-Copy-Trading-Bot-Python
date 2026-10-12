"""Shared test fixtures (Issue #75).

Promotes the ad-hoc ``_make_exec_config`` / ``_make_gateway`` / ``_StubClob``
helpers that lived inline in ``tests/test_issue66_gateway_isolation.py`` to
the project-wide fixture surface so the release-gate suite (and any future
integration tests) can construct a fail-closed-compatible configuration and
    a mock unified CLOB gateway without duplicating boilerplate.

The fixtures here are deliberately minimal: they expose just enough surface
for ``ExecutionService``, ``ReconciliationService``, ``ReservationService``,
and the WSS clients to function under a paper-trade harness.
"""

from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.integrations.polymarket.models import (
    VenueCancelResult,
    VenueOrderState,
    VenueSubmitResult,
)
from pmcopy.interfaces.clob_gateway import ClobGateway


@pytest.fixture(autouse=True)
def _hermetic_deployment_root(
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the config deployment root to an empty per-test directory.

    ``config.app_config._resolve_deployment_root`` resolves to the
    ``PMCOPY_DEPLOYMENT_ROOT`` env var or the CWD, and would otherwise merge
    the operator's real ``config/runtime.local.yml`` (e.g. ``trading_mode:
    live``) and ``secrets.env`` into any test that calls ``load_config()``
    with default paths while the suite runs from the checkout root, making
    the suite non-hermetic. Tests that exercise the env-var/CWD resolution
    itself delete or override ``PMCOPY_DEPLOYMENT_ROOT`` explicitly via
    ``monkeypatch``.
    """
    root = tmp_path_factory.mktemp("hermetic-deployment-root")
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(root))


@pytest.fixture(autouse=True)
def _hermetic_legacy_equity_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent legacy equity helpers from reading or writing operator state."""
    import pmcopy.utils.dayEquity as day_equity

    monkeypatch.setattr(day_equity, "_STATE_FILE", tmp_path / "day_equity.json")


@pytest.fixture(autouse=True)
def _run_unit_thread_work_inline(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep unit tests deterministic and free of real executor threads.

    Integration and release-gate tests retain the real ``asyncio.to_thread``
    behavior. Unit tests use mocked synchronous boundaries, so starting real
    worker threads only adds teardown risk and cannot validate RPC behavior.
    Dedicated adapter tests replace this shim to verify non-blocking calls.
    """
    if request.node.get_closest_marker("integration") is not None:
        return
    if request.node.get_closest_marker("release_gate") is not None:
        return

    async def _inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", _inline)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Keep Docker/Testcontainers tests out of the portable default tier.

    Several historical tests instantiated ``PostgresContainer`` but forgot an
    integration marker, making a plain ``pytest`` depend on a Docker socket.
    Classify by source dependency so new Testcontainers tests cannot repeat the
    same mistake.
    """
    source_cache: dict[Path, bool] = {}
    for item in items:
        path = Path(str(item.path))
        if path not in source_cache:
            try:
                source = path.read_text(encoding="utf-8")
            except OSError:
                source = ""
            source_cache[path] = "testcontainers" in source or "PostgresContainer" in source
        if source_cache[path]:
            item.add_marker(pytest.mark.integration)


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
        submit_result = VenueSubmitResult(success=True, order_id="ord-mock", status="submitted")
    gateway = MagicMock(spec=ClobGateway)
    if isinstance(submit_result, Exception):
        gateway.submit_marketable_order = AsyncMock(side_effect=submit_result)
    else:
        gateway.submit_marketable_order = AsyncMock(return_value=submit_result)
    gateway.submit_limit_order = AsyncMock(
        return_value=VenueSubmitResult(success=True, order_id="ord-limit", status="submitted")
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
    gateway.get_execution_metadata = AsyncMock(
        return_value=ExecutionMetadata(
            token_id="0xtest_token",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
        )
    )
    gateway.get_orderbook = AsyncMock()
    gateway.cancel_order = AsyncMock(
        return_value=VenueCancelResult(success=True, order_id="ord-mock")
    )
    gateway.check_compatibility = AsyncMock(
        return_value={"available": True, "authenticated": False, "live_execution_ready": False}
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
