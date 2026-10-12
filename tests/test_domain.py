"""Phase 2 tests — domain ports and pure domain-model behaviour.

Validates:
    * Every port ABC rejects direct instantiation (``TypeError``).
    * Partial implementations that omit an abstract method are still abstract.
    * Fully-implemented subclasses instantiate cleanly and are recognised as
      instances of their port (LSP check).
    * Existing domain models (``TradeSignal``) preserve frozen semantics.
"""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from decimal import Decimal
from typing import Any

import pytest

from pmcopy.domain.models import TradeSignal
from pmcopy.domain.ports import (
    AbstractAnalyticsClient,
    AbstractBalanceClient,
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractHttpClient,
    AbstractLeaderRepository,
    AbstractMarketClient,
    AbstractNotifier,
    AbstractOrderGateway,
    AbstractOrderRepository,
    AbstractPositionRepository,
)

ALL_PORTS = [
    AbstractAnalyticsClient,
    AbstractBalanceClient,
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractHttpClient,
    AbstractLeaderRepository,
    AbstractMarketClient,
    AbstractNotifier,
    AbstractOrderGateway,
    AbstractOrderRepository,
    AbstractPositionRepository,
]


# ---------------------------------------------------------------------------
# ABC instantiation guards
# ---------------------------------------------------------------------------


class TestAbstractBaseClasses:
    @pytest.mark.parametrize("port", ALL_PORTS, ids=lambda p: p.__name__)
    def test_cannot_instantiate_directly(self, port: type) -> None:
        with pytest.raises(TypeError, match="abstract"):
            port()  # type: ignore[abstract]

    @pytest.mark.parametrize("port", ALL_PORTS, ids=lambda p: p.__name__)
    def test_exposes_at_least_one_abstract_method(self, port: type) -> None:
        assert len(port.__abstractmethods__) >= 1

    def test_partial_impl_remains_abstract(self) -> None:
        class HalfRepo(AbstractCursorRepository):
            def get_cursor(self, leader_id: str):  # type: ignore[override]
                return None

            # set_cursor intentionally not implemented

        with pytest.raises(TypeError, match="set_cursor"):
            HalfRepo()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# Liskov conformance — stub implementations are usable
# ---------------------------------------------------------------------------


class _StubCursorRepo(AbstractCursorRepository):
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}

    def get_cursor(self, leader_id: str):  # type: ignore[override]
        from pmcopy.storage.postgres.cursors_repo import TradeCursor

        return self.store.get(leader_id.lower(), TradeCursor(timestamp=0, tx_hash=""))

    def set_cursor(self, leader_id: str, cursor) -> None:  # type: ignore[override]
        self.store[leader_id.lower()] = cursor


class _StubNotifier(AbstractNotifier):
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def notify(
        self,
        *,
        title: str,
        message: str,
        severity: str = "info",
        context: dict[str, Any] | None = None,
    ) -> None:
        self.messages.append(
            {"title": title, "message": message, "severity": severity, "context": context}
        )


class TestLiskovConformance:
    def test_stub_cursor_repo_is_instance(self) -> None:
        repo = _StubCursorRepo()
        assert isinstance(repo, AbstractCursorRepository)

    @pytest.mark.asyncio
    async def test_stub_notifier_records_messages(self) -> None:
        notifier = _StubNotifier()
        await notifier.notify(title="ok", message="done")
        assert notifier.messages == [
            {"title": "ok", "message": "done", "severity": "info", "context": None}
        ]


# ---------------------------------------------------------------------------
# Client ports declare async abstracts — make sure signatures are coroutines
# ---------------------------------------------------------------------------


class TestClientPortsAreAsync:
    @pytest.mark.parametrize(
        "port",
        [
            AbstractMarketClient,
            AbstractBalanceClient,
            AbstractOrderGateway,
            AbstractAnalyticsClient,
            AbstractHttpClient,
            AbstractNotifier,
        ],
        ids=lambda p: p.__name__,
    )
    def test_all_abstracts_are_coroutines(self, port: type) -> None:
        for name in port.__abstractmethods__:
            method = getattr(port, name)
            assert inspect.iscoroutinefunction(method), f"{port.__name__}.{name} should be async"


# ---------------------------------------------------------------------------
# Domain models — frozen semantics preserved
# ---------------------------------------------------------------------------


class TestTradeSignalImmutability:
    def test_trade_signal_is_frozen(self) -> None:
        sig = TradeSignal(
            leader_id="0xabc",
            condition_id="cid",
            token_id="tid",
            side="BUY",
            size=Decimal("10"),
            price=Decimal("0.5"),
            usdc_size=Decimal("5"),
            tx_hash="0xdead",
            event_ts=1700000000,
        )
        with pytest.raises(FrozenInstanceError):
            sig.price = Decimal("0.6")  # type: ignore[misc]

    def test_trade_signal_default_optional_fields(self) -> None:
        sig = TradeSignal(
            leader_id="0xabc",
            condition_id="cid",
            token_id="tid",
            side="SELL",
            size=Decimal("1"),
            price=Decimal("0.5"),
            usdc_size=Decimal("0.5"),
            tx_hash="0xfeed",
            event_ts=1700000000,
        )
        assert sig.title == ""
        assert sig.outcome == ""
        assert sig.submarket == ""
