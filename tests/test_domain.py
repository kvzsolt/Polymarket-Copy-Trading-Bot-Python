"""Canonical repository port ABCs and pure domain-model immutability tests."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from decimal import Decimal
from typing import Any

import pytest

from pmcopy.domain.models import TradeSignal
from pmcopy.ports.repositories import (
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractLeaderRepository,
    AbstractOrderRepository,
    AbstractPositionRepository,
    AbstractReservationRepository,
)

ALL_PORTS = [
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractLeaderRepository,
    AbstractOrderRepository,
    AbstractPositionRepository,
    AbstractReservationRepository,
]


class TestAbstractBaseClasses:
    @pytest.mark.parametrize("port", ALL_PORTS, ids=lambda p: p.__name__)
    def test_cannot_instantiate_directly(self, port: type) -> None:
        with pytest.raises(TypeError, match="abstract"):
            port()  # type: ignore[abstract]

    @pytest.mark.parametrize("port", ALL_PORTS, ids=lambda p: p.__name__)
    def test_exposes_at_least_one_abstract_method(self, port: type) -> None:
        assert len(port.__abstractmethods__) >= 1

    @pytest.mark.parametrize("port", ALL_PORTS, ids=lambda p: p.__name__)
    def test_all_abstracts_are_coroutines(self, port: type) -> None:
        for name in port.__abstractmethods__:
            method = getattr(port, name)
            assert inspect.iscoroutinefunction(method), f"{port.__name__}.{name} should be async"

    def test_partial_impl_remains_abstract(self) -> None:
        class HalfRepo(AbstractCursorRepository):
            async def get_cursor(self, leader_id: str):  # type: ignore[override]
                return None

        with pytest.raises(TypeError, match="set_cursor"):
            HalfRepo()  # type: ignore[abstract]


class _StubCursorRepo(AbstractCursorRepository):
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}

    async def get_cursor(self, leader_id: str):
        from pmcopy.storage.postgres.cursors_repo import TradeCursor

        return self.store.get(leader_id.lower(), TradeCursor(timestamp=0, tx_hash=""))

    async def set_cursor(self, leader_id: str, cursor) -> None:
        self.store[leader_id.lower()] = cursor


class TestLiskovConformance:
    def test_stub_cursor_repo_is_instance(self) -> None:
        repo = _StubCursorRepo()
        assert isinstance(repo, AbstractCursorRepository)


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
