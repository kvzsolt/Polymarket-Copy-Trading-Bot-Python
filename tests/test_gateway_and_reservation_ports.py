"""Tests for WP-E role-segregated gateway protocols and WP-G reservation ports."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.order_lifecycle import IntentReason, IntentSide, OrderIntent, OrderIntentKey
from pmcopy.interfaces.clob_gateway import (
    ClobGateway,
    MarketDataGateway,
    OrderControlGateway,
    OrderQueryGateway,
    OrderSubmissionGateway,
    PriceQuoteGateway,
    PrivateHistoryGateway,
    VenueCompatibilityProbe,
)
from pmcopy.interfaces.repositories import (
    AbstractReservationRepository,
    DuplicateActiveReservationError,
)
from pmcopy.services.execution.reservation_service import (
    ReservationConflict,
    ReservationService,
)
from pmcopy.storage.postgres.reservations_repo import ReservationsRepo


class TestSegregatedGatewayProtocols:
    """ClobGateway composes narrow role protocols without duplicating signatures."""

    def test_clob_gateway_inherits_role_protocols(self) -> None:
        mro = set(ClobGateway.__mro__)
        assert MarketDataGateway in mro
        assert OrderSubmissionGateway in mro
        assert OrderQueryGateway in mro
        assert PrivateHistoryGateway in mro
        assert OrderControlGateway in mro
        assert VenueCompatibilityProbe in mro
        assert PriceQuoteGateway not in mro

    def test_narrow_read_only_double_satisfies_only_its_role(self) -> None:
        class _MarketReaderOnly:
            async def get_market_info(self, token_id: str):
                return {}

            async def get_orderbook(self, token_id: str):
                raise NotImplementedError

            async def get_execution_metadata(self, token_id: str, *, condition_id=None):
                raise NotImplementedError

        reader = _MarketReaderOnly()
        assert isinstance(reader, MarketDataGateway)
        assert not isinstance(reader, OrderSubmissionGateway)
        assert not isinstance(reader, ClobGateway)


class TestReservationServicePersistenceInversion:
    """ReservationService is decoupled from asyncpg and concrete Postgres repos."""

    def test_reservation_service_has_no_asyncpg_or_postgres_repo_imports(self) -> None:
        src = Path("src/pmcopy/services/execution/reservation_service.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(src)
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
        assert "asyncpg" not in imported
        assert not any(m.startswith("pmcopy.storage.postgres") for m in imported)

    def test_postgres_reservations_repo_implements_abstract_port(self) -> None:
        assert issubclass(ReservationsRepo, AbstractReservationRepository)

    @pytest.mark.asyncio
    async def test_duplicate_active_reservation_error_translates_to_conflict(self) -> None:
        repo = AsyncMock(spec=AbstractReservationRepository)
        repo.get_active_by_intent_key = AsyncMock(return_value=None)
        repo.sum_active_quote_for_user = AsyncMock(return_value=Decimal("0"))
        repo.create_buy_reservation_checked_by_intent = AsyncMock(
            side_effect=DuplicateActiveReservationError(
                "duplicate active intent", constraint_name="uq_active_intent"
            )
        )
        svc = ReservationService(
            reservations_repo=repo,
            positions_repo=AsyncMock(),
            get_balance_func=AsyncMock(return_value=Decimal("100")),
            funder_address="0x0000000000000000000000000000000000000001",
        )
        intent = OrderIntent(
            key=OrderIntentKey(
                user_id="u1",
                leader_wallet="0x0000000000000000000000000000000000000002",
                condition_id="0xcond",
                token_id="tok-1",
                side=IntentSide.BUY,
                reason=IntentReason.COPY_BUY,
            ),
            size=Decimal("5"),
            price=Decimal("0.40"),
            max_slippage_bps=100,
            created_at=datetime.now(timezone.utc),
        )
        with pytest.raises(ReservationConflict) as exc_info:
            await svc.reserve_for_intent(intent)
        assert isinstance(exc_info.value.__cause__, DuplicateActiveReservationError)
        assert exc_info.value.__cause__.constraint_name == "uq_active_intent"
