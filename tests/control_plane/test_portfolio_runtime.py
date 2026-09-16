"""Bounded cached portfolio runtime tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal


class MutableClock:
    """Expose deterministic aware time that tests can advance."""

    def __init__(self) -> None:
        self.value = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        """Return the current test instant."""

        return self.value


class FixedSource:
    """Return one fixed portfolio account snapshot."""

    def __init__(self, snapshot: object) -> None:
        self._snapshot = snapshot

    async def read(self) -> object:
        """Return the configured snapshot."""

        return self._snapshot


class FailingSource:
    """Raise one sanitized boundary failure."""

    async def read(self) -> object:
        """Fail without returning a partial snapshot."""

        raise RuntimeError("provider response with token material")


class DeferredSource:
    """Hold a refresh read until the test releases it explicitly."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def read(self) -> object:
        """Wait for deterministic release before returning a snapshot."""

        self.entered.set()
        await self.release.wait()
        return account_snapshot()


def account_snapshot() -> object:
    """Return one exact account snapshot from the production contract."""

    from services.control_plane.portfolio import PortfolioAccountSnapshot

    return PortfolioAccountSnapshot(
        follower_account="user-a",
        aliases=("0xFollower",),
        cash_usdc=Decimal("100.005"),
        reserved_usdc=Decimal("2.50"),
        positions_cost_usdc=Decimal("25.125"),
        position_count=2,
    )


async def test_portfolio_cache_serves_aliases_and_marks_expired_snapshot_stale() -> None:
    """GUI reads stay local and stale state is computed from injected time."""

    from services.control_plane.portfolio import PortfolioSnapshotCache

    clock = MutableClock()
    cache = PortfolioSnapshotCache(
        clock=clock,
        stale_after=timedelta(seconds=30),
    )
    cache.update(account_snapshot())  # type: ignore[arg-type]

    current = await cache.get_cached_portfolio("0xfollower")
    assert current is not None
    assert current.follower_account == "user-a"
    assert current.cash_usdc == "100.005"
    assert current.reserved_usdc == "2.50"
    assert current.copyable_balance_usdc == "97.505"
    assert current.positions_cost_usdc == "25.125"
    assert current.positions_value_usdc is None
    assert current.equity_usdc is None
    assert current.position_count == 2
    assert current.valuation_basis == "unavailable"
    assert current.unrealized_pnl_usdc is None
    assert current.stale is False

    clock.value += timedelta(seconds=31)
    stale = await cache.get_cached_portfolio("user-a")
    assert stale is not None and stale.stale is True


async def test_refresh_service_updates_successes_and_contains_source_failures() -> None:
    """One failed follower cannot erase or block another cached snapshot."""

    from services.control_plane.portfolio import (
        PortfolioRefreshService,
        PortfolioSnapshotCache,
    )

    cache = PortfolioSnapshotCache(
        clock=MutableClock(),
        stale_after=timedelta(seconds=30),
    )
    service = PortfolioRefreshService(
        sources=(FixedSource(account_snapshot()), FailingSource()),  # type: ignore[arg-type]
        cache=cache,
        refresh_interval_seconds=10.0,
    )

    result = await service.refresh_once()

    assert result.updated_count == 1
    assert result.failed_count == 1
    assert await cache.get_cached_portfolio("user-a") is not None


async def test_refresh_service_start_never_waits_for_provider_io() -> None:
    """Optional portfolio I/O cannot delay control-plane or trading startup."""

    from services.control_plane.portfolio import (
        PortfolioRefreshService,
        PortfolioSnapshotCache,
    )

    source = DeferredSource()
    cache = PortfolioSnapshotCache(
        clock=MutableClock(),
        stale_after=timedelta(seconds=30),
    )
    service = PortfolioRefreshService(
        sources=(source,),  # type: ignore[arg-type]
        cache=cache,
        refresh_interval_seconds=10.0,
    )
    start_task = asyncio.create_task(service.start())
    try:
        await asyncio.sleep(0)
        assert start_task.done()
        await start_task
        await source.entered.wait()
    finally:
        source.release.set()
        if not start_task.done():
            start_task.cancel()
        await service.close()


@dataclass(frozen=True, slots=True)
class Position:
    """Minimal durable position used by the context source."""

    size_tokens: Decimal
    avg_entry_price: Decimal


class Positions:
    """Return deterministic durable positions."""

    async def list_held_positions(self, user_id: str) -> list[Position]:
        """Return two exact cost-basis rows."""

        assert user_id == "user-a"
        return [
            Position(Decimal("10"), Decimal("0.4")),
            Position(Decimal("5"), Decimal("0.25")),
        ]


class Reservations:
    """Return active locally reserved quote collateral."""

    async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
        """Return deterministic active reservations."""

        assert user_id == "user-a"
        return Decimal("2.5")


class Balance:
    """Return current follower collateral through an injected adapter."""

    async def read(self, wallet: str) -> Decimal | None:
        """Return deterministic exact collateral."""

        assert wallet == "0xfollower"
        return Decimal("100")


async def test_context_portfolio_source_uses_cost_basis_and_active_reservations() -> None:
    """Build a truthful snapshot without inventing market marks or PnL."""

    from orchestrators.control_plane_portfolio import TradingContextPortfolioSource

    source = TradingContextPortfolioSource(
        follower_account="user-a",
        wallet="0xfollower",
        positions=Positions(),
        reservations=Reservations(),
        balance=Balance(),
    )

    snapshot = await source.read()

    assert snapshot.cash_usdc == Decimal("100")
    assert snapshot.reserved_usdc == Decimal("2.5")
    assert snapshot.positions_cost_usdc == Decimal("5.25")
    assert snapshot.position_count == 2


async def test_portfolio_runtime_factory_composes_injected_account_bindings() -> None:
    """Composition creates cache and refresh owners without provider I/O."""

    from orchestrators.control_plane_portfolio import (
        PortfolioAccountBinding,
        PortfolioRuntimeFactory,
    )

    bundle = PortfolioRuntimeFactory(
        clock=MutableClock(),
        balance=Balance(),
    ).build(
        bindings=(
            PortfolioAccountBinding(
                follower_account="user-a",
                wallet="0xfollower",
                positions=Positions(),
                reservations=Reservations(),
            ),
        ),
        refresh_interval_seconds=10.0,
        stale_after_seconds=30.0,
    )

    assert bundle is not None
    assert await bundle.cache.get_cached_portfolio("user-a") is None
    result = await bundle.refresh.refresh_once()
    assert result.updated_count == 1
    assert await bundle.cache.get_cached_portfolio("user-a") is not None


def test_portfolio_runtime_factory_omits_unbound_accounts() -> None:
    """No portfolio capability is composed when no account is fully bound."""

    from orchestrators.control_plane_portfolio import PortfolioRuntimeFactory

    bundle = PortfolioRuntimeFactory(
        clock=MutableClock(),
        balance=Balance(),
    ).build(
        bindings=(),
        refresh_interval_seconds=10.0,
        stale_after_seconds=30.0,
    )

    assert bundle is None
