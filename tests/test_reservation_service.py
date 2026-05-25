import pytest
pytestmark = pytest.mark.skip(reason="sqlite-bound; pending PG port")
from decimal import Decimal
from datetime import datetime, timezone
import pytest

from domain.order_lifecycle import OrderIntent, OrderIntentKey, IntentSide, IntentReason
from services.execution.reservation_service import ReservationService, ReservationConflict, InsufficientFreeQuote, InsufficientFreeShares, Clock
from storage.sqlite.reservations_repo import ReservationsRepo
from storage.sqlite.positions_repo import PositionsRepo
from storage.sqlite.migrations import apply_migrations

class MockClock(Clock):
    def __init__(self, current_time: int):
        self._current_time = current_time
        
    def utcnow_timestamp(self) -> int:
        return self._current_time
        
    def advance(self, seconds: int):
        self._current_time += seconds

async def mock_get_balance(address: str) -> float | None:
    if address == "0xfunder":
        return 100.0
    return None

@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test_reservations.db"
    from storage.sqlite.connection import create_connection
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
    return str(path)

@pytest.fixture
def reservations_repo(db_path):
    return ReservationsRepo(db_path)

@pytest.fixture
def positions_repo(db_path):
    return PositionsRepo(db_path)

@pytest.fixture
def mock_clock():
    return MockClock(1000)

@pytest.fixture
def reservation_service(reservations_repo, positions_repo, mock_clock):
    svc = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=mock_get_balance,
        funder_address="0xfunder",
        clock=mock_clock,
        ttl_seconds=60,
    )
    async def fake_reconciler(res_id: str) -> bool:
        return True
    svc.reconciler_callback = fake_reconciler
    return svc

def create_mock_intent(side: IntentSide, size: Decimal = Decimal("10"), price: Decimal = Decimal("0.5")) -> OrderIntent:
    return OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="cond1",
            token_id="1" * 70,
            side=side,
            reason=IntentReason.COPY_BUY if side == IntentSide.BUY else IntentReason.COPY_SELL,
        ),
        size=size,
        price=price,
        max_slippage_bps=200,
        created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
    )

@pytest.mark.asyncio
async def test_reserve_buy_success(reservation_service, mock_clock):
    intent = create_mock_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
    res = await reservation_service.reserve_for_intent(intent)
    
    assert res.status == "active"
    assert res.reserved_usdc == Decimal("5.0")
    assert res.reserved_size == Decimal("0")
    assert res.expires_at.timestamp() == 1060

@pytest.mark.asyncio
async def test_reserve_sell_success(reservation_service, positions_repo):
    # Need a position first
    positions_repo.apply_fill(
        user_id="u1",
        leader_id="0xleader",
        condition_id="cond1",
        token_id="1" * 70,
        side="BUY",
        fill_size=Decimal("20"),
        fill_price=Decimal("0.5"),
    )
    
    intent = create_mock_intent(IntentSide.SELL, size=Decimal("15"))
    res = await reservation_service.reserve_for_intent(intent)
    
    assert res.status == "active"
    assert res.reserved_size == Decimal("15")
    assert res.reserved_usdc == Decimal("0")

@pytest.mark.asyncio
async def test_reserve_duplicate_intent_raises_conflict(reservation_service):
    intent = create_mock_intent(IntentSide.BUY)
    await reservation_service.reserve_for_intent(intent)
    
    with pytest.raises(ReservationConflict):
        await reservation_service.reserve_for_intent(intent)

@pytest.mark.asyncio
async def test_reserve_insufficient_usdc(reservation_service):
    # Balance is 100
    intent = create_mock_intent(IntentSide.BUY, size=Decimal("300"), price=Decimal("0.5"))
    with pytest.raises(InsufficientFreeQuote, match="insufficient free USDC"):
        await reservation_service.reserve_for_intent(intent)

@pytest.mark.asyncio
async def test_reserve_insufficient_position(reservation_service, positions_repo):
    # No position exists
    intent = create_mock_intent(IntentSide.SELL, size=Decimal("15"))
    with pytest.raises(InsufficientFreeShares, match="no position found"):
        await reservation_service.reserve_for_intent(intent)

@pytest.mark.asyncio
async def test_expired_reservation_allows_new_reservation(reservation_service, mock_clock):
    intent = create_mock_intent(IntentSide.BUY)
    res = await reservation_service.reserve_for_intent(intent)
    
    # Fast forward past TTL
    mock_clock.advance(61)
    
    # Should not raise conflict
    res2 = await reservation_service.reserve_for_intent(intent)
    assert res2.id != res.id
    assert res2.status == "active"

@pytest.mark.asyncio
async def test_release_terminal(reservation_service, reservations_repo):
    intent = create_mock_intent(IntentSide.BUY)
    res = await reservation_service.reserve_for_intent(intent)
    
    reservation_service.release_terminal(res.id, "cancelled")
    
    # Should not be active anymore
    active = reservations_repo.get_active_by_intent_key(intent.key.stable_key())
    assert active is None
