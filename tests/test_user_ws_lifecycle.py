"""PR 5: UserWebSocket classifies trade statuses → lifecycle logs.

Pins:
  - CONFIRMED event emits status="confirmed" lifecycle log
  - MATCHED / MINED / RETRYING emit non-terminal lifecycle logs
  - FAILED emits status="failed" lifecycle log
  - UserWebSocket never touches positions/reservations
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.clients.ws.user_ws import UserWebSocket
from pmcopy.utils.logger import _reset_lifecycle_dedup_for_tests


@pytest.fixture(autouse=True)
def _clear_dedup():
    _reset_lifecycle_dedup_for_tests()
    yield
    _reset_lifecycle_dedup_for_tests()


def _ws() -> UserWebSocket:
    return UserWebSocket(cache=MarketStateStore())


def _fill_event(trade_status: str, *, order_id: str = "ord-1", token: str = "0xtok") -> dict:
    return {
        "event_type": "trade",
        "asset_id": token,
        "order_id": order_id,
        "side": "BUY",
        "trade_status": trade_status,
        "size": "5.0",
        "price": "0.50",
        "condition_id": "cond-1",
    }


@pytest.mark.parametrize(
    "trade_status,canonical",
    [
        ("MATCHED", "matched"),
        ("MINED", "mined"),
        ("CONFIRMED", "confirmed"),
        ("RETRYING", "retrying"),
        ("FAILED", "failed"),
    ],
)
def test_wss_event_emits_canonical_lifecycle_status(trade_status, canonical):
    ws = _ws()
    with (
        patch("pmcopy.clients.ws.user_ws._log_lc_fallback", create=True),
        patch("pmcopy.utils.logger.log_order_lifecycle") as lc,
    ):
        ws._handle_fill(_fill_event(trade_status, order_id=f"ord-{canonical}"))
    assert lc.call_count == 1
    kwargs = lc.call_args.kwargs
    assert kwargs["status"] == canonical
    assert kwargs["side"] == "BUY"
    assert kwargs["order_id"] == f"ord-{canonical}"


def test_unknown_trade_status_does_not_emit_lifecycle_log():
    ws = _ws()
    with patch("pmcopy.utils.logger.log_order_lifecycle") as lc:
        ws._handle_fill(_fill_event("WEIRD_STATUS"))
    lc.assert_not_called()


def test_user_websocket_does_not_carry_positions_or_reservations():
    """Structural pin: settlement remains with ReconciliationService."""
    ws = _ws()
    assert not hasattr(ws, "positions_repo")
    assert not hasattr(ws, "fills_repo")
    assert not hasattr(ws, "reservation_service")


def test_fill_event_invalidates_token_cache():
    """PR4 invariant: own fill evicts market state for that token."""
    cache = MarketStateStore()
    ws = UserWebSocket(cache=cache)
    # Seed some state then verify it's evicted.
    from decimal import Decimal

    cache.set_quote("0xtok", best_bid=Decimal("0.49"), best_ask=Decimal("0.51"))
    ws._handle_fill(_fill_event("CONFIRMED"))
    assert cache.get_quote("0xtok") is None
