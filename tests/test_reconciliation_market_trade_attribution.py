"""Market trade-history fill attribution for ambiguous orders.

2026-09-19 incident class: an ambiguous POST retry signs a fresh venue order
with a fresh hash, so the locally persisted order hash never matches the fill
legs the venue attributed the trade to. Direct probes (get_order / get_fills by
order id) are all absent while the venue trade history holds exactly one
matching trade for the market, side, price bound, time window and requested
amount. Attribution must resolve that unique match as a fill and must stay
fail-closed for zero or multiple matches.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from integrations.polymarket.errors import VenueOrderNotFoundError
from services.execution.reconciliation_service import ReconciliationService

ROW_ORDER_ID = "0xROWHASH"
SIBLING_ORDER_ID = "0xSIBLING"


def _order_row(
    now: float,
    *,
    order_id: str = ROW_ORDER_ID,
    side: str = "BUY",
    amount: str = "4.056",
    price_limit: str = "0.3570",
    age_seconds: float = 120.0,
    status: str = "ambiguous_timeout",
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "user_id": "0xUSER",
        "leader_id": "0xLEADER",
        "status": status,
        "created_at": datetime.fromtimestamp(now - age_seconds, tz=timezone.utc),
        "request": {
            "side": side,
            "amount": amount,
            "price_limit": price_limit,
            "condition_id": "cond-1",
            "token_id": "tok-1",
            "reservation_id": "res-1",
        },
        "response": {"status": "ambiguous_timeout", "orderID": order_id, "size_matched": "0"},
        "matched_total": "0",
    }


def _fill(
    *,
    fill_id: str,
    order_id: str,
    side: str = "BUY",
    size: str,
    price: str,
    timestamp: float,
    status: str = "CONFIRMED",
) -> dict[str, Any]:
    return {
        "fill_id": fill_id,
        "order_id": order_id,
        "side": side,
        "size": size,
        "price": price,
        "timestamp": int(timestamp),
        "status": status,
        "transaction_hash": "0xTX-" + fill_id,
        "fee": None,
    }


class _Gateway:
    """Incident-shaped gateway: order absent, open orders empty."""

    def __init__(self, market_fills: list[dict[str, Any]]):
        self.market_fills = market_fills
        self.market_calls = 0

    async def get_order(self, order_id: str) -> Any:
        raise VenueOrderNotFoundError("order_not_found")

    async def list_open_orders(self, *, order_id: str | None = None, market: str | None = None):
        return []

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ):
        if order_id is not None:
            return [fill for fill in self.market_fills if fill.get("order_id") == order_id]
        if trade_id is not None:
            return [fill for fill in self.market_fills if fill.get("fill_id") == trade_id]
        self.market_calls += 1
        return self.market_fills


class _OrdersRepo:
    def __init__(self, row: dict[str, Any], rows_by_order_id: dict[str, Any] | None = None):
        self.row = row
        self.rows_by_order_id = rows_by_order_id or {}
        self.updates: list[dict[str, Any]] = []
        self.processed_marks: list[dict[str, Any]] = []
        self.processed_deletes: list[str] = []

    async def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        if order_id == self.row.get("order_id"):
            return self.row
        return self.rows_by_order_id.get(order_id)

    async def update_submission_status(self, **kwargs) -> None:
        self.updates.append(kwargs)
        if kwargs.get("order_id") == self.row.get("order_id"):
            self.row["status"] = kwargs.get("status")
            patch = kwargs.get("response_patch") or {}
            response = dict(self.row.get("response") or {})
            response.update(patch)
            self.row["response"] = response

    async def mark_processed_trade(self, **kwargs) -> None:
        self.processed_marks.append(kwargs)

    async def delete_processed_trade(self, trade_key: str) -> None:
        self.processed_deletes.append(trade_key)


class _FillsRepo:
    def __init__(self):
        self.appends: list[Any] = []

    async def append_fill_and_apply(self, record, positions_repo, *, raw_payload=None):
        self.appends.append(record)
        return 1, MagicMock()


class _ReservationService:
    def __init__(self):
        self.releases: list[tuple[str, str]] = []

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.releases.append((reservation_id, reason))


class _AuditLogger:
    def __init__(self):
        self.events: list[tuple[str, dict]] = []

    def log_event(self, event_name: str, payload: dict) -> None:
        self.events.append((event_name, payload))


def _service(gateway: _Gateway, orders_repo: _OrdersRepo, *, fills_repo=None, reservations=None):
    return ReconciliationService(
        orders_repo=orders_repo,
        fills_repo=fills_repo or _FillsRepo(),
        positions_repo=MagicMock(),
        reservation_service=reservations or _ReservationService(),
        clob_gateway=gateway,
        audit_logger=_AuditLogger(),
    )


@pytest.mark.asyncio
async def test_unique_buy_trade_attributed_from_market_history():
    now = time.time()
    row = _order_row(now)
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    gateway = _Gateway(fills)
    svc = _service(gateway, _OrdersRepo(row))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["confirmation_source"] == "market_trade_attribution"
    assert result["status"] == "confirmed"
    assert result["size_matched"] == "11.58"
    assert result["avg_price"] == "0.35"
    assert result["trade_ids"] == ["tr-1"]
    assert result["attributed_order_ids"] == [SIBLING_ORDER_ID]
    assert gateway.market_calls == 1


@pytest.mark.asyncio
async def test_pending_legs_attributed_as_matched_not_confirmed():
    now = time.time()
    row = _order_row(now)
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-2",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
            status="MATCHED",
        )
    ]
    svc = _service(_Gateway(fills), _OrdersRepo(row))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["confirmation_source"] == "market_trade_attribution"
    assert result["status"] == "matched"
    assert result["size_matched"] == "11.58"


@pytest.mark.asyncio
async def test_two_matching_trades_fail_closed_unknown():
    now = time.time()
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        ),
        _fill(
            fill_id="tr-2",
            order_id="0xSIBLING2",
            size="11.58",
            price="0.35",
            timestamp=created + 10,
        ),
    ]
    gateway = _Gateway(fills)
    svc = _service(gateway, _OrdersRepo(_order_row(now)))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"
    assert result["size_matched"] == "0"
    assert gateway.market_calls == 1


@pytest.mark.asyncio
async def test_fill_above_buy_price_limit_excluded():
    now = time.time()
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.36",
            timestamp=created + 5,
        )
    ]
    svc = _service(_Gateway(fills), _OrdersRepo(_order_row(now)))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"


@pytest.mark.asyncio
async def test_fill_outside_time_window_excluded():
    now = time.time()
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 400,
        )
    ]
    svc = _service(_Gateway(fills), _OrdersRepo(_order_row(now)))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"


@pytest.mark.asyncio
async def test_fill_under_other_order_row_excluded():
    now = time.time()
    created = now - 120.0
    other_row = _order_row(now, order_id="0xOTHERROW")
    fills = [
        _fill(
            fill_id="tr-1",
            order_id="0xOTHERROW",
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    svc = _service(
        _Gateway(fills), _OrdersRepo(_order_row(now), rows_by_order_id={"0xOTHERROW": other_row})
    )

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_patch",
    [
        {"price_limit": None},
        {"amount": None},
    ],
)
async def test_missing_preconditions_skip_market_probe(request_patch):
    now = time.time()
    row = _order_row(now)
    row["request"].update({k: v for k, v in request_patch.items() if v is not None})
    for key, value in request_patch.items():
        if value is None:
            row["request"].pop(key, None)
    gateway = _Gateway([])
    svc = _service(gateway, _OrdersRepo(row))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"
    assert gateway.market_calls == 0


@pytest.mark.asyncio
async def test_recent_order_not_attributed():
    now = time.time()
    row = _order_row(now, age_seconds=10.0)
    created = now - 10.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    gateway = _Gateway(fills)
    svc = _service(gateway, _OrdersRepo(row))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"
    assert gateway.market_calls == 0


@pytest.mark.asyncio
async def test_sell_exact_share_match_attributed():
    now = time.time()
    created = now - 120.0
    row = _order_row(now, side="SELL", amount="25", price_limit="0.20")
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            side="SELL",
            size="25",
            price="0.22",
            timestamp=created + 5,
        )
    ]
    svc = _service(_Gateway(fills), _OrdersRepo(row))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["confirmation_source"] == "market_trade_attribution"
    assert result["status"] == "confirmed"
    assert result["size_matched"] == "25"
    assert result["avg_price"] == "0.22"


@pytest.mark.asyncio
async def test_sell_size_mismatch_fail_closed():
    now = time.time()
    created = now - 120.0
    row = _order_row(now, side="SELL", amount="25", price_limit="0.20")
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            side="SELL",
            size="24.99",
            price="0.22",
            timestamp=created + 5,
        )
    ]
    svc = _service(_Gateway(fills), _OrdersRepo(row))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["status"] == "unknown"


@pytest.mark.asyncio
async def test_direct_order_fills_short_circuit_attribution():
    now = time.time()
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=ROW_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    gateway = _Gateway(fills)
    svc = _service(gateway, _OrdersRepo(_order_row(now)))

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["confirmation_source"] == "account_trades"
    assert result["size_matched"] == "11.58"
    assert gateway.market_calls == 0


@pytest.mark.asyncio
async def test_attribution_audit_event_emitted():
    now = time.time()
    created = now - 120.0
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    orders_repo = _OrdersRepo(_order_row(now))
    svc = _service(_Gateway(fills), orders_repo)
    audit = svc.audit_logger

    await svc.confirm_fill(ROW_ORDER_ID)

    names = [name for name, _ in audit.events]
    assert "reconciliation_market_trade_attribution" in names


@pytest.mark.asyncio
async def test_reconcile_trade_record_applies_attributed_fill_once():
    now = time.time()
    created = now - 120.0
    row = _order_row(now)
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    fills_repo = _FillsRepo()
    reservations = _ReservationService()
    orders_repo = _OrdersRepo(row)
    svc = _service(_Gateway(fills), orders_repo, fills_repo=fills_repo, reservations=reservations)
    record = {
        "trade_key": "sig-1",
        "user_id": "0xUSER",
        "leader_id": "0xLEADER",
        "tx_hash": "sig-1",
        "asset": "tok-1",
        "condition_id": "cond-1",
        "order_id": ROW_ORDER_ID,
        "request": row["request"],
        "status": "ambiguous_timeout",
        "reservation_id": "res-1",
    }

    applied = await svc.reconcile_trade_record(record, "BUY", confirm_fill=svc.confirm_fill)

    assert applied is True
    assert len(fills_repo.appends) == 1
    fill_record = fills_repo.appends[0]
    assert fill_record.side == "BUY"
    assert fill_record.size_tokens == Decimal("11.58")
    assert fill_record.price == Decimal("0.35")
    assert fill_record.dedup_key == "11.58"
    assert reservations.releases == [("res-1", "confirmed")]
    statuses = [update["status"] for update in orders_repo.updates]
    assert "filled_reconciled" in statuses

    # Second pass is idempotent: no double fill append, no second release.
    record["status"] = "filled_reconciled"
    applied_again = await svc.reconcile_trade_record(
        record, "BUY", confirm_fill=svc.confirm_fill
    )

    assert applied_again is True
    assert len(fills_repo.appends) == 1
    assert reservations.releases == [("res-1", "confirmed")]

@pytest.mark.asyncio
async def test_auto_release_clean_when_delta_matches_sibling_fills():
    """A position delta fully explained by a *different* known order is clean.

    The venue fill's order hash belongs to another orders row whose fills sum
    exactly to the local delta, so this order provably did not move the
    position and auto-release may proceed.
    """
    from unittest.mock import AsyncMock

    now = time.time()
    created = now - 120.0
    row = _order_row(now)
    fills = [
        _fill(
            fill_id="tr-1",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    gateway = _Gateway(fills)
    orders_repo = _OrdersRepo(
        row,
        rows_by_order_id={SIBLING_ORDER_ID: _order_row(now, order_id=SIBLING_ORDER_ID)},
    )
    svc = _service(gateway, orders_repo)

    pos = MagicMock()
    pos.size_tokens = Decimal("11.58")
    svc.positions_repo.get_position = AsyncMock(return_value=pos)

    probes = await svc._run_auto_release_probes(
        order_id=ROW_ORDER_ID,
        venue_order_id=ROW_ORDER_ID,
        market_id="cond-1",
        trade_ids=[],
        user_id="0xUSER",
        condition_id="cond-1",
        token_id="tok-1",
        expected_size=Decimal("4.056"),
        side="BUY",
        pre_submit_position_size=Decimal("0"),
        created_ts=created,
    )

    assert probes["inconclusive"] == 0
    assert probes["clean"] > 0
    assert "sibling_attributed" in probes["details"]["position_delta"]


@pytest.mark.asyncio
async def test_auto_release_inconclusive_when_delta_legs_unattributed():
    """An unattributed window leg keeps position_delta inconclusive (fail closed).

    The position did change, but the fill's order hash is owned by no known
    orders row — exactly the sibling-hash incident shape — so auto-release must
    stay blocked.
    """
    from unittest.mock import AsyncMock

    now = time.time()
    created = now - 120.0
    row = _order_row(now)
    fills = [
        _fill(
            fill_id="tr-unattributed",
            order_id=SIBLING_ORDER_ID,
            size="11.58",
            price="0.35",
            timestamp=created + 5,
        )
    ]
    svc = _service(_Gateway(fills), _OrdersRepo(row))
    pos = MagicMock()
    pos.size_tokens = Decimal("11.58")
    svc.positions_repo.get_position = AsyncMock(return_value=pos)

    probes = await svc._run_auto_release_probes(
        order_id=ROW_ORDER_ID,
        venue_order_id=ROW_ORDER_ID,
        market_id="cond-1",
        trade_ids=[],
        user_id="0xUSER",
        condition_id="cond-1",
        token_id="tok-1",
        expected_size=Decimal("4.056"),
        side="BUY",
        pre_submit_position_size=Decimal("0"),
        created_ts=created,
    )

    assert probes["inconclusive"] >= 1
    assert "sibling_attributed" not in probes["details"]["position_delta"]

