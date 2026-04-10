from __future__ import annotations

from decimal import Decimal
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from services.discovery.capture_service import DiscoveryCaptureService
from storage.sqlite.connection import create_connection
from storage.sqlite.discovery_repo import DiscoveryRepo
from storage.sqlite.migrations import apply_migrations

VALID_TOKEN_YES = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
VALID_TOKEN_NO = "71321045679252212594626385532706912750332728571942532223535764789812338637397"


def _init_db(path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


@pytest.mark.asyncio
async def test_capture_observed_trades_persists_trade_market_and_orderbook(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    service = DiscoveryCaptureService(str(db_path))
    repo = DiscoveryRepo(str(db_path))
    raw_trade = {
        "proxyWallet": "0x1111111111111111111111111111111111111111",
        "conditionId": "cond-1",
        "asset": VALID_TOKEN_YES,
        "side": "BUY",
        "size": "25",
        "price": "0.44",
        "usdcSize": "11",
        "transactionHash": "0xabc",
        "timestamp": 1712300000,
        "id": "trade-1",
        "outcome": "Yes",
        "title": "Will example happen?",
        "marketSlug": "will-example-happen",
        "eventSlug": "example-event",
        "category": "Politics",
        "endDate": "2026-12-31T23:59:59Z",
        "tokens": [
            {"outcome": "Yes", "token_id": VALID_TOKEN_YES},
            {"outcome": "No", "token_id": VALID_TOKEN_NO},
        ],
    }
    orderbook_payload = {
        "bids": [{"price": "0.43", "size": "120"}, {"price": "0.42", "size": "80"}],
        "asks": [{"price": "0.45", "size": "140"}, {"price": "0.46", "size": "90"}],
        "neg_risk": False,
    }

    with patch("services.discovery.capture_service.fetch_data", new=AsyncMock(return_value=orderbook_payload)):
        await service.capture_observed_trades([raw_trade])

    trades = repo.list_trades("0x1111111111111111111111111111111111111111")
    assert len(trades) == 1
    trade = trades[0]
    assert trade.outcome == "Yes"
    assert trade.title == "Will example happen?"
    assert trade.market_slug == "will-example-happen"
    assert trade.event_slug == "example-event"
    assert trade.raw_json

    market = repo.get_market("cond-1")
    assert market is not None
    assert market.question == "Will example happen?"
    assert market.slug == "will-example-happen"
    assert market.event_slug == "example-event"
    assert market.yes_token_id == VALID_TOKEN_YES
    assert market.no_token_id == VALID_TOKEN_NO
    assert market.category == "Politics"

    snapshots = repo.list_orderbook_snapshots(VALID_TOKEN_YES, reference_tx_hash="0xabc")
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.reference_trade_id == "trade-1"
    assert snapshot.reference_event_ts == 1712300000
    assert snapshot.best_bid == Decimal("0.43")
    assert snapshot.best_ask == Decimal("0.45")
    assert snapshot.mid_price == Decimal("0.44")
    assert snapshot.spread == Decimal("0.02")


@pytest.mark.asyncio
async def test_capture_position_snapshots_persists_reported_position_fields(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    service = DiscoveryCaptureService(str(db_path))
    repo = DiscoveryRepo(str(db_path))

    count = await service.capture_position_snapshots(
        "0x2222222222222222222222222222222222222222",
        [
            {
                "conditionId": "cond-2",
                "asset": "token-no",
                "size": "14",
                "avgPrice": "0.41",
                "currentValue": "7.84",
                "curPrice": "0.56",
                "realizedPnl": "1.20",
                "title": "Another market",
            }
        ],
        snapshot_ts=1712300100,
    )

    assert count == 1
    snapshot = repo.latest_position_snapshot(
        "0x2222222222222222222222222222222222222222",
        "cond-2",
    )
    assert snapshot is not None
    assert snapshot.size == Decimal("14")
    assert snapshot.avg_price == Decimal("0.41")
    assert snapshot.current_value == Decimal("7.84")
    assert snapshot.current_price == Decimal("0.56")
    assert snapshot.realized_pnl == Decimal("1.20")


@pytest.mark.asyncio
async def test_capture_observed_trades_invalid_token_logs_terminal_without_fetch(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    service = DiscoveryCaptureService(str(db_path))
    raw_trade = {
        "proxyWallet": "0x1111111111111111111111111111111111111111",
        "conditionId": "cond-1",
        "asset": "invalid-token",
        "side": "BUY",
        "size": "25",
        "price": "0.44",
        "usdcSize": "11",
        "transactionHash": "0xabc",
        "timestamp": 1712300000,
        "id": "trade-1",
        "outcome": "Yes",
        "title": "Will example happen?",
    }

    with patch("services.discovery.capture_service.fetch_data", new=AsyncMock()) as fetch_mock, \
         patch("services.discovery.capture_service.logger.warning") as warn_mock:
        await service.capture_observed_trades([raw_trade])

    fetch_mock.assert_not_called()
    assert warn_mock.call_count >= 1
    warning_extra = warn_mock.call_args.kwargs["extra"]
    assert warning_extra["failure_class"] == "invalid_token_id"
    assert warning_extra["retryable"] is False
    assert warning_extra["terminal"] is True


@pytest.mark.asyncio
async def test_capture_observed_trades_transport_error_marked_retryable(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    service = DiscoveryCaptureService(str(db_path))
    raw_trade = {
        "proxyWallet": "0x1111111111111111111111111111111111111111",
        "conditionId": "cond-1",
        "asset": VALID_TOKEN_YES,
        "side": "BUY",
        "size": "25",
        "price": "0.44",
        "usdcSize": "11",
        "transactionHash": "0xabc",
        "timestamp": 1712300000,
        "id": "trade-1",
        "outcome": "Yes",
        "title": "Will example happen?",
    }

    with patch("services.discovery.capture_service.fetch_data", new=AsyncMock(side_effect=asyncio.TimeoutError("timeout"))), \
         patch("services.discovery.capture_service.logger.warning") as warn_mock:
        await service.capture_observed_trades([raw_trade])

    assert warn_mock.call_count >= 1
    warning_extra = warn_mock.call_args.kwargs["extra"]
    assert warning_extra["failure_class"] == "transport_error"
    assert warning_extra["retryable"] is True
    assert warning_extra["terminal"] is False


@pytest.mark.asyncio
async def test_capture_observed_trades_throttles_duplicate_orderbook_failures(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    service = DiscoveryCaptureService(str(db_path), orderbook_error_log_window_seconds=60.0)
    raw_trade = {
        "proxyWallet": "0x1111111111111111111111111111111111111111",
        "conditionId": "cond-1",
        "asset": VALID_TOKEN_YES,
        "side": "BUY",
        "size": "25",
        "price": "0.44",
        "usdcSize": "11",
        "transactionHash": "0xabc",
        "timestamp": 1712300000,
        "id": "trade-1",
        "outcome": "Yes",
        "title": "Will example happen?",
    }

    with patch("services.discovery.capture_service.fetch_data", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch("services.discovery.capture_service.logger.warning") as warn_mock, \
         patch("services.discovery.capture_service.time.monotonic", side_effect=[0.0, 1.0, 61.0]):
        await service.capture_observed_trades([dict(raw_trade, id="trade-1", transactionHash="0xabc1")])
        await service.capture_observed_trades([dict(raw_trade, id="trade-2", transactionHash="0xabc2")])
        await service.capture_observed_trades([dict(raw_trade, id="trade-3", transactionHash="0xabc3")])

    assert warn_mock.call_count == 2
    first_extra = warn_mock.call_args_list[0].kwargs["extra"]
    second_extra = warn_mock.call_args_list[1].kwargs["extra"]
    assert first_extra["failure_class"] == "unexpected_error"
    assert "suppressed_duplicates" not in first_extra
    assert second_extra["suppressed_duplicates"] == 1
