"""Behavioural tests for ManualSyncService — TS manualSyncService.ts port."""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.manual_sync_service import ManualSyncService
from storage.postgres.positions_repo import PositionRecord


FUNDER = "0xfunder0000000000000000000000000000000001"


def _trade(
    *,
    side: str = "BUY",
    size: float = 10.0,
    price: float = 0.5,
    tx: str = "0xtx123",
    asset: str = "0xasset1",
    cond: str = "0xcond1",
    ts: int = 1_700_000_000,
) -> dict[str, Any]:
    return {
        "side": side,
        "size": size,
        "price": price,
        "usdcSize": size * price,
        "transactionHash": tx,
        "asset": asset,
        "conditionId": cond,
        "timestamp": ts,
        "title": "Market X",
        "outcome": "YES",
    }


def _live_position(
    *,
    asset: str = "0xasset1",
    cond: str = "0xcond1",
    size: float = 10.0,
    avg: float = 0.5,
    current_value: float | None = None,
) -> dict[str, Any]:
    payload = {
        "asset": asset,
        "conditionId": cond,
        "size": size,
        "avgPrice": avg,
        "title": "Market X",
    }
    if current_value is not None:
        payload["currentValue"] = current_value
    return payload


def _make_service(
    *,
    fetcher_side_effect=None,
    positions_repo=None,
    market_store=None,
    debounce_seconds: float = 0.0,
) -> tuple[ManualSyncService, MagicMock, MagicMock]:
    positions_repo = positions_repo or MagicMock()
    positions_repo.list_open_positions = AsyncMock(return_value=[])
    positions_repo.upsert_position = AsyncMock()
    market_store = market_store or MagicMock()
    market_store.evict = MagicMock()
    svc = ManualSyncService(
        funder_address=FUNDER,
        user_id=FUNDER,
        positions_repo=positions_repo,
        market_store=market_store,
        debounce_seconds=debounce_seconds,
        rest_poll_interval_seconds=0.05,
    )
    return svc, positions_repo, market_store


@pytest.mark.asyncio
async def test_manual_trade_not_in_db_is_inserted_logged_and_cache_evicted():
    """Trade with new tx_hash → user_activities insert + log + cache evict."""
    svc, positions_repo, market_store = _make_service()

    insert_many = AsyncMock()
    activity_model = MagicMock()
    activity_model.insert_many = insert_many

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xnew", asset="0xtoken1")],  # activity API
            [],                                       # positions API
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.sync_now(reason="test")

    assert mock_print.call_count == 1
    inserted_payload = insert_many.call_args.args[0][0]
    assert inserted_payload["transactionHash"] == "0xnew"
    assert inserted_payload["bot"] is False
    market_store.evict.assert_called_once_with("0xtoken1")


@pytest.mark.asyncio
async def test_trade_already_in_user_activities_is_skipped_silently():
    """tx_hash present in user_activities → no log, no insert, no evict."""
    svc, _, market_store = _make_service()
    insert_many = AsyncMock()
    activity_model = MagicMock(insert_many=insert_many)

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xdup")],
            [],
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=True)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.sync_now(reason="test")

    mock_print.assert_not_called()
    insert_many.assert_not_called()
    market_store.evict.assert_not_called()


@pytest.mark.asyncio
async def test_on_wss_authenticated_stops_rest_fallback_and_triggers_sync():
    """WSS up → REST fallback cancelled, single sync fires."""
    svc, _, _ = _make_service(debounce_seconds=0.0)
    await svc._start_rest_fallback()
    assert svc._rest_task is not None

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(return_value=[])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
    ):
        await svc.on_wss_authenticated()
        await asyncio.sleep(0.05)  # let debounce task finish

    assert svc._rest_task is None
    assert svc._wss_alive is True
    await svc.stop()


@pytest.mark.asyncio
async def test_on_wss_disconnected_starts_rest_fallback():
    """WSS down → REST fallback poller is spawned."""
    svc, _, _ = _make_service()
    await svc.on_wss_disconnected()
    assert svc._rest_task is not None
    assert not svc._rest_task.done()
    await svc.stop()


@pytest.mark.asyncio
async def test_debounced_trigger_coalesces_burst_into_single_sync():
    """N rapid trigger_sync calls in a 1s window → one sync_now invocation."""
    svc, _, _ = _make_service(debounce_seconds=0.05)
    counter = {"n": 0}

    async def _fake_sync(*, reason: str) -> None:
        counter["n"] += 1

    with patch.object(svc, "sync_now", side_effect=_fake_sync):
        # 5 events arrive within the debounce window — should coalesce to 1.
        for _ in range(5):
            await svc.trigger_sync(reason="wss_fill")
        await asyncio.sleep(0.15)

    assert counter["n"] == 1, f"expected 1 sync, got {counter['n']}"
    await svc.stop()


@pytest.mark.asyncio
async def test_position_discrepancy_emits_log_and_upsert():
    """Venue position not in local DB → discrepancy log + upsert_position fires."""
    svc, positions_repo, _ = _make_service()
    # No local positions — every venue position is a new insert (discrepancy).
    positions_repo.list_open_positions = AsyncMock(return_value=[])

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],  # activity
            [_live_position(size=12.5, avg=0.4)],  # positions
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_sync_discrepancy") as mock_log,
    ):
        await svc.sync_now(reason="test")

    mock_log.assert_called_once()
    args, kwargs = mock_log.call_args
    assert kwargs["real_size"] == 12.5
    assert kwargs["tracked_size"] == 0.0
    positions_repo.upsert_position.assert_called_once()


@pytest.mark.asyncio
async def test_zero_value_position_is_not_seeded_from_stale_data_api():
    """Data API can echo non-zero size for retired tokens with value=0.

    Startup health/drift exclude those rows; manual sync must not resurrect
    them into canonical positions.
    """
    svc, positions_repo, _ = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[])
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=12.1851, avg=0.34, current_value=0.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=user_pos_model)),
        patch("services.manual_sync_service.print_manual_sync_discrepancy") as mock_log,
    ):
        await svc.sync_now(reason="startup")

    user_pos_model.delete_many.assert_called_once()
    user_pos_model.insert_many.assert_not_called()
    mock_log.assert_not_called()
    positions_repo.upsert_position.assert_not_called()


@pytest.mark.asyncio
async def test_position_already_matching_does_not_log_discrepancy():
    """Venue size == local size → no discrepancy log, but upsert still refreshes."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond1",
            token_id="0xasset1",
            leader_id=None,
            size_tokens=Decimal("10.0"),
            avg_entry_price=Decimal("0.5"),
        )
    ])

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=10.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_sync_discrepancy") as mock_log,
    ):
        await svc.sync_now(reason="test")

    mock_log.assert_not_called()
    # upsert still fires to keep updated_at fresh.
    positions_repo.upsert_position.assert_called_once()


@pytest.mark.asyncio
async def test_initial_sync_runs_silent_full_sweep():
    """initial_sync → sync_now called with reason='startup' AND silent_trades=True."""
    svc, _, _ = _make_service()
    calls = []

    async def _capture_sync(*, reason: str, silent_trades: bool = False) -> None:
        calls.append((reason, silent_trades))

    with patch.object(svc, "sync_now", side_effect=_capture_sync):
        await svc.initial_sync()

    assert calls == [("startup", True)]


@pytest.mark.asyncio
async def test_initial_sync_inserts_silently_no_panel():
    """Historical trades on first boot land in DB but no panel fires."""
    svc, _, _ = _make_service()
    insert_many = AsyncMock()
    activity_model = MagicMock(insert_many=insert_many)

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xhist1"), _trade(tx="0xhist2", side="SELL")],
            [],
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.initial_sync()

    # Both trades inserted into user_activities
    assert insert_many.call_count == 2
    # ZERO panels — silent mode
    mock_print.assert_not_called()


@pytest.mark.asyncio
async def test_stop_cancels_pending_debounce_and_rest_tasks():
    """stop() → cancels both debounce and REST tasks idempotently."""
    svc, _, _ = _make_service(debounce_seconds=10.0)
    await svc.trigger_sync(reason="wss_fill")
    await svc._start_rest_fallback()

    await svc.stop()

    assert svc._stopped is True
    # Calling stop again must be safe.
    await svc.stop()


@pytest.mark.asyncio
async def test_fetch_returns_non_list_does_not_crash():
    """activity/positions API returning {} or None must not raise."""
    svc, _, _ = _make_service()
    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[None, {"error": "bad"}])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
    ):
        await svc.sync_now(reason="test")  # must not raise


@pytest.mark.asyncio
async def test_manual_sync_skips_upsert_when_drift_recently_pruned():
    """Drift just pruned token T → manual_sync must NOT call upsert_position
    for T, even when data-api echoes a venue-positive size (the eventually
    consistent /positions endpoint produces this exact race in production).
    """
    svc, positions_repo, _ = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[])
    drift_mock = MagicMock()
    drift_mock.was_recently_pruned = MagicMock(return_value=True)
    svc._position_drift_service = drift_mock

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=10.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_sync_discrepancy") as mock_disc,
    ):
        await svc.sync_now(reason="test")

    # Drift was consulted with the exact scope.
    drift_mock.was_recently_pruned.assert_called_once()
    call_kwargs = drift_mock.was_recently_pruned.call_args.kwargs
    assert call_kwargs["token_id"] == "0xasset1"
    assert call_kwargs["condition_id"] == "0xcond1"
    # No upsert + no discrepancy panel → the holdoff fired.
    positions_repo.upsert_position.assert_not_called()
    mock_disc.assert_not_called()


@pytest.mark.asyncio
async def test_manual_sync_upserts_when_drift_not_recent():
    """Drift returns False → manual_sync resumes its normal upsert path."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[])
    drift_mock = MagicMock()
    drift_mock.was_recently_pruned = MagicMock(return_value=False)
    svc._position_drift_service = drift_mock

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=10.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
    ):
        await svc.sync_now(reason="test")

    drift_mock.was_recently_pruned.assert_called_once()
    positions_repo.upsert_position.assert_called_once()


@pytest.mark.asyncio
async def test_manual_sync_handshake_only_blocks_new_upserts_not_size_updates():
    """Holdoff only applies when the row is missing locally (existing is
    None). If the row exists, size-update alignment must continue so live
    fills propagate even on recently-pruned conditions (after re-creation).
    """
    svc, positions_repo, _ = _make_service()
    # Local already holds the position → existing is not None.
    positions_repo.list_open_positions = AsyncMock(return_value=[
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond1",
            token_id="0xasset1",
            leader_id=None,
            size_tokens=Decimal("5.0"),
            avg_entry_price=Decimal("0.5"),
        )
    ])
    drift_mock = MagicMock()
    drift_mock.was_recently_pruned = MagicMock(return_value=True)
    svc._position_drift_service = drift_mock

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=10.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
    ):
        await svc.sync_now(reason="test")

    # Drift NOT consulted (existing is not None → no holdoff check needed).
    drift_mock.was_recently_pruned.assert_not_called()
    positions_repo.upsert_position.assert_called_once()
