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
    wss_settle_delay_seconds: float = 0.0,
    reservation_service=None,
    orders_repo=None,
) -> tuple[ManualSyncService, MagicMock, MagicMock]:
    positions_repo = positions_repo or MagicMock()
    positions_repo.list_open_positions = AsyncMock(return_value=[])
    positions_repo.upsert_position = AsyncMock()
    positions_repo.delete_position = AsyncMock()
    market_store = market_store or MagicMock()
    market_store.evict = MagicMock()
    svc = ManualSyncService(
        funder_address=FUNDER,
        user_id=FUNDER,
        positions_repo=positions_repo,
        market_store=market_store,
        debounce_seconds=debounce_seconds,
        wss_settle_delay_seconds=wss_settle_delay_seconds,
        rest_poll_interval_seconds=0.05,
        reservation_service=reservation_service,
        orders_repo=orders_repo,
    )
    return svc, positions_repo, market_store


def _orders_repo(*, has_recent=False):
    """Build an orders_repo stub whose ``has_recent_processed_trade`` returns
    (or raises) ``has_recent``. Pass an Exception instance to simulate a probe
    failure.
    """
    repo = MagicMock()
    if isinstance(has_recent, BaseException):
        repo.has_recent_processed_trade = AsyncMock(side_effect=has_recent)
    else:
        repo.has_recent_processed_trade = AsyncMock(return_value=has_recent)
    return repo


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
async def test_wss_fill_sync_waits_for_reconciliation_settle_delay():
    svc, _, _ = _make_service(
        debounce_seconds=0.0,
        wss_settle_delay_seconds=0.05,
    )
    counter = {"n": 0}

    async def _fake_sync(*, reason: str) -> None:
        counter["n"] += 1

    with patch.object(svc, "sync_now", side_effect=_fake_sync):
        await svc.trigger_sync(reason="wss_fill")
        await asyncio.sleep(0.01)
        assert counter["n"] == 0
        await asyncio.sleep(0.07)

    assert counter["n"] == 1
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
async def test_position_sync_reaps_expired_reservations_after_venue_fill():
    """Venue confirms a larger position → existing expiry reaper runs."""
    reservation_service = MagicMock()
    reservation_service.release_expired_active = AsyncMock(return_value=["res-buy-1"])
    svc, positions_repo, _ = _make_service(reservation_service=reservation_service)
    positions_repo.list_open_positions = AsyncMock(return_value=[])

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=12.5, avg=0.4)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        await svc.sync_now(reason="test")

    reservation_service.release_expired_active.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_deletion_skipped_when_api_response_is_empty():
    """Empty positions API → local positions NOT deleted (empty = suspect response).

    A genuinely zero-position portfolio is a valid state, but the Data API
    returning an empty list is far more likely to be a transient failure than
    proof the user sold everything.  Wiping all local rows on an empty response
    would cause cascading failures (risk-monitor probes against retired tokens,
    reservations not released, etc.).  Deletion is deferred to the next sync.
    """
    svc, positions_repo, market_store = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond-stale",
            token_id="0xasset-stale",
            leader_id="0xleader",
            size_tokens=Decimal("7.5"),
            avg_entry_price=Decimal("0.42"),
        )
    ])
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],  # activity
            [],  # positions — empty → deletion guard fires
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=user_pos_model)),
    ):
        await svc.sync_now(reason="manual_sell_to_zero")

    positions_repo.delete_position.assert_not_called()
    market_store.evict.assert_not_called()


@pytest.mark.asyncio
async def test_deletion_skipped_when_api_response_smaller_than_local():
    """Sparse positions API (fewer items than local) → deletion skipped.

    If the Data API returns fewer positions than we track locally the response
    may be truncated or paginated.  Deleting rows whose tokens are simply absent
    from this snapshot would silently wipe live positions, so the entire
    deletion pass is skipped and logged.  The next sync will retry.
    """
    svc, positions_repo, market_store = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond1",
            token_id="0xasset1",
            leader_id=None,
            size_tokens=Decimal("10.0"),
            avg_entry_price=Decimal("0.5"),
        ),
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond2",
            token_id="0xasset2",
            leader_id=None,
            size_tokens=Decimal("5.0"),
            avg_entry_price=Decimal("0.3"),
        ),
    ])
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    # API only returns 1 position — 1 < 2 local → sparse-response guard fires.
    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(asset="0xasset1", cond="0xcond1", size=10.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=user_pos_model)),
    ):
        await svc.sync_now(reason="test")

    # 0xasset2 is absent from the API response but deletion must be skipped.
    positions_repo.delete_position.assert_not_called()
    market_store.evict.assert_not_called()


@pytest.mark.asyncio
async def test_deletion_proceeds_when_api_count_matches_and_token_absent():
    """API returns as many positions as we have locally, one token missing → delete.

    When the raw API count equals (or exceeds) the local tracked count the
    response is treated as complete.  A token absent from that complete snapshot
    is genuinely closed and should be removed from local state.
    """
    svc, positions_repo, market_store = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond-closed",
            token_id="0xasset-closed",
            leader_id=None,
            size_tokens=Decimal("7.5"),
            avg_entry_price=Decimal("0.42"),
        )
    ])
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    # API returns 1 position (different token) — count 1 == local count 1.
    # The old token is absent from the snapshot → deletion should proceed.
    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(asset="0xasset-new", cond="0xcond-new", size=5.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=user_pos_model)),
    ):
        await svc.sync_now(reason="manual_switch")

    positions_repo.delete_position.assert_awaited_once_with(
        FUNDER,
        "0xcond-closed",
        token_id="0xasset-closed",
    )
    market_store.evict.assert_called_once_with("0xasset-closed")


@pytest.mark.asyncio
async def test_zero_value_position_deletes_existing_local_position():
    """Data API stale zero-value rows are treated as absent for local state."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_open_positions = AsyncMock(return_value=[
        PositionRecord(
            user_id=FUNDER,
            condition_id="0xcond1",
            token_id="0xasset1",
            leader_id=None,
            size_tokens=Decimal("12.1851"),
            avg_entry_price=Decimal("0.34"),
        )
    ])

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(size=12.1851, avg=0.34, current_value=0.0)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
    ):
        await svc.sync_now(reason="startup")

    positions_repo.upsert_position.assert_not_called()
    positions_repo.delete_position.assert_awaited_once_with(
        FUNDER,
        "0xcond1",
        token_id="0xasset1",
    )


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


# ----------------------------------------------------------------------
# Own-fill duplicate-order recognition (sym 3 + 4)
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_own_copy_fill_recorded_as_bot_not_manual():
    """A data-api trade matching a recent bot processed_trade is the bot's OWN
    copy fill: record it bot=True with the real settlement tx and do NOT surface
    it as a manual trade. Closes the cross-identifier dedup gap."""
    orders_repo = _orders_repo(has_recent=True)
    svc, positions_repo, market_store = _make_service(orders_repo=orders_repo)

    insert_many = AsyncMock()
    activity_model = MagicMock(insert_many=insert_many)

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xsettle", asset="0xtokenOwn", side="BUY")],  # activity
            [],                                                        # positions
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.sync_now(reason="wss_fill")

    # Recognised as own → no manual panel.
    mock_print.assert_not_called()
    # Recorded with the REAL settlement tx and bot=True (so later sweeps dedup).
    inserted_payload = insert_many.call_args.args[0][0]
    assert inserted_payload["transactionHash"] == "0xsettle"
    assert inserted_payload["bot"] is True
    assert inserted_payload["own_detected_via"] == "processed_trades"
    # Probed processed_trades on the exact (token, side) scope.
    orders_repo.has_recent_processed_trade.assert_awaited()
    probe_kwargs = orders_repo.has_recent_processed_trade.call_args.kwargs
    assert probe_kwargs["asset"] == "0xtokenOwn"
    assert probe_kwargs["side"] == "BUY"
    # Not treated as manual → the manual-path cache evict did not fire.
    market_store.evict.assert_not_called()


@pytest.mark.asyncio
async def test_genuine_manual_trade_still_detected_when_orders_repo_present():
    """orders_repo wired but no recent bot trade → still classified manual
    (printed + inserted bot=False). The dedup never suppresses real manuals."""
    orders_repo = _orders_repo(has_recent=False)
    svc, positions_repo, market_store = _make_service(orders_repo=orders_repo)

    insert_many = AsyncMock()
    activity_model = MagicMock(insert_many=insert_many)

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xmanual", asset="0xtokenM", side="SELL")],
            [],
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.sync_now(reason="wss_fill")

    mock_print.assert_called_once()
    inserted_payload = insert_many.call_args.args[0][0]
    assert inserted_payload["transactionHash"] == "0xmanual"
    assert inserted_payload["bot"] is False
    market_store.evict.assert_called_once_with("0xtokenM")


@pytest.mark.asyncio
async def test_own_trade_probe_failure_falls_back_to_manual():
    """If the processed_trades probe errors, the trade is NOT suppressed — it
    falls back to the legacy manual classification (no silent loss of a real
    manual trade)."""
    orders_repo = _orders_repo(has_recent=RuntimeError("db down"))
    svc, _, _ = _make_service(orders_repo=orders_repo)

    insert_many = AsyncMock()
    activity_model = MagicMock(insert_many=insert_many)

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xfail", asset="0xtokenF")],
            [],
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.sync_now(reason="wss_fill")

    mock_print.assert_called_once()
    assert insert_many.call_args.args[0][0]["bot"] is False


@pytest.mark.asyncio
async def test_orders_repo_unwired_preserves_legacy_manual_detection():
    """orders_repo None (paper/test contexts) → own-recognition is skipped and
    the legacy tx_hash-only manual classification is unchanged."""
    svc, _, market_store = _make_service(orders_repo=None)

    insert_many = AsyncMock()
    activity_model = MagicMock(insert_many=insert_many)

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [_trade(tx="0xlegacy", asset="0xtokenL")],
            [],
        ])),
        patch("services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)),
        patch("services.manual_sync_service.get_user_activity_model", MagicMock(return_value=activity_model)),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_trade_detected") as mock_print,
    ):
        await svc.sync_now(reason="wss_fill")

    mock_print.assert_called_once()
    assert insert_many.call_args.args[0][0]["bot"] is False


@pytest.mark.asyncio
async def test_position_seed_held_off_while_bot_order_in_flight():
    """existing is None + a recent bot order on the token → manual-sync must NOT
    seed the absolute venue size (the reconciler owns the first incremental
    write; seeding here double-counts)."""
    orders_repo = _orders_repo(has_recent=True)
    svc, positions_repo, _ = _make_service(orders_repo=orders_repo)
    positions_repo.list_open_positions = AsyncMock(return_value=[])

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],                                       # activity
            [_live_position(asset="0xasset1", size=2.55, avg=0.40)],  # positions
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_sync_discrepancy") as mock_disc,
    ):
        await svc.sync_now(reason="wss_fill")

    positions_repo.upsert_position.assert_not_called()
    mock_disc.assert_not_called()
    # Seed hold-off probes the BUY side (the row-creating side that races the
    # reconciler's first incremental write) within the short hold-off window.
    probe_kwargs = orders_repo.has_recent_processed_trade.call_args.kwargs
    assert probe_kwargs["asset"] == "0xasset1"
    assert probe_kwargs["side"] == "BUY"


@pytest.mark.asyncio
async def test_position_seed_proceeds_when_no_recent_bot_order():
    """existing is None + no recent bot order → manual-sync seeds the venue
    position as before (safety net for genuinely off-bot / failed-reconcile
    holdings)."""
    orders_repo = _orders_repo(has_recent=False)
    svc, positions_repo, _ = _make_service(orders_repo=orders_repo)
    positions_repo.list_open_positions = AsyncMock(return_value=[])

    with (
        patch("services.manual_sync_service.fetch_data", AsyncMock(side_effect=[
            [],
            [_live_position(asset="0xasset1", size=2.55, avg=0.40)],
        ])),
        patch("services.manual_sync_service.get_user_position_model", MagicMock(return_value=MagicMock(
            delete_many=AsyncMock(), insert_many=AsyncMock(),
        ))),
        patch("services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        await svc.sync_now(reason="wss_fill")

    positions_repo.upsert_position.assert_called_once()
