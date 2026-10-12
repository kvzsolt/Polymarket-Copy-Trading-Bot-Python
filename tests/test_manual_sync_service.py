"""Behavioural tests for ManualSyncService — TS manualSyncService.ts port."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import pmcopy.services.manual_sync_service as manual_sync_module
from pmcopy.services.manual_sync_service import ManualSyncService
from pmcopy.storage.postgres.positions_repo import PositionRecord


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
    positions_repo.list_held_positions = AsyncMock(return_value=[])
    positions_repo.upsert_position = AsyncMock()
    positions_repo.set_position_size = AsyncMock(return_value=True)
    positions_repo.delete_position_if_unchanged = AsyncMock(return_value=True)
    market_store = market_store or MagicMock()
    market_store.evict = MagicMock()

    async def positions_fetcher(wallet):
        return await manual_sync_module.fetch_data(
            f"https://example.invalid/positions?user={wallet}", silent=True
        )

    svc = ManualSyncService(
        positions_fetcher=positions_fetcher,
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
        repo.has_unconfirmed_processed_trade = AsyncMock(side_effect=has_recent)
    else:
        repo.has_recent_processed_trade = AsyncMock(return_value=has_recent)
        repo.has_unconfirmed_processed_trade = AsyncMock(return_value=has_recent)
    return repo


@pytest.mark.asyncio
async def test_manual_trade_not_in_db_is_inserted_logged_and_cache_evicted():
    """Trade with new tx_hash → user_activities insert + log + cache evict."""
    svc, positions_repo, market_store = _make_service()

    insert_many = AsyncMock()
    activity_model = MagicMock()
    activity_model.insert_many = insert_many

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xnew", asset="0xtoken1")],  # activity API
                    [],  # positions API
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xdup")],
                    [],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=True)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
        patch("pmcopy.services.manual_sync_service.fetch_data", AsyncMock(return_value=[])),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
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
    positions_repo.list_held_positions = AsyncMock(return_value=[])

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],  # activity
                    [_live_position(size=12.5, avg=0.4)],  # positions
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy") as mock_log,
    ):
        await svc.sync_now(reason="test")

    mock_log.assert_called_once()
    args, kwargs = mock_log.call_args
    assert kwargs["real_size"] == 12.5
    assert kwargs["tracked_size"] == 0.0
    positions_repo.set_position_size.assert_called_once()


@pytest.mark.asyncio
async def test_position_sync_reaps_expired_reservations_after_venue_fill():
    """Venue confirms a larger position → existing expiry reaper runs."""
    reservation_service = MagicMock()
    reservation_service.release_expired_active = AsyncMock(return_value=["res-buy-1"])
    svc, positions_repo, _ = _make_service(reservation_service=reservation_service)
    positions_repo.list_held_positions = AsyncMock(return_value=[])

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=12.5, avg=0.4)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        await svc.sync_now(reason="test")

    reservation_service.release_expired_active.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_complete_empty_snapshot_deletes_absent_local_position():
    """A complete empty snapshot establishes absence and permits guarded pruning."""
    svc, positions_repo, market_store = _make_service()
    positions_repo.list_held_positions = AsyncMock(
        return_value=[
            PositionRecord(
                user_id=FUNDER,
                condition_id="0xcond-stale",
                token_id="0xasset-stale",
                leader_id="0xleader",
                size_tokens=Decimal("7.5"),
                avg_entry_price=Decimal("0.42"),
            )
        ]
    )
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],  # activity
                    [],  # complete positions snapshot
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(return_value=user_pos_model),
        ),
    ):
        await svc.sync_now(reason="manual_sell_to_zero")

    positions_repo.delete_position_if_unchanged.assert_awaited_once_with(
        FUNDER, "0xcond-stale", token_id="0xasset-stale", expected_size=Decimal("7.5")
    )
    market_store.evict.assert_called_once_with("0xasset-stale")


@pytest.mark.asyncio
async def test_complete_smaller_snapshot_deletes_only_absent_local_position():
    """Completeness comes from the reader, independent of the local row count."""
    svc, positions_repo, market_store = _make_service()
    positions_repo.list_held_positions = AsyncMock(
        return_value=[
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
        ]
    )
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(asset="0xasset1", cond="0xcond1", size=10.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(return_value=user_pos_model),
        ),
    ):
        await svc.sync_now(reason="test")

    positions_repo.delete_position_if_unchanged.assert_awaited_once_with(
        FUNDER, "0xcond2", token_id="0xasset2", expected_size=Decimal("5")
    )
    market_store.evict.assert_called_once_with("0xasset2")


@pytest.mark.asyncio
async def test_deletion_proceeds_when_api_count_matches_and_token_absent():
    """A complete snapshot containing another token permits guarded disposal."""
    svc, positions_repo, market_store = _make_service()
    positions_repo.list_held_positions = AsyncMock(
        return_value=[
            PositionRecord(
                user_id=FUNDER,
                condition_id="0xcond-closed",
                token_id="0xasset-closed",
                leader_id=None,
                size_tokens=Decimal("7.5"),
                avg_entry_price=Decimal("0.42"),
            )
        ]
    )
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(asset="0xasset-new", cond="0xcond-new", size=5.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(return_value=user_pos_model),
        ),
    ):
        await svc.sync_now(reason="manual_switch")

    positions_repo.delete_position_if_unchanged.assert_awaited_once_with(
        FUNDER,
        "0xcond-closed",
        token_id="0xasset-closed",
        expected_size=Decimal("7.5"),
    )
    market_store.evict.assert_called_once_with("0xasset-closed")


@pytest.mark.asyncio
async def test_zero_value_position_preserves_existing_local_position():
    """A zero mark does not establish sale or redemption of held shares."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_held_positions = AsyncMock(
        return_value=[
            PositionRecord(
                user_id=FUNDER,
                condition_id="0xcond1",
                token_id="0xasset1",
                leader_id=None,
                size_tokens=Decimal("12.1851"),
                avg_entry_price=Decimal("0.34"),
            )
        ]
    )

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=12.1851, avg=0.34, current_value=0.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
    ):
        await svc.sync_now(reason="startup")

    positions_repo.set_position_size.assert_awaited_once()
    positions_repo.delete_position_if_unchanged.assert_not_awaited()


@pytest.mark.asyncio
async def test_zero_value_position_is_seeded_as_held_inventory():
    """Positive shares remain inventory even if the reported mark is zero."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_held_positions = AsyncMock(return_value=[])
    user_pos_model = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=12.1851, avg=0.34, current_value=0.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(return_value=user_pos_model),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy") as mock_log,
    ):
        await svc.sync_now(reason="startup")

    user_pos_model.delete_many.assert_called_once()
    user_pos_model.insert_many.assert_awaited_once()
    mock_log.assert_called_once()
    positions_repo.set_position_size.assert_awaited_once()


@pytest.mark.asyncio
async def test_position_already_matching_does_not_log_discrepancy():
    """Venue size == local size → no discrepancy log, but upsert still refreshes."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_held_positions = AsyncMock(
        return_value=[
            PositionRecord(
                user_id=FUNDER,
                condition_id="0xcond1",
                token_id="0xasset1",
                leader_id=None,
                size_tokens=Decimal("10.0"),
                avg_entry_price=Decimal("0.5"),
            )
        ]
    )

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=10.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy") as mock_log,
    ):
        await svc.sync_now(reason="test")

    mock_log.assert_not_called()
    # upsert still fires to keep updated_at fresh.
    positions_repo.set_position_size.assert_called_once()
    # P1-b: the align hands the pre-read local size as expected_size so the
    # locked repo write can skip (defer) if a reconciler delta raced it.
    _, kwargs = positions_repo.set_position_size.call_args
    assert kwargs["expected_size"] == Decimal("10.0")


@pytest.mark.asyncio
async def test_initial_sync_runs_silent_full_sweep():
    """initial_sync → sync_now called with reason='startup' AND silent_trades=True."""
    svc, _, _ = _make_service()
    calls = []

    async def _capture_sync(*, reason: str, silent_trades: bool = False) -> bool:
        calls.append((reason, silent_trades))
        return True

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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xhist1"), _trade(tx="0xhist2", side="SELL")],
                    [],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(side_effect=[None, {"error": "bad"}]),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
    ):
        await svc.sync_now(reason="test")  # must not raise


@pytest.mark.asyncio
async def test_manual_sync_skips_upsert_when_drift_recently_pruned():
    """Drift just pruned token T → manual_sync must NOT call upsert_position
    for T, even when data-api echoes a venue-positive size (the eventually
    consistent /positions endpoint produces this exact race in production).
    """
    svc, positions_repo, _ = _make_service()
    positions_repo.list_held_positions = AsyncMock(return_value=[])
    drift_mock = MagicMock()
    drift_mock.was_recently_pruned = MagicMock(return_value=True)
    svc._position_drift_service = drift_mock

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=10.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy") as mock_disc,
    ):
        await svc.sync_now(reason="test")

    # Drift was consulted with the exact scope.
    drift_mock.was_recently_pruned.assert_called_once()
    call_kwargs = drift_mock.was_recently_pruned.call_args.kwargs
    assert call_kwargs["token_id"] == "0xasset1"
    assert call_kwargs["condition_id"] == "0xcond1"
    # No upsert + no discrepancy panel → the holdoff fired.
    positions_repo.set_position_size.assert_not_called()
    mock_disc.assert_not_called()


@pytest.mark.asyncio
async def test_manual_sync_upserts_when_drift_not_recent():
    """Drift returns False → manual_sync resumes its normal upsert path."""
    svc, positions_repo, _ = _make_service()
    positions_repo.list_held_positions = AsyncMock(return_value=[])
    drift_mock = MagicMock()
    drift_mock.was_recently_pruned = MagicMock(return_value=False)
    svc._position_drift_service = drift_mock

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=10.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
    ):
        await svc.sync_now(reason="test")

    drift_mock.was_recently_pruned.assert_called_once()
    positions_repo.set_position_size.assert_called_once()


@pytest.mark.asyncio
async def test_manual_sync_handshake_only_blocks_new_upserts_not_size_updates():
    """Holdoff only applies when the row is missing locally (existing is
    None). If the row exists, size-update alignment must continue so live
    fills propagate even on recently-pruned conditions (after re-creation).
    """
    svc, positions_repo, _ = _make_service()
    # Local already holds the position → existing is not None.
    positions_repo.list_held_positions = AsyncMock(
        return_value=[
            PositionRecord(
                user_id=FUNDER,
                condition_id="0xcond1",
                token_id="0xasset1",
                leader_id=None,
                size_tokens=Decimal("5.0"),
                avg_entry_price=Decimal("0.5"),
            )
        ]
    )
    drift_mock = MagicMock()
    drift_mock.was_recently_pruned = MagicMock(return_value=True)
    svc._position_drift_service = drift_mock

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(size=10.0)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
    ):
        await svc.sync_now(reason="test")

    # Drift NOT consulted (existing is not None → no holdoff check needed).
    drift_mock.was_recently_pruned.assert_not_called()
    positions_repo.set_position_size.assert_called_once()


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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xsettle", asset="0xtokenOwn", side="BUY")],  # activity
                    [],  # positions
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xmanual", asset="0xtokenM", side="SELL")],
                    [],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xprobefail", asset="0xtokenF")],
                    [],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [_trade(tx="0xlegacy", asset="0xtokenL")],
                    [],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.find_activity_by_tx_hash", AsyncMock(return_value=False)
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_activity_model",
            MagicMock(return_value=activity_model),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_trade_detected") as mock_print,
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
    positions_repo.list_held_positions = AsyncMock(return_value=[])

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],  # activity
                    [_live_position(asset="0xasset1", size=2.55, avg=0.40)],  # positions
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy") as mock_disc,
    ):
        await svc.sync_now(reason="wss_fill")

    positions_repo.set_position_size.assert_not_called()
    mock_disc.assert_not_called()
    # Both sides retain reconciliation ownership until their order is terminal.
    probe_kwargs = orders_repo.has_unconfirmed_processed_trade.call_args.kwargs
    assert probe_kwargs["asset"] == "0xasset1"
    assert probe_kwargs["side"] is None


@pytest.mark.asyncio
async def test_position_seed_proceeds_when_no_recent_bot_order():
    """existing is None + no recent bot order → manual-sync seeds the venue
    position as before (safety net for genuinely off-bot / failed-reconcile
    holdings)."""
    orders_repo = _orders_repo(has_recent=False)
    svc, positions_repo, _ = _make_service(orders_repo=orders_repo)
    positions_repo.list_held_positions = AsyncMock(return_value=[])

    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(
                side_effect=[
                    [],
                    [_live_position(asset="0xasset1", size=2.55, avg=0.40)],
                ]
            ),
        ),
        patch(
            "pmcopy.services.manual_sync_service.get_user_position_model",
            MagicMock(
                return_value=MagicMock(
                    delete_many=AsyncMock(),
                    insert_many=AsyncMock(),
                )
            ),
        ),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        await svc.sync_now(reason="wss_fill")

    positions_repo.set_position_size.assert_called_once()


@pytest.mark.asyncio
async def test_marked_zero_still_represents_held_inventory() -> None:
    """A zero mark cannot prove sale, redemption, or zero shares."""
    svc, repo, _ = _make_service()
    history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(return_value=[_live_position(size=12.1851, avg=0.34, current_value=0.0)]),
        ),
        patch("pmcopy.services.manual_sync_service.get_user_position_model", return_value=history),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        await svc._sync_positions_and_tracking()
    repo.set_position_size.assert_awaited_once()
    repo.delete_position_if_unchanged.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_inventory_failure_is_not_reported_as_applied() -> None:
    svc, _, _ = _make_service()
    with (
        patch("pmcopy.services.manual_sync_service.fetch_data", AsyncMock(return_value=None)),
        pytest.raises(RuntimeError, match="Initial inventory synchronization is incomplete"),
    ):
        await svc.initial_sync()


@pytest.mark.asyncio
async def test_required_position_read_failure_prevents_writes_and_reports_failure() -> None:
    svc, repo, _ = _make_service()
    repo.list_held_positions.side_effect = ValueError("SYNTHETIC_POSITION_SECRET")
    history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(side_effect=[[], [_live_position()]]),
        ),
        patch("pmcopy.services.manual_sync_service.get_user_position_model", return_value=history),
    ):
        result = await svc.sync_now(reason="test")
    repo.set_position_size.assert_not_awaited()
    assert result is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["set_position_size", "delete_position_if_unchanged"])
async def test_required_position_write_failure_is_reported(failure) -> None:
    svc, repo, _ = _make_service()
    getattr(repo, failure).side_effect = ValueError("SYNTHETIC_POSITION_SECRET")
    if failure == "delete_position_if_unchanged":
        repo.list_held_positions.return_value = [
            PositionRecord(
                user_id=FUNDER,
                condition_id="condition-old",
                token_id="token-old",
                leader_id=None,
                size_tokens=Decimal(1),
                avg_entry_price=Decimal("0.5"),
            )
        ]
    history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(side_effect=[[], [_live_position()]]),
        ),
        patch("pmcopy.services.manual_sync_service.get_user_position_model", return_value=history),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        result = await svc.sync_now(reason="test")
    assert result is False


@pytest.mark.asyncio
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("venue_size", ["0", "4", "12"])
async def test_pending_fill_keeps_existing_position_under_reconciler_ownership(
    side: str, venue_size: str
) -> None:
    orders = _orders_repo()
    orders.has_unconfirmed_processed_trade.side_effect = lambda **scope: (
        scope["side"] in (None, side)
    )
    svc, repo, store = _make_service(orders_repo=orders)
    repo.list_held_positions.return_value = [
        PositionRecord(FUNDER, "0xcond1", "0xasset1", "leader", Decimal(10), Decimal("0.5"))
    ]
    history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(side_effect=[[], [_live_position(size=venue_size)]]),
        ),
        patch("pmcopy.services.manual_sync_service.get_user_position_model", return_value=history),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        assert await svc.sync_now(reason="pending_fill") is True
    repo.set_position_size.assert_not_awaited()
    repo.delete_position_if_unchanged.assert_not_awaited()
    store.evict.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("condition", [None, "", True])
async def test_missing_held_position_identity_cannot_be_treated_as_disposal(condition) -> None:
    svc, repo, _ = _make_service()
    repo.list_held_positions.return_value = [
        PositionRecord(FUNDER, "0xcond1", "0xasset1", "leader", Decimal(10), Decimal("0.5"))
    ]
    history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(side_effect=[[], [_live_position(cond=condition)]]),
        ),
        patch("pmcopy.services.manual_sync_service.get_user_position_model", return_value=history),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        assert await svc.sync_now(reason="invalid_identity") is False
    repo.set_position_size.assert_not_awaited()
    repo.delete_position_if_unchanged.assert_not_awaited()
    history.delete_many.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("size", "owner"), [("12", None), ("8", "leader-a")])
async def test_manual_increment_does_not_inherit_a_copied_position_owner(size, owner) -> None:
    svc, repo, _ = _make_service()
    repo.list_held_positions.return_value = [
        PositionRecord(FUNDER, "0xcond1", "0xasset1", "leader-a", Decimal(10), Decimal(".5"))
    ]
    history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
    with (
        patch(
            "pmcopy.services.manual_sync_service.fetch_data",
            AsyncMock(side_effect=[[], [_live_position(size=size)]]),
        ),
        patch("pmcopy.services.manual_sync_service.get_user_position_model", return_value=history),
        patch("pmcopy.services.manual_sync_service.print_manual_sync_discrepancy"),
    ):
        assert await svc.sync_now(reason="manual_inventory") is True
    assert repo.set_position_size.await_args.kwargs["leader_id"] == owner
