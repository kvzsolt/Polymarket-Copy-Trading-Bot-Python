from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import services.tradeMonitor as trade_monitor_module
from services.tradeMonitor import (
    LeaderMonitorState,
    _guarded_leader_fetch,
    _monitor_loop_healthy,
    _persist_cursor,
    fetch_trade_data,
    init,
    trade_monitor,
)
from storage.postgres.cursors_repo import TradeCursor

# Shared test fixtures for DI refactor
_MOCK_READINESS = MagicMock()
_MOCK_CONFIG = MagicMock()
_MOCK_CONFIG.polling.fetch_interval = 5


@pytest.fixture(autouse=True)
def _reset_trade_monitor_fetch_guards():
    trade_monitor_module._LEADER_FETCH_LOCKS.clear()
    trade_monitor_module._LEADER_FETCH_LAST_TS.clear()
    trade_monitor_module._LEADER_FETCH_INFLIGHT.clear()
    trade_monitor_module._fetch_spacing_config = None
    yield
    trade_monitor_module._LEADER_FETCH_LOCKS.clear()
    trade_monitor_module._LEADER_FETCH_LAST_TS.clear()
    trade_monitor_module._LEADER_FETCH_INFLIGHT.clear()
    trade_monitor_module._fetch_spacing_config = None


@pytest.mark.asyncio
async def test_init_sets_startup_watermark_from_latest_activity_when_no_cursor_exists():
    activities = [
        {
            "id": "old",
            "transactionHash": "0xold",
            "timestamp": 1_700_000_000,
            "type": "TRADE",
        },
        {
            "id": "new",
            "transactionHash": "0xnew",
            "timestamp": 1_700_000_010,
            "type": "TRADE",
        },
    ]
    state = LeaderMonitorState()

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=activities):
        await init("0x1111111111111111111111111111111111111111", state)

    assert int(state.last_cursor.timestamp) == 1_700_000_010
    assert state.last_cursor.tx_hash == "0xnew"
    assert state.last_cursor.trade_id == "new"
    assert state.last_processed_ts == float(1_700_000_010)


@pytest.mark.asyncio
async def test_late_trade_newer_than_cursor_is_processed_once():
    now = int(time.time())
    late_trade = {
        "transactionHash": "0xlate",
        "timestamp": now - 120,  # older than 60s, must still be processed
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-1",
        "conditionId": "cond-1",
        "size": 10,
        "price": 0.5,
        "usdcSize": 5,
    }
    calls: list[list[dict]] = []

    async def on_trade(trades: list[dict]):
        calls.append(trades)

    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 300, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 300),
    )

    with patch(
        "services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[late_trade]
    ):
        await fetch_trade_data("0xleader", on_trade, state, _MOCK_READINESS, _MOCK_CONFIG)
        await fetch_trade_data("0xleader", on_trade, state, _MOCK_READINESS, _MOCK_CONFIG)

    assert len(calls) == 1
    assert calls[0][0]["transactionHash"] == "0xlate"
    assert int(state.last_cursor.timestamp) == int(now - 120)
    assert state.last_cursor.tx_hash == "0xlate"


@pytest.mark.asyncio
async def test_init_preserves_existing_cursor_on_restart():
    activities = [
        {
            "id": "newer-but-should-not-overwrite",
            "transactionHash": "0xnew",
            "timestamp": 1_700_000_010,
            "type": "TRADE",
        }
    ]
    state = LeaderMonitorState(
        last_cursor=TradeCursor(
            timestamp=1_700_000_100, tx_hash="0xpersisted", trade_id="persisted"
        ),
        last_processed_ts=1_700_000_100.0,
    )

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=activities):
        await init("0x1111111111111111111111111111111111111111", state)

    assert int(state.last_cursor.timestamp) == 1_700_000_100
    assert state.last_cursor.tx_hash == "0xpersisted"
    assert state.last_cursor.trade_id == "persisted"


@pytest.mark.asyncio
async def test_new_trade_schedules_research_capture_without_blocking_execution():
    now = int(time.time())
    late_trade = {
        "transactionHash": "0xlate",
        "timestamp": now - 120,
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-1",
        "conditionId": "cond-1",
        "size": 10,
        "price": 0.5,
        "usdcSize": 5,
    }
    on_trade = AsyncMock()
    research_capture = AsyncMock()
    research_capture.capture_observed_trades = AsyncMock()
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 300, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 300),
    )

    scheduled: list[asyncio.Task] = []
    original_create_task = asyncio.create_task

    def _capture_task(coro):
        task = original_create_task(coro)
        scheduled.append(task)
        return task

    with (
        patch(
            "services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[late_trade]
        ),
        patch("services.tradeMonitor.asyncio.create_task", side_effect=_capture_task),
    ):
        await fetch_trade_data(
            "0xleader",
            on_trade,
            state,
            _MOCK_READINESS,
            _MOCK_CONFIG,
            research_capture_service=research_capture,
        )
        if scheduled:
            await asyncio.gather(*scheduled)

    on_trade.assert_awaited_once()
    research_capture.capture_observed_trades.assert_awaited_once()


@pytest.mark.asyncio
async def test_cursor_comparison_normalizes_hash_and_trade_id_on_restart():
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=1_700_000_010, tx_hash="0XABC", trade_id="TRADE-1"),
        last_processed_ts=1_700_000_010.0,
    )
    duplicate_trade = {
        "id": "trade-1",
        "transactionHash": "0xabc",
        "timestamp": 1_700_000_010,
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-1",
        "conditionId": "cond-1",
        "size": 5,
        "price": 0.4,
        "usdcSize": 2,
    }
    on_trade = AsyncMock()

    with patch(
        "services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[duplicate_trade]
    ):
        await fetch_trade_data("0xleader", on_trade, state, _MOCK_READINESS, _MOCK_CONFIG)

    on_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_sell_trade_is_annotated_with_leader_sell_fraction():
    now = int(time.time())
    sell_trade = {
        "id": "trade-sell-1",
        "transactionHash": "0xsell1",
        "timestamp": now,
        "type": "TRADE",
        "side": "SELL",
        "asset": "token-2",
        "conditionId": "cond-2",
        "size": 50,
        "price": 0.60,
        "usdcSize": 30,
    }
    leader_positions = [
        {
            "conditionId": "cond-2",
            "asset": "token-2",
            "size": 50,
        }
    ]
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 100, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 100),
    )
    captured: list[list[dict]] = []

    async def on_trade(trades: list[dict]):
        captured.append(trades)

    with patch(
        "services.tradeMonitor.fetch_data",
        new_callable=AsyncMock,
        side_effect=[[sell_trade], leader_positions],
    ):
        await fetch_trade_data("0xleader", on_trade, state, _MOCK_READINESS, _MOCK_CONFIG)

    assert len(captured) == 1
    emitted = captured[0][0]
    assert emitted["_leader_position_size_after"] == pytest.approx(50.0)
    assert emitted["_leader_position_size_before"] == pytest.approx(100.0)
    assert emitted["_leader_sell_fraction"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_detected_trade_logs_emit_before_trade_callback():
    now = int(time.time())
    trade = {
        "id": "trade-2",
        "transactionHash": "0xabc2",
        "timestamp": now,
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-2",
        "conditionId": "cond-2",
        "size": 8,
        "price": 0.25,
        "usdcSize": 2,
    }
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 100, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 100),
    )
    events: list[str] = []

    async def on_trade(_trades: list[dict]):
        events.append("callback")

    with (
        patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[trade]),
        patch(
            "services.tradeMonitor.print_new_trade_detected",
            side_effect=lambda _trade: events.append("console"),
        ) as print_detected,
        patch(
            "services.tradeMonitor.log_trade_detected",
            side_effect=lambda *_args, **_kwargs: events.append("structured"),
        ) as log_detected,
    ):
        await fetch_trade_data("0xleader", on_trade, state, _MOCK_READINESS, _MOCK_CONFIG)

    print_detected.assert_called_once()
    log_detected.assert_called_once()
    assert events == ["console", "structured", "callback"]


def test_monitor_loop_healthy_requires_recent_poll_and_ready_dependencies():
    from services.healthServer import ReadinessTracker

    tracker = ReadinessTracker()
    tracker.update(db_ok=True, api_ok=True, last_trade_poll_ts=100)
    with patch("services.tradeMonitor.time.time", return_value=120):
        assert (
            _monitor_loop_healthy(fetch_ok=True, fetch_interval_s=5, readiness_tracker=tracker)
            is True
        )


def test_monitor_loop_healthy_rejects_stale_or_failed_poll():
    from services.healthServer import ReadinessTracker

    tracker = ReadinessTracker()
    tracker.update(db_ok=True, api_ok=True, last_trade_poll_ts=100)
    with patch("services.tradeMonitor.time.time", return_value=140):
        assert (
            _monitor_loop_healthy(fetch_ok=True, fetch_interval_s=5, readiness_tracker=tracker)
            is False
        )
    with patch("services.tradeMonitor.time.time", return_value=110):
        assert (
            _monitor_loop_healthy(fetch_ok=False, fetch_interval_s=5, readiness_tracker=tracker)
            is False
        )


@pytest.mark.asyncio
async def test_trade_monitor_emits_heartbeat_when_monitor_is_healthy():
    leader_wallet = "0x1111111111111111111111111111111111111111"
    on_trade = AsyncMock()
    cursor_store = MagicMock()
    cursor_store.get_cursor = AsyncMock(
        return_value=TradeCursor(timestamp=0, tx_hash="", trade_id=None)
    )
    cursor_store.set_cursor = AsyncMock()

    with (
        patch(
            "services.tradeMonitor.LeaderCursorStore.create",
            new_callable=AsyncMock,
            return_value=cursor_store,
        ),
        patch("services.tradeMonitor.init", new_callable=AsyncMock),
        patch(
            "services.tradeMonitor.fetch_trade_data",
            new_callable=AsyncMock,
            return_value=(True, 17),
        ),
        patch("services.tradeMonitor._monitor_loop_healthy", return_value=True),
        patch("services.tradeMonitor.monitor_heartbeat_tick") as heartbeat_tick,
        patch("services.tradeMonitor.asyncio.sleep", side_effect=asyncio.CancelledError),
    ):
        with pytest.raises(asyncio.CancelledError):
            await trade_monitor(
                {"leaderWallet": leader_wallet, "FETCH_INTERVAL": 1, "MONITORED_LEADER_COUNT": 3},
                on_trade,
                readiness_tracker=MagicMock(),
                config=MagicMock(),
            )

    heartbeat_tick.assert_called_once_with(
        trader_count=3,
        checked_activities=17,
        leader_wallet=leader_wallet,
        monitored_leader_count=3,
        symbol="🟢",
    )


@pytest.mark.asyncio
async def test_trade_monitor_skips_heartbeat_when_monitor_is_unhealthy():
    leader_wallet = "0x1111111111111111111111111111111111111111"
    on_trade = AsyncMock()
    cursor_store = MagicMock()
    cursor_store.get_cursor = AsyncMock(
        return_value=TradeCursor(timestamp=0, tx_hash="", trade_id=None)
    )
    cursor_store.set_cursor = AsyncMock()

    with (
        patch(
            "services.tradeMonitor.LeaderCursorStore.create",
            new_callable=AsyncMock,
            return_value=cursor_store,
        ),
        patch("services.tradeMonitor.init", new_callable=AsyncMock),
        patch(
            "services.tradeMonitor.fetch_trade_data",
            new_callable=AsyncMock,
            return_value=(False, 0),
        ),
        patch("services.tradeMonitor._monitor_loop_healthy", return_value=False),
        patch("services.tradeMonitor.monitor_heartbeat_tick") as heartbeat_tick,
        patch("services.tradeMonitor.asyncio.sleep", side_effect=asyncio.CancelledError),
    ):
        with pytest.raises(asyncio.CancelledError):
            await trade_monitor(
                {"leaderWallet": leader_wallet, "FETCH_INTERVAL": 1},
                on_trade,
                readiness_tracker=MagicMock(),
                config=MagicMock(),
            )

    heartbeat_tick.assert_not_called()


# --- P2-h: guarded cursor persist must not crash the leader monitor ---


@pytest.mark.asyncio
async def test_persist_cursor_swallows_transient_store_error():
    cursor_store = MagicMock()
    cursor_store.set_cursor = AsyncMock(side_effect=RuntimeError("pg pool exhausted"))
    cursor = TradeCursor(timestamp=123, tx_hash="0xabc", trade_id="t1")

    with patch.object(trade_monitor_module.metrics, "inc") as inc:
        ok = await _persist_cursor(cursor_store, "0xleader", cursor)

    assert ok is False
    cursor_store.set_cursor.assert_awaited_once_with("0xleader", cursor)
    inc.assert_any_call("leader_cursor_persist_failed_total")


@pytest.mark.asyncio
async def test_persist_cursor_returns_true_on_success():
    cursor_store = MagicMock()
    cursor_store.set_cursor = AsyncMock()
    cursor = TradeCursor(timestamp=123, tx_hash="0xabc", trade_id="t1")

    ok = await _persist_cursor(cursor_store, "0xleader", cursor)

    assert ok is True
    cursor_store.set_cursor.assert_awaited_once_with("0xleader", cursor)


@pytest.mark.asyncio
async def test_persist_cursor_propagates_cancellation():
    cursor_store = MagicMock()
    cursor_store.set_cursor = AsyncMock(side_effect=asyncio.CancelledError)
    cursor = TradeCursor(timestamp=1, tx_hash="", trade_id=None)

    with pytest.raises(asyncio.CancelledError):
        await _persist_cursor(cursor_store, "0xleader", cursor)


@pytest.mark.asyncio
async def test_trade_monitor_survives_set_cursor_failure_and_keeps_looping():
    """A transient set_cursor failure must NOT crash trade_monitor.

    Pre-fix, an unguarded ``await cursor_store.set_cursor`` propagated out of
    trade_monitor -> run_leader_monitor and silently killed the leader monitor
    task (the app supervisor logs-and-drops it without restart). Here the store
    raises on every write; the loop must keep iterating (proven by it reaching
    the second sleep and raising the injected CancelledError) rather than
    bubbling the RuntimeError.
    """
    leader_wallet = "0x1111111111111111111111111111111111111111"
    on_trade = AsyncMock()

    cursor_store = MagicMock()
    cursor_store.get_cursor = AsyncMock(
        return_value=TradeCursor(timestamp=0, tx_hash="", trade_id=None)
    )
    # Every set_cursor (initial + per-loop) raises a transient error.
    cursor_store.set_cursor = AsyncMock(side_effect=RuntimeError("transient pg"))

    # Each fetch advances the cursor so the per-loop persist branch is entered.
    advanced = TradeCursor(timestamp=999, tx_hash="0xnew", trade_id="new")

    async def _fake_fetch(*_args, **_kwargs):
        # Mutate the caller's state object so the cursor-changed branch fires.
        state_obj = _args[2]
        state_obj.last_cursor = advanced
        return True, 1

    sleeps: list[float] = []

    async def _fake_sleep(_seconds):
        sleeps.append(_seconds)
        # Let the loop run twice, then stop it deterministically.
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    with (
        patch(
            "services.tradeMonitor.LeaderCursorStore.create",
            new_callable=AsyncMock,
            return_value=cursor_store,
        ),
        patch("services.tradeMonitor.init", new_callable=AsyncMock),
        patch("services.tradeMonitor.fetch_trade_data", side_effect=_fake_fetch),
        patch("services.tradeMonitor._monitor_loop_healthy", return_value=False),
        patch("services.tradeMonitor.asyncio.sleep", side_effect=_fake_sleep),
    ):
        with pytest.raises(asyncio.CancelledError):
            await trade_monitor(
                {"leaderWallet": leader_wallet, "FETCH_INTERVAL": 1},
                on_trade,
                readiness_tracker=MagicMock(),
                config=_MOCK_CONFIG,
            )

    # Loop iterated at least twice despite set_cursor raising every time —
    # i.e. the RuntimeError did not escape and kill the monitor.
    assert len(sleeps) >= 2
    # set_cursor was attempted (initial + at least one per-loop persist).
    assert cursor_store.set_cursor.await_count >= 2


@pytest.mark.asyncio
async def test_guarded_leader_fetch_singleflights_same_wallet_and_kind():
    calls: list[str] = []

    async def _fake_fetch(url: str, *, silent: bool = False):
        calls.append(url)
        await asyncio.sleep(0.01)
        return [{"ok": True, "url": url}]

    wallet = "0xabc"
    url = f"https://data-api.polymarket.com/activity?user={wallet}&limit=50&offset=0"

    with (
        patch("services.tradeMonitor._fetch_spacing_config", None),
        patch("services.tradeMonitor.fetch_data", side_effect=_fake_fetch),
    ):
        first, second = await asyncio.gather(
            _guarded_leader_fetch("activity", wallet, url),
            _guarded_leader_fetch("activity", wallet, url),
        )

    assert len(calls) == 1
    assert first == second


@pytest.mark.asyncio
async def test_guarded_leader_fetch_keeps_different_wallets_parallel():
    calls: list[str] = []

    async def _fake_fetch(url: str, *, silent: bool = False):
        calls.append(url)
        await asyncio.sleep(0.01)
        return [{"ok": True, "url": url}]

    with (
        patch("services.tradeMonitor._fetch_spacing_config", None),
        patch("services.tradeMonitor.fetch_data", side_effect=_fake_fetch),
    ):
        first, second = await asyncio.gather(
            _guarded_leader_fetch(
                "activity",
                "0xaaa",
                "https://data-api.polymarket.com/activity?user=0xaaa&limit=50&offset=0",
            ),
            _guarded_leader_fetch(
                "activity",
                "0xbbb",
                "https://data-api.polymarket.com/activity?user=0xbbb&limit=50&offset=0",
            ),
        )

    assert len(calls) == 2
    assert first[0]["url"] != second[0]["url"]


@pytest.mark.asyncio
async def test_v1_mode_preserves_individual_trade_identities_without_aggregation():
    """Deliver each REST fill separately so on-chain aliases deduplicate exactly."""

    now = int(time.time())
    trades = [
        {
            "id": "trade-a",
            "transactionHash": "0xtx-a",
            "timestamp": now,
            "type": "TRADE",
            "side": "BUY",
            "asset": "token-1",
            "conditionId": "cond-1",
            "size": 5,
            "price": 0.4,
            "usdcSize": 2,
        },
        {
            "id": "trade-b",
            "transactionHash": "0xtx-b",
            "timestamp": now + 1,
            "type": "TRADE",
            "side": "BUY",
            "asset": "token-1",
            "conditionId": "cond-1",
            "size": 6,
            "price": 0.4,
            "usdcSize": 2.4,
        },
    ]
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 1, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 1),
    )
    on_trade = AsyncMock()

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=trades):
        await fetch_trade_data(
            "0xleader",
            on_trade,
            state,
            _MOCK_READINESS,
            _MOCK_CONFIG,
            aggregate_signals=False,
        )

    delivered = on_trade.await_args.args[0]
    assert [trade["transactionHash"] for trade in delivered] == ["0xtx-a", "0xtx-b"]
    assert all("_txHashes" not in trade for trade in delivered)
