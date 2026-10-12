"""Phase 2 — WSS heartbeat / pong-timeout / reconnect-resync tests.

Covers:
  - 5s heartbeat cadence (constant)
  - Pong timeout of 10s
  - Missed-pong closes the socket so the reconnect loop kicks in
  - Inbound activity counts as a pong (pipe alive)
  - Reconnect performs REST bootstrap (resync) — already in service, smoke check
  - Dedupe: overlapping bootstrap calls collapse to a single in-flight one
  - Exponential backoff progression: 1, 2, 4, 8, …, capped at 30
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.clients.market_ws import (
    HEARTBEAT_INTERVAL,
    MarketWebSocket,
    _MAX_RECONNECT_DELAY,
    _MIN_RECONNECT_DELAY,
)
from pmcopy.clients.ws_constants import (
    MARKET_HEARTBEAT_INTERVAL,
    PONG_TIMEOUT_SECONDS,
    USER_HEARTBEAT_INTERVAL,
)


TOK = "0xtok"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_market_heartbeat_5s(self) -> None:
        assert MARKET_HEARTBEAT_INTERVAL == 5
        assert HEARTBEAT_INTERVAL == 5

    def test_user_heartbeat_5s(self) -> None:
        assert USER_HEARTBEAT_INTERVAL == 5

    def test_pong_timeout_10s(self) -> None:
        assert PONG_TIMEOUT_SECONDS == 10.0

    def test_reconnect_delays_match_spec(self) -> None:
        assert _MIN_RECONNECT_DELAY == 1.0
        assert _MAX_RECONNECT_DELAY == 30.0


# ---------------------------------------------------------------------------
# Pong-timeout detector
# ---------------------------------------------------------------------------


class TestPongTimeout:
    def test_no_ping_yet_is_not_missed(self) -> None:
        ws = MarketWebSocket(MarketStateStore())
        assert ws._is_pong_missed() is False

    def test_recent_pong_keeps_alive(self) -> None:
        ws = MarketWebSocket(MarketStateStore())
        now = time.monotonic()
        ws._last_ping_monotonic = now
        ws._last_pong_monotonic = now  # pong at same instant — alive
        assert ws._is_pong_missed() is False

    def test_pong_within_window_is_not_missed(self) -> None:
        # Semantics (post-fix): elapsed = now - last_alive. Anchor on
        # time.monotonic() instead of a stale ping marker.
        ws = MarketWebSocket(MarketStateStore())
        now = time.monotonic()
        ws._last_ping_monotonic = now - 5.0
        ws._last_pong_monotonic = now - 5.0  # 5s ago — within 10s window
        assert ws._is_pong_missed() is False

    def test_pong_older_than_timeout_is_missed(self) -> None:
        ws = MarketWebSocket(MarketStateStore())
        now = time.monotonic()
        ws._last_ping_monotonic = now - 100.0
        ws._last_pong_monotonic = now - 100.0  # 100s ago — clearly missed
        assert ws._is_pong_missed() is True

    def test_no_pong_ever_after_ping_is_missed_if_aged(self) -> None:
        # Real loop seeds _last_pong_monotonic at connect time as the
        # "alive" marker. If a long time elapses between that marker and
        # the most recent PING send with no inbound activity, the
        # detector must fire.
        ws = MarketWebSocket(MarketStateStore())
        now = time.monotonic()
        ws._last_pong_monotonic = now - 30.0  # connection established 30s ago, silent since
        ws._last_ping_monotonic = now
        assert ws._is_pong_missed() is True

    @pytest.mark.asyncio
    async def test_heartbeat_closes_socket_on_missed_pong(self) -> None:
        ws_client = MarketWebSocket(MarketStateStore(), pong_timeout_seconds=10.0)
        ws_client._asset_ids = [TOK]
        ws_client._running = True

        sock = MagicMock()
        sock.send = AsyncMock()
        sock.close = AsyncMock()

        async def fake_sleep(_secs: float) -> None:
            # After the first PING, simulate that the previous PING was long
            # ago and no pong came back.
            ws_client._last_ping_monotonic = time.monotonic()
            ws_client._last_pong_monotonic = time.monotonic() - 30.0
            ws_client._running = False  # avoid infinite loop

        with patch("pmcopy.clients.market_ws.asyncio.sleep", side_effect=fake_sleep):
            # Pre-seed the missed-pong state directly so the first iteration
            # detects it.
            ws_client._last_ping_monotonic = time.monotonic()
            ws_client._last_pong_monotonic = time.monotonic() - 30.0
            await ws_client._heartbeat(sock)

        sock.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_inbound_message_resets_pong_timer(self) -> None:
        ws_client = MarketWebSocket(MarketStateStore())
        ws_client._last_pong_monotonic = None

        # Simulate one inbound book event by calling the handler — _message_loop
        # is what sets _last_pong_monotonic from any frame; emulate by hand.
        ws_client._handle_message("PONG")
        assert ws_client._last_pong_monotonic is not None

    @pytest.mark.asyncio
    async def test_heartbeat_loop_detects_silent_server_with_seed(self) -> None:
        """Silent-server regression: server sends ZERO frames after connect.

        Prior bug: ``_last_ping_monotonic`` was refreshed every iteration and
        the missed-pong check ran immediately after, so the elapsed gap was
        always ~0. The fix seeds ``_last_pong_monotonic`` at connect time so
        the anchor is stable and the gap grows each iteration until it
        exceeds the pong-timeout window.
        """
        ws_client = MarketWebSocket(MarketStateStore(), pong_timeout_seconds=10.0)
        ws_client._asset_ids = [TOK]
        ws_client._running = True
        # Mimic what _reconnect_loop does on a successful connect.
        ws_client._last_pong_monotonic = time.monotonic() - 100.0  # very stale

        sock = MagicMock()
        sock.send = AsyncMock()
        sock.close = AsyncMock()

        async def fake_sleep(_secs: float) -> None:
            ws_client._running = False  # one iteration only

        with patch("pmcopy.clients.market_ws.asyncio.sleep", side_effect=fake_sleep):
            await ws_client._heartbeat(sock)

        sock.close.assert_awaited()
        assert sock.send.await_count >= 1

    @pytest.mark.asyncio
    async def test_heartbeat_loop_detects_silent_server_no_seed(self) -> None:
        """Silent-server regression: no anchor seeded, no inbound ever.

        Defence in depth: even when ``_last_pong_monotonic`` never gets set,
        ``_is_pong_missed`` must fall back to time-since-last-ping rather
        than returning False forever.
        """
        ws_client = MarketWebSocket(MarketStateStore(), pong_timeout_seconds=10.0)
        ws_client._asset_ids = [TOK]
        ws_client._running = True
        ws_client._last_pong_monotonic = None  # explicit: no seed

        sock = MagicMock()
        sock.send = AsyncMock()
        sock.close = AsyncMock()

        async def fake_sleep(_secs: float) -> None:
            # Age the recorded PING into the past so the fallback branch trips.
            if ws_client._last_ping_monotonic is not None:
                ws_client._last_ping_monotonic -= 30.0
            ws_client._running = False

        with patch("pmcopy.clients.market_ws.asyncio.sleep", side_effect=fake_sleep):
            await ws_client._heartbeat(sock)

        sock.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_user_ws_pong_detector_fires(self) -> None:
        """User WS must have the same pong-timeout detector as market WS."""
        from pmcopy.cache.market_state_store import MarketStateStore as _S
        from pmcopy.clients.user_ws import UserWebSocket

        ws_client = UserWebSocket(_S(), pong_timeout_seconds=10.0)
        ws_client._running = True
        ws_client._last_pong_monotonic = time.monotonic() - 100.0

        sock = MagicMock()
        sock.send = AsyncMock()
        sock.close = AsyncMock()

        async def fake_sleep(_secs: float) -> None:
            ws_client._running = False  # one iteration only

        with patch("pmcopy.clients.user_ws.asyncio.sleep", side_effect=fake_sleep):
            await ws_client._heartbeat(sock)

        sock.close.assert_awaited()


# ---------------------------------------------------------------------------
# REST-bootstrap dedupe
# ---------------------------------------------------------------------------


class TestBootstrapDedupe:
    @pytest.mark.asyncio
    async def test_overlapping_bootstraps_collapse(self) -> None:
        cache = MarketStateStore()
        calls = 0
        gate = asyncio.Event()

        async def slow_bootstrap(asset_ids: list[str]) -> None:
            nonlocal calls
            calls += 1
            await gate.wait()

        ws = MarketWebSocket(
            cache,
            rest_bootstrap=slow_bootstrap,
        )
        ws._asset_ids = [TOK]

        first = asyncio.create_task(ws._run_rest_bootstrap())
        # Yield so the in-flight flag is set before the duplicate fires.
        await asyncio.sleep(0)
        second = asyncio.create_task(ws._run_rest_bootstrap())
        await asyncio.sleep(0)

        # Second should be a no-op because the first is still in flight.
        gate.set()
        await asyncio.gather(first, second)
        assert calls == 1
        # Flag must clear after completion so subsequent reconnects can resync.
        assert ws._bootstrap_in_flight is False

    @pytest.mark.asyncio
    async def test_sequential_bootstraps_both_run(self) -> None:
        cache = MarketStateStore()
        calls = 0

        async def boot(asset_ids: list[str]) -> None:
            nonlocal calls
            calls += 1

        ws = MarketWebSocket(cache, rest_bootstrap=boot)
        ws._asset_ids = [TOK]
        await ws._run_rest_bootstrap()
        await ws._run_rest_bootstrap()
        assert calls == 2

    @pytest.mark.asyncio
    async def test_bootstrap_in_flight_clears_after_exception(self) -> None:
        """Even when the bootstrap callable raises, the in-flight flag must
        clear so the next reconnect can issue a fresh resync."""
        cache = MarketStateStore()

        async def boom(asset_ids: list[str]) -> None:
            raise RuntimeError("rest blew up")

        ws = MarketWebSocket(cache, rest_bootstrap=boom)
        ws._asset_ids = [TOK]

        await ws._run_rest_bootstrap()
        assert ws._bootstrap_in_flight is False

        # A subsequent call must not be silently skipped.
        called_again = 0

        async def ok(asset_ids: list[str]) -> None:
            nonlocal called_again
            called_again += 1

        ws._rest_bootstrap = ok
        await ws._run_rest_bootstrap()
        assert called_again == 1


# ---------------------------------------------------------------------------
# Reconnect-resync (smoke)
# ---------------------------------------------------------------------------


class TestReconnectResync:
    @pytest.mark.asyncio
    async def test_reconnect_loop_calls_rest_bootstrap(self) -> None:
        cache = MarketStateStore()
        seen: list[list[str]] = []

        async def boot(asset_ids: list[str]) -> None:
            seen.append(list(asset_ids))

        ws = MarketWebSocket(cache, rest_bootstrap=boot)
        ws._asset_ids = [TOK]
        ws._running = True

        attempts = 0

        async def fail_connect(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts >= 2:
                ws._running = False
            raise OSError("nope")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = fail_connect
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("pmcopy.clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("pmcopy.clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        assert seen and seen[0] == [TOK]

    @pytest.mark.asyncio
    async def test_reconnect_resets_pong_state(self) -> None:
        cache = MarketStateStore()
        ws = MarketWebSocket(cache)
        ws._asset_ids = [TOK]
        ws._running = True
        # Seed stale pong/ping state from a prior connection.
        ws._last_ping_monotonic = 1.0
        ws._last_pong_monotonic = 2.0

        attempts = 0

        async def fail_connect(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts >= 1:
                ws._running = False
            raise OSError("nope")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = fail_connect
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("pmcopy.clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("pmcopy.clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        assert ws._last_ping_monotonic is None
        assert ws._last_pong_monotonic is None


# ---------------------------------------------------------------------------
# Exponential backoff
# ---------------------------------------------------------------------------


class TestExponentialBackoff:
    @pytest.mark.asyncio
    async def test_backoff_doubles_until_cap(self) -> None:
        cache = MarketStateStore()
        ws = MarketWebSocket(cache)
        ws._asset_ids = [TOK]
        ws._running = True

        sleeps: list[float] = []
        attempts = 0

        async def record_sleep(secs: float) -> None:
            sleeps.append(secs)

        async def fail_connect(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts >= 7:
                ws._running = False
            raise OSError("nope")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = fail_connect
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("pmcopy.clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("pmcopy.clients.market_ws.asyncio.sleep", side_effect=record_sleep):
                await ws._reconnect_loop()

        # First several backoffs should follow 1, 2, 4, 8, 16, 30 (capped).
        assert sleeps[:6] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
        assert all(s <= 30.0 for s in sleeps)
