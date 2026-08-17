"""Tests for orchestrators.user_data_runtime — UserDataRuntime.

Covers:
  - _api_creds_valid: rejects missing/empty keys, accepts full set
  - start() with invalid creds: skips ws creation, marks readiness not-connecting
  - start() with valid creds: creates ws client, marks readiness connecting
  - _on_authenticated: sets status.authenticated, flips wss_user_ok=True
  - _on_disconnected: clears status.authenticated, flips wss_user_ok=False
  - _run_ws exception: captures error in status, updates readiness, re-raises
  - stop(): cancels task, clears ws_client, updates readiness wss_user_ok=False
  - status(): returns snapshot copy, not live reference
  - _update_readiness: swallows readiness errors gracefully
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cache.market_state_store import MarketStateStore
from orchestrators.user_data_runtime import UserDataRuntime, UserDataRuntimeStatus
from services.healthServer import ReadinessTracker


VALID_CREDS = {
    "api_key": "ck1",
    "api_secret": "ck2",
    "api_passphrase": "ck3",
}

USER_ID = "user-test-001"


def _make_runtime(
    *,
    creds: dict | None = None,
    store: MarketStateStore | None = None,
    readiness: ReadinessTracker | None = None,
) -> UserDataRuntime:
    return UserDataRuntime(
        market_store=store or MarketStateStore(),
        readiness=readiness or ReadinessTracker(),
        api_creds=creds if creds is not None else dict(VALID_CREDS),
        user_id=USER_ID,
    )


# ---------------------------------------------------------------------------
# _api_creds_valid (static, pure)
# ---------------------------------------------------------------------------


class TestApiCredsValid:
    def test_accepts_full_credentials(self) -> None:
        assert UserDataRuntime._api_creds_valid(VALID_CREDS) is True

    def test_rejects_missing_api_key(self) -> None:
        creds = {k: v for k, v in VALID_CREDS.items() if k != "api_key"}
        assert UserDataRuntime._api_creds_valid(creds) is False

    def test_rejects_missing_api_secret(self) -> None:
        creds = {k: v for k, v in VALID_CREDS.items() if k != "api_secret"}
        assert UserDataRuntime._api_creds_valid(creds) is False

    def test_rejects_missing_api_passphrase(self) -> None:
        creds = {k: v for k, v in VALID_CREDS.items() if k != "api_passphrase"}
        assert UserDataRuntime._api_creds_valid(creds) is False

    def test_rejects_empty_string_api_key(self) -> None:
        creds = {**VALID_CREDS, "api_key": ""}
        assert UserDataRuntime._api_creds_valid(creds) is False

    def test_rejects_whitespace_only_value(self) -> None:
        creds = {**VALID_CREDS, "api_secret": "   "}
        assert UserDataRuntime._api_creds_valid(creds) is False

    def test_rejects_none_value(self) -> None:
        creds = {**VALID_CREDS, "api_passphrase": None}  # type: ignore[dict-item]
        assert UserDataRuntime._api_creds_valid(creds) is False

    def test_rejects_empty_dict(self) -> None:
        assert UserDataRuntime._api_creds_valid({}) is False


# ---------------------------------------------------------------------------
# start() — invalid credentials
# ---------------------------------------------------------------------------


class TestStartInvalidCreds:
    @pytest.mark.asyncio
    async def test_start_missing_creds_does_not_create_ws_client(self) -> None:
        rt = _make_runtime(creds={"api_key": "", "api_secret": "", "api_passphrase": ""})
        await rt.start()
        assert rt._ws_client is None

    @pytest.mark.asyncio
    async def test_start_missing_creds_sets_last_error(self) -> None:
        rt = _make_runtime(creds={})
        await rt.start()
        assert rt._status.last_error is not None

    @pytest.mark.asyncio
    async def test_start_missing_creds_readiness_wss_user_ok_false(self) -> None:
        readiness = ReadinessTracker()
        rt = _make_runtime(creds={}, readiness=readiness)
        await rt.start()
        assert readiness.snapshot()["wss_user_ok"] is False

    @pytest.mark.asyncio
    async def test_start_missing_creds_readiness_connecting_false(self) -> None:
        readiness = ReadinessTracker()
        rt = _make_runtime(creds={}, readiness=readiness)
        await rt.start()
        snap = readiness.snapshot()
        assert snap.get("wss_user_connecting") is False

    @pytest.mark.asyncio
    async def test_start_missing_creds_running_flag_set(self) -> None:
        rt = _make_runtime(creds={})
        await rt.start()
        assert rt._status.running is True


# ---------------------------------------------------------------------------
# start() — valid credentials
# ---------------------------------------------------------------------------


class TestStartValidCreds:
    @pytest.mark.asyncio
    async def test_start_valid_creds_creates_ws_client(self) -> None:
        rt = _make_runtime()
        with patch("orchestrators.user_data_runtime.UserWebSocket") as MockWS:
            mock_ws = AsyncMock()
            mock_ws.connect = AsyncMock(return_value=None)
            MockWS.return_value = mock_ws

            await rt.start()
            await asyncio.sleep(0)

        assert rt._ws_client is not None
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_valid_creds_marks_connecting(self) -> None:
        readiness = ReadinessTracker()
        rt = _make_runtime(readiness=readiness)

        with patch("orchestrators.user_data_runtime.UserWebSocket") as MockWS:
            mock_ws = AsyncMock()
            mock_ws.connect = AsyncMock(return_value=None)
            MockWS.return_value = mock_ws

            await rt.start()
            await asyncio.sleep(0)

        snap = readiness.snapshot()
        assert snap.get("wss_user_connecting") is True or snap.get("wss_user_ok") is False
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_valid_creds_creates_background_task(self) -> None:
        rt = _make_runtime()

        with patch("orchestrators.user_data_runtime.UserWebSocket") as MockWS:
            mock_ws = AsyncMock()
            mock_ws.connect = AsyncMock(return_value=None)
            MockWS.return_value = mock_ws

            await rt.start()
            await asyncio.sleep(0)

        assert rt._ws_task is not None
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_sets_running_true(self) -> None:
        rt = _make_runtime()

        with patch("orchestrators.user_data_runtime.UserWebSocket") as MockWS:
            mock_ws = AsyncMock()
            mock_ws.connect = AsyncMock(return_value=None)
            MockWS.return_value = mock_ws

            await rt.start()
            await asyncio.sleep(0)

        assert rt._status.running is True
        await rt.stop()


# ---------------------------------------------------------------------------
# _on_authenticated
# ---------------------------------------------------------------------------


class TestOnAuthenticated:
    @pytest.mark.asyncio
    async def test_on_authenticated_sets_status_authenticated(self) -> None:
        rt = _make_runtime()
        await rt._on_authenticated()
        assert rt._status.authenticated is True

    @pytest.mark.asyncio
    async def test_on_authenticated_clears_last_error(self) -> None:
        rt = _make_runtime()
        rt._status.last_error = "stale error"
        await rt._on_authenticated()
        assert rt._status.last_error is None

    @pytest.mark.asyncio
    async def test_on_authenticated_sets_wss_user_ok_true(self) -> None:
        readiness = ReadinessTracker()
        rt = _make_runtime(readiness=readiness)
        await rt._on_authenticated()
        assert readiness.snapshot()["wss_user_ok"] is True

    @pytest.mark.asyncio
    async def test_on_authenticated_clears_connecting_flag(self) -> None:
        readiness = ReadinessTracker()
        readiness.update(wss_user_connecting=True)
        rt = _make_runtime(readiness=readiness)
        await rt._on_authenticated()
        assert readiness.snapshot().get("wss_user_connecting") is False


# ---------------------------------------------------------------------------
# _on_disconnected
# ---------------------------------------------------------------------------


class TestOnDisconnected:
    @pytest.mark.asyncio
    async def test_on_disconnected_clears_authenticated(self) -> None:
        rt = _make_runtime()
        rt._status.authenticated = True
        await rt._on_disconnected()
        assert rt._status.authenticated is False

    @pytest.mark.asyncio
    async def test_on_disconnected_sets_wss_user_ok_false(self) -> None:
        readiness = ReadinessTracker()
        readiness.update(wss_user_ok=True)
        rt = _make_runtime(readiness=readiness)
        await rt._on_disconnected()
        assert readiness.snapshot()["wss_user_ok"] is False

    @pytest.mark.asyncio
    async def test_on_disconnected_clears_connecting_flag(self) -> None:
        readiness = ReadinessTracker()
        readiness.update(wss_user_connecting=True)
        rt = _make_runtime(readiness=readiness)
        await rt._on_disconnected()
        assert readiness.snapshot().get("wss_user_connecting") is False


# ---------------------------------------------------------------------------
# _on_fill and _on_order_update (future PR stubs — must not raise)
# ---------------------------------------------------------------------------


class TestEventCallbacks:
    @pytest.mark.asyncio
    async def test_on_fill_does_not_raise(self) -> None:
        rt = _make_runtime()
        await rt._on_fill({"asset_id": "0xabc", "order_id": "ord-1", "side": "BUY"})

    @pytest.mark.asyncio
    async def test_on_order_update_does_not_raise(self) -> None:
        rt = _make_runtime()
        await rt._on_order_update({"order_id": "ord-1", "status": "MATCHED"})

    @pytest.mark.asyncio
    async def test_on_fill_empty_event_does_not_raise(self) -> None:
        rt = _make_runtime()
        await rt._on_fill({})

    @pytest.mark.asyncio
    async def test_on_order_update_empty_event_does_not_raise(self) -> None:
        rt = _make_runtime()
        await rt._on_order_update({})


# ---------------------------------------------------------------------------
# _run_ws exception path
# ---------------------------------------------------------------------------


class TestRunWsException:
    @pytest.mark.asyncio
    async def test_run_ws_exception_sets_last_error(self) -> None:
        rt = _make_runtime()
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock(side_effect=RuntimeError("connect boom"))
        rt._ws_client = mock_ws

        with pytest.raises(RuntimeError, match="connect boom"):
            await rt._run_ws()

        assert rt._status.last_error is not None
        assert "connect boom" in rt._status.last_error

    @pytest.mark.asyncio
    async def test_run_ws_exception_clears_authenticated(self) -> None:
        rt = _make_runtime()
        rt._status.authenticated = True
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock(side_effect=OSError("net fail"))
        rt._ws_client = mock_ws

        with pytest.raises(OSError):
            await rt._run_ws()

        assert rt._status.authenticated is False

    @pytest.mark.asyncio
    async def test_run_ws_exception_sets_readiness_not_ok(self) -> None:
        readiness = ReadinessTracker()
        readiness.update(wss_user_ok=True)
        rt = _make_runtime(readiness=readiness)
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock(side_effect=RuntimeError("ws error"))
        rt._ws_client = mock_ws

        with pytest.raises(RuntimeError):
            await rt._run_ws()

        assert readiness.snapshot()["wss_user_ok"] is False

    @pytest.mark.asyncio
    async def test_run_ws_cancelled_propagates(self) -> None:
        rt = _make_runtime()
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock(side_effect=asyncio.CancelledError())
        rt._ws_client = mock_ws

        with pytest.raises(asyncio.CancelledError):
            await rt._run_ws()


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


class TestStop:
    @pytest.mark.asyncio
    async def test_stop_clears_ws_client(self) -> None:
        rt = _make_runtime()

        with patch("orchestrators.user_data_runtime.UserWebSocket") as MockWS:
            mock_ws = AsyncMock()
            mock_ws.connect = AsyncMock(return_value=None)
            MockWS.return_value = mock_ws

            await rt.start()
            await asyncio.sleep(0)
            await rt.stop()

        assert rt._ws_client is None

    @pytest.mark.asyncio
    async def test_stop_cancels_ws_task(self) -> None:
        rt = _make_runtime()

        with patch("orchestrators.user_data_runtime.UserWebSocket") as MockWS:
            mock_ws = AsyncMock()
            mock_ws.connect = AsyncMock(return_value=None)
            MockWS.return_value = mock_ws

            await rt.start()
            await asyncio.sleep(0)
            task = rt._ws_task
            await rt.stop()

        assert task is not None
        assert task.cancelled() or task.done()
        assert rt._ws_task is None

    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self) -> None:
        rt = _make_runtime()
        rt._status.running = True
        await rt.stop()
        assert rt._status.running is False

    @pytest.mark.asyncio
    async def test_stop_clears_authenticated(self) -> None:
        rt = _make_runtime()
        rt._status.authenticated = True
        await rt.stop()
        assert rt._status.authenticated is False

    @pytest.mark.asyncio
    async def test_stop_sets_readiness_wss_user_ok_false(self) -> None:
        readiness = ReadinessTracker()
        readiness.update(wss_user_ok=True)
        rt = _make_runtime(readiness=readiness)
        await rt.stop()
        assert readiness.snapshot()["wss_user_ok"] is False

    @pytest.mark.asyncio
    async def test_stop_ws_stop_exception_does_not_raise(self) -> None:
        rt = _make_runtime()
        mock_ws = MagicMock()
        mock_ws.stop = AsyncMock(side_effect=RuntimeError("stop fail"))
        rt._ws_client = mock_ws
        await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_idempotent(self) -> None:
        rt = _make_runtime()
        await rt.stop()
        await rt.stop()
        assert rt._ws_client is None
        assert rt._ws_task is None


# ---------------------------------------------------------------------------
# status()
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_returns_copy(self) -> None:
        rt = _make_runtime()
        s1 = rt.status()
        s2 = rt.status()
        assert s1 is not s2

    def test_status_reflects_running_false_initially(self) -> None:
        rt = _make_runtime()
        assert rt.status().running is False

    def test_status_reflects_authenticated_false_initially(self) -> None:
        rt = _make_runtime()
        assert rt.status().authenticated is False

    def test_status_reflects_last_error_none_initially(self) -> None:
        rt = _make_runtime()
        assert rt.status().last_error is None

    def test_status_type(self) -> None:
        rt = _make_runtime()
        assert isinstance(rt.status(), UserDataRuntimeStatus)

    @pytest.mark.asyncio
    async def test_status_reflects_authenticated_after_callback(self) -> None:
        rt = _make_runtime()
        await rt._on_authenticated()
        assert rt.status().authenticated is True


# ---------------------------------------------------------------------------
# _update_readiness — error resilience
# ---------------------------------------------------------------------------


class TestUpdateReadiness:
    def test_update_readiness_swallows_exception(self) -> None:
        rt = _make_runtime()
        bad_readiness = MagicMock()
        bad_readiness.update = MagicMock(side_effect=RuntimeError("tracker broken"))
        rt.readiness = bad_readiness
        rt._update_readiness(wss_user_ok=True)

    def test_update_readiness_calls_tracker_update(self) -> None:
        readiness = ReadinessTracker()
        rt = _make_runtime(readiness=readiness)
        rt._update_readiness(wss_user_ok=True, wss_user_connecting=False)
        snap = readiness.snapshot()
        assert snap["wss_user_ok"] is True
        assert snap["wss_user_connecting"] is False


# ---------------------------------------------------------------------------
# Wake-reconciler (PR 4)
# ---------------------------------------------------------------------------


def _make_runtime_with_signal(callback) -> UserDataRuntime:
    return UserDataRuntime(
        market_store=MarketStateStore(),
        readiness=ReadinessTracker(),
        api_creds=dict(VALID_CREDS),
        user_id=USER_ID,
        on_reconcile_signal=callback,
    )


class TestReconcileSignalWake:
    @pytest.mark.asyncio
    async def test_on_fill_invokes_reconcile_signal(self) -> None:
        callback = AsyncMock()
        rt = _make_runtime_with_signal(callback)
        event = {"asset_id": "0xtok", "order_id": "ord-pr4-fill", "side": "BUY"}

        await rt._on_fill(event)

        callback.assert_awaited_once_with(event)

    @pytest.mark.asyncio
    async def test_on_order_update_invokes_reconcile_signal(self) -> None:
        callback = AsyncMock()
        rt = _make_runtime_with_signal(callback)
        event = {"order_id": "ord-pr4-ou", "status": "filled"}

        await rt._on_order_update(event)

        callback.assert_awaited_once_with(event)

    @pytest.mark.asyncio
    async def test_failing_callback_does_not_propagate(self) -> None:
        async def _raise(_event):
            raise RuntimeError("boom")

        rt = _make_runtime_with_signal(_raise)

        # Must not raise — WSS loop must keep running.
        await rt._on_fill({"asset_id": "0xtok"})
        await rt._on_order_update({"order_id": "ord-pr4-err"})

    @pytest.mark.asyncio
    async def test_no_callback_is_a_noop(self) -> None:
        rt = _make_runtime_with_signal(None)
        await rt._on_fill({"asset_id": "0xtok"})
        await rt._on_order_update({"order_id": "ord-pr4-none"})

    def test_user_data_runtime_does_not_carry_positions_or_reservations(self) -> None:
        """Structural pin: settlement stays with ReconciliationService."""
        rt = _make_runtime_with_signal(None)
        assert not hasattr(rt, "positions_repo")
        assert not hasattr(rt, "fills_repo")
        assert not hasattr(rt, "reservation_service")


class TestMarketTokenSubscriptionWake:
    @pytest.mark.asyncio
    async def test_on_fill_subscribes_seen_token_to_market_runtime(self) -> None:
        callback = AsyncMock()
        rt = UserDataRuntime(
            market_store=MarketStateStore(),
            readiness=ReadinessTracker(),
            api_creds=dict(VALID_CREDS),
            user_id=USER_ID,
            on_market_token_seen=callback,
        )

        await rt._on_fill({"asset_id": " 12345 ", "side": "BUY"})

        callback.assert_awaited_once_with("12345")

    @pytest.mark.asyncio
    async def test_on_fill_accepts_asset_field_for_seen_token(self) -> None:
        callback = AsyncMock()
        rt = UserDataRuntime(
            market_store=MarketStateStore(),
            readiness=ReadinessTracker(),
            api_creds=dict(VALID_CREDS),
            user_id=USER_ID,
            on_market_token_seen=callback,
        )

        await rt._on_fill({"asset": " 67890 ", "side": "SELL"})

        callback.assert_awaited_once_with("67890")
