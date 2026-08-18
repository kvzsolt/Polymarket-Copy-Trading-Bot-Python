"""Runtime owner for user-specific Polymarket WebSocket events.

This runtime owns UserWebSocket lifecycle and readiness state.

Responsibilities:
  - connect/authenticate user WSS
  - update wss_user_ok only after auth success
  - mark wss_user_ok false on disconnect
  - invalidate market state on own fills through UserWebSocket
  - optionally forward order/fill events to reconciliation in a later PR

It does not settle positions directly.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cache.market_state_store import MarketStateStore
from clients.user_ws import DEFAULT_USER_WS_URL, UserWebSocket
from services.healthServer import ReadinessTracker
from services.telemetry import WssTelemetryEmitter
from utils.logger import get_logger, redact_error_detail

if TYPE_CHECKING:
    from services.manual_sync_service import ManualSyncService

ReconcileSignal = Callable[[dict[str, Any]], Awaitable[None]]
# Sync callback: clear the risk-monitor price cache for ``token_id`` so a
# previously dormant (zero-bid) market re-arms as soon as a fill lands.
RiskCacheEvict = Callable[[str], None]
# Async callback: user fills reveal tokens that must be subscribed on the
# market WSS so risk/execution can read fresh quotes from MarketStateStore.
MarketTokenSubscribe = Callable[[str], Awaitable[None]]

logger = get_logger("UserDataRuntime")

_WSS_STREAM = "user"
"""Bounded telemetry stream name owned by this runtime."""


@dataclass
class UserDataRuntimeStatus:
    running: bool = False
    authenticated: bool = False
    last_error: str | None = None


@dataclass
class UserDataRuntime:
    market_store: MarketStateStore
    readiness: ReadinessTracker
    api_creds: dict[str, str]
    user_id: str
    ws_url: str = DEFAULT_USER_WS_URL
    # Optional wake-reconciler callback. Fired on own fills and order updates
    # so the canonical ReconciliationService converges immediately instead of
    # waiting for the next polled cycle. UserDataRuntime never mutates
    # positions/reservations directly — settlement remains owned by the
    # reconciler.
    on_reconcile_signal: ReconcileSignal | None = None
    # Optional manual-sync hook. When attached, lifecycle + fill events are
    # forwarded so the service can detect off-bot trades, seed positions
    # from venue on startup, and run a REST fallback when WSS is down.
    # ``None`` keeps the runtime backwards-compatible.
    manual_sync_service: "ManualSyncService | None" = None
    # Optional risk-cache evict hook. When set, every WSS fill on a token
    # clears the risk-monitor cache for that token so a previously dormant
    # market (cached at price=0) is re-probed on the next risk tick.
    on_risk_cache_evict: RiskCacheEvict | None = None
    # Optional market-data subscription hook. Manual/off-bot fills arrive via
    # user WSS; without this, the risk monitor falls back to REST /price
    # because MarketWS was never subscribed for that token.
    on_market_token_seen: MarketTokenSubscribe | None = None
    # Operator WSS lifecycle emitter. ``None`` keeps the runtime free of both
    # cost and branching; the emitter itself is synchronous, never awaited and
    # never raises, so a degraded telemetry backend cannot break the stream.
    telemetry: WssTelemetryEmitter | None = None

    _ws_client: UserWebSocket | None = field(default=None, init=False)
    _ws_task: asyncio.Task | None = field(default=None, init=False)
    _status: UserDataRuntimeStatus = field(
        default_factory=UserDataRuntimeStatus,
        init=False,
    )
    # Distinguishes the first successful authentication from every later one,
    # so an operator sees "connected" once and "reconnected" for each recovery.
    _ever_connected: bool = field(default=False, init=False)

    async def start(self) -> None:
        self._status.running = True
        self._status.authenticated = False
        self._status.last_error = None

        if not self._api_creds_valid(self.api_creds):
            self._status.last_error = "missing user websocket api credentials"
            self._update_readiness(
                wss_user_ok=False,
                wss_user_connecting=False,
                last_error=self._status.last_error,
            )
            logger.warning(
                "user_data_runtime_not_started",
                extra={"user_id": self.user_id, "reason": self._status.last_error},
            )
            return

        self._ws_client = UserWebSocket(
            cache=self.market_store,
            ws_url=self.ws_url,
            on_authenticated=self._on_authenticated,
            on_disconnected=self._on_disconnected,
            on_fill=self._on_fill,
            on_order_update=self._on_order_update,
        )

        self._ws_task = asyncio.create_task(
            self._run_ws(),
            name=f"user-wss-runtime-{self.user_id}",
        )

        self._update_readiness(
            wss_user_ok=False,
            wss_user_connecting=True,
        )

        logger.info("user_data_runtime_starting", extra={"user_id": self.user_id})

    async def stop(self) -> None:
        self._status.running = False
        self._status.authenticated = False

        if self.manual_sync_service is not None:
            try:
                await self.manual_sync_service.stop()
            except Exception as exc:
                logger.warning(
                    "manual_sync_stop_error",
                    extra={
                        "user_id": self.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

        if self._ws_client is not None:
            try:
                await self._ws_client.stop()
            except Exception as exc:
                logger.warning(
                    "user_ws_stop_error",
                    extra={
                        "user_id": self.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

        if self._ws_task is not None:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning(
                    "user_ws_task_stop_error",
                    extra={
                        "user_id": self.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

        self._ws_client = None
        self._ws_task = None

        self._update_readiness(
            wss_user_ok=False,
            wss_user_connecting=False,
        )

        logger.info("user_data_runtime_stopped", extra={"user_id": self.user_id})

    def status(self) -> UserDataRuntimeStatus:
        return UserDataRuntimeStatus(
            running=self._status.running,
            authenticated=self._status.authenticated,
            last_error=self._status.last_error,
        )

    async def _run_ws(self) -> None:
        assert self._ws_client is not None

        try:
            await self._ws_client.connect(self.api_creds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._status.last_error = repr(exc)
            self._status.authenticated = False
            self._update_readiness(
                wss_user_ok=False,
                wss_user_connecting=False,
                last_error=repr(exc),
            )
            logger.exception(
                "user_ws_task_failed",
                extra={"user_id": self.user_id},
            )
            raise

    def _emit_wss_connected(self) -> None:
        """Publish the first authentication as CONNECTED, later ones RECONNECTED."""
        telemetry = self.telemetry
        if telemetry is None:
            return
        if self._ever_connected:
            telemetry.reconnected(_WSS_STREAM)
            return
        self._ever_connected = True
        telemetry.connected(_WSS_STREAM)

    async def _on_authenticated(self) -> None:
        self._status.authenticated = True
        self._status.last_error = None

        self._update_readiness(
            wss_user_ok=True,
            wss_user_connecting=False,
        )

        logger.info("user_wss_authenticated", extra={"user_id": self.user_id})
        self._emit_wss_connected()

        if self.manual_sync_service is not None:
            try:
                await self.manual_sync_service.on_wss_authenticated()
            except Exception as exc:
                logger.warning(
                    "manual_sync_wss_authenticated_hook_failed",
                    extra={
                        "user_id": self.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

    async def _on_disconnected(self) -> None:
        self._status.authenticated = False

        self._update_readiness(
            wss_user_ok=False,
            wss_user_connecting=False,
            last_error="user websocket disconnected",
        )

        logger.warning("user_wss_disconnected", extra={"user_id": self.user_id})
        if self.telemetry is not None:
            self.telemetry.disconnected(_WSS_STREAM)

        if self.manual_sync_service is not None:
            try:
                await self.manual_sync_service.on_wss_disconnected()
            except Exception as exc:
                logger.warning(
                    "manual_sync_wss_disconnected_hook_failed",
                    extra={
                        "user_id": self.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

    async def _on_fill(self, event: dict[str, Any]) -> None:
        token_id = str(
            event.get("asset_id")
            or event.get("asset")
            or event.get("token_id")
            or event.get("tokenID")
            or event.get("tokenId")
            or ""
        ).strip()
        logger.debug(
            "user_wss_fill_event",
            extra={
                "user_id": self.user_id,
                "order_id": event.get("order_id") or event.get("id"),
                "asset_id": token_id or None,
            },
        )
        if token_id and self.on_risk_cache_evict is not None:
            try:
                self.on_risk_cache_evict(token_id)
            except Exception as exc:
                logger.warning(
                    "risk_cache_evict_failed",
                    extra={
                        "user_id": self.user_id,
                        "token_id": token_id,
                        "error": redact_error_detail(exc),
                    },
                )
        if token_id and self.on_market_token_seen is not None:
            try:
                await self.on_market_token_seen(token_id)
            except Exception as exc:
                logger.warning(
                    "market_token_subscribe_failed",
                    extra={
                        "user_id": self.user_id,
                        "token_id": token_id,
                        "error": redact_error_detail(exc),
                    },
                )
        await self._wake_reconciler(event)
        await self._wake_manual_sync(reason="wss_fill")

    async def _on_order_update(self, event: dict[str, Any]) -> None:
        logger.debug(
            "user_wss_order_update",
            extra={
                "user_id": self.user_id,
                "order_id": event.get("order_id") or event.get("id"),
                "status": event.get("status") or event.get("state"),
            },
        )
        await self._wake_reconciler(event)
        await self._wake_manual_sync(reason="wss_order_update")

    async def _wake_manual_sync(self, *, reason: str) -> None:
        """Forward a WSS event to ManualSyncService.trigger_sync (debounced)."""
        if self.manual_sync_service is None:
            return
        try:
            await self.manual_sync_service.trigger_sync(reason=reason)
        except Exception as exc:
            logger.warning(
                "manual_sync_trigger_failed",
                extra={
                    "user_id": self.user_id,
                    "reason": reason,
                    "error": redact_error_detail(exc),
                },
            )

    async def _wake_reconciler(self, event: dict[str, Any]) -> None:
        """Fire the optional reconcile-wake hook.

        A slow or failing callback must not break the WSS message loop. The
        callback may run the canonical reconcile_submitted_unconfirmed for
        the user — but it never settles positions itself; that stays inside
        ReconciliationService.
        """
        if self.on_reconcile_signal is None:
            return
        try:
            await self.on_reconcile_signal(event)
        except Exception as exc:
            logger.warning(
                "user_wss_reconcile_signal_failed",
                extra={
                    "user_id": self.user_id,
                    "error": redact_error_detail(exc),
                },
            )

    def _update_readiness(self, **fields: Any) -> None:
        try:
            self.readiness.update(**fields)
        except Exception as exc:
            logger.debug(
                "user_data_readiness_update_failed",
                extra={
                    "user_id": self.user_id,
                    "error": redact_error_detail(exc),
                },
            )

    @staticmethod
    def _api_creds_valid(api_creds: dict[str, str]) -> bool:
        required = ("api_key", "api_secret", "api_passphrase")
        return all(str(api_creds.get(key) or "").strip() for key in required)
