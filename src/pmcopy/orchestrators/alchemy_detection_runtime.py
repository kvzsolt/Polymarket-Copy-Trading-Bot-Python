"""Unified Alchemy-only detection runtime over one singleton multiplexed WebSocket.

Supports strictly the three cost-controlled detection modes:
  1. ``on_chain_logs_only`` (server-side ``topics[2]`` maker + ``topics[3]`` taker filters)
  2. ``on_chain_pending`` (singleton ``toAddress``-locked Exchange pending stream + hex pre-filter)
  3. ``confirmed+mempool_reserved`` (both streams multiplexed on 1 socket + 15s reservation state machine)

Enforces ``PMCOPY__MAX_HOURLY_WS_MB`` via ``AlchemyWsMeter``: if hourly WebSocket
bytes exceed the ceiling while in ``on_chain_pending`` or
``confirmed+mempool_reserved``, logs a ``CRITICAL`` alert, unsubscribes
``alchemy_pendingTransactions`` on the open socket, keeps/activates the two
user-filtered ``logs`` subscriptions, and degrades in-place to
``on_chain_logs_only``.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
from collections.abc import Awaitable, Callable, Iterable
from decimal import Decimal
from typing import Any, Protocol

from pmcopy.clients.alchemy_multiplex_ws import (
    AlchemyMultiplexWebSocket,
    SlotSubscription,
)
from pmcopy.config.addresses import verified_polymarket_exchange_addresses
from pmcopy.config.app_config import AppConfig
from pmcopy.domain.detection import DetectionMode
from pmcopy.domain.tracked_users import TrackedUserDirectory
from pmcopy.services.onchain_detection.alchemy_ws_meter import AlchemyWsMeter
from pmcopy.services.onchain_detection.condition_id_resolver import (
    GammaConditionIdResolver,
)
from pmcopy.services.onchain_detection.mempool_reservation_state import (
    MempoolReservationStateMachine,
)
from pmcopy.services.onchain_detection.mined_fill_deduplicator import (
    DEFAULT_LOG_LRU_CAPACITY,
    LogEventLruDeduplicator,
)
from pmcopy.services.onchain_detection.order_filled_detector import (
    build_signals_from_log_for_directory,
    build_user_filtered_log_subscriptions,
    decode_order_filled_log,
)
from pmcopy.services.onchain_detection.pending_tx_detector import (
    PendingTxHashCache,
    build_direct_user_pending_subscriptions,
    build_exchange_pending_subscription_params,
    build_signals_from_pending_tx_for_directory,
)
from pmcopy.services.onchain_detection.trade_detection_models import OnchainTradeSignal
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("AlchemyDetectionRuntime")


class _DispatchSink(Protocol):
    async def dispatch(self, signal: OnchainTradeSignal) -> None: ...


class ConditionIdResolverProtocol(Protocol):
    async def resolve(self, token_id: str) -> str: ...


SignalConsumer = _DispatchSink | Callable[[OnchainTradeSignal], Awaitable[None]]


class AlchemyDetectionRuntime:
    """Production Alchemy-only detection coordinator over one shared WebSocket."""

    def __init__(
        self,
        *,
        ws_url: str,
        mode: DetectionMode,
        active_exchange_addresses: tuple[str, ...],
        include_legacy_v1_exchanges: bool,
        track_direct_user_txs: bool,
        max_hourly_ws_mb: Decimal,
        subscription_debounce_seconds: float,
        mempool_reservation_ttl_seconds: float,
        initial_directory: TrackedUserDirectory,
        directory_builder: Callable[[Iterable[str]], TrackedUserDirectory],
        sink: SignalConsumer,
        condition_id_resolver: ConditionIdResolverProtocol | None = None,
        status_changed: Callable[[], None] | None = None,
        connect_factory: Callable[..., Any] | None = None,
        telemetry_interval_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._configured_mode = DetectionMode.parse_config_mode(mode)
        self._effective_mode = self._configured_mode
        self._active_exchanges = tuple(a.lower() for a in active_exchange_addresses)
        self._include_legacy_v1 = bool(include_legacy_v1_exchanges)
        self._track_direct_user_txs = bool(track_direct_user_txs)
        self._directory = initial_directory
        self._directory_builder = directory_builder
        self._sink = sink
        self._condition_id_resolver = condition_id_resolver
        self._status_changed = status_changed
        self._telemetry_interval_seconds = max(0.05, float(telemetry_interval_seconds))
        self._clock = clock

        self._log_lru = LogEventLruDeduplicator(max_entries=DEFAULT_LOG_LRU_CAPACITY)
        self._pending_tx_cache = PendingTxHashCache(clock=clock)
        self._reservation_sm = MempoolReservationStateMachine(
            ttl_seconds=mempool_reservation_ttl_seconds,
            clock=clock,
        )

        self._ws: AlchemyMultiplexWebSocket | None = None
        self._meter = AlchemyWsMeter(
            max_hourly_ws_mb=max_hourly_ws_mb,
            mode_getter=lambda: self._effective_mode,
            active_subscriptions_getter=lambda: (
                self._ws.active_subscription_count if self._ws is not None else 0
            ),
            on_hourly_limit_exceeded=self._on_hourly_bandwidth_exceeded,
            clock=clock,
        )
        self._ws = AlchemyMultiplexWebSocket(
            ws_url=ws_url,
            meter=self._meter,
            debounce_seconds=subscription_debounce_seconds,
            connect_factory=connect_factory,
        )

        self._stop_event = asyncio.Event()
        self._telemetry_task: asyncio.Task[None] | None = None
        self._expiry_task: asyncio.Task[None] | None = None
        self._running = False

    @classmethod
    def compose(
        cls,
        *,
        config: AppConfig,
        wallets: Iterable[str],
        sink: SignalConsumer,
        status_changed: Callable[[], None] | None = None,
        condition_id_resolver: ConditionIdResolverProtocol | None = None,
        use_default_condition_resolver: bool = True,
        connect_factory: Callable[..., Any] | None = None,
        telemetry_interval_seconds: float = 60.0,
    ) -> "AlchemyDetectionRuntime":
        """Compose ``AlchemyDetectionRuntime`` from validated ``AppConfig``."""
        mode = DetectionMode.parse_config_mode(config.detection.mode)
        onchain_cfg = config.detection.onchain
        ws_url = str(onchain_cfg.alchemy_wss_url or onchain_cfg.onchain_ws_url or "").strip()
        if not ws_url:
            raise ValueError("PMCOPY__ALCHEMY_WSS_URL is required for AlchemyDetectionRuntime")

        exchanges = (
            tuple(onchain_cfg.active_exchange_addresses)
            if onchain_cfg.active_exchange_addresses
            else verified_polymarket_exchange_addresses(
                include_legacy_v1=onchain_cfg.include_legacy_v1_exchanges
            )
        )

        def _build_dir(active: Iterable[str]) -> TrackedUserDirectory:
            return TrackedUserDirectory.from_config(config, active_wallets=active)

        initial_dir = _build_dir(wallets)
        resolver: ConditionIdResolverProtocol | None
        if condition_id_resolver is not None:
            resolver = condition_id_resolver
        elif use_default_condition_resolver:
            resolver = GammaConditionIdResolver()
        else:
            resolver = None
        return cls(
            ws_url=ws_url,
            mode=mode,
            active_exchange_addresses=exchanges,
            include_legacy_v1_exchanges=onchain_cfg.include_legacy_v1_exchanges,
            track_direct_user_txs=onchain_cfg.track_direct_user_txs,
            max_hourly_ws_mb=onchain_cfg.max_hourly_ws_mb,
            subscription_debounce_seconds=onchain_cfg.subscription_debounce_seconds,
            mempool_reservation_ttl_seconds=onchain_cfg.mempool_reservation_ttl_seconds,
            initial_directory=initial_dir,
            directory_builder=_build_dir,
            sink=sink,
            condition_id_resolver=resolver,
            status_changed=status_changed,
            connect_factory=connect_factory,
            telemetry_interval_seconds=telemetry_interval_seconds,
        )

    @property
    def configured_mode(self) -> DetectionMode:
        """Return startup-configured ``DetectionMode``."""
        return self._configured_mode

    @property
    def effective_mode(self) -> DetectionMode:
        """Return currently active ``DetectionMode`` (reflects auto-downgrade)."""
        return self._effective_mode

    @property
    def meter(self) -> AlchemyWsMeter:
        """Return real-time WebSocket UTF-8 byte / CU meter."""
        return self._meter

    @property
    def ws(self) -> AlchemyMultiplexWebSocket:
        """Return singleton multiplexed Alchemy WebSocket transport."""
        assert self._ws is not None
        return self._ws

    @property
    def reservation_state(self) -> MempoolReservationStateMachine:
        """Return ``confirmed+mempool_reserved`` reservation state machine."""
        return self._reservation_sm

    @property
    def connected(self) -> bool:
        """Return True when the shared WebSocket transport is healthy."""
        return self._ws.connected if self._ws is not None else False

    def active_wallets(self) -> tuple[str, ...]:
        """Return canonical leader wallets currently tracked by the runtime."""
        return tuple(sorted(self._directory.leader_wallets))

    def update_watched_wallets(self, wallets: Iterable[str]) -> None:
        """Dynamically update tracked trader set with 2s debounced resubscription."""
        new_dir = self._directory_builder(wallets)
        if new_dir == self._directory:
            return
        self._directory = new_dir
        if self._ws is not None:
            self._ws.schedule_reconcile(self._build_desired_slots())
        self._notify_status_changed()

    async def start(self) -> None:
        """Start singleton WebSocket subscriptions, 60s telemetry, and TTL sweeper."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        assert self._ws is not None
        desired = self._build_desired_slots()
        await self._ws.start(desired)
        self._telemetry_task = asyncio.create_task(
            self._meter.run_telemetry_loop(
                self._stop_event,
                interval_seconds=self._telemetry_interval_seconds,
            ),
            name="alchemy-ws-telemetry-loop",
        )
        self._expiry_task = asyncio.create_task(
            self._run_reservation_expiry_loop(),
            name="alchemy-mempool-reservation-expiry",
        )
        self._notify_status_changed()
        logger.info(
            "alchemy_detection_runtime_started | mode=%s tracked_leaders=%d "
            "tracked_addresses=%d initial_slots=%d endpoint=%s",
            self._effective_mode.value,
            len(self._directory.entries),
            len(self._directory.addresses_with_prefix),
            len(desired),
            self._ws.redacted_endpoint,
        )

    async def stop(self) -> None:
        """Stop background loops, expire pending reservations, and close the socket."""
        self._running = False
        self._stop_event.set()
        for task in (self._telemetry_task, self._expiry_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._telemetry_task = None
        self._expiry_task = None
        self._reservation_sm.expire_all_active()
        if self._ws is not None:
            await self._ws.stop()
        self._notify_status_changed()

    def _build_desired_slots(self) -> dict[str, SlotSubscription]:
        # Section 3 invariant: empty tracked user set opens ZERO subscriptions!
        if self._directory.is_empty:
            return {}

        slots: dict[str, SlotSubscription] = {}
        mode = self._effective_mode

        if mode in {
            DetectionMode.ON_CHAIN_LOGS_ONLY,
            DetectionMode.CONFIRMED_MEMPOOL_RESERVED,
        }:
            log_subs = build_user_filtered_log_subscriptions(
                self._active_exchanges,
                self._directory.padded_topics,
                include_legacy_v1=self._include_legacy_v1,
            )
            if len(log_subs) == 2:
                slots["logs_maker"] = SlotSubscription(
                    params=log_subs[0],
                    on_message=self._handle_log_notification,
                )
                slots["logs_taker"] = SlotSubscription(
                    params=log_subs[1],
                    on_message=self._handle_log_notification,
                )

        if mode in {
            DetectionMode.ON_CHAIN_PENDING,
            DetectionMode.CONFIRMED_MEMPOOL_RESERVED,
        }:
            pending_sub = build_exchange_pending_subscription_params(self._active_exchanges)
            slots["pending_exchange"] = SlotSubscription(
                params=pending_sub,
                on_message=self._handle_pending_notification,
            )
            if self._track_direct_user_txs:
                direct_subs = build_direct_user_pending_subscriptions(
                    self._directory.addresses_with_prefix,
                    track_direct_user_txs=True,
                )
                for idx, dsub in enumerate(direct_subs):
                    slots[f"pending_direct_users_{idx}"] = SlotSubscription(
                        params=dsub,
                        on_message=self._handle_pending_notification,
                    )

        return slots

    async def _on_hourly_bandwidth_exceeded(self, rolling_bytes: int) -> None:
        """Auto-downgrade in-place to ``on_chain_logs_only`` on the same open socket."""
        prev_mode = self._effective_mode
        self._effective_mode = DetectionMode.ON_CHAIN_LOGS_ONLY
        expired = self._reservation_sm.expire_all_active()
        if self._ws is not None:
            await self._ws.reconcile_now(self._build_desired_slots())
        self._notify_status_changed()
        logger.warning(
            "alchemy_detection_auto_downgraded | previous_mode=%s effective_mode=%s "
            "rolling_hour_bytes=%d expired_reservations=%d active_subscriptions=%d",
            prev_mode.value,
            self._effective_mode.value,
            rolling_bytes,
            len(expired),
            self._ws.active_subscription_count if self._ws is not None else 0,
        )

    def telemetry_snapshot(self) -> dict[str, Any]:
        """Return a read-only telemetry snapshot of Alchemy WSS and reservation state."""
        meter_snap = self._meter.live_snapshot()
        return {
            **meter_snap,
            "configured_mode": self._configured_mode.value,
            "effective_mode": self._effective_mode.value,
            "connected": self.connected,
            "tracked_leaders": len(self._directory.entries),
            "tracked_addresses": len(self._directory.addresses_with_prefix),
            "active_reservations": self._reservation_sm.active_reservation_count,
            "promoted_confirmed": self._reservation_sm.promoted_count,
            "direct_confirmed": self._reservation_sm.direct_confirmed_count,
            "expired_ghost_fills": self._reservation_sm.expired_ghost_fill_count,
            "dropped_ws_notifications": (
                self._ws.dropped_notifications if self._ws is not None else 0
            ),
        }

    async def _handle_log_notification(self, raw_log: dict[str, Any]) -> None:
        if self._log_lru.is_duplicate_raw_log(raw_log):
            return
        decoded = decode_order_filled_log(
            raw_log,
            include_legacy_v1=self._include_legacy_v1,
        )
        if decoded is None:
            return
        signals = build_signals_from_log_for_directory(
            decoded,
            self._directory,
            event_ts=None,
            source_receipt=raw_log.get("_source_receipt"),
        )
        if not signals:
            return

        self._meter.record_matched_user_event(len(signals))
        for sig in signals:
            if self._effective_mode is DetectionMode.CONFIRMED_MEMPOOL_RESERVED:
                decision = self._reservation_sm.promote_confirmed(sig)
                if not decision.should_dispatch:
                    continue
                sig = decision.signal
            sig = await self._ensure_condition_id(sig)
            try:
                await self._emit_to_sink(sig)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "alchemy_detection_sink_error | tx=%s error=%s",
                    sig.tx_hash,
                    redact_error_detail(exc),
                )

    async def _handle_pending_notification(self, raw_tx: dict[str, Any]) -> None:
        if self._effective_mode is DetectionMode.ON_CHAIN_LOGS_ONLY:
            return
        receipt = raw_tx.get("_source_receipt")
        receipt_ms = getattr(receipt, "wall_timestamp_ms", None)
        now_ms = int(receipt_ms) if isinstance(receipt_ms, int) else int(time.time() * 1000)
        signals = build_signals_from_pending_tx_for_directory(
            raw_tx,
            self._directory,
            include_legacy_v1=self._include_legacy_v1,
            tx_hash_cache=self._pending_tx_cache,
            recv_ts_ms=now_ms,
        )
        if not signals:
            return

        self._meter.record_matched_user_event(len(signals))
        if self._effective_mode is DetectionMode.CONFIRMED_MEMPOOL_RESERVED:
            for sig in signals:
                sig = await self._ensure_condition_id(sig)
                self._reservation_sm.reserve_pending(sig)
            return

        for sig in signals:
            sig = await self._ensure_condition_id(sig)
            try:
                await self._emit_to_sink(sig)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "alchemy_detection_pending_sink_error | tx=%s error=%s",
                    sig.tx_hash,
                    redact_error_detail(exc),
                )

    async def _ensure_condition_id(self, signal: OnchainTradeSignal) -> OnchainTradeSignal:
        if signal.condition_id or self._condition_id_resolver is None:
            return signal
        try:
            resolved = await self._condition_id_resolver.resolve(signal.token_id)
        except Exception as exc:
            logger.debug(
                "condition_id_resolve_failed | token_id=%s error=%s",
                signal.token_id,
                redact_error_detail(exc),
            )
            return signal
        if resolved:
            return dataclasses.replace(signal, condition_id=resolved)
        return signal

    async def _emit_to_sink(self, signal: OnchainTradeSignal) -> None:
        if callable(self._sink):
            await self._sink(signal)
            return
        dispatch_fn = getattr(self._sink, "dispatch", None)
        if callable(dispatch_fn):
            await dispatch_fn(signal)  # type: ignore[misc]

    async def _run_reservation_expiry_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
                return
            except TimeoutError:
                self._reservation_sm.expire_stale()

    def _notify_status_changed(self) -> None:
        if self._status_changed is None:
            return
        with contextlib.suppress(Exception):
            self._status_changed()


__all__ = [
    "AlchemyDetectionRuntime",
    "SignalConsumer",
]
