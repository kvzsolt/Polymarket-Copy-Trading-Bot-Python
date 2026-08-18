"""On-chain detection runtime orchestrator (PR2).

Owns:
  * the mined-``OrderFilled`` log WebSocket subscription (always safe to run;
    mined fills are authoritative),
  * the optional pending-transaction WebSocket subscription (gated by
    ``ENABLE_ONCHAIN_PENDING_DETECTION``, default off),
  * an ``asyncio.Queue`` of :class:`OnchainTradeSignal`,
  * a dispatcher task that enriches ``condition_id`` and routes each signal
    to the copy pipeline via the injected ``signal_sink``.

The ``signal_sink`` is an async callable ``OnchainTradeSignal -> None``. In
production it is a wrapper around ``trade_pipeline.execute_trades``; in tests
it is a mock that records dispatched signals.

Safety invariants:
  * Pending signals are **provisional**: they only reach the sink when the
    pending flag is on, and they are superseded by the mined signal that
    shares their ``tx_hash`` (no double dispatch).
  * **Per-fill idempotency at the dispatcher** (Greptile P1 #2 re-review):
    mined signals are keyed by ``(tx_hash, log_index)`` so a single
    settlement tx that emits multiple ``OrderFilled`` logs produces one
    dispatch *per fill*, not one per tx. Pending signals (which have no
    ``log_index``) are keyed by ``tx_hash`` alone. Cross-suppression: if a
    mined fill for a ``tx_hash`` has already been dispatched, any later
    pending signal for that same ``tx_hash`` is dropped (the authoritative
    mined copy already replaced the provisional). The reverse direction
    (pending dispatched first, then mined arrives) dispatches both — the
    copy pipeline's ``build_trade_key`` (keyed on
    ``leader|tx_hash|side|asset``) catches the duplicate and amends/drops
    the second at the *copy* layer, which is the correct place for
    cross-signal dedup.
  * The runtime never blocks the copy path on WSS health: if the WS is down,
    no on-chain signals are produced and the existing REST-polling leader
    path continues unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from typing import Any, Callable, Coroutine

from clients.onchain_ws import OnchainWebSocket
from services.onchain_detection.mined_fill_deduplicator import MinedFillDeduplicator
from services.onchain_detection.order_filled_detector import (
    decode_order_filled_log,
    build_signal_from_log,
    log_subscription_params,
)
from services.onchain_detection.pending_tx_detector import (
    PendingTxConfig,
    build_signals_from_pending_tx,
    is_stale_pending,
    pending_subscription_params,
)
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)
from utils.logger import get_logger, redact_error_detail

logger = get_logger("OnchainDetectionRuntime")

SignalSink = Callable[[OnchainTradeSignal], Coroutine[Any, Any, None]]
ConditionIdResolver = Callable[[str], Coroutine[Any, Any, str]]
WebSocketFactory = Callable[..., OnchainWebSocket]
MinedSubscriptionParamsFactory = Callable[[frozenset[str]], list[list[Any]]]

_CONFIDENCE_RANK = {
    DetectionConfidence.MARKET_UNATTRIBUTED: 0,
    DetectionConfidence.PENDING_PROBABLE: 1,
    DetectionConfidence.MINED_AUTHORITATIVE: 2,
    DetectionConfidence.CONFIRMED_FINAL: 3,
}


def _confidence_meets_required(
    confidence: DetectionConfidence,
    required: DetectionConfidence,
) -> bool:
    return _CONFIDENCE_RANK.get(confidence, -1) >= _CONFIDENCE_RANK.get(required, 0)


class OnchainDetectionRuntime:
    """Owns the two WS subscriptions + the dispatcher loop."""

    def __init__(
        self,
        *,
        watched_wallets: set[str],
        ws_url: str,
        signal_sink: SignalSink,
        enable_pending: bool = False,
        enable_mined: bool = True,
        pending_config: PendingTxConfig | None = None,
        condition_id_resolver: ConditionIdResolver | None = None,
        websocket_factory: WebSocketFactory = OnchainWebSocket,
        mined_subscription_params_factory: MinedSubscriptionParamsFactory | None = None,
        queue_maxsize: int = 1000,
        dispatched_cap: int = 10_000,
    ) -> None:
        self._watched = {w.lower() for w in watched_wallets}
        self._ws_url = ws_url
        self._signal_sink = signal_sink
        self._enable_pending = bool(enable_pending)
        self._enable_mined = bool(enable_mined)
        self._pending_config = pending_config or PendingTxConfig()
        self._resolver = condition_id_resolver
        self._websocket_factory = websocket_factory
        self._mined_params_factory = mined_subscription_params_factory
        self._dedupe = MinedFillDeduplicator()
        self._queue: asyncio.Queue[OnchainTradeSignal] = asyncio.Queue(maxsize=queue_maxsize)
        self._mined_sockets: list[OnchainWebSocket] = []
        self._pending_ws: OnchainWebSocket | None = None
        self._dispatcher_task: asyncio.Task | None = None
        self._running = False
        # Roster resubscribe (watched-topic-filtered modes only): at most one
        # background rebuild task exists; ``_resubscribe_requested`` coalesces
        # multiple roster updates into one rebuild off the signal hot path.
        self._resubscribe_requested = False
        self._resubscribe_task: asyncio.Task | None = None
        # Per-fill idempotency state for the dispatcher (Greptile P1 #2
        # re-review). All dispatched keys — mined log keys
        # ``(tx_hash, log_index)``, pending keys ``(tx_hash, leader)``,
        # and mined-leader cross-suppression keys ``(tx_hash, leader)`` —
        # live in a single ``_dispatched_keys`` set + FIFO so that
        # ``dispatched_cap`` bounds ALL state uniformly (no unbounded
        # auxiliary set). Mined signals add both their log key and their
        # leader key; pending signals add only their leader key. Cross-
        # suppression: a pending is dropped if its ``(tx_hash, leader)``
        # key is already in the set (a mined or a prior pending for the
        # same leader already dispatched).
        #
        # FIFO-evicted at ``dispatched_cap`` to bound memory in long-
        # running processes.
        self._dispatched_lock = asyncio.Lock()
        self._dispatched_keys: set[tuple[str, int] | tuple[str, str]] = set()
        self._dispatched_fifo: deque[tuple[str, int] | tuple[str, str]] = deque()
        self._dispatched_cap = int(dispatched_cap)

    @property
    def deduplicator(self) -> MinedFillDeduplicator:
        return self._dedupe

    @property
    def connected(self) -> bool | None:
        """Return whether any composed detection socket is subscribed.

        The operator control plane reads this as a late-bound health probe, so
        it must stay O(1) and must never claim a state it cannot observe:
        before ``start`` (and after ``stop``) no socket exists and the answer
        is ``None``, which the read surface renders as "unknown" rather than
        "disconnected".

        Returns:
            ``True`` when at least one composed socket is subscribed, ``False``
            when every composed socket is down, ``None`` when none exists.
        """

        sockets = [*self._mined_sockets]
        if self._pending_ws is not None:
            sockets.append(self._pending_ws)
        if not sockets:
            return None
        return any(bool(getattr(socket, "connected", False)) for socket in sockets)

    async def _resolve_condition_id(self, token_id: str) -> str:
        if self._resolver is None or not token_id:
            return ""
        try:
            return await self._resolver(token_id)
        except Exception as exc:
            logger.debug(
                "onchain_condition_id_resolve_failed | token_id=%s error=%s",
                token_id,
                redact_error_detail(exc),
            )
            return ""

    async def _on_mined_log(self, log: dict[str, Any]) -> None:
        decoded = decode_order_filled_log(log)
        if decoded is None:
            return
        # Dedupe by (tx_hash, log_index) — the authoritative key.
        if await self._dedupe.is_duplicate_mined(decoded.tx_hash, decoded.log_index):
            logger.debug(
                "onchain_mined_dup_dropped | tx=%s log_index=%s",
                decoded.tx_hash,
                decoded.log_index,
            )
            return
        condition_id = await self._resolve_condition_id(str(decoded.token_id))
        event_ts = int(time.time() * 1000)
        signal = build_signal_from_log(
            decoded,
            watched_wallets=self._watched,
            event_ts=event_ts,
            condition_id=condition_id,
        )
        if signal is None:
            return
        # If a pending signal for this (tx_hash, leader_wallet) was recorded,
        # mark it superseded so the dispatcher drops the provisional copy in
        # favour of this mined one. Per-leader keying (not tx-wide) ensures a
        # mined fill for leaderA does NOT suppress a pending for leaderB in
        # the same tx (Greptile P1 re-review: "Pending matches are dropped").
        await self._dedupe.supersede_pending(decoded.tx_hash, signal.leader_wallet)
        await self._enqueue(signal)

    async def _on_pending_tx(self, tx: dict[str, Any]) -> None:
        if not self._enable_pending:
            return
        now_ms = int(time.time() * 1000)
        signals = build_signals_from_pending_tx(
            tx,
            watched_wallets=self._watched,
            recv_ts_ms=now_ms,
            config=self._pending_config,
        )
        for signal in signals:
            # Record the pending (tx_hash, leader_wallet); if a pending for
            # this leader in this tx was already recorded, drop the duplicate
            # (do not double-dispatch). Per-leader keying allows distinct
            # watched-wallet matches in the same tx to each be recorded.
            is_new = await self._dedupe.record_pending(signal.tx_hash, signal.leader_wallet)
            if not is_new:
                logger.debug(
                    "onchain_pending_dup_dropped | tx=%s leader=%s",
                    signal.tx_hash,
                    signal.leader_wallet,
                )
                continue
            await self._enqueue(signal)

    async def _enqueue(self, signal: OnchainTradeSignal) -> None:
        # ``await asyncio.sleep(0)`` yields control to the event loop so
        # SonarCloud S7503 recognises this as a genuine async function
        # (the body is otherwise sync-only queue operations). The yield is
        # a noop and does not change dispatch ordering.
        await asyncio.sleep(0)
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull:
            logger.warning(
                "onchain_signal_queue_full | dropping oldest | confidence=%s tx=%s",
                signal.confidence,
                signal.tx_hash,
            )
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(signal)
            except asyncio.QueueFull:
                logger.warning(
                    "onchain_signal_drop | confidence=%s tx=%s",
                    signal.confidence,
                    signal.tx_hash,
                )

    async def _dispatcher_loop(self) -> None:
        while self._running:
            try:
                signal = await self._queue.get()
            except asyncio.CancelledError:
                raise
            try:
                if signal.provisional:
                    # Pending signals only dispatch when the flag is on; the
                    # _on_pending_tx path already checked, but this is a
                    # belt-and-braces guard in case a pending signal was
                    # enqueued before the flag was toggled off.
                    if not self._enable_pending:
                        continue
                    now_ms = int(time.time() * 1000)
                    if is_stale_pending(
                        signal,
                        now_ms,
                        self._pending_config.max_signal_age_ms,
                    ):
                        logger.debug(
                            "onchain_pending_stale_dropped | tx=%s leader=%s age_ms=%s",
                            signal.tx_hash,
                            signal.leader_wallet,
                            now_ms - signal.event_ts,
                        )
                        continue
                    if not _confidence_meets_required(
                        signal.confidence,
                        self._pending_config.confidence_required,
                    ):
                        logger.debug(
                            "onchain_pending_confidence_dropped | tx=%s leader=%s "
                            "confidence=%s required=%s",
                            signal.tx_hash,
                            signal.leader_wallet,
                            signal.confidence,
                            self._pending_config.confidence_required,
                        )
                        continue
                    # If a mined signal for this (tx_hash, leader_wallet)
                    # has already superseded this pending signal, drop it
                    # (no double dispatch). Per-leader keying ensures a
                    # mined for leaderA does not suppress a pending for
                    # leaderB in the same tx.
                    if not await self._dedupe.pending_known(signal.tx_hash, signal.leader_wallet):
                        continue
                # Per-fill idempotency gate (Greptile P1 #2 re-review):
                # mined signals are keyed by ``(tx_hash, log_index)`` so a
                # single settlement tx with multiple ``OrderFilled`` logs
                # produces one dispatch per fill. Pending signals (no
                # log_index) are keyed by ``tx_hash`` alone and are dropped
                # if a mined fill for the same tx already dispatched (the
                # authoritative copy replaces the provisional).
                if await self._claim_or_drop(signal) is False:
                    continue
                await self._signal_sink(signal)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "onchain_dispatcher_signal_failed | confidence=%s tx=%s error=%s",
                    getattr(signal, "confidence", "?"),
                    getattr(signal, "tx_hash", ""),
                    redact_error_detail(exc),
                )

    async def _claim_or_drop(self, signal: OnchainTradeSignal) -> bool:
        """Atomically claim a fill for dispatch.

        Returns True if this caller is the first to claim the fill
        (the sink should be invoked). Returns False if a prior signal
        for the same fill already claimed the slot (the duplicate is
        dropped).

        Idempotency keys (Greptile P1 #2 re-review + "Mined keys never
        expire" re-review):
          * Mined signals (``log_index`` is not None): the log key
            ``(tx_hash, log_index)`` is the dedupe key (one dispatch per
            distinct fill). The leader key ``(tx_hash, leader_wallet)``
            is ALSO added to the set so a later pending for the same
            leader is cross-suppressed (dropped — the authoritative copy
            already placed). Both keys go into the FIFO so the cap bounds
            all state uniformly.
          * Pending signals (``log_index`` is None): the leader key
            ``(tx_hash, leader_wallet)`` is the dedupe key AND the
            cross-suppression check target. If it is already in the set
            (a prior pending OR a mined for the same leader already
            dispatched), the pending is dropped.

        The reverse direction (pending dispatched first, then mined
        arrives for the same leader) dispatches both; the copy pipeline's
        ``build_trade_key`` (``leader|tx_hash|side|asset``) dedupes at
        the copy layer — the dispatcher's job is per-fill uniqueness.

        Signals without a ``tx_hash`` are always dispatched (they bypass
        the idempotency gate; this preserves the legacy watch path that
        predates the on-chain detection dispatcher).
        """
        if not signal.tx_hash:
            return True
        leader = signal.leader_wallet.lower()
        async with self._dispatched_lock:
            if signal.log_index is not None:
                log_key: tuple[str, int] = (signal.tx_hash, signal.log_index)
                if log_key in self._dispatched_keys:
                    logger.debug(
                        "onchain_dispatch_dropped_dup | tx=%s log_index=%s",
                        signal.tx_hash,
                        signal.log_index,
                    )
                    return False
                self._dispatched_keys.add(log_key)
                self._dispatched_fifo.append(log_key)
                # Cross-suppression key: a later pending for the same
                # (tx_hash, leader) will see this in the set and drop.
                leader_key: tuple[str, str] = (signal.tx_hash, leader)
                if leader_key not in self._dispatched_keys:
                    self._dispatched_keys.add(leader_key)
                    self._dispatched_fifo.append(leader_key)
            else:
                pending_key: tuple[str, str] = (signal.tx_hash, leader)
                if pending_key in self._dispatched_keys:
                    logger.debug(
                        "onchain_dispatch_dropped_dup | tx=%s leader=%s",
                        signal.tx_hash,
                        leader,
                    )
                    return False
                self._dispatched_keys.add(pending_key)
                self._dispatched_fifo.append(pending_key)
            while len(self._dispatched_fifo) > self._dispatched_cap:
                old = self._dispatched_fifo.popleft()
                self._dispatched_keys.discard(old)
            return True

    def dispatched_tx_hashes_snapshot(
        self,
    ) -> set[tuple[str, int] | tuple[str, str]]:
        """Test-only: return a copy of all dispatched idempotency keys."""
        return set(self._dispatched_keys)

    def reset_dispatched(self) -> None:
        """Test-only: clear all dispatched idempotency state."""
        self._dispatched_keys.clear()
        self._dispatched_fifo.clear()

    async def start(self) -> None:
        # ``await asyncio.sleep(0)`` yields control to the event loop so
        # SonarCloud S7503 recognises ``start`` as a genuine async function.
        # The subsequent ``OnchainWebSocket.start()`` calls and
        # ``asyncio.create_task(...)`` schedule background work without
        # using the ``await`` keyword, which the rule's analyzer does not
        # count as an async feature.
        await asyncio.sleep(0)
        if self._running:
            return
        self._running = True
        if self._enable_mined:
            for params in self._mined_subscription_params():
                mined_ws = self._websocket_factory(
                    ws_url=self._ws_url,
                    subscribe_params=params,
                    name="orderfilled",
                    on_message=self._on_mined_log,
                )
                mined_ws.start()
                self._mined_sockets.append(mined_ws)
            if not self._mined_sockets and not self._watched:
                logger.info(
                    "onchain_mined_subscriptions_idle | watched=%d — no leader filter to open "
                    "yet; a non-empty roster update reopens them",
                    len(self._watched),
                )
        if self._enable_pending:
            self._pending_ws = self._websocket_factory(
                ws_url=self._ws_url,
                subscribe_params=pending_subscription_params(
                    hashes_only=self._pending_config.hashes_only,
                ),
                name="pendingtx",
                on_message=self._on_pending_tx,
            )
            self._pending_ws.start()
        self._dispatcher_task = asyncio.create_task(
            self._dispatcher_loop(), name="onchain-dispatcher"
        )
        logger.info(
            "onchain_detection_runtime_started | mined=%s mined_subscriptions=%d pending=%s "
            "watched=%d",
            self._enable_mined,
            len(self._mined_sockets),
            self._enable_pending,
            len(self._watched),
        )

    def _mined_subscription_params(self) -> list[list[Any]]:
        """Return the ``eth_subscribe`` params for every mined subscription.

        With a params factory (logs-only confirmed mode) the filters embed the
        watched leaders as topic2/topic3 OR-lists; without one the runtime
        keeps the single address-only subscription of the gen-1/pending shape.
        """
        if self._mined_params_factory is None:
            return [log_subscription_params()]
        return list(self._mined_params_factory(frozenset(self._watched)))

    def _schedule_mined_resubscribe(self) -> None:
        """Request one roster-driven rebuild of the mined subscriptions.

        Bounded: at most one background rebuild task exists, and roster
        updates that arrive while a rebuild is running coalesce into a single
        follow-up pass that reads the latest watched set.
        """
        if not self._running or not self._enable_mined:
            return
        self._resubscribe_requested = True
        if self._resubscribe_task is None or self._resubscribe_task.done():
            self._resubscribe_task = asyncio.create_task(
                self._resubscribe_mined_loop(), name="onchain-mined-resubscribe"
            )

    async def _resubscribe_mined_loop(self) -> None:
        while self._running and self._resubscribe_requested:
            self._resubscribe_requested = False
            try:
                await self._resubscribe_mined_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A failed rebuild must not kill the loop (the next roster
                # update re-schedules it) nor leak the failure silently.
                logger.warning(
                    "onchain_mined_resubscribe_failed | error=%s",
                    redact_error_detail(exc),
                )

    async def _resubscribe_mined_once(self) -> None:
        """Rebuild the mined subscriptions without a detection gap.

        The replacement sockets open first and only then are the superseded
        ones stopped, so a fill that lands during the switch is delivered by
        the new subscription (the per-fill ``(tx_hash, log_index)`` dedupe
        absorbs any overlap). A raise before the swap leaves the current
        sockets subscribed.
        """
        new_sockets: list[OnchainWebSocket] = []
        if self._running and self._enable_mined:
            try:
                for params in self._mined_subscription_params():
                    mined_ws = self._websocket_factory(
                        ws_url=self._ws_url,
                        subscribe_params=params,
                        name="orderfilled",
                        on_message=self._on_mined_log,
                    )
                    mined_ws.start()
                    new_sockets.append(mined_ws)
            except Exception:
                # A partial replacement batch must not leak: stop what was
                # started, keep the current sockets subscribed, and let the
                # guarded loop report the failure.
                await self._stop_sockets(new_sockets, "onchain_mined_partial_batch_stop_failed")
                raise
        old_sockets, self._mined_sockets = self._mined_sockets, new_sockets
        await self._stop_sockets(old_sockets, "onchain_mined_socket_stop_failed")
        if new_sockets:
            logger.info(
                "onchain_mined_resubscribed | subscriptions=%d watched=%d",
                len(self._mined_sockets),
                len(self._watched),
            )
        elif not self._watched:
            logger.info(
                "onchain_mined_subscriptions_idle | watched=%d — filters closed; a non-empty "
                "roster update reopens them",
                len(self._watched),
            )

    async def _stop_sockets(self, sockets: list[OnchainWebSocket], event: str) -> None:
        """Stop each socket, logging (never raising) per-socket stop failures.

        Args:
            sockets: Sockets to stop; each failure is isolated so one bad
                socket cannot keep its siblings open.
            event: Structured log event name used for a failed stop.
        """
        for socket in sockets:
            try:
                await socket.stop()
            except Exception as exc:
                logger.warning("%s | error=%s", event, redact_error_detail(exc))

    async def stop(self) -> None:
        self._running = False
        if self._resubscribe_task is not None:
            self._resubscribe_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._resubscribe_task
            self._resubscribe_task = None
        for mined_ws in self._mined_sockets:
            await mined_ws.stop()
        self._mined_sockets = []
        if self._pending_ws is not None:
            await self._pending_ws.stop()
            self._pending_ws = None
        if self._dispatcher_task is not None:
            self._dispatcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatcher_task
            self._dispatcher_task = None
        logger.info("onchain_detection_runtime_stopped")

    def update_watched_wallets(self, wallets: set[str]) -> None:
        """Runtime-update the watched set (lowercased).

        In watched-topic-filtered modes the provider-side filters embed the
        roster, so a roster change also schedules a resubscribe (off the
        signal hot path). In the legacy address-only mode nothing resubscribes
        — the client-side maker/taker match reads the updated set directly.
        """
        self._watched = {w.lower() for w in wallets}
        if self._mined_params_factory is not None:
            self._schedule_mined_resubscribe()
