"""ManualSyncService — Python port of TS manualSyncService.

Detects manual (off-bot) trades on the funder wallet and keeps local state
in sync with the venue. The TS implementation lives at
``Polymarket-Copy-Trading-Bot-Typescript/src/services/manualSyncService.ts``.

Responsibilities
================
1. WSS-triggered sync: when the authenticated user WebSocket emits any
   fill/order/trade event, this service runs a debounced sweep (1s window)
   that fetches the funder's recent activity from the Polymarket REST API,
   inserts trades the bot never executed as manual trades, and refreshes
   positions + cache.
2. REST fallback: when the WSS connection is down, a polling loop sweeps
   on a fixed interval so manual trades are still detected.
3. Startup seed: ``initial_sync()`` runs once at boot to populate
   ``positions`` + ``user_positions`` from the venue snapshot. This closes
   the gap where ``startup_health.reconcile_persisted_positions_to_live``
   only PRUNES stale local rows but never INSERTS venue-only positions.

Detection
=========
A trade is classified as **manual** when its ``transaction_hash`` is not
already present in ``user_activities`` for the funder wallet. Copy-bot
fills land in that table via the reconciler write-back at terminal
confirmation (see ``services/execution/reconciliation_service.py``), so a
single existence query gives a clean dedup boundary.

Cache + DB writes
=================
On every detected manual trade the service:
  * inserts a row into ``user_activities`` (``bot: False`` in the JSON payload)
  * evicts ``MarketStateStore`` for the affected token so the next quote
    fetch goes through REST/WSS rather than a stale local snapshot
  * upserts every venue position into ``positions`` via ``PositionsRepo``
    AND into ``user_positions`` via ``models.userHistory``

Lifecycle
=========
``UserDataRuntime`` owns lifecycle. Call ``on_wss_authenticated`` /
``on_wss_disconnected`` from the existing WSS callbacks; the service starts
and stops its REST fallback automatically.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from models.userHistory import (
    find_activity_by_tx_hash,
    get_user_activity_model,
    get_user_position_model,
)
from utils.fetchData import fetch_data
from utils.logger import (
    get_logger,
    print_manual_sync_discrepancy,
    print_manual_trade_detected,
    redact_error_detail,
)

if TYPE_CHECKING:
    from cache.market_state_store import MarketStateStore
    from storage.postgres.fills_repo import FillsRepo
    from storage.postgres.orders_repo import OrdersRepo
    from storage.postgres.positions_repo import PositionsRepo

logger = get_logger("ManualSync")

_ACTIVITY_URL = (
    "https://data-api.polymarket.com/activity?user={wallet}&type=TRADE&limit=20"
)
_POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}"

# Size-comparison epsilon. Matches the TS ZERO_THRESHOLD constant.
_SIZE_EPSILON = Decimal("0.001")
_WSS_TRIGGER_REASONS = frozenset({"wss_fill", "wss_order_update"})
_DEFAULT_WSS_SETTLE_DELAY_SECONDS = 10.0
# How far back to look in ``processed_trades`` when deciding whether a
# data-api ``/activity`` row is the bot's OWN copy fill (vs a genuine off-bot
# manual trade). Generous because mislabeling our own fill as "manual" is the
# failure we are closing; missing a real manual trade on the same (token, side)
# within this window is the rare, low-harm trade-off (the position sync still
# reflects it). See classification path in ``_sync_manual_trades``.
_DEFAULT_OWN_TRADE_DEDUP_WINDOW_SECONDS = 300.0
# Compatibility hold-off for injected/legacy repositories that do not expose
# the durable pending-order probe. Production blocks a seed for as long as
# reconciliation owns a non-terminal BUY.
_DEFAULT_OWN_FILL_SEED_HOLDOFF_SECONDS = 45.0


class ManualSyncService:
    """Funder-scoped manual-trade detector + venue→DB position seeder."""

    def __init__(
        self,
        *,
        funder_address: str,
        positions_repo: "PositionsRepo",
        market_store: "MarketStateStore",
        fills_repo: "FillsRepo | None" = None,
        orders_repo: "OrdersRepo | None" = None,
        user_id: str | None = None,
        debounce_seconds: float = 1.0,
        rest_poll_interval_seconds: float = 30.0,
        wss_settle_delay_seconds: float = _DEFAULT_WSS_SETTLE_DELAY_SECONDS,
        own_trade_dedup_window_seconds: float = _DEFAULT_OWN_TRADE_DEDUP_WINDOW_SECONDS,
        own_fill_seed_holdoff_seconds: float = _DEFAULT_OWN_FILL_SEED_HOLDOFF_SECONDS,
        position_drift_service: Any | None = None,
        reservation_service: Any | None = None,
    ) -> None:
        if not funder_address:
            raise ValueError("ManualSyncService requires a non-empty funder_address")
        self._funder = funder_address.lower()
        # user_id is the positions_repo key — must match the casing used by
        # the rest of the runtime (RuntimeUser.user_id = checksum funder).
        # Lowercasing here would create rows the drift check / coordinator
        # cannot find, leaving scopes paused after a successful sync.
        self._user_id = user_id if user_id else funder_address
        self._positions_repo = positions_repo
        self._market_store = market_store
        self._fills_repo = fills_repo
        # orders_repo is consulted to recognise the bot's OWN in-flight copy
        # orders (recorded in ``processed_trades`` at submit time) so they are
        # not misclassified as off-bot manual trades, and so a brand-new
        # position is not seeded from the venue while the reconciler still owns
        # the first incremental write. ``None`` preserves the legacy
        # tx_hash-only behaviour for paper/test contexts.
        self._orders_repo = orders_repo
        self._debounce_seconds = max(0.0, float(debounce_seconds))
        self._rest_poll_interval = max(1.0, float(rest_poll_interval_seconds))
        self._wss_settle_delay = max(0.0, float(wss_settle_delay_seconds))
        self._own_trade_dedup_window = max(0.0, float(own_trade_dedup_window_seconds))
        self._own_fill_seed_holdoff = max(0.0, float(own_fill_seed_holdoff_seconds))
        # Drift handshake: when set, ``_sync_positions_and_tracking`` consults
        # ``was_recently_pruned`` before upserting so an eventually-consistent
        # data-api echo of a venue-positive size cannot resurrect a row that
        # drift just authoritatively deleted. ``None`` keeps the legacy
        # always-upsert behaviour for tests and paper-mode fixtures.
        self._position_drift_service = position_drift_service
        self._reservation_service = reservation_service

        self._lock = asyncio.Lock()
        self._debounce_task: asyncio.Task | None = None
        self._rest_task: asyncio.Task | None = None
        self._stopped = False
        self._wss_alive = False

    # ------------------------------------------------------------------
    # Lifecycle hooks fired by UserDataRuntime
    # ------------------------------------------------------------------
    async def on_wss_authenticated(self) -> None:
        """Stop the REST fallback (WSS is the primary trigger) and run one sync."""
        self._wss_alive = True
        await self._stop_rest_fallback()
        await self.trigger_sync(reason="wss_authenticated")

    async def on_wss_disconnected(self) -> None:
        """Start the REST fallback so manual trades still get detected."""
        self._wss_alive = False
        await self._start_rest_fallback()

    async def stop(self) -> None:
        """Cancel debounce + REST tasks. Idempotent."""
        self._stopped = True
        await self._stop_rest_fallback()
        if self._debounce_task is not None and not self._debounce_task.done():
            self._debounce_task.cancel()
            try:
                await self._debounce_task
            except (asyncio.CancelledError, Exception):
                pass
        self._debounce_task = None

    # ------------------------------------------------------------------
    # Sync entry points
    # ------------------------------------------------------------------
    async def trigger_sync(self, *, reason: str = "wss_event") -> None:
        """Debounced sync trigger. Coalesces bursts of WSS events into one sweep."""
        if self._stopped:
            return
        # Cancel any in-flight debounce window — the new event extends it.
        if self._debounce_task is not None and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = asyncio.create_task(
            self._debounced_run(reason),
            name=f"manual-sync-debounce-{self._user_id}",
        )

    async def _debounced_run(self, reason: str) -> None:
        delay = self._debounce_seconds
        if reason in _WSS_TRIGGER_REASONS:
            # User WSS events are also emitted for bot-origin fills. Run the
            # manual detector after reconciliation has had a chance to write
            # the bot=True activity row; otherwise the bot's own fill can be
            # fetched from /activity and printed as "manual".
            delay = max(delay, self._wss_settle_delay)
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        await self.sync_now(reason=reason)

    async def sync_now(self, *, reason: str, silent_trades: bool = False) -> None:
        """Un-debounced sync. Serialized via ``self._lock``.

        ``silent_trades`` suppresses the per-trade Rich panel — used by
        ``initial_sync`` so a fresh DB doesn't flood the console with
        historical trades that the bot has been blind to until now. The
        trades are still inserted; only the panel is suppressed.
        """
        if self._stopped:
            return
        async with self._lock:
            logger.info("manual_sync_run | reason=%s funder=%s", reason, self._funder)
            try:
                await self._sync_manual_trades(silent=silent_trades)
            except Exception as exc:
                logger.warning(
                    "manual_sync_trades_failed | error=%s",
                    redact_error_detail(exc),
                )
            try:
                await self._sync_positions_and_tracking()
            except Exception as exc:
                logger.warning(
                    "manual_sync_positions_failed | error=%s",
                    redact_error_detail(exc),
                )

    async def initial_sync(self) -> None:
        """Run one full sweep at startup. Seeds positions table from venue.

        Manual-trade panel logging is suppressed for this initial sweep —
        a fresh user_activities table means every historical trade looks
        new and would otherwise flood the console. Subsequent WSS-triggered
        syncs log normally so genuinely new manual trades are surfaced.
        """
        logger.info("manual_sync_startup_begin | funder=%s", self._funder)
        await self.sync_now(reason="startup", silent_trades=True)
        logger.info("manual_sync_startup_complete | funder=%s", self._funder)

    # ------------------------------------------------------------------
    # REST fallback poller
    # ------------------------------------------------------------------
    async def _start_rest_fallback(self) -> None:
        if self._stopped or self._rest_task is not None and not self._rest_task.done():
            return
        logger.info(
            "manual_sync_rest_fallback_started | interval_s=%.1f funder=%s",
            self._rest_poll_interval,
            self._funder,
        )
        self._rest_task = asyncio.create_task(
            self._rest_fallback_loop(),
            name=f"manual-sync-rest-{self._user_id}",
        )

    async def _stop_rest_fallback(self) -> None:
        if self._rest_task is None:
            return
        if not self._rest_task.done():
            self._rest_task.cancel()
            try:
                await self._rest_task
            except (asyncio.CancelledError, Exception):
                pass
        self._rest_task = None
        logger.info("manual_sync_rest_fallback_stopped | funder=%s", self._funder)

    async def _rest_fallback_loop(self) -> None:
        try:
            while not self._stopped and not self._wss_alive:
                await self.sync_now(reason="rest_fallback")
                if self._stopped or self._wss_alive:
                    return
                try:
                    await asyncio.sleep(self._rest_poll_interval)
                except asyncio.CancelledError:
                    return
        except asyncio.CancelledError:
            return

    # ------------------------------------------------------------------
    # Manual trade detection
    # ------------------------------------------------------------------
    async def _sync_manual_trades(self, *, silent: bool = False) -> None:
        url = _ACTIVITY_URL.format(wallet=self._funder)
        activities = await fetch_data(url, silent=True)
        if not isinstance(activities, list) or not activities:
            return
        inserted = 0

        # Process oldest→newest so per-trade size adjustments compound correctly.
        sorted_activities = sorted(
            activities,
            key=lambda a: int(a.get("timestamp") or 0),
        )

        for trade in sorted_activities:
            tx_hash = str(trade.get("transactionHash") or "").strip()
            side = str(trade.get("side") or "").upper()
            if not tx_hash or side not in {"BUY", "SELL"}:
                continue

            try:
                already_tracked = await find_activity_by_tx_hash(self._funder, tx_hash)
            except Exception as exc:
                logger.warning(
                    "manual_sync_dedup_check_failed | tx=%s error=%s",
                    tx_hash,
                    redact_error_detail(exc),
                )
                continue

            if already_tracked:
                continue

            # Bot's OWN copy fill? ``processed_trades`` carries a recent row for
            # this (token, side) written at submit time. Record it as bot=True
            # with the real settlement tx (so later sweeps dedup via tx_hash) and
            # do NOT surface it as a manual trade. Closes the cross-identifier
            # gap: the reconciler write-back keys the leader's tx, never this one.
            own_token_id = str(trade.get("asset") or trade.get("tokenId") or "").strip()
            if own_token_id and await self._is_own_recent_bot_trade(
                token_id=own_token_id,
                side=side,
                max_age_seconds=self._own_trade_dedup_window,
            ):
                try:
                    await self._record_own_bot_activity(trade)
                    logger.info(
                        "manual_sync_own_fill_recognized | token=%s side=%s tx=%s",
                        own_token_id,
                        side,
                        tx_hash,
                    )
                    # Correlation breadcrumb: a recognized own fill while a bot
                    # order for this token is still unconfirmed usually means
                    # the fill settled under an identifier the reconciler is
                    # not tracking (e.g. an ambiguous_timeout order). Operators
                    # can pair this with reconciliation_ambiguous_stalled.
                    if await self._has_pending_bot_trade(
                        token_id=own_token_id, side=side
                    ):
                        logger.warning(
                            "manual_sync_own_fill_with_pending_order | token=%s "
                            "side=%s tx=%s",
                            own_token_id,
                            side,
                            tx_hash,
                        )
                except Exception as exc:
                    # Insert failed → do NOT claim recognition. The processed_trades
                    # row is still fresh, so the next sweep re-detects and retries
                    # (insert_many is idempotent on the tx_hash).
                    logger.warning(
                        "manual_sync_own_activity_insert_failed | tx=%s error=%s",
                        tx_hash,
                        redact_error_detail(exc),
                    )
                continue

            # Manual trade — log + persist + evict cache. ``silent`` skips
            # the per-trade Rich panel; the activity row + cache evict still
            # run so future runs dedup correctly.
            if not silent:
                try:
                    print_manual_trade_detected(trade)
                except Exception:
                    pass

            try:
                await self._insert_manual_activity(trade)
            except Exception as exc:
                logger.warning(
                    "manual_sync_activity_insert_failed | tx=%s error=%s",
                    tx_hash,
                    redact_error_detail(exc),
                )
                continue
            inserted += 1

            token_id = str(trade.get("asset") or trade.get("tokenId") or "").strip()
            if token_id:
                try:
                    self._market_store.evict(token_id)
                except Exception as exc:
                    logger.debug(
                        "manual_sync_cache_evict_failed | token=%s error=%s",
                        token_id,
                        redact_error_detail(exc),
                    )

        if silent and inserted > 0:
            logger.info(
                "manual_sync_startup_seeded | inserted=%d funder=%s",
                inserted,
                self._funder,
            )

    async def _insert_manual_activity(self, trade: dict[str, Any]) -> None:
        payload = dict(trade)
        # Tag explicitly so future readers can distinguish this row from
        # the reconciler write-back (which sets bot=True).
        payload["bot"] = False
        payload["manual_sync_recorded_at"] = int(time.time())
        await get_user_activity_model(self._funder).insert_many([payload])

    async def _is_own_recent_bot_trade(
        self,
        *,
        token_id: str,
        side: str | None,
        max_age_seconds: float,
    ) -> bool:
        """Whether the bot itself recently acted on this ``(token[, side])`` scope.

        Consults ``processed_trades`` via ``orders_repo``. Those rows are written
        at submit time — BEFORE the async reconciler settles the fill — so a
        recent row is the authoritative early signal that a venue holding/fill is
        the bot's OWN copy rather than an off-bot manual trade. Returns ``False``
        when orders_repo is unwired or the probe errors, preserving the legacy
        tx_hash-only behaviour (no regression, never suppresses a real manual
        trade on a probe failure).
        """
        repo = self._orders_repo
        if repo is None or not token_id:
            return False
        probe = getattr(repo, "has_recent_processed_trade", None)
        if not callable(probe):
            return False
        try:
            return bool(
                await probe(
                    user_id=self._user_id,
                    asset=token_id,
                    side=side,
                    max_age_seconds=max_age_seconds,
                )
            )
        except Exception as exc:
            logger.warning(
                "manual_sync_own_trade_probe_failed | token=%s side=%s error=%s",
                token_id,
                side,
                redact_error_detail(exc),
            )
            return False

    async def _has_pending_bot_trade(
        self,
        *,
        token_id: str,
        side: str | None,
    ) -> bool:
        """Return True while reconciliation owns a non-terminal bot order.

        Probe failures are conservative for position seeding: an unavailable
        orders repository must not permit an absolute venue size to race an
        incremental reconciliation write.
        """
        repo = self._orders_repo
        if repo is None or not token_id:
            return False
        probe = getattr(repo, "has_unconfirmed_processed_trade", None)
        if not callable(probe):
            # Compatibility for older injected repositories: retain the
            # bounded guard until they implement the durable pending probe.
            return await self._is_own_recent_bot_trade(
                token_id=token_id,
                side=side,
                max_age_seconds=self._own_fill_seed_holdoff,
            )
        try:
            return bool(
                await probe(
                    user_id=self._user_id,
                    asset=token_id,
                    side=side,
                )
            )
        except Exception as exc:
            logger.warning(
                "manual_sync_pending_trade_probe_failed | token=%s side=%s error=%s",
                token_id,
                side,
                redact_error_detail(exc),
            )
            return True

    async def _record_own_bot_activity(self, trade: dict[str, Any]) -> None:
        """Persist the bot's OWN settlement activity (``bot=True``) keyed by the
        real on-chain ``transactionHash`` from the data-api feed.

        The reconciler write-back stores the LEADER's tx_hash, which never
        matches the data-api settlement tx — so without this row every sweep
        would re-evaluate the bot's own fill. Recording it here with the correct
        tx both attributes it properly and lets ``find_activity_by_tx_hash`` dedup
        every later sweep.
        """
        payload = dict(trade)
        payload["bot"] = True
        payload["manual_sync_recorded_at"] = int(time.time())
        payload["own_detected_via"] = "processed_trades"
        await get_user_activity_model(self._funder).insert_many([payload])

    # ------------------------------------------------------------------
    # Position + tracking sync
    # ------------------------------------------------------------------
    async def _sync_positions_and_tracking(self) -> None:
        url = _POSITIONS_URL.format(wallet=self._funder)
        positions = await fetch_data(url, silent=True)
        if not isinstance(positions, list):
            return

        live = [p for p in positions if _is_live_position(p)]

        # 1. Refresh user_positions (delete-then-insert pattern already used in tradeMonitor).
        try:
            user_pos = get_user_position_model(self._funder)
            await user_pos.delete_many({})
            if live:
                await user_pos.insert_many(live)
        except Exception as exc:
            logger.warning(
                "manual_sync_user_positions_refresh_failed | error=%s",
                redact_error_detail(exc),
            )

        # 2. Upsert canonical positions_repo rows + emit discrepancy log when
        #    the local tracked size disagrees with the venue.
        try:
            persisted = await self._positions_repo.list_open_positions(self._user_id)
        except Exception as exc:
            logger.warning(
                "manual_sync_positions_repo_list_failed | error=%s",
                redact_error_detail(exc),
            )
            persisted = []

        persisted_by_token: dict[str, Any] = {
            str(record.token_id): record for record in persisted
        }
        live_tokens: set[str] = set()
        increased_position_scopes = 0

        for pos in live:
            token_id = str(pos.get("asset") or pos.get("tokenId") or "").strip()
            condition_id = str(
                pos.get("conditionId") or pos.get("condition_id") or ""
            ).strip()
            if not token_id or not condition_id:
                continue
            live_tokens.add(token_id)
            venue_size = _decimal_or_zero(pos.get("size"))
            avg_price = _decimal_or_zero(pos.get("avgPrice"))

            existing = persisted_by_token.get(token_id)
            local_size = existing.size_tokens if existing is not None else Decimal("0")
            venue_size_increased = venue_size > local_size + _SIZE_EPSILON

            # Drift handshake: if PositionDriftService just pruned this scope
            # (venue=0 observed authoritatively), skip the upsert even when
            # data-api still echoes a stale venue-positive size. Without this
            # the eventually-consistent /positions endpoint re-creates the
            # row drift just deleted, and risk-monitor /price probes loop
            # against a retired token (HTTP 404). The holdoff applies only
            # to (re-)creating a row drift wiped — once the row exists and
            # is being kept in sync, the size-mismatch branch still updates
            # it normally.
            if existing is None and self._position_drift_service is not None:
                try:
                    pruned_recently = self._position_drift_service.was_recently_pruned(
                        user_id=self._user_id,
                        condition_id=condition_id,
                        token_id=token_id,
                    )
                except Exception:
                    pruned_recently = False
                if pruned_recently:
                    logger.info(
                        "manual_sync_skipped_recent_prune | "
                        "token=%s cond=%s venue_size=%s",
                        token_id,
                        condition_id,
                        venue_size,
                    )
                    continue

            # Reconciler-first guard: a brand-new long position row is created by
            # a BUY fill. While the bot's own copy BUY is recently in-flight on
            # this token, the reconciler owns the FIRST position write (an
            # incremental delta from cumulative matched size). Seeding the
            # absolute venue size here races that delta apply and double-counts
            # (manual SET 2.55, then reconciler ADD 2.55 -> 5.10). Skip creating
            # the row; reconciliation will create it with correct accounting.
            # Scoped to BUY (the row-creating side) so a recent SELL exit does
            # not suppress seeding a residual. Production holds this guard for
            # the full non-terminal lifetime, including prolonged venue outages.
            if existing is None and await self._has_pending_bot_trade(
                token_id=token_id,
                side="BUY",
            ):
                logger.info(
                    "manual_sync_skipped_pending_bot_fill | "
                    "token=%s cond=%s venue_size=%s",
                    token_id,
                    condition_id,
                    venue_size,
                )
                continue

            if abs(venue_size - local_size) > _SIZE_EPSILON:
                action = (
                    "Upserting venue position into local DB"
                    if existing is None
                    else f"Aligning local size {local_size} → venue size {venue_size}"
                )
                try:
                    print_manual_sync_discrepancy(
                        market=str(pos.get("title") or token_id[:16]),
                        real_size=float(venue_size),
                        tracked_size=float(local_size),
                        action=action,
                    )
                except Exception:
                    pass

            try:
                await self._upsert_position(
                    condition_id=condition_id,
                    token_id=token_id,
                    size_tokens=venue_size,
                    avg_entry_price=avg_price,
                    leader_id=(existing.leader_id if existing is not None else None),
                    expected_size=local_size,
                )
                if venue_size_increased:
                    increased_position_scopes += 1
            except Exception as exc:
                logger.warning(
                    "manual_sync_positions_repo_upsert_failed | token=%s error=%s",
                    token_id,
                    redact_error_detail(exc),
                )

        # Guard: skip deletion when the Data-API response cannot be trusted as a
        # complete picture of the user's holdings.  Two signals:
        #
        # (a) Empty raw response — almost certainly a transient API failure, not
        #     "the user has zero positions".  Wiping all local rows on an empty
        #     response risks runaway risk-monitor probes against retired tokens.
        #
        # (b) Raw response smaller than local tracked count — suggests a paginated
        #     or truncated response.  Skipping avoids deleting a position whose
        #     token was transiently absent from this single snapshot.
        #
        # In both cases we log a warning and defer deletion to the next sync when
        # the API hopefully returns a complete picture.  Zero-value positions
        # returned by the API (currentValue=0) are still deleted because they
        # appear in the raw response — only the count-based guards affect that.
        deleted_position_scopes = 0
        _can_delete: bool
        if not positions:
            if persisted_by_token:
                logger.warning(
                    "manual_sync_deletion_skipped_empty_api | local_count=%d user=%s",
                    len(persisted_by_token),
                    self._user_id,
                )
            _can_delete = False
        elif len(positions) < len(persisted_by_token):
            logger.warning(
                "manual_sync_deletion_skipped_sparse_api | "
                "api_count=%d local_count=%d user=%s",
                len(positions),
                len(persisted_by_token),
                self._user_id,
            )
            _can_delete = False
        else:
            _can_delete = True

        if _can_delete:
            for token_id, record in persisted_by_token.items():
                if token_id in live_tokens:
                    continue
                condition_id = str(getattr(record, "condition_id", "") or "").strip()
                if not token_id or not condition_id:
                    continue
                try:
                    await self._positions_repo.delete_position(
                        self._user_id,
                        condition_id,
                        token_id=token_id,
                    )
                    deleted_position_scopes += 1
                    try:
                        self._market_store.evict(token_id)
                    except Exception as exc:
                        logger.debug(
                            "manual_sync_cache_evict_failed | token=%s error=%s",
                            token_id,
                            redact_error_detail(exc),
                        )
                    logger.info(
                        "manual_sync_deleted_stale_position | user=%s cond=%s token=%s",
                        self._user_id,
                        condition_id,
                        token_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "manual_sync_positions_repo_delete_failed | token=%s error=%s",
                        token_id,
                        redact_error_detail(exc),
                    )

        await self._reap_expired_reservations_after_position_change(
            changed_scopes=increased_position_scopes + deleted_position_scopes,
        )

    async def _upsert_position(
        self,
        *,
        condition_id: str,
        token_id: str,
        size_tokens: Decimal,
        avg_entry_price: Decimal,
        leader_id: str | None,
        expected_size: Decimal | None = None,
    ) -> None:
        # P1-b: route the absolute venue-size align through the locked
        # ``set_position_size`` so a reconciler ``apply_fill`` delta committing
        # inside this read→write window is not clobbered. The repo re-reads the
        # row ``FOR UPDATE`` under the position advisory lock and skips the
        # write when a concurrent delta changed the size since our snapshot
        # (``expected_size``), deferring the align to the next sync cycle.
        written = await self._positions_repo.set_position_size(
            user_id=self._user_id,
            condition_id=condition_id,
            token_id=token_id,
            size_tokens=size_tokens,
            avg_entry_price=avg_entry_price,
            leader_id=leader_id,
            expected_size=expected_size,
        )
        if written is False:
            logger.info(
                "manual_sync_align_deferred_concurrent_change | token=%s cond=%s",
                token_id,
                condition_id,
            )

    async def _reap_expired_reservations_after_position_change(
        self,
        *,
        changed_scopes: int,
    ) -> None:
        if self._reservation_service is None:
            return
        if getattr(self._reservation_service, "enabled", True) is False:
            return
        if changed_scopes <= 0:
            return
        release_expired = getattr(
            self._reservation_service, "release_expired_active", None
        )
        if not callable(release_expired):
            return
        try:
            released_ids = await release_expired()
        except Exception as exc:
            logger.warning(
                "manual_sync_reservation_reap_failed | changed_scopes=%d error=%s",
                changed_scopes,
                redact_error_detail(exc),
            )
            return
        try:
            count = len(released_ids or [])
        except TypeError:
            count = 0
        if count:
            logger.info(
                "manual_sync_reaped_expired_reservations | count=%d changed_scopes=%d user=%s",
                count,
                changed_scopes,
                self._user_id,
            )


def _decimal_or_zero(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _is_live_position(position: dict[str, Any]) -> bool:
    """Return True for positions that should seed canonical local state.

    Polymarket's Data API can keep returning non-zero ``size`` for retired
    tokens while ``currentValue`` is already zero. Startup health/drift treat
    those as not live; manual sync must use the same rule or it resurrects
    rows that drift immediately prunes.
    """
    if _decimal_or_zero(position.get("size")) <= _SIZE_EPSILON:
        return False

    # Older fixtures/API shapes may omit currentValue. In that case keep the
    # legacy size-based behavior rather than dropping otherwise valid data.
    if "currentValue" not in position and "current_value" not in position:
        return True

    current_value = _decimal_or_zero(
        position.get("currentValue") or position.get("current_value")
    )
    return current_value > _SIZE_EPSILON
