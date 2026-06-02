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
    from storage.postgres.positions_repo import PositionsRepo

logger = get_logger("ManualSync")

_ACTIVITY_URL = "https://data-api.polymarket.com/activity?user={wallet}&type=TRADE&limit=20"
_POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}"

# Size-comparison epsilon. Matches the TS ZERO_THRESHOLD constant.
_SIZE_EPSILON = Decimal("0.001")


class ManualSyncService:
    """Funder-scoped manual-trade detector + venue→DB position seeder."""

    def __init__(
        self,
        *,
        funder_address: str,
        positions_repo: "PositionsRepo",
        market_store: "MarketStateStore",
        fills_repo: "FillsRepo | None" = None,
        user_id: str | None = None,
        debounce_seconds: float = 1.0,
        rest_poll_interval_seconds: float = 30.0,
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
        self._debounce_seconds = max(0.0, float(debounce_seconds))
        self._rest_poll_interval = max(1.0, float(rest_poll_interval_seconds))
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
        try:
            await asyncio.sleep(self._debounce_seconds)
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
            self._rest_poll_interval, self._funder,
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
                    tx_hash, redact_error_detail(exc),
                )
                continue

            if already_tracked:
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
                    tx_hash, redact_error_detail(exc),
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
                        token_id, redact_error_detail(exc),
                    )

        if silent and inserted > 0:
            logger.info(
                "manual_sync_startup_seeded | inserted=%d funder=%s",
                inserted, self._funder,
            )

    async def _insert_manual_activity(self, trade: dict[str, Any]) -> None:
        payload = dict(trade)
        # Tag explicitly so future readers can distinguish this row from
        # the reconciler write-back (which sets bot=True).
        payload["bot"] = False
        payload["manual_sync_recorded_at"] = int(time.time())
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
        increased_position_scopes = 0

        for pos in live:
            token_id = str(pos.get("asset") or pos.get("tokenId") or "").strip()
            condition_id = str(pos.get("conditionId") or pos.get("condition_id") or "").strip()
            if not token_id or not condition_id:
                continue
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
                        token_id, condition_id, venue_size,
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
                )
                if venue_size_increased:
                    increased_position_scopes += 1
            except Exception as exc:
                logger.warning(
                    "manual_sync_positions_repo_upsert_failed | token=%s error=%s",
                    token_id, redact_error_detail(exc),
                )

        await self._reap_expired_reservations_after_position_increase(
            increased_scopes=increased_position_scopes,
        )

    async def _upsert_position(
        self,
        *,
        condition_id: str,
        token_id: str,
        size_tokens: Decimal,
        avg_entry_price: Decimal,
        leader_id: str | None,
    ) -> None:
        # Import here so module-level import doesn't pull asyncpg into test
        # contexts that stub out PositionsRepo with a MagicMock.
        from storage.postgres.positions_repo import PositionRecord

        record = PositionRecord(
            user_id=self._user_id,
            condition_id=condition_id,
            token_id=token_id,
            leader_id=leader_id,
            size_tokens=size_tokens,
            avg_entry_price=avg_entry_price,
        )
        await self._positions_repo.upsert_position(record)

    async def _reap_expired_reservations_after_position_increase(
        self,
        *,
        increased_scopes: int,
    ) -> None:
        if self._reservation_service is None:
            return
        if increased_scopes <= 0:
            return
        release_expired = getattr(self._reservation_service, "release_expired_active", None)
        if not callable(release_expired):
            return
        try:
            released_ids = await release_expired()
        except Exception as exc:
            logger.warning(
                "manual_sync_reservation_reap_failed | increased_scopes=%d error=%s",
                increased_scopes,
                redact_error_detail(exc),
            )
            return
        try:
            count = len(released_ids or [])
        except TypeError:
            count = 0
        if count:
            logger.info(
                "manual_sync_reaped_expired_reservations | count=%d increased_scopes=%d user=%s",
                count,
                increased_scopes,
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
