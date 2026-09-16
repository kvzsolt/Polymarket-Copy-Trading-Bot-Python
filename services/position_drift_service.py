"""Position drift detection service (PR7).

Compares the bot's local view (positions_repo + active reservations) against
the venue's truth (live positions snapshot). Records drift per scope
(``user_id, condition_id, token_id``) and pauses scopes that need operator
review. The ExecutionCoordinator checks ``is_scope_paused`` before every
production submit and fails closed when set.

This service does NOT submit trades. It does NOT mint synthetic fills. It
only:
  - prunes a local zero position when the venue agrees and no reservations
    are in flight (and only with explicit config opt-in);
  - emits structured ``position_drift`` logs for every record;
  - records ``paused_scopes`` for runtime gating.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

from adapters.clock import SystemClock
from domain.position_drift import (
    DriftReport,
    PositionDriftAction,
    PositionDriftRecord,
    PositionDriftStatus,
)
from interfaces.source_transactions import Clock
from utils.logger import get_logger

if TYPE_CHECKING:
    from config.app_config import PositionDriftConfig

_log = get_logger("PositionDriftService")

# Holdoff during which ``was_recently_pruned`` returns True for a scope_key
# whose row was just deleted by ``PRUNE_LOCAL_ZERO_POSITION``. Polymarket's
# data-api ``/positions`` endpoint is eventually consistent and lags the
# live order book by tens of seconds, so manual_sync routinely sees a
# resurrected size after drift has authoritatively pruned the position.
# Runtime config can raise this for stale markets where the Data API lags
# longer than the default convergence window.
DRIFT_PRUNE_HOLDOFF_SECONDS = 60.0


LivePositionsFetcher = Callable[[str], Awaitable[list[dict[str, Any]]]]


class DriftPosition(Protocol):
    """Expose immutable inventory identity and size for a drift comparison."""

    @property
    def condition_id(self) -> str:
        """Return the held market's condition identity."""
        ...

    @property
    def token_id(self) -> str:
        """Return the raw outcome token identity."""
        ...

    @property
    def size_tokens(self) -> Decimal:
        """Return the exact number of held shares."""
        ...


class DriftInventoryRepository(Protocol):
    """Read complete holdings and delete only inventory proven unchanged."""

    async def list_held_positions(self, user_id: str) -> Sequence[DriftPosition]:
        """Return held inventory including settled and unknown lifecycle rows."""
        ...

    async def delete_position_if_unchanged(
        self,
        user_id: str,
        condition_id: str,
        *,
        token_id: str,
        expected_size: Decimal,
    ) -> bool:
        """Delete under lock only if size matches and no order owns the scope."""
        ...


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _venue_size_for(positions: list[dict[str, Any]], token_id: str) -> Decimal:
    """Extract the venue position size for a specific token id."""
    for row in positions or []:
        for key in ("asset", "asset_id", "assetId", "token_id", "tokenId", "clobTokenId"):
            value = row.get(key)
            if value and str(value) == token_id:
                size = row.get("size") or row.get("size_tokens") or row.get("position") or 0
                return _decimal_or_zero(size)
    return Decimal("0")


def _all_venue_tokens(positions: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """Return set of (token_id, condition_id_or_empty) tuples from venue rows."""
    seen: set[tuple[str, str]] = set()
    for row in positions or []:
        token_id = None
        for key in ("asset", "asset_id", "assetId", "token_id", "tokenId", "clobTokenId"):
            value = row.get(key)
            if value:
                token_id = str(value)
                break
        if not token_id:
            continue
        cond = str(row.get("conditionId") or row.get("condition_id") or row.get("eventId") or "")
        seen.add((token_id, cond))
    return seen


class PositionDriftService:
    """Proactive position-drift detector.

    One instance per user runtime. Holds the set of currently-paused scopes
    and the most recent ``PositionDriftRecord`` per scope.
    """

    def __init__(
        self,
        *,
        positions_repo: DriftInventoryRepository,
        reservations_repo: Any | None,
        live_positions_fetcher: LivePositionsFetcher,
        config: "PositionDriftConfig",
        audit_logger: Any | None = None,
        clock: Clock | None = None,
    ) -> None:
        """Inject inventory, venue, ownership, and time boundaries."""
        self._positions_repo = positions_repo
        self._reservations_repo = reservations_repo
        self._live_positions_fetcher = live_positions_fetcher
        self._config = config
        self._audit_logger = audit_logger
        self._clock = clock or SystemClock()
        # user_id -> set of scope_key
        self._paused_scopes: dict[str, set[str]] = {}
        # scope_key -> latest PositionDriftRecord
        self._latest: dict[str, PositionDriftRecord] = {}
        # scope_key -> wall timestamp of the last PRUNE_LOCAL_ZERO_POSITION
        # decision. Consulted by manual_sync via ``was_recently_pruned`` so an
        # eventually-consistent data-api echo cannot resurrect the row inside
        # ``DRIFT_PRUNE_HOLDOFF_SECONDS``.
        self._prune_timestamps: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def scan_user(self, *, user_id: str, wallet: str) -> DriftReport:
        """Run a full drift sweep using local state captured before venue I/O."""
        try:
            local_positions = tuple(await self._positions_repo.list_held_positions(user_id))
        except Exception as exc:
            _log.warning("drift_scan_state_unavailable error_type=%s", type(exc).__name__)
            return self._unavailable_report(
                user_id=user_id,
                reason=f"inventory reconciliation unavailable: {type(exc).__name__}",
            )

        try:
            venue_positions = await self._live_positions_fetcher(wallet)
        except Exception as exc:
            _log.warning("drift_scan_fetch_failed | error_type=%s", type(exc).__name__)
            return self._unavailable_report(
                user_id=user_id,
                reason=f"venue snapshot fetch failed: {type(exc).__name__}",
            )

        try:
            return await self._scan_snapshot(
                user_id=user_id,
                local_positions=local_positions,
                venue_positions=venue_positions,
            )
        except Exception as exc:
            _log.warning("drift_scan_state_unavailable error_type=%s", type(exc).__name__)
            return self._unavailable_report(
                user_id=user_id,
                reason=f"inventory reconciliation unavailable: {type(exc).__name__}",
            )

    def _unavailable_report(self, *, user_id: str, reason: str) -> DriftReport:
        """Keep the entire account paused when any required evidence is unavailable."""
        record = PositionDriftRecord(
            user_id=user_id,
            condition_id="",
            token_id="",
            local_size=Decimal("0"),
            venue_size=Decimal("0"),
            drift_size=Decimal("0"),
            status=PositionDriftStatus.UNRESOLVED,
            action=PositionDriftAction.REQUIRE_OPERATOR_REVIEW,
            reason=reason,
        )
        self._pause(user_id, record.scope_key)
        self._latest[record.scope_key] = record
        return DriftReport(
            user_id=user_id,
            clean=False,
            records=(record,),
            generated_at=int(self._clock.now().timestamp()),
            paused_scopes=tuple(sorted(self._paused_scopes.get(user_id, set()))),
        )

    async def _scan_snapshot(
        self,
        *,
        user_id: str,
        local_positions: Sequence[DriftPosition],
        venue_positions: list[dict[str, Any]],
    ) -> DriftReport:
        records: list[PositionDriftRecord] = []
        seen_scopes: set[str] = set()

        for pos in local_positions:
            condition_id = str(getattr(pos, "condition_id", "") or "")
            token_id = str(getattr(pos, "token_id", "") or "")
            local_size = _decimal_or_zero(getattr(pos, "size_tokens", 0))
            venue_size = _venue_size_for(venue_positions, token_id)
            record = await self._classify(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=local_size,
                venue_size=venue_size,
            )
            records.append(record)
            seen_scopes.add(record.scope_key)
            self._latest[record.scope_key] = record

        # Venue rows with no local position.
        for token_id, condition_id in _all_venue_tokens(venue_positions):
            scope_key = f"{user_id}|{condition_id}|{token_id}"
            if scope_key in seen_scopes:
                continue
            venue_size = _venue_size_for(venue_positions, token_id)
            if venue_size <= self._config.max_size_epsilon:
                continue
            record = await self._classify(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=Decimal("0"),
                venue_size=venue_size,
            )
            records.append(record)
            seen_scopes.add(scope_key)
            self._latest[scope_key] = record

        snapshot_scope = f"{user_id}||"
        for scope_key in sorted(self._paused_scopes.get(user_id, set()) - seen_scopes):
            if scope_key == snapshot_scope:
                continue
            previous = self._latest[scope_key]
            record = await self._classify(
                user_id=user_id,
                condition_id=previous.condition_id,
                token_id=previous.token_id,
                local_size=Decimal("0"),
                venue_size=_venue_size_for(venue_positions, previous.token_id),
            )
            records.append(record)
            self._latest[scope_key] = record

        for record in records:
            if record.is_clean:
                self._paused_scopes.get(user_id, set()).discard(record.scope_key)
        self._paused_scopes.get(user_id, set()).discard(snapshot_scope)
        self._latest.pop(snapshot_scope, None)
        clean = all(r.is_clean for r in records) and not self._paused_scopes.get(user_id)
        return DriftReport(
            user_id=user_id,
            clean=clean,
            records=tuple(records),
            generated_at=int(self._clock.now().timestamp()),
            paused_scopes=tuple(sorted(self._paused_scopes.get(user_id, set()))),
        )

    def paused_scopes(self, user_id: str) -> set[str]:
        """Return a copy of the account's unresolved trading scopes."""
        return set(self._paused_scopes.get(user_id, set()))

    def is_scope_paused(self, *, user_id: str, condition_id: str, token_id: str) -> bool:
        """Return whether account or outcome uncertainty prevents submission."""
        scope_key = f"{user_id}|{condition_id}|{token_id}"
        paused = self._paused_scopes.get(user_id, set())
        return f"{user_id}||" in paused or scope_key in paused

    def mark_resolved(
        self, *, user_id: str, condition_id: str, token_id: str, reason: str = "operator_resolved"
    ) -> None:
        """Clear an explicitly operator-resolved scope without changing holdings."""
        scope_key = f"{user_id}|{condition_id}|{token_id}"
        scopes = self._paused_scopes.get(user_id)
        if scopes is not None:
            scopes.discard(scope_key)
        self._latest.pop(scope_key, None)
        _log.info(
            "drift_scope_resolved | user=%s cond=%s token=%s reason=%s",
            user_id,
            condition_id,
            token_id,
            reason,
        )

    def latest_record(self, scope_key: str) -> PositionDriftRecord | None:
        """Return the most recent observed outcome for a scope."""
        return self._latest.get(scope_key)

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------
    async def _classify(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        local_size: Decimal,
        venue_size: Decimal,
    ) -> PositionDriftRecord:
        epsilon = self._config.max_size_epsilon
        drift = venue_size - local_size
        reservations = await self._active_reservations(
            user_id=user_id, condition_id=condition_id, token_id=token_id
        )
        reservation_ids = tuple(r.id for r in reservations)
        any_active = bool(reservations)

        # Clean — sizes match within epsilon and no orphaned reservation.
        if abs(drift) <= epsilon and (local_size > 0 or venue_size > 0 or not any_active):
            return self._record(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=local_size,
                venue_size=venue_size,
                drift_size=drift,
                status=PositionDriftStatus.CLEAN,
                action=PositionDriftAction.NO_ACTION,
                reason="local and venue agree",
                reservation_ids=reservation_ids,
            )

        # Local present, venue zero.
        if local_size > epsilon and venue_size <= epsilon:
            if any_active:
                return self._record_and_pause(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    local_size=local_size,
                    venue_size=venue_size,
                    drift_size=drift,
                    status=PositionDriftStatus.UNRESOLVED,
                    action=PositionDriftAction.PAUSE_SCOPE,
                    reason="local position exists but venue reports zero with active reservation",
                    reservation_ids=reservation_ids,
                )
            if self._config.allow_auto_prune_zero:
                deleted = await self._positions_repo.delete_position_if_unchanged(
                    user_id,
                    condition_id,
                    token_id=token_id,
                    expected_size=local_size,
                )
                if deleted is not True:
                    return self._record_and_pause(
                        user_id=user_id,
                        condition_id=condition_id,
                        token_id=token_id,
                        local_size=local_size,
                        venue_size=venue_size,
                        drift_size=drift,
                        status=PositionDriftStatus.UNRESOLVED,
                        action=PositionDriftAction.PAUSE_SCOPE,
                        reason="position or order ownership changed before guarded prune",
                        reservation_ids=reservation_ids,
                    )
                return self._record(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    local_size=local_size,
                    venue_size=venue_size,
                    drift_size=drift,
                    status=PositionDriftStatus.RECONCILED,
                    action=PositionDriftAction.PRUNE_LOCAL_ZERO_POSITION,
                    reason="local position pruned; venue reports zero, no active orders",
                    reservation_ids=reservation_ids,
                )
            return self._record_and_pause(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=local_size,
                venue_size=venue_size,
                drift_size=drift,
                status=PositionDriftStatus.LOCAL_MISSING_VENUE_POSITION,
                action=PositionDriftAction.PAUSE_SCOPE,
                reason="local position exists but venue reports zero; auto-prune disabled",
                reservation_ids=reservation_ids,
            )

        # Venue present, local missing.
        if venue_size > epsilon and local_size <= epsilon:
            if self._config.allow_external_position_tracking:
                return self._record(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    local_size=local_size,
                    venue_size=venue_size,
                    drift_size=drift,
                    status=PositionDriftStatus.VENUE_MISSING_LOCAL_POSITION,
                    action=PositionDriftAction.UPDATE_LOCAL_READONLY,
                    reason="venue position observed; tracked read-only",
                    reservation_ids=reservation_ids,
                )
            return self._record_and_pause(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=local_size,
                venue_size=venue_size,
                drift_size=drift,
                status=PositionDriftStatus.VENUE_MISSING_LOCAL_POSITION,
                action=PositionDriftAction.PAUSE_SCOPE,
                reason="venue holds position but local has none; external tracking disabled",
                reservation_ids=reservation_ids,
            )

        # Size mismatch above epsilon.
        if drift > epsilon:  # venue > local
            if self._config.allow_external_position_tracking:
                return self._record(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    local_size=local_size,
                    venue_size=venue_size,
                    drift_size=drift,
                    status=PositionDriftStatus.SIZE_MISMATCH_POSITIVE,
                    action=PositionDriftAction.UPDATE_LOCAL_READONLY,
                    reason="venue size exceeds local; tracked read-only",
                    reservation_ids=reservation_ids,
                )
            return self._record_and_pause(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=local_size,
                venue_size=venue_size,
                drift_size=drift,
                status=PositionDriftStatus.SIZE_MISMATCH_POSITIVE,
                action=PositionDriftAction.PAUSE_SCOPE,
                reason="venue size exceeds local; external tracking disabled",
                reservation_ids=reservation_ids,
            )

        if drift < -epsilon:  # local > venue (negative drift)
            return self._record_and_pause(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                local_size=local_size,
                venue_size=venue_size,
                drift_size=drift,
                status=PositionDriftStatus.SIZE_MISMATCH_NEGATIVE,
                action=PositionDriftAction.REQUIRE_OPERATOR_REVIEW,
                reason="local size exceeds venue; possible off-bot SELL",
                reservation_ids=reservation_ids,
            )

        return self._record_and_pause(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            local_size=local_size,
            venue_size=venue_size,
            drift_size=drift,
            status=PositionDriftStatus.RESERVATION_ORPHANED,
            action=PositionDriftAction.REQUIRE_OPERATOR_REVIEW,
            reason="reservation remains active without local or venue inventory",
            reservation_ids=reservation_ids,
        )

    async def _active_reservations(
        self, *, user_id: str, condition_id: str, token_id: str
    ) -> list[Any]:
        if self._reservations_repo is None:
            return []
        try:
            result = await self._reservations_repo.list_active_for_scope(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
            )
            return list(result or [])
        except Exception:
            raise RuntimeError("Reservation ownership is unavailable") from None

    def _record(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        local_size: Decimal,
        venue_size: Decimal,
        drift_size: Decimal,
        status: PositionDriftStatus,
        action: PositionDriftAction,
        reason: str,
        reservation_ids: tuple[str, ...],
    ) -> PositionDriftRecord:
        record = PositionDriftRecord(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            local_size=local_size,
            venue_size=venue_size,
            drift_size=drift_size,
            status=status,
            action=action,
            reason=reason,
            reservation_ids=reservation_ids,
        )
        # Stamp the prune timestamp so manual_sync can refuse to re-upsert a
        # row that drift just authoritatively deleted. We track the action,
        # not the status, because PRUNE_LOCAL_ZERO_POSITION is the only path
        # that mutates positions_repo — other LOCAL_MISSING_VENUE_POSITION
        # branches either pause or update read-only and shouldn't suppress
        # manual_sync.
        if action == PositionDriftAction.PRUNE_LOCAL_ZERO_POSITION:
            self._prune_timestamps[record.scope_key] = self._clock.now().timestamp()
        self._emit(record)
        return record

    def _record_and_pause(self, **kwargs: Any) -> PositionDriftRecord:
        record = self._record(**kwargs)
        if self._config.pause_scope_on_unresolved:
            self._pause(record.user_id, record.scope_key)
        return record

    # ------------------------------------------------------------------
    # Prune-holdoff handshake
    # ------------------------------------------------------------------
    def was_recently_pruned(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Return whether this scope was pruned inside the holdoff window.

        ``ManualSyncService`` consults this before upserting a
        venue-positive row so the eventually-consistent data-api can't
        resurrect a position drift just deleted. ``now`` overrides
        the injected clock for compatibility with explicit-time callers.
        """
        scope_key = f"{user_id}|{condition_id}|{token_id}"
        ts = self._prune_timestamps.get(scope_key)
        if ts is None:
            return False
        current = now if now is not None else self._clock.now().timestamp()
        ttl = (
            ttl_seconds
            if ttl_seconds is not None
            else getattr(
                self._config,
                "prune_holdoff_seconds",
                DRIFT_PRUNE_HOLDOFF_SECONDS,
            )
        )
        if (current - ts) > max(0.0, float(ttl)):
            # TTL elapsed — drop the stamp so memory stays bounded.
            self._prune_timestamps.pop(scope_key, None)
            return False
        return True

    def clear_prune_record(self, *, user_id: str, condition_id: str, token_id: str) -> None:
        """Manual override to drop the holdoff for a scope. Used by tests
        and by operators who legitimately reopened a position via an
        off-bot trade.
        """
        scope_key = f"{user_id}|{condition_id}|{token_id}"
        self._prune_timestamps.pop(scope_key, None)

    def _pause(self, user_id: str, scope_key: str) -> None:
        self._paused_scopes.setdefault(user_id, set()).add(scope_key)

    def _emit(self, record: PositionDriftRecord) -> None:
        try:
            from utils.logger import log_position_drift

            log_position_drift(
                _log,
                status=record.status.value,
                action=record.action.value,
                user_id=record.user_id,
                condition_id=record.condition_id,
                asset=record.token_id,
                local_size=float(record.local_size),
                venue_size=float(record.venue_size),
                drift_size=float(record.drift_size),
                reason=record.reason,
                reservation_ids=list(record.reservation_ids),
            )
        except Exception:
            _log.debug("drift_log_emit_failed", exc_info=True)
        if self._audit_logger is not None:
            try:
                self._audit_logger.log_event(
                    "position_drift",
                    {
                        "user_id": record.user_id,
                        "condition_id": record.condition_id,
                        "token_id": record.token_id,
                        "local_size": str(record.local_size),
                        "venue_size": str(record.venue_size),
                        "drift_size": str(record.drift_size),
                        "status": record.status.value,
                        "action": record.action.value,
                        "reason": record.reason,
                    },
                )
            except Exception:
                pass
