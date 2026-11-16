"""``confirmed+mempool_reserved`` reservation state machine (15s TTL + Ghost-Fill expiry).

Coordinates provisional pending ``matchOrders`` sightings with authoritative
user-filtered ``OrderFilled`` log confirmations over the single shared Alchemy
WebSocket connection:
  * Pending ``matchOrders`` creates a ``MEMPOOL_RESERVED`` entry keyed by
    deterministic order/fill fingerprint (``orderHash`` or
    ``(leader_wallet, token_id, side, size_micro, usdc_micro)``) with a 15s TTL
    without dispatching an order.
  * Confirmed ``OrderFilled`` log promotes the matching ``MEMPOOL_RESERVED``
    entry to ``CONFIRMED``, carries forward any pre-warmed ``condition_id``, and
    dispatches exactly once.
  * Stale reservations from reverted or dropped mempool transactions ("Ghost
    Fills") expire cleanly after ``ttl_seconds``.
"""

from __future__ import annotations

import dataclasses
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from pmcopy.services.onchain_detection.trade_detection_models import OnchainTradeSignal

_MICRO_SCALE = Decimal("1000000")
DEFAULT_MEMPOOL_RESERVATION_TTL_SECONDS = 15.0
DEFAULT_MAX_RESERVATIONS = 16_384


class ReservationStatus(StrEnum):
    """Lifecycle states of a mempool-reserved leader fill."""

    MEMPOOL_RESERVED = "MEMPOOL_RESERVED"
    CONFIRMED = "CONFIRMED"
    EXPIRED_GHOST_FILL = "EXPIRED_GHOST_FILL"


@dataclass(frozen=True, slots=True)
class FillFingerprint:
    """Deterministic economic fill fingerprint ``(user, tokenId, side, size, usdc)``."""

    leader_wallet: str
    token_id: str
    side: str
    size_micro: int
    usdc_micro: int

    @classmethod
    def from_signal(cls, signal: OnchainTradeSignal) -> "FillFingerprint":
        size_micro = int((signal.size * _MICRO_SCALE).to_integral_value())
        usdc_micro = int((signal.usdc_size * _MICRO_SCALE).to_integral_value())
        return cls(
            leader_wallet=signal.leader_wallet.strip().lower(),
            token_id=str(signal.token_id),
            side=signal.side.strip().upper(),
            size_micro=size_micro,
            usdc_micro=usdc_micro,
        )


@dataclass(slots=True)
class MempoolReservation:
    """Tracked state for one pending ``matchOrders`` reservation."""

    reservation_id: int
    fingerprint: FillFingerprint
    order_hash: str | None
    tx_hash: str
    status: ReservationStatus
    reserved_at_mono: float
    expires_at_mono: float
    pending_signal: OnchainTradeSignal
    prewarmed_condition_id: str = ""
    confirmed_at_mono: float | None = None
    confirmed_tx_hash: str | None = None
    confirmed_log_index: int | None = None


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    """Outcome of evaluating an authoritative ``OrderFilled`` log."""

    should_dispatch: bool
    promoted_from_mempool: bool
    signal: OnchainTradeSignal
    reservation: MempoolReservation | None = None


class MempoolReservationStateMachine:
    """Bounded state machine promoting ``MEMPOOL_RESERVED`` fills to ``CONFIRMED``."""

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_MEMPOOL_RESERVATION_TTL_SECONDS,
        max_entries: int = DEFAULT_MAX_RESERVATIONS,
        clock: Callable[[], float] = time.monotonic,
        on_ghost_fill_expired: Callable[[MempoolReservation], None] | None = None,
    ) -> None:
        if ttl_seconds <= 0.0:
            raise ValueError("ttl_seconds must be > 0")
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        self._ttl_seconds = float(ttl_seconds)
        self._max_entries = int(max_entries)
        self._clock = clock
        self._on_ghost_fill_expired = on_ghost_fill_expired

        self._next_id = 0
        self._active_by_id: OrderedDict[int, MempoolReservation] = OrderedDict()
        self._id_by_order_hash: dict[str, int] = {}
        self._id_by_fingerprint: dict[FillFingerprint, int] = {}
        self._id_by_tx_leg: dict[tuple[str, str, str, str], int] = {}

        self._confirmed_logs: OrderedDict[tuple[str, int], None] = OrderedDict()
        self._confirmed_tx_legs: OrderedDict[tuple[str, str, str, str], None] = OrderedDict()
        self._promoted_count = 0
        self._direct_confirmed_count = 0
        self._expired_ghost_fill_count = 0

    @property
    def active_reservation_count(self) -> int:
        """Return number of currently active ``MEMPOOL_RESERVED`` entries."""
        self.expire_stale()
        return len(self._active_by_id)

    @property
    def promoted_count(self) -> int:
        """Return cumulative ``MEMPOOL_RESERVED -> CONFIRMED`` promotions."""
        return self._promoted_count

    @property
    def direct_confirmed_count(self) -> int:
        """Return confirmed fills that arrived without prior mempool sighting."""
        return self._direct_confirmed_count

    @property
    def expired_ghost_fill_count(self) -> int:
        """Return cumulative expired Ghost-Fill reservations."""
        return self._expired_ghost_fill_count

    def reserve_pending(
        self, pending_signal: OnchainTradeSignal
    ) -> tuple[MempoolReservation | None, bool]:
        """Create or return an existing ``MEMPOOL_RESERVED`` entry (never dispatches)."""
        now = self._clock()
        self._expire_stale_at(now)

        fp = FillFingerprint.from_signal(pending_signal)
        tx_hash = pending_signal.tx_hash.strip().lower()
        tx_leg = (tx_hash, fp.leader_wallet, fp.token_id, fp.side)
        order_hash = (
            pending_signal.order_hash.strip().lower() if pending_signal.order_hash else None
        )

        # If the confirmed log already arrived before this delayed mempool frame, ignore.
        if tx_hash and tx_leg in self._confirmed_tx_legs:
            return None, False

        existing = self._lookup_active(order_hash=order_hash, fingerprint=fp, tx_leg=tx_leg)
        if existing is not None:
            if not existing.prewarmed_condition_id and pending_signal.condition_id:
                existing.prewarmed_condition_id = pending_signal.condition_id
            return existing, False

        self._next_id += 1
        entry = MempoolReservation(
            reservation_id=self._next_id,
            fingerprint=fp,
            order_hash=order_hash,
            tx_hash=tx_hash,
            status=ReservationStatus.MEMPOOL_RESERVED,
            reserved_at_mono=now,
            expires_at_mono=now + self._ttl_seconds,
            pending_signal=pending_signal,
            prewarmed_condition_id=pending_signal.condition_id,
        )
        self._active_by_id[entry.reservation_id] = entry
        if order_hash:
            self._id_by_order_hash[order_hash] = entry.reservation_id
        self._id_by_fingerprint[fp] = entry.reservation_id
        if tx_hash:
            self._id_by_tx_leg[tx_leg] = entry.reservation_id

        while len(self._active_by_id) > self._max_entries:
            oldest_id, oldest_entry = self._active_by_id.popitem(last=False)
            self._remove_indexes(oldest_id, oldest_entry)

        return entry, True

    def promote_confirmed(self, confirmed_signal: OnchainTradeSignal) -> PromotionDecision:
        """Promote matching ``MEMPOOL_RESERVED`` entry to ``CONFIRMED`` at-most-once."""
        now = self._clock()
        self._expire_stale_at(now)

        tx_hash = confirmed_signal.tx_hash.strip().lower()
        log_index = int(confirmed_signal.log_index or 0)
        log_key = (tx_hash, log_index)
        if tx_hash and log_key in self._confirmed_logs:
            self._confirmed_logs.move_to_end(log_key)
            return PromotionDecision(
                should_dispatch=False,
                promoted_from_mempool=False,
                signal=confirmed_signal,
                reservation=None,
            )

        if tx_hash:
            self._confirmed_logs[log_key] = None
            while len(self._confirmed_logs) > self._max_entries:
                self._confirmed_logs.popitem(last=False)

        fp = FillFingerprint.from_signal(confirmed_signal)
        tx_leg = (tx_hash, fp.leader_wallet, fp.token_id, fp.side)
        if tx_hash:
            self._confirmed_tx_legs[tx_leg] = None
            while len(self._confirmed_tx_legs) > self._max_entries:
                self._confirmed_tx_legs.popitem(last=False)

        order_hash = (
            confirmed_signal.order_hash.strip().lower() if confirmed_signal.order_hash else None
        )
        matched = self._lookup_active(order_hash=order_hash, fingerprint=fp, tx_leg=tx_leg)
        if matched is not None:
            self._active_by_id.pop(matched.reservation_id, None)
            self._remove_indexes(matched.reservation_id, matched)
            matched.status = ReservationStatus.CONFIRMED
            matched.confirmed_at_mono = now
            matched.confirmed_tx_hash = tx_hash
            matched.confirmed_log_index = log_index
            enriched = confirmed_signal
            if not enriched.condition_id and matched.prewarmed_condition_id:
                enriched = dataclasses.replace(
                    enriched,
                    condition_id=matched.prewarmed_condition_id,
                )
            self._promoted_count += 1
            return PromotionDecision(
                should_dispatch=True,
                promoted_from_mempool=True,
                signal=enriched,
                reservation=matched,
            )

        self._direct_confirmed_count += 1
        return PromotionDecision(
            should_dispatch=True,
            promoted_from_mempool=False,
            signal=confirmed_signal,
            reservation=None,
        )

    def expire_stale(self) -> tuple[MempoolReservation, ...]:
        """Expire reservations past ``ttl_seconds`` as Ghost Fills."""
        return self._expire_stale_at(self._clock())

    def expire_all_active(self) -> tuple[MempoolReservation, ...]:
        """Cleanly expire all outstanding ``MEMPOOL_RESERVED`` entries (e.g. on downgrade)."""
        expired: list[MempoolReservation] = []
        while self._active_by_id:
            res_id, entry = self._active_by_id.popitem(last=False)
            self._remove_indexes(res_id, entry)
            entry.status = ReservationStatus.EXPIRED_GHOST_FILL
            self._expired_ghost_fill_count += 1
            expired.append(entry)
            if self._on_ghost_fill_expired is not None:
                self._on_ghost_fill_expired(entry)
        return tuple(expired)

    def _lookup_active(
        self,
        *,
        order_hash: str | None,
        fingerprint: FillFingerprint,
        tx_leg: tuple[str, str, str, str],
    ) -> MempoolReservation | None:
        if order_hash and order_hash in self._id_by_order_hash:
            found = self._active_by_id.get(self._id_by_order_hash[order_hash])
            if found is not None:
                return found
        if fingerprint in self._id_by_fingerprint:
            found = self._active_by_id.get(self._id_by_fingerprint[fingerprint])
            if found is not None:
                return found
        if tx_leg[0] and tx_leg in self._id_by_tx_leg:
            found = self._active_by_id.get(self._id_by_tx_leg[tx_leg])
            if found is not None:
                return found
        return None

    def _expire_stale_at(self, now: float) -> tuple[MempoolReservation, ...]:
        expired: list[MempoolReservation] = []
        while self._active_by_id:
            first_id, first_entry = next(iter(self._active_by_id.items()))
            if now < first_entry.expires_at_mono:
                break
            self._active_by_id.pop(first_id, None)
            self._remove_indexes(first_id, first_entry)
            first_entry.status = ReservationStatus.EXPIRED_GHOST_FILL
            self._expired_ghost_fill_count += 1
            expired.append(first_entry)
            if self._on_ghost_fill_expired is not None:
                self._on_ghost_fill_expired(first_entry)
        return tuple(expired)

    def _remove_indexes(self, res_id: int, entry: MempoolReservation) -> None:
        if entry.order_hash and self._id_by_order_hash.get(entry.order_hash) == res_id:
            self._id_by_order_hash.pop(entry.order_hash, None)
        if self._id_by_fingerprint.get(entry.fingerprint) == res_id:
            self._id_by_fingerprint.pop(entry.fingerprint, None)
        if entry.tx_hash:
            tx_leg = (
                entry.tx_hash,
                entry.fingerprint.leader_wallet,
                entry.fingerprint.token_id,
                entry.fingerprint.side,
            )
            if self._id_by_tx_leg.get(tx_leg) == res_id:
                self._id_by_tx_leg.pop(tx_leg, None)


__all__ = [
    "DEFAULT_MAX_RESERVATIONS",
    "DEFAULT_MEMPOOL_RESERVATION_TTL_SECONDS",
    "FillFingerprint",
    "MempoolReservation",
    "MempoolReservationStateMachine",
    "PromotionDecision",
    "ReservationStatus",
]
