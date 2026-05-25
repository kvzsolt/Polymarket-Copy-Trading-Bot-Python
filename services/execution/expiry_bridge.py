"""Stale reservation expiry → reconciliation bridge (Issue #70).

When a reservation's TTL expires, the system must NOT release it
immediately — the corresponding order may still be live on the venue.
Instead, expired reservations trigger reconciliation of the associated
order before any release.

This module provides:

* :func:`check_expired_reservations` — scan for expired active
  reservations and enqueue them for reconciliation instead of releasing.
* :func:`reconcile_then_release` — reconcile an order, then release its
  reservation once the terminal state is known.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Protocol

from domain.order_lifecycle import OrderLifecycleStatus
from services.execution.reservation_service import ReservationService

if TYPE_CHECKING:
    from storage.postgres.orders_repo import OrdersRepo
    from storage.postgres.reservations_repo import ReservationsRepo

logger = logging.getLogger(__name__)


class Reconciler(Protocol):
    async def enqueue(self, order_id: str) -> None: ...


class ExpiryChecker:
    """Check for expired reservations and enqueue them for reconciliation.

    Intended to be called periodically (e.g. every 30s) by the bot's
    main loop or a background task.
    """

    def __init__(
        self,
        reservations_repo: ReservationsRepo,
        orders_repo: OrdersRepo,
        reservation_service: ReservationService,
        reconciler: Reconciler,
    ) -> None:
        self._reservations_repo = reservations_repo
        self._orders_repo = orders_repo
        self._reservation_service = reservation_service
        self._reconciler = reconciler

    async def check_expired_reservations(self) -> int:
        """Scan for expired active reservations and reconcile their orders.

        Returns the number of expired reservations found.
        """
        now_ts = int(datetime.now(timezone.utc).timestamp())
        expired = self._reservations_repo.list_expired_active(now_ts)
        if not expired:
            return 0

        reconciled = 0
        for reservation_row in expired:
            res_id = reservation_row.id if hasattr(reservation_row, "id") else str(reservation_row)
            order_id = getattr(reservation_row, "order_id", None)
            venue_order_id = getattr(reservation_row, "venue_order_id", None)
            reconcile_id = order_id or venue_order_id

            logger.info(
                "reservation.expired id=%s order_id=%s venue_order_id=%s — enqueuing for reconciliation",
                res_id, order_id, venue_order_id,
            )

            if reconcile_id:
                try:
                    await self._reconciler.enqueue(reconcile_id)
                except Exception as exc:
                    logger.warning(
                        "reservation.expiry.enqueue_failed id=%s order_id=%s error=%s",
                        res_id, reconcile_id, exc,
                    )
                    continue
            else:
                # No associated order — safe to release immediately.
                logger.info(
                    "reservation.expiry.no_order id=%s — releasing immediately",
                    res_id,
                )
                self._reservation_service.release_terminal(res_id, "expired_no_order")

            reconciled += 1

        logger.info("reservation.expiry checked=%d reconciled=%d", len(expired), reconciled)
        return reconciled

    async def reconcile_then_release(
        self,
        reservation_id: str,
        order_id: str,
        terminal_status: OrderLifecycleStatus,
        reason: str,
    ) -> None:
        """Release a reservation after reconciliation confirms terminal state.

        This is called by the reconciler once it has determined the final
        state of an order whose reservation had expired.
        """
        logger.info(
            "reservation.release_after_reconcile id=%s order_id=%s "
            "terminal=%s reason=%s",
            reservation_id, order_id, terminal_status.value, reason,
        )
        self._reservation_service.release_terminal(reservation_id, reason)