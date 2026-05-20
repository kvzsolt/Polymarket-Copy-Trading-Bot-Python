"""Exchange-side reconciliation service.

Periodically compares local order state against CLOB exchange state
to detect drift:
  - Stale open orders that should have filled or been cancelled
  - Orders that filled on-chain but were not recorded locally
  - Orders we think are open but the exchange no longer knows about

Convergence ownership model (issue #29)
---------------------------------------
This service is intentionally **read-heavy and conservative**. It owns:

* open-order drift detection against the venue
* local-vs-exchange order identity matching for already-submitted venue orders
* optional cancellation of stale open venue orders

It does **not** invent fills or mutate persisted position state. The current
state-mutating convergence path for delayed fills lives in
``orchestrators.trade_pipeline.reconcile_submitted_unconfirmed(...)`` and the
in-memory runtime shadow rebuild lives in ``services.riskMonitor`` via
``rehydrate_position_manager(...)``.

That ownership split matters: this module should classify venue orders against
local durable order records using the **venue order id** (``order_id``), not by
mistaking it for the local submission idempotency key.
"""

from __future__ import annotations

import asyncio
import logging
import time
from config.env import ENV
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReconciliationResult:
    checked_at: float
    open_orders_on_exchange: int
    locally_unknown_orders: int
    stale_orders_cancelled: int
    # Phase 4 — exchange-side reconciliation observations.
    partial_fills_observed: int = 0
    partial_fill_deltas: tuple[tuple[str, str], ...] = ()
    unexpected_fills_observed: int = 0
    unexpected_cancellations: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


_TERMINAL_FILLED_STATUSES = {"matched", "filled", "complete", "settled"}
_TERMINAL_CANCELLED_STATUSES = {"cancelled", "canceled", "expired", "rejected"}


def _safe_decimal(value: Any, default: str = "0") -> Any:
    """Parse a value as Decimal without importing Decimal at module top.

    Kept lightweight so call sites in the reconciler do not pay an import
    every loop iteration.
    """
    from decimal import Decimal

    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _matched_size(order: dict[str, Any]) -> Any:
    """Extract cumulative matched size from a venue order payload.

    Polymarket exposes ``size_matched`` on the CLOB; other shapes seen in the
    wild include ``filled`` and ``matched_size``. We accept all three so the
    reconciler is robust to schema drift.
    """
    for key in ("size_matched", "filled", "matched_size", "filled_size"):
        if key in order and order[key] is not None:
            return _safe_decimal(order[key])
    return _safe_decimal("0")


def _order_status(order: dict[str, Any]) -> str:
    return str(order.get("status") or "").strip().lower()


def _order_age_seconds(order: dict[str, Any], now: float) -> float:
    """Best-effort age from order timestamp fields."""
    for key in ("timestamp", "created_at", "createdAt"):
        ts = order.get(key)
        if ts is not None:
            try:
                ts_f = float(ts)
                # If it looks like milliseconds, convert
                if ts_f > 1e12:
                    ts_f /= 1000.0
                return max(now - ts_f, 0.0)
            except (TypeError, ValueError):
                continue
    return 0.0


# ---------------------------------------------------------------------------
# Reconciliation Service
# ---------------------------------------------------------------------------

class ReconciliationService:
    """Compares local order state against exchange state."""

    def __init__(
        self,
        clob_client: Any,
        orders_repo: Any | None = None,
        stale_threshold_seconds: float | None = None,
        auto_cancel_stale: bool | None = None,
        *,
        config: Any | None = None,
    ):
        self.clob_client = clob_client
        self.orders_repo = orders_repo

        # Priority: explicit arg > AppConfig > ENV fallback
        if stale_threshold_seconds is not None:
            self.stale_threshold = float(stale_threshold_seconds)
        elif config is not None and hasattr(config, 'reconciliation'):
            self.stale_threshold = config.reconciliation.stale_threshold_seconds
        else:
            try:
                self.stale_threshold = float(ENV.get("RECONCILE_STALE_SECONDS", 3600))
            except (TypeError, ValueError):
                self.stale_threshold = 3600.0

        if auto_cancel_stale is not None:
            self.auto_cancel = auto_cancel_stale
        elif config is not None and hasattr(config, 'reconciliation'):
            self.auto_cancel = config.reconciliation.auto_cancel_stale
        else:
            self.auto_cancel = (
                str(ENV.get("RECONCILE_AUTO_CANCEL", "false")).lower() in ("1", "true", "yes")
            )

    @staticmethod
    def _supports_order_management(candidate: Any) -> bool:
        if candidate is None:
            return False
        return all(
            callable(getattr(candidate, method_name, None))
            for method_name in ("get_orders", "cancel_orders")
        )

    def _orders_client(self) -> Any:
        """Return a client object that exposes blocking order-management methods."""
        if self._supports_order_management(self.clob_client):
            return self.clob_client

        nested_client = getattr(self.clob_client, "client", None)
        if self._supports_order_management(nested_client):
            return nested_client

        return self.clob_client

    async def reconcile(self) -> ReconciliationResult:
        """Run one reconciliation pass."""
        now = time.time()
        errors: list[str] = []
        open_count = 0
        unknown_count = 0
        cancelled_count = 0
        partial_fills: list[tuple[str, Any]] = []  # (order_id, delta)
        unexpected_fills = 0
        unexpected_cancellations = 0

        # 1. Fetch open orders from exchange
        exchange_orders: list[dict[str, Any]] = []
        try:
            exchange_orders = await self._fetch_open_orders()
        except Exception as exc:
            msg = f"Failed to fetch open orders: {exc}"
            logger.error(msg)
            errors.append(msg)
            return ReconciliationResult(
                checked_at=now,
                open_orders_on_exchange=0,
                locally_unknown_orders=0,
                stale_orders_cancelled=0,
                errors=errors,
            )

        open_count = len(exchange_orders)

        # 2. Check each order against local state
        stale_order_ids: list[str] = []

        for order in exchange_orders:
            order_id = order.get("id", "")
            if not order_id:
                continue

            # Check if we know about this venue order locally. Reconciliation
            # must match on the venue order id, not the locally generated
            # idempotency key.
            local_record = None
            if self.orders_repo:
                try:
                    if hasattr(self.orders_repo, "get_by_order_id"):
                        local_record = self.orders_repo.get_by_order_id(order_id)
                    elif hasattr(self.orders_repo, "get_by_idempotency_key"):
                        # Backwards-compatible fallback for older mock repos.
                        local_record = self.orders_repo.get_by_idempotency_key(order_id)
                except Exception:
                    pass
                if local_record is None:
                    unknown_count += 1

            # Phase 4 — partial-fill / unexpected-fill / cancellation detection.
            try:
                delta = self._classify_order_state(
                    order=order,
                    local_record=local_record,
                )
            except Exception as exc:  # pragma: no cover — defensive
                logger.error(
                    "reconcile_classify_failed | order_id=%s err=%s",
                    order_id, exc,
                )
                delta = None
            if delta is not None:
                kind = delta.get("kind")
                if kind == "partial_fill":
                    partial_fills.append((order_id, str(delta.get("delta"))))
                elif kind == "unexpected_fill":
                    unexpected_fills += 1
                elif kind == "unexpected_cancellation":
                    unexpected_cancellations += 1

            # Check staleness
            age = _order_age_seconds(order, now)
            if age > self.stale_threshold:
                stale_order_ids.append(order_id)
                logger.warning(
                    "Stale open order detected: %s (age=%.0fs, threshold=%.0fs)",
                    order_id, age, self.stale_threshold,
                )

        # 3. Cancel stale orders if configured
        if self.auto_cancel and stale_order_ids:
            for oid in stale_order_ids:
                try:
                    await self._cancel_order(oid)
                    cancelled_count += 1
                    logger.info("Cancelled stale order: %s", oid)
                except Exception as exc:
                    msg = f"Failed to cancel stale order {oid}: {exc}"
                    logger.error(msg)
                    errors.append(msg)

        if (
            open_count > 0
            or stale_order_ids
            or partial_fills
            or unexpected_fills
            or unexpected_cancellations
        ):
            logger.info(
                "Reconciliation: %d open, %d unknown, %d stale (%d cancelled), "
                "%d partial-fill, %d unexpected-fill, %d unexpected-cancel",
                open_count,
                unknown_count,
                len(stale_order_ids),
                cancelled_count,
                len(partial_fills),
                unexpected_fills,
                unexpected_cancellations,
            )

        return ReconciliationResult(
            checked_at=now,
            open_orders_on_exchange=open_count,
            locally_unknown_orders=unknown_count,
            stale_orders_cancelled=cancelled_count,
            partial_fills_observed=len(partial_fills),
            partial_fill_deltas=tuple(partial_fills),
            unexpected_fills_observed=unexpected_fills,
            unexpected_cancellations=unexpected_cancellations,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Phase 4 — order-state classification + repo update helpers.
    # ------------------------------------------------------------------
    def _classify_order_state(
        self,
        *,
        order: dict[str, Any],
        local_record: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Compare a venue order against the locally persisted record.

        Returns a dict describing the transition the reconciler observed, or
        ``None`` when there is nothing to report. The classification is
        deliberately conservative: it never invents fills the venue did not
        report, and it never overwrites a richer local terminal status.
        """
        venue_status = _order_status(order)
        venue_matched = _matched_size(order)
        order_id = str(order.get("id") or "")
        if not order_id:
            return None

        # Unexpected venue terminal states — local record absent or still
        # thinks the order is in-flight.
        if local_record is None:
            if venue_status in _TERMINAL_FILLED_STATUSES and venue_matched > 0:
                logger.warning(
                    "reconcile_unexpected_fill | order_id=%s status=%s matched=%s "
                    "(no local record)",
                    order_id, venue_status, venue_matched,
                )
                return {"kind": "unexpected_fill", "matched": venue_matched}
            if venue_status in _TERMINAL_CANCELLED_STATUSES:
                logger.info(
                    "reconcile_unexpected_cancellation | order_id=%s status=%s "
                    "(no local record)",
                    order_id, venue_status,
                )
                return {"kind": "unexpected_cancellation"}
            return None

        # Local record exists — see what changed since last observation.
        local_matched = _safe_decimal(local_record.get("matched_total") or "0")

        if venue_status in _TERMINAL_CANCELLED_STATUSES and local_matched <= 0:
            # A cancellation we did not initiate locally.
            local_status = str(local_record.get("status") or "").lower()
            if local_status not in _TERMINAL_CANCELLED_STATUSES:
                logger.info(
                    "reconcile_unexpected_cancellation | order_id=%s "
                    "venue_status=%s local_status=%s",
                    order_id, venue_status, local_status,
                )
                return {"kind": "unexpected_cancellation"}

        if venue_matched > local_matched:
            delta = venue_matched - local_matched
            self._record_partial_fill(
                order_id=order_id,
                local_record=local_record,
                venue_matched=venue_matched,
                venue_status=venue_status,
            )
            logger.info(
                "reconcile_partial_fill | order_id=%s status=%s "
                "local_matched=%s venue_matched=%s delta=%s",
                order_id, venue_status, local_matched, venue_matched, delta,
            )
            return {
                "kind": "partial_fill",
                "local": local_matched,
                "venue": venue_matched,
                "delta": delta,
            }
        return None

    def _record_partial_fill(
        self,
        *,
        order_id: str,
        local_record: dict[str, Any],
        venue_matched: Any,
        venue_status: str,
    ) -> None:
        """Persist the latest cumulative ``matched_total`` from the venue.

        Settlement state-transitions are still owned by the dedicated trade
        pipeline reconciler (see module docstring). This helper just makes
        sure the local row reflects the most recent venue-confirmed cumulative
        size so the next pass can compute a correct delta.
        """
        if not self.orders_repo:
            return
        try:
            if hasattr(self.orders_repo, "upsert_matched_total"):
                self.orders_repo.upsert_matched_total(
                    order_id=order_id,
                    matched=str(venue_matched),
                )
        except Exception as exc:
            logger.error(
                "reconcile_partial_fill_persist_failed | order_id=%s err=%s",
                order_id, exc,
            )

    async def _fetch_open_orders(self) -> list[dict[str, Any]]:
        """Fetch all open orders from the CLOB."""
        loop = asyncio.get_running_loop()
        orders_client = self._orders_client()
        raw = await loop.run_in_executor(
            None, lambda: orders_client.get_orders()
        )
        # py-clob-client-v2 returns a list or paginated dict
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            return raw.get("data", raw.get("orders", []))
        return []

    async def _cancel_order(self, order_id: str) -> None:
        """Cancel a single order on the CLOB."""
        loop = asyncio.get_running_loop()
        orders_client = self._orders_client()
        await loop.run_in_executor(
            None, lambda: orders_client.cancel_orders([order_id])
        )


# ---------------------------------------------------------------------------
# Periodic reconciliation loop (for use from app.py)
# ---------------------------------------------------------------------------

async def reconciliation_loop(
    service: ReconciliationService,
    interval_seconds: float | None = None,
    shutdown_event: asyncio.Event | None = None,
    *,
    config: Any | None = None,
) -> None:
    """Run reconciliation periodically."""
    if interval_seconds is not None:
        interval = float(interval_seconds)
    elif config is not None and hasattr(config, 'polling'):
        interval = float(config.polling.reconcile_interval_seconds)
    else:
        interval = float(
            ENV.get("RECONCILE_INTERVAL_SECONDS", 300) or 300
        )
    logger.info("Reconciliation loop started (interval=%.0fs)", interval)

    while True:
        if shutdown_event and shutdown_event.is_set():
            logger.info("Reconciliation loop: shutdown requested")
            break
        try:
            result = await service.reconcile()
            if result.errors:
                logger.warning("Reconciliation completed with %d errors", len(result.errors))
        except Exception as exc:
            logger.error("Reconciliation loop error: %s", exc)
        await asyncio.sleep(interval)
