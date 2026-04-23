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
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


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
    ):
        self.clob_client = clob_client
        self.orders_repo = orders_repo

        if stale_threshold_seconds is not None:
            self.stale_threshold = float(stale_threshold_seconds)
        else:
            try:
                self.stale_threshold = float(ENV.get("RECONCILE_STALE_SECONDS", 3600))
            except (TypeError, ValueError):
                self.stale_threshold = 3600.0
        self.auto_cancel = auto_cancel_stale if auto_cancel_stale is not None else (
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

        if open_count > 0 or stale_order_ids:
            logger.info(
                "Reconciliation: %d open on exchange, %d locally unknown, "
                "%d stale (%d cancelled)",
                open_count, unknown_count, len(stale_order_ids), cancelled_count,
            )

        return ReconciliationResult(
            checked_at=now,
            open_orders_on_exchange=open_count,
            locally_unknown_orders=unknown_count,
            stale_orders_cancelled=cancelled_count,
            errors=errors,
        )

    async def _fetch_open_orders(self) -> list[dict[str, Any]]:
        """Fetch all open orders from the CLOB."""
        loop = asyncio.get_running_loop()
        orders_client = self._orders_client()
        raw = await loop.run_in_executor(
            None, lambda: orders_client.get_orders()
        )
        # py-clob-client returns a list or paginated dict
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
) -> None:
    """Run reconciliation periodically."""
    interval = interval_seconds or float(
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
