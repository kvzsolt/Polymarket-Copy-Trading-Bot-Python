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
    complete_fills_observed: int = 0
    complete_fill_deltas: tuple[tuple[str, str], ...] = ()
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


def _original_size(order: dict[str, Any]) -> Any:
    """Extract the original requested size from a venue order payload.

    Various venue schemas expose this as ``original_size``, ``size``,
    ``order_size``, or ``original_amount``. Returns ``0`` when none of those
    fields are present so callers can fall back on ``status``-based detection.
    """
    for key in ("original_size", "order_size", "original_amount", "size"):
        if key in order and order[key] is not None:
            return _safe_decimal(order[key])
    return _safe_decimal("0")


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
        balance_regime: Any | None = None,
    ):
        self.clob_client = clob_client
        self.orders_repo = orders_repo
        # Phase 4 — optional handle on the balance-regime ledger so the
        # reconciler can roll back daily-spend when the venue confirms an
        # unexpected cancellation/rejection for a previously submitted BUY.
        self.balance_regime = balance_regime

        # Priority: explicit arg > AppConfig > fail-closed default.
        if stale_threshold_seconds is not None:
            self.stale_threshold = float(stale_threshold_seconds)
        elif config is not None and hasattr(config, 'reconciliation'):
            self.stale_threshold = config.reconciliation.stale_threshold_seconds
        else:
            self.stale_threshold = 3600.0

        if auto_cancel_stale is not None:
            self.auto_cancel = auto_cancel_stale
        elif config is not None and hasattr(config, 'reconciliation'):
            self.auto_cancel = config.reconciliation.auto_cancel_stale
        else:
            self.auto_cancel = False

        # Cap on per-pass closed-order venue probes (N+1 protection). The
        # venue rate-limits order lookups, so an unbounded local in-flight
        # backlog must not translate into an unbounded probe storm.
        if config is not None and hasattr(config, 'reconciliation'):
            self.max_closed_probes = max(1, int(config.reconciliation.max_closed_probes))
        else:
            self.max_closed_probes = 10

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
        complete_fills: list[tuple[str, Any]] = []  # (order_id, delta)
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
                        local_record = await self.orders_repo.get_by_order_id(order_id)
                    elif hasattr(self.orders_repo, "get_by_idempotency_key"):
                        # Backwards-compatible fallback for older mock repos.
                        local_record = await self.orders_repo.get_by_idempotency_key(order_id)
                except Exception:
                    pass
                if local_record is None:
                    unknown_count += 1

            # Phase 4 — partial-fill / unexpected-fill / cancellation detection.
            try:
                delta = await self._classify_order_state(
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
                elif kind == "filled":
                    # Distinct from partial_fill so downstream consumers don't
                    # double-apply the delta on a subsequent reconcile pass.
                    complete_fills.append((order_id, str(delta.get("delta"))))
                elif kind == "unexpected_fill":
                    unexpected_fills += 1
                elif kind == "unexpected_cancellation":
                    unexpected_cancellations += 1
                    self._rollback_balance_regime_on_cancel(
                        order_id=order_id,
                        local_record=local_record,
                    )
                elif kind == "cancel_after_partial":
                    unexpected_cancellations += 1
                    self._rollback_balance_regime_on_cancel(
                        order_id=order_id,
                        local_record=local_record,
                        venue_matched=delta.get("venue"),
                    )

            # Check staleness
            age = _order_age_seconds(order, now)
            if age > self.stale_threshold:
                stale_order_ids.append(order_id)
                logger.warning(
                    "Stale open order detected: %s (age=%.0fs, threshold=%.0fs)",
                    order_id, age, self.stale_threshold,
                )

        # 2b. Detect terminal states (fills / cancels) on orders we believe
        # are still in-flight but the venue did NOT return in the open set.
        # ``get_orders()`` only returns OPEN orders, so any order that filled
        # or was cancelled would otherwise stay invisible to this pass. We
        # walk the local in-flight set, look up each missing-from-open order
        # individually, and classify it the same way as if it had appeared
        # in the open list.
        try:
            seen_open_ids = {
                str(o.get("id") or "") for o in exchange_orders if o.get("id")
            }
            # Bound the per-pass venue probes. ``_fetch_single_order`` is one
            # venue call each; an unbounded backlog of stale-but-open local
            # rows would otherwise turn into an N+1 storm against the venue's
            # rate-limited order endpoints.
            probes_done = 0
            probe_cap_hit = False
            for local_row in self._local_in_flight_orders():
                local_oid = str(local_row.get("order_id") or "")
                if not local_oid or local_oid in seen_open_ids:
                    continue
                if probes_done >= self.max_closed_probes:
                    probe_cap_hit = True
                    break
                probes_done += 1
                probed = await self._fetch_single_order(local_oid)
                if probed is None:
                    continue
                try:
                    delta = await self._classify_order_state(
                        order=probed,
                        local_record=local_row,
                    )
                except Exception as exc:  # pragma: no cover — defensive
                    logger.error(
                        "reconcile_classify_failed_closed | order_id=%s err=%s",
                        local_oid, exc,
                    )
                    delta = None
                if delta is None:
                    continue
                kind = delta.get("kind")
                if kind in ("unexpected_fill", "partial_fill"):
                    # A "partial_fill" delta against an order that has fallen
                    # off the open list is now a real fill we never observed
                    # locally — count it as an unexpected fill so downstream
                    # alerting fires and book-keeping reconciles.
                    unexpected_fills += 1
                    logger.warning(
                        "reconcile_unexpected_fill_closed | order_id=%s "
                        "matched=%s status=%s",
                        local_oid,
                        str(delta.get("venue") or delta.get("matched")),
                        _order_status(probed),
                    )
                elif kind == "unexpected_cancellation":
                    unexpected_cancellations += 1
                    self._rollback_balance_regime_on_cancel(
                        order_id=local_oid,
                        local_record=local_row,
                    )
                elif kind == "cancel_after_partial":
                    unexpected_cancellations += 1
                    self._rollback_balance_regime_on_cancel(
                        order_id=local_oid,
                        local_record=local_row,
                        venue_matched=delta.get("venue"),
                    )
                elif kind == "filled":
                    unexpected_fills += 1
                    logger.warning(
                        "reconcile_filled_off_open | order_id=%s matched=%s",
                        local_oid, str(delta.get("venue") or delta.get("matched")),
                    )
            if probe_cap_hit:
                logger.warning(
                    "reconcile_closed_order_probe_cap_hit | cap=%d "
                    "(remaining in-flight orders deferred to next pass)",
                    self.max_closed_probes,
                )
        except Exception as exc:  # pragma: no cover — defensive
            logger.error("reconcile_closed_order_probe_failed | err=%s", exc)

        # 3. Cancel stale orders if configured
        if self.auto_cancel and stale_order_ids:
            for oid in stale_order_ids:
                try:
                    await self._cancel_order(oid)
                    cancelled_count += 1
                    logger.info("Cancelled stale order: %s", oid)
                    # Phase 4 — auto-cancel of a stale BUY must release the
                    # daily-cap reservation. Look the local row up again so
                    # ``_rollback_balance_regime_on_cancel`` can resolve the
                    # original requested notional.
                    if self.balance_regime is not None and self.orders_repo:
                        local_row = None
                        try:
                            if hasattr(self.orders_repo, "get_by_order_id"):
                                local_row = await self.orders_repo.get_by_order_id(oid)
                        except Exception:
                            local_row = None
                        self._rollback_balance_regime_on_cancel(
                            order_id=oid,
                            local_record=local_row,
                        )
                except Exception as exc:
                    msg = f"Failed to cancel stale order {oid}: {exc}"
                    logger.error(msg)
                    errors.append(msg)

        if (
            open_count > 0
            or stale_order_ids
            or partial_fills
            or complete_fills
            or unexpected_fills
            or unexpected_cancellations
        ):
            logger.info(
                "Reconciliation: %d open, %d unknown, %d stale (%d cancelled), "
                "%d partial-fill, %d complete-fill, %d unexpected-fill, "
                "%d unexpected-cancel",
                open_count,
                unknown_count,
                len(stale_order_ids),
                cancelled_count,
                len(partial_fills),
                len(complete_fills),
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
            complete_fills_observed=len(complete_fills),
            complete_fill_deltas=tuple(complete_fills),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Phase 4 — order-state classification + repo update helpers.
    # ------------------------------------------------------------------
    async def _classify_order_state(
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

        if (
            venue_status in _TERMINAL_CANCELLED_STATUSES
            and local_matched > 0
            and venue_matched <= local_matched
        ):
            # Order filled partially, then terminally cancelled with no new
            # matched delta since the last pass. The filled portion stays;
            # only the *unfilled* remainder must roll back against the daily
            # cap. Without this branch the remainder leaks forever (it falls
            # through ``venue_matched > local_matched`` below and returns None).
            local_status = str(local_record.get("status") or "").lower()
            if local_status not in _TERMINAL_CANCELLED_STATUSES:
                logger.info(
                    "reconcile_cancel_after_partial | order_id=%s "
                    "venue_status=%s local_matched=%s venue_matched=%s",
                    order_id, venue_status, local_matched, venue_matched,
                )
                return {
                    "kind": "cancel_after_partial",
                    "local": local_matched,
                    "venue": venue_matched,
                }

        if venue_matched > local_matched:
            delta = venue_matched - local_matched
            await self._record_partial_fill(
                order_id=order_id,
                local_record=local_record,
                venue_matched=venue_matched,
                venue_status=venue_status,
            )
            # Tag as a *complete* fill (kind="filled") when the venue reports
            # a terminal-filled status, or when the cumulative matched size
            # has reached the original requested size. Without this guard,
            # a complete fill is mislabeled as "partial_fill" and downstream
            # logic (which expects partial → more fills to come) ends up
            # double-counting on the next reconcile pass.
            original_size = _original_size(order)
            is_complete = (
                venue_status in _TERMINAL_FILLED_STATUSES
                or (original_size > 0 and venue_matched >= original_size)
            )
            if is_complete:
                logger.info(
                    "reconcile_filled | order_id=%s status=%s "
                    "local_matched=%s venue_matched=%s delta=%s original=%s",
                    order_id, venue_status, local_matched, venue_matched,
                    delta, original_size,
                )
                return {
                    "kind": "filled",
                    "local": local_matched,
                    "venue": venue_matched,
                    "delta": delta,
                    "original": original_size,
                }
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

    @staticmethod
    def _resolve_requested_notional(local_record: dict[str, Any] | None) -> Any:
        """Resolve the submitted BUY notional from a persisted orders row.

        Canonical schema stores it under ``request.amount`` (falling back to
        ``request.size``), matching ``_resolve_buy_requested_amount`` in
        ``services/execution/reconciliation_service.py``. Top-level keys are
        probed last for back-compat with older/mocked row shapes.
        """
        if not local_record:
            return _safe_decimal("0")
        request = local_record.get("request")
        if isinstance(request, dict):
            raw = request.get("amount")
            if raw is None:
                raw = request.get("size")
            if raw is not None:
                return _safe_decimal(raw)
        for key in ("notional_usdc", "size_usdc", "requested_usdc", "size"):
            if local_record.get(key) is not None:
                return _safe_decimal(local_record[key])
        return _safe_decimal("0")

    @staticmethod
    def _resolve_request_price(local_record: dict[str, Any] | None) -> Any:
        if not local_record:
            return _safe_decimal("0")
        request = local_record.get("request")
        if isinstance(request, dict) and request.get("price") is not None:
            return _safe_decimal(request["price"])
        if local_record.get("price") is not None:
            return _safe_decimal(local_record["price"])
        return _safe_decimal("0")

    def _rollback_balance_regime_on_cancel(
        self,
        *,
        order_id: str,
        local_record: dict[str, Any] | None,
        venue_matched: Any | None = None,
    ) -> None:
        """Hand a cancel back to the balance regime so daily-spend rolls back.

        The venue has confirmed the order is terminal-cancelled (or rejected).
        Whatever the gate provisionally recorded at submit time has to come
        back to the daily budget, otherwise the cap can lock the bot out for
        the rest of the window.

        When ``venue_matched`` is supplied (cancel-after-partial), only the
        *unfilled* remainder is refunded — the filled tokens genuinely spent
        cash and must stay on the ledger. The filled notional is estimated as
        ``venue_matched * request.price``.
        """
        regime = self.balance_regime
        if regime is None:
            return
        if not order_id:
            return
        side = (
            (local_record or {}).get("side")
            or (local_record or {}).get("Side")
            or ""
        )
        if isinstance(side, str) and side.upper() == "SELL":
            return  # SELLs do not consume the daily-spend budget

        requested = self._resolve_requested_notional(local_record)
        try:
            matched = _safe_decimal(venue_matched) if venue_matched is not None else None
            if matched is not None and matched > 0:
                # Partial-then-cancel: refund only the unfilled remainder. The
                # filled tokens genuinely spent cash; that portion stays on the
                # ledger.
                price = self._resolve_request_price(local_record)
                if price <= 0:
                    # PR #105 review (Cubic P1): price unresolved → we cannot
                    # estimate the filled notional. Refunding *anything* here
                    # risks under-counting actual spend on the daily cap.
                    # Refuse the refund and log so manual reconciliation owns
                    # the cleanup. The cap stays conservative.
                    logger.warning(
                        "partial_cancel_price_unresolved | order_id=%s matched=%s "
                        "requested=%s — skipping refund (manual reconcile required)",
                        order_id, str(matched), str(requested),
                    )
                    return
                filled_notional = matched * price
                if hasattr(regime, "record_partial_fill"):
                    regime.record_partial_fill(
                        recorded_usdc=requested,
                        actual_usdc=filled_notional,
                        order_id=order_id,
                    )
                    return
                # PR #105 review (Greptile P1): regime lacks record_partial_fill.
                # Falling through to record_cancel(requested) would refund the
                # full requested notional including the filled portion — over-
                # refund. Refund only the unfilled remainder.
                unfilled = requested - filled_notional
                if unfilled <= 0:
                    return
                regime.record_cancel(str(unfilled), order_id=order_id)
                return
            regime.record_cancel(str(requested), order_id=order_id)
        except Exception as exc:  # pragma: no cover — defensive
            logger.error(
                "balance_regime_rollback_failed | order_id=%s err=%s",
                order_id, exc,
            )

    async def _record_partial_fill(
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
                await self.orders_repo.upsert_matched_total(
                    order_id=order_id,
                    matched=str(venue_matched),
                )
        except Exception as exc:
            logger.error(
                "reconcile_partial_fill_persist_failed | order_id=%s err=%s",
                order_id, exc,
            )

    def _local_in_flight_orders(self) -> list[dict[str, Any]]:
        """Return locally persisted orders that we still believe are open.

        Used to detect terminal venue states for orders that have fallen off
        the open-orders list (filled, cancelled) but our local view has not
        caught up yet.
        """
        repo = self.orders_repo
        if repo is None:
            return []
        getter = getattr(repo, "get_unconfirmed_order_submissions", None)
        if not callable(getter):
            return []
        try:
            rows = getter() or []
        except Exception as exc:
            logger.error("reconcile_local_in_flight_lookup_failed | err=%s", exc)
            return []
        return [r for r in rows if isinstance(r, dict)]

    async def _fetch_single_order(self, order_id: str) -> dict[str, Any] | None:
        """Probe the venue for a single order by id.

        Returns a dict in the same shape as entries from ``get_orders()`` so
        ``_classify_order_state`` can reuse its logic. Returns ``None`` on
        any error (we never want a single bad lookup to abort the pass).
        """
        client = self._orders_client()
        getter = getattr(client, "get_order", None)
        if not callable(getter):
            return None
        try:
            loop = asyncio.get_running_loop()
            raw = await loop.run_in_executor(
                None, lambda: getter(order_id)
            )
        except Exception as exc:
            logger.debug(
                "reconcile_single_order_fetch_failed | order_id=%s err=%s",
                order_id, exc,
            )
            return None
        if raw is None:
            return None
        if isinstance(raw, dict):
            # Ensure ``id`` is populated so downstream classification works.
            raw.setdefault("id", order_id)
            return raw
        # Handle a venue VenueOrderState-style object: copy attrs into a dict.
        try:
            out: dict[str, Any] = {"id": order_id}
            for attr in (
                "status",
                "size_matched",
                "filled",
                "matched_size",
                "filled_size",
                "original_size",
                "size",
                "timestamp",
                "created_at",
            ):
                val = getattr(raw, attr, None)
                if val is not None:
                    out[attr] = val
            return out
        except Exception:
            return None

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
        # Fail-closed default; production always passes config.polling.
        interval = 300.0
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
        # Sleep interruptibly: wake immediately when shutdown is requested
        # instead of blocking for the full interval (up to 300s by default).
        if shutdown_event is not None:
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
                logger.info("Reconciliation loop: shutdown requested")
                break
            except asyncio.TimeoutError:
                continue
        await asyncio.sleep(interval)
