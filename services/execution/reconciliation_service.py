"""Canonical reconciliation service (Issue #71).

The reconciler is the **sole owner** of terminal order settlement and startup
convergence for unconfirmed orders. Other components (``trade_pipeline``,
``riskMonitor``) MUST delegate per-record settlement here instead of applying
fills directly. The execution service submits orders; it never self-settles.

This module exposes two public surfaces:

* The original ``ReconciliationService.reconcile_order`` Protocol-based path
  used by unit tests and (eventually) the V2 gateway-driven flow described in
  issue #66. It is preserved for backward compatibility.
* ``reconcile_trade_record`` and ``reconcile_risk_record`` — concrete methods
  that operate on the dict-shaped records produced by the production
  ``OrdersRepo``. These hold the canonical fill-application logic that used to
  live in ``orchestrators/trade_pipeline._reconcile_one`` and the inner loop of
  ``services/riskMonitor.reconcile_unconfirmed_risk_orders``.

Both concrete methods enforce:

* venue ``order_id`` lookup as the primary key (idempotency-key fallback only
  for legacy rows handled in ``OrdersRepo.get_by_order_id``);
* incremental delta application — only ``current_matched - previous_matched``
  is applied, so restarts cannot double-count fills;
* terminal-state ownership — only this service transitions an order to a
  terminal status and persists the matched_total back to the order row;
* optional reservation release on terminal transitions when the order record
  carries a ``reservation_id``.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable, Protocol, cast

from domain.order_lifecycle import OrderLifecycleStatus
from utils.logger import get_logger, redact_error_detail
from storage.postgres.fills_repo import FillRecord

_log = get_logger("ReconciliationService")


# ---------------------------------------------------------------------------
# Protocol-based skeleton (existing tests + V2 gateway path)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReconciliationResult:
    order_id: str
    is_resolved: bool
    status: OrderLifecycleStatus
    error: str = ""

    @classmethod
    def resolved(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=True, status=status)

    @classmethod
    def pending(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=False, status=status)

    @classmethod
    def unresolved(
        cls, order_id: str, status: OrderLifecycleStatus, error: str = ""
    ) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=False, status=status, error=error)


class OrderRecord(Protocol):
    id: str
    venue_order_id: str | None
    reservation_id: str
    status: str
    market_id: str | None


class OrdersRepo(Protocol):
    async def get(self, order_id: str) -> OrderRecord: ...
    async def mark_submitted_unconfirmed(self, order_id: str) -> None: ...
    async def mark_terminal(self, order_id: str, status: str) -> None: ...
    async def mark_reconciliation_required(self, order_id: str) -> None: ...
    async def mark_filled_or_partially_filled(self, order_id: str) -> None: ...


class FillsRepo(Protocol):
    async def upsert_venue_fill(self, order: OrderRecord, venue_fill: Any) -> Any: ...


class PositionsRepo(Protocol):
    def upsert_position(self, fill: Any) -> None: ...


class ReservationService(Protocol):
    def release_terminal(self, reservation_id: str, reason: str) -> None: ...


# NOTE: The canonical CLOB venue gateway Protocol lives in
# ``interfaces.clob_gateway.ClobGateway``. ``OrderQueryGateway`` is the
# query-only subset used by the reconciler; satisfied by the canonical
# gateway directly (``get_order`` and ``get_fills`` signatures match).
# See issue #66 for context.
class OrderQueryGateway(Protocol):
    async def get_order(self, venue_order_id: str) -> Any: ...
    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[Any]: ...
    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[Any]: ...


# Backward-compatible alias for any out-of-tree imports that referenced the
# previous local name. New code should import ``OrderQueryGateway`` directly
# or the canonical ``interfaces.clob_gateway.ClobGateway``.
ClobGateway = OrderQueryGateway


class DbTransaction(Protocol):
    async def __aenter__(self) -> None: ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...


class DbProvider(Protocol):
    def transaction(self) -> DbTransaction: ...


# ---------------------------------------------------------------------------
# Constants shared by the concrete record paths
# ---------------------------------------------------------------------------
_TERMINAL_NO_FILL_STATUSES = frozenset(
    {
        "cancelled",
        "canceled",
        "expired",
        "rejected",
        "failed",
        "order_status_canceled",
        "order_status_canceled_market_resolved",
        "order_status_invalid",
    }
)
# Repeated 404s are tracked only to throttle observability. They never settle an
# order: a missing order lookup plus missing fallback evidence is inconclusive.
_VENUE_ORDER_NOT_FOUND_THRESHOLD = 3

_FINAL_FILL_STATUSES_TRADE = frozenset({"filled", "confirmed"})
# Risk reconcile historically treated empty status + matched > 0 as final fill
# (see services/riskMonitor.reconcile_unconfirmed_risk_orders) — preserve.
_FINAL_FILL_STATUSES_RISK = frozenset({"filled", "confirmed", ""})
# Non-terminal intermediate statuses that should keep the order row alive in
# its current submitted/delayed/live state. Polymarket POST /order can
# return live/matched/delayed. ``matched``/``mined``/``retrying`` remain pending
# until authenticated trade history reports CONFIRMED or FAILED.
_VENUE_NON_TERMINAL = frozenset(
    {
        "live",
        "delayed",
        "order_status_live",
        "submitted",
        "submitted_unconfirmed",
        "submitted_delayed",
        "order_live",
        "matched",
        "order_status_matched",
        "matched_pending_confirmation",
        "trade_mined",
        "trade_retrying",
        "mined",
        "retrying",
        "poll_unavailable",
        "poll_deferred",
        "ambiguous_timeout",
        # P1-q: a marketable order that did not match after the matching delay is
        # placed ON THE BOOK as a resting order (Context7: order status 'unmatched'
        # = "marketable order placed on the book after the delay expired without a
        # match"). It is live / non-terminal — keep polling; do NOT fall through to
        # the ("failed","failed") default and settle a live resting order as failed.
        "unmatched",
    }
)

# Map normalised venue status -> (local submission status, lifecycle log status).
_VENUE_TO_LIFECYCLE: dict[str, tuple[str, str]] = {
    "live": ("order_live", "live"),
    "order_status_live": ("order_live", "live"),
    "order_live": ("order_live", "live"),
    # P1-q: 'unmatched' marketable order is resting on the book → treat as live.
    "unmatched": ("order_live", "live"),
    "delayed": ("submitted_delayed", "delayed"),
    "submitted_delayed": ("submitted_delayed", "delayed"),
    "matched": ("matched_pending_confirmation", "matched"),
    "order_status_matched": ("matched_pending_confirmation", "matched"),
    "matched_pending_confirmation": ("matched_pending_confirmation", "matched"),
    "mined": ("trade_mined", "mined"),
    "trade_mined": ("trade_mined", "mined"),
    "retrying": ("trade_retrying", "retrying"),
    "trade_retrying": ("trade_retrying", "retrying"),
    "confirmed": ("filled_reconciled", "confirmed"),
    "trade_confirmed": ("filled_reconciled", "confirmed"),
    "filled": ("filled_reconciled", "confirmed"),
    "cancelled": ("cancelled", "cancelled"),
    "canceled": ("cancelled", "cancelled"),
    "order_status_canceled": ("cancelled", "cancelled"),
    "order_status_canceled_market_resolved": ("cancelled", "cancelled"),
    "rejected": ("rejected", "failed"),
    "order_status_invalid": ("rejected", "failed"),
    "failed": ("failed", "failed"),
    "expired": ("expired", "failed"),
    "submitted_unconfirmed": ("submitted_unconfirmed", "submitted_unconfirmed"),
    "submitted": ("submitted_unconfirmed", "submitted_unconfirmed"),
}


def _normalize_venue_status(raw: Any) -> str:
    return str(raw or "").strip().lower()


def _normalize_trade_status(raw: Any) -> str:
    status = _normalize_venue_status(raw)
    for prefix in ("trade_status_", "trade_"):
        if status.startswith(prefix):
            status = status[len(prefix) :]
    return status


def _record_created_at(record: dict[str, Any]) -> float | None:
    """Best-effort timestamp from a processed_trade / order row."""
    for key in (
        "created_at",
        "createdAt",
        "submitted_at",
        "submittedAt",
        "processed_at",
        "processedAt",
        "timestamp",
    ):
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, datetime):
            return value.timestamp()
        try:
            ts = float(value)
        except (TypeError, ValueError):
            continue
        if ts > 1e12:
            ts /= 1000.0
        return ts
    return None


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_response(payload: dict[str, Any] | None) -> Decimal:
    if not isinstance(payload, dict):
        return Decimal("0")
    return _decimal_or_zero(
        payload.get("size_matched")
        or payload.get("filled_size")
        or payload.get("matched_size")
        or payload.get("matched_amount")
        or payload.get("sizeMatched")
        or payload.get("filledSize")
        or payload.get("matchedSize")
        or payload.get("matchedAmount")
        or payload.get("size")
        or 0
    )


def _order_id_candidates(order_id: Any) -> list[str]:
    raw = str(order_id or "").strip()
    if not raw:
        return []
    candidates: list[str] = []
    for part in raw.split(","):
        candidate = part.strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    if raw not in candidates:
        candidates.insert(0, raw)
    return candidates


ConfirmFillCallable = Callable[[str], Awaitable[dict[str, Any]]]


# ---------------------------------------------------------------------------
# Canonical reconciler
# ---------------------------------------------------------------------------
class ReconciliationService:
    """Sole owner of terminal order settlement.

    Constructed once per user runtime; safe to share across
    ``trade_pipeline`` and ``riskMonitor`` callers. The ``orders_repo`` /
    ``fills_repo`` / ``positions_repo`` arguments accept either the
    Protocol-based mocks (used by ``reconcile_order``) or the concrete
    ``storage.postgres.*`` repos (used by ``reconcile_trade_record`` and
    ``reconcile_risk_record``).
    """

    def __init__(
        self,
        orders_repo: Any,
        fills_repo: Any,
        positions_repo: Any,
        reservation_service: Any | None = None,
        clob_gateway: Any | None = None,
        db: Any | None = None,
        audit_logger: Any | None = None,
        risk_blocks_repo: Any | None = None,
        risk_triggers_repo: Any | None = None,
        funder_address: str | None = None,
        order_confirmation_cfg: Any | None = None,
    ):
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.reservation_service = reservation_service
        self.clob_gateway = clob_gateway
        self.db = db
        self.audit_logger = audit_logger
        self.risk_blocks_repo = risk_blocks_repo
        self.risk_triggers_repo = risk_triggers_repo
        self.order_confirmation_cfg = order_confirmation_cfg
        # Funder wallet for the user_activities write-back. When set, every
        # terminal "filled_reconciled" emission also writes a bot=true row
        # into user_activities so the manual-sync detector has a single
        # source of truth for tx_hash dedup. None preserves legacy behaviour
        # for bot_cli / test contexts that don't need the write-back.
        self.funder_address = funder_address.lower() if funder_address else None
        self._venue_poll_backoffs: dict[str, dict[str, Any]] = {}
        self._last_order_poll_at: dict[str, float] = {}
        # Per-order 404 streak used only for throttled observability. Missing
        # lookup evidence never settles or unlocks an order.
        self._venue_not_found_counts: dict[str, int] = {}
        # Per-order auto-release pacing: (next_probe_at_monotonic, interval).
        # An ambiguous order that cannot be released (venue outage, probe
        # errors) is re-probed on an escalating schedule instead of every
        # reconcile tick.
        self._auto_release_next_probe_at: dict[str, tuple[float, float]] = {}
        # Per-order abort streak, used to throttle abort logs/audits.
        self._auto_release_abort_counts: dict[str, int] = {}
        # Per-order monotonic timestamp of the last "stalled" audit emission.
        self._ambiguous_stalled_last_audit: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Fill confirmation (venue poll)
    # ------------------------------------------------------------------
    async def _hydrate_order_record(self, order_id: str) -> dict[str, Any]:
        getter = getattr(self.orders_repo, "get_by_order_id", None)
        if not callable(getter):
            return {}
        try:
            row = await cast(Callable[[str], Awaitable[Any]], getter)(order_id)
        except Exception as exc:
            _log.debug(
                "confirm_fill order hydration failed | order_id=%s error=%s",
                order_id,
                redact_error_detail(exc),
            )
            return {}
        return row if isinstance(row, dict) else {}

    @staticmethod
    def _submission_sequences(record: dict[str, Any]) -> tuple[list[str], list[str]]:
        response = record.get("response")
        if not isinstance(response, dict):
            return [], []
        raw = response.get("raw")
        if not isinstance(raw, dict):
            raw = {}
        raw_data = raw.get("data")
        if not isinstance(raw_data, dict):
            raw_data = {}

        def _values(*keys: str) -> list[str]:
            value: Any = None
            for source in (response, raw, raw_data):
                for key in keys:
                    if source.get(key) is not None:
                        value = source.get(key)
                        break
                if value is not None:
                    break
            if isinstance(value, str):
                value = [value]
            if not isinstance(value, (list, tuple, set)):
                return []
            return [str(item) for item in value if str(item or "").strip()]

        return (
            _values("trade_ids", "tradeIds"),
            _values("transactions_hashes", "transaction_hashes", "transactionHashes"),
        )

    @staticmethod
    def _venue_state_dict(venue_state: Any, order_id: str) -> dict[str, Any]:
        if venue_state is None:
            return {"status": "unknown", "size_matched": "0"}
        if isinstance(venue_state, dict):
            return dict(venue_state)
        matched = (
            getattr(venue_state, "size_matched", None)
            or getattr(venue_state, "filled_size", None)
            or getattr(venue_state, "matched_size", None)
            or getattr(venue_state, "matched_amount", None)
            or "0"
        )
        avg_price = (
            getattr(venue_state, "avg_price", None)
            or getattr(venue_state, "avg_fill_price", None)
            or getattr(venue_state, "average_price", None)
            or "0"
        )
        return {
            "status": getattr(venue_state, "status", "unknown"),
            "size_matched": str(matched),
            "avg_price": str(avg_price),
            "order_id": getattr(venue_state, "order_id", order_id),
        }

    @staticmethod
    def _fill_value(fill: Any, key: str, default: Any = None) -> Any:
        if isinstance(fill, dict):
            return fill.get(key, default)
        return getattr(fill, key, default)

    @classmethod
    def _aggregate_fills(
        cls,
        fills: list[Any],
        *,
        status: str,
        source: str,
    ) -> dict[str, Any]:
        total = Decimal("0")
        notional = Decimal("0")
        tx_hashes: list[str] = []
        fill_ids: list[str] = []
        for fill in fills:
            size = _decimal_or_zero(cls._fill_value(fill, "size", 0))
            price = _decimal_or_zero(cls._fill_value(fill, "price", 0))
            if size <= 0 or price <= 0:
                continue
            total += size
            notional += size * price
            tx_hash = str(cls._fill_value(fill, "transaction_hash", "") or "")
            fill_id = str(cls._fill_value(fill, "fill_id", "") or "")
            if tx_hash and tx_hash not in tx_hashes:
                tx_hashes.append(tx_hash)
            if fill_id and fill_id not in fill_ids:
                fill_ids.append(fill_id)
        avg_price = notional / total if total > 0 else Decimal("0")
        return {
            "status": status,
            "size_matched": str(total),
            "avg_price": str(avg_price),
            "transaction_hashes": tx_hashes,
            "trade_ids": fill_ids,
            "confirmation_source": source,
        }

    def _source_backed_off(self, source: str) -> tuple[bool, str]:
        state = self._venue_poll_backoffs.get(source)
        if not state:
            return False, ""
        if time.monotonic() >= float(state.get("until", 0.0)):
            return False, ""
        return True, str(state.get("reason") or "venue_unavailable")

    def _record_poll_success(self, source: str) -> None:
        # Decay instead of reset: one lucky success (or a 404) during a
        # flapping outage must not wipe the exponential ladder that repeated
        # failures built up. The source becomes immediately pollable again,
        # but the next failure escalates from the decayed count.
        state = self._venue_poll_backoffs.get(source)
        if not state:
            return
        failures = int(state.get("failures", 0)) - 1
        if failures <= 0:
            self._venue_poll_backoffs.pop(source, None)
            return
        self._venue_poll_backoffs[source] = {
            "failures": failures,
            "until": 0.0,
            "reason": str(state.get("reason") or ""),
            "base_delay": float(state.get("base_delay", 0.0)),
        }

    def _record_poll_failure(self, source: str, order_id: str, exc: Exception) -> str:
        previous = self._venue_poll_backoffs.get(source, {})
        if previous and time.monotonic() < float(previous.get("until", 0.0)):
            # A backoff window is already active: concurrent callers that
            # slipped past the gate before it was set must not escalate the
            # failure count (thundering-herd guard).
            return str(previous.get("reason") or "venue_unavailable")
        failures = int(previous.get("failures", 0)) + 1
        base_delay = min(5.0 * (2 ** (failures - 1)), 60.0)
        delay = base_delay * random.uniform(0.8, 1.2)
        reason = redact_error_detail(exc)
        self._venue_poll_backoffs[source] = {
            "failures": failures,
            "until": time.monotonic() + delay,
            "reason": reason,
            "base_delay": base_delay,
        }
        _log.warning(
            "confirm_fill transient source failure; backing off CLOB polls | "
            "source=%s base_delay_s=%.0f retry_in_s=%.1f order_id=%s error=%s",
            source,
            base_delay,
            delay,
            order_id,
            reason,
        )
        return reason

    async def _poll_source(
        self,
        source: str,
        order_id: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> tuple[str, Any, str]:
        backed_off, reason = self._source_backed_off(source)
        if backed_off:
            return "unavailable", None, reason
        try:
            value = await operation()
        except Exception as exc:
            try:
                from integrations.polymarket.errors import (
                    VenueOrderNotFoundError,
                    VenueRateLimitError,
                    VenueUnavailableError,
                )

                if isinstance(exc, VenueOrderNotFoundError):
                    self._record_poll_success(source)
                    return "not_found", None, "order_not_found"
                if isinstance(exc, (VenueRateLimitError, VenueUnavailableError)):
                    return (
                        "unavailable",
                        None,
                        self._record_poll_failure(source, order_id, exc),
                    )
            except Exception:
                pass
            reason = redact_error_detail(exc)
            _log.warning(
                "confirm_fill source failed | source=%s order_id=%s error=%s",
                source,
                order_id,
                reason,
            )
            return "error", None, reason
        self._record_poll_success(source)
        return "ok", value, ""

    async def confirm_fill(self, order_id: str) -> dict[str, Any]:
        """Resolve an order from independent authenticated venue evidence.

        A failing single-order endpoint no longer blocks trade history and open
        order fallbacks. Missing evidence is always fail-closed: it keeps the
        order pending and never implies cancellation or permission to resubmit.
        """
        gateway = self.clob_gateway
        if gateway is None:
            return {"status": "unknown", "size_matched": "0"}

        record = await self._hydrate_order_record(order_id)
        cfg = self.order_confirmation_cfg
        response_value = record.get("response")
        response: dict[str, Any] = response_value if isinstance(response_value, dict) else {}
        last_status = _normalize_venue_status(response.get("status"))
        if self._should_skip_for_delay(
            record,
            order_confirmation_cfg=cfg,
            last_known_venue_status=last_status,
        ):
            return {"status": "poll_deferred", "size_matched": "0"}
        if cfg is not None:
            now = time.monotonic()
            last_poll = self._last_order_poll_at.get(order_id)
            interval = max(0.0, float(cfg.poll_interval_seconds))
            if last_poll is not None and now - last_poll < interval:
                return {"status": "poll_deferred", "size_matched": "0"}
            self._last_order_poll_at[order_id] = now

        trade_ids, transaction_hashes = self._submission_sequences(record)
        legacy_matched = _matched_amount_from_response(response)
        if transaction_hashes and not trade_ids and legacy_matched > 0:
            legacy = dict(response)
            legacy.update(
                {
                    "status": "confirmed",
                    "size_matched": str(legacy_matched),
                    "confirmation_source": "submission_transaction_hash",
                }
            )
            return legacy

        request_value = record.get("request")
        request: dict[str, Any] = request_value if isinstance(request_value, dict) else {}
        market_id = (
            str(
                request.get("market")
                or request.get("market_id")
                or request.get("condition_id")
                or ""
            )
            or None
        )
        unavailable: list[tuple[str, str]] = []
        direct: dict[str, Any] | None = None

        order_probe, venue_state, reason = await self._poll_source(
            "order",
            order_id,
            lambda: gateway.get_order(order_id),
        )
        if order_probe == "ok":
            self._venue_not_found_counts.pop(order_id, None)
            # Venue is answering for this order again: drop the escalated
            # auto-release pacing so the next grace check probes promptly.
            self._clear_auto_release_state(order_id)
            direct = self._venue_state_dict(venue_state, order_id)
            direct_status = _normalize_venue_status(direct.get("status"))
            direct_matched = _matched_amount_from_response(direct)
            if direct_status in _TERMINAL_NO_FILL_STATUSES and direct_matched <= 0:
                direct["confirmation_source"] = "order"
                return direct
            if direct_status in {"confirmed", "filled"} and direct_matched > 0:
                direct["confirmation_source"] = "order"
                return direct
        elif order_probe == "not_found":
            count = self._venue_not_found_counts.get(order_id, 0) + 1
            self._venue_not_found_counts[order_id] = count
            if count == 1 or count == _VENUE_ORDER_NOT_FOUND_THRESHOLD:
                _log.info(
                    "confirm_fill order lookup absent; checking fallback evidence | "
                    "order_id=%s consecutive=%d",
                    order_id,
                    count,
                )
        else:
            unavailable.append(("order", reason))

        confirmed_trade_fills: list[Any] = []
        trade_states: list[str] = []
        if trade_ids:
            for trade_id in trade_ids:

                async def _get_trade_fills(tid: str = trade_id) -> list[Any]:
                    return await gateway.get_fills(
                        market=market_id,
                        trade_id=tid,
                        order_id=order_id,
                    )

                probe, fills, reason = await self._poll_source(
                    "trade_fills", order_id, _get_trade_fills
                )
                if probe != "ok":
                    if probe in {"unavailable", "error"}:
                        unavailable.append(("trade_fills", reason))
                    trade_states.append("missing")
                    continue
                related = list(fills or [])
                if not related:
                    trade_states.append("missing")
                    continue
                statuses = {
                    _normalize_trade_status(self._fill_value(fill, "status", ""))
                    for fill in related
                }
                if "confirmed" in statuses:
                    trade_states.append("confirmed")
                    confirmed_trade_fills.extend(
                        fill
                        for fill in related
                        if _normalize_trade_status(self._fill_value(fill, "status", ""))
                        == "confirmed"
                    )
                elif statuses and statuses <= {"failed"}:
                    trade_states.append("failed")
                else:
                    pending = next(
                        (s for s in ("retrying", "mined", "matched") if s in statuses),
                        "matched",
                    )
                    trade_states.append(pending)

            if len(trade_states) == len(trade_ids) and all(
                state in {"confirmed", "failed"} for state in trade_states
            ):
                if confirmed_trade_fills:
                    return self._aggregate_fills(
                        confirmed_trade_fills,
                        status="confirmed",
                        source="trade_ids",
                    )
                return {
                    "status": "failed",
                    "size_matched": "0",
                    "confirmation_source": "trade_ids",
                }

        async def _list_open_orders() -> list[Any]:
            try:
                return await gateway.list_open_orders(
                    order_id=order_id,
                    market=market_id,
                )
            except TypeError:
                orders = await gateway.list_open_orders()
                return [
                    item
                    for item in orders or []
                    if str(self._fill_value(item, "order_id", "")) == order_id
                ]

        open_probe, open_orders, reason = await self._poll_source(
            "open_orders", order_id, _list_open_orders
        )
        if open_probe != "ok":
            unavailable.append(("open_orders", reason))
            open_orders = []
        is_open = bool(open_orders)

        async def _get_order_fills() -> list[Any]:
            try:
                return await gateway.get_fills(
                    market=market_id,
                    order_id=order_id,
                )
            except TypeError:
                fills = await gateway.get_fills(market=market_id)
                return [
                    fill
                    for fill in fills or []
                    if str(self._fill_value(fill, "order_id", "")) == order_id
                ]

        fill_probe, account_fills, reason = await self._poll_source(
            "account_fills", order_id, _get_order_fills
        )
        if fill_probe != "ok":
            unavailable.append(("account_fills", reason))
            account_fills = []

        related_fills = list(account_fills or [])
        pending_statuses = {
            _normalize_trade_status(self._fill_value(fill, "status", ""))
            for fill in related_fills
            if _normalize_trade_status(self._fill_value(fill, "status", ""))
            not in {"confirmed", "failed"}
        }
        confirmed = [
            fill
            for fill in related_fills
            if _normalize_trade_status(self._fill_value(fill, "status", "")) == "confirmed"
        ]
        if pending_statuses:
            pending = next(
                (s for s in ("retrying", "mined", "matched") if s in pending_statuses),
                "matched",
            )
            return {
                "status": pending,
                "size_matched": "0",
                "confirmation_source": "account_trades",
            }
        if confirmed:
            return self._aggregate_fills(
                confirmed,
                status="live" if is_open else "confirmed",
                source="account_trades",
            )
        if (
            related_fills
            and all(
                _normalize_trade_status(self._fill_value(fill, "status", "")) == "failed"
                for fill in related_fills
            )
            and not is_open
        ):
            return {
                "status": "failed",
                "size_matched": "0",
                "confirmation_source": "account_trades",
            }
        if is_open:
            state = self._venue_state_dict(open_orders[0], order_id)
            state.update(
                {
                    "status": _normalize_venue_status(state.get("status")) or "live",
                    "size_matched": "0",
                    "confirmation_source": "open_orders",
                }
            )
            return state
        if trade_states:
            pending = next(
                (s for s in ("retrying", "mined", "matched") if s in trade_states),
                "matched",
            )
            return {
                "status": pending,
                "size_matched": "0",
                "confirmation_source": "trade_ids",
            }
        if direct is not None:
            if _matched_amount_from_response(direct) > 0:
                direct["status"] = "matched"
            direct["size_matched"] = "0"
            direct["confirmation_source"] = "order"
            return direct
        if unavailable:
            return {
                "status": "poll_unavailable",
                "size_matched": "0",
                "poll_error": unavailable[0][1],
                "unavailable_sources": [source for source, _ in unavailable],
                "retryable": True,
            }
        return {"status": "unknown", "size_matched": "0"}

    # ------------------------------------------------------------------
    # Manual-sync write-back
    # ------------------------------------------------------------------
    async def _record_funder_activity(
        self,
        *,
        record: dict[str, Any],
        side: str,
        order_id: str | None,
        size: Any,
        price: Any,
    ) -> None:
        """Write the copy-trade tx_hash into ``user_activities`` for the funder.

        The manual-sync detector dedups against ``user_activities`` by
        ``transaction_hash``; without this write-back the first manual-sync
        pass after a copy execution would mis-classify the bot's own fill
        as a manual trade. Idempotent via ``find_activity_by_tx_hash`` so
        retried reconciliations don't double-insert.
        """
        if not self.funder_address:
            return
        tx_hash = str(record.get("tx_hash") or "").strip()
        if not tx_hash:
            return
        try:
            from models.userHistory import (
                find_activity_by_tx_hash,
                get_user_activity_model,
            )

            if await find_activity_by_tx_hash(self.funder_address, tx_hash):
                return

            payload = {
                "transactionHash": tx_hash,
                "side": side,
                "size": str(size) if size is not None else "0",
                "price": str(price) if price is not None else "0",
                "asset": record.get("asset"),
                "conditionId": record.get("condition_id"),
                "orderId": order_id,
                "leaderId": record.get("leader_id"),
                "bot": True,
                "recordedAt": int(__import__("time").time()),
            }
            await get_user_activity_model(self.funder_address).insert_many([payload])
        except Exception as exc:
            # Write-back is best-effort — never break reconciliation on a
            # logging-only side effect.
            _log.debug(
                "reconcile_funder_activity_writeback_failed | tx=%s error=%s",
                tx_hash,
                redact_error_detail(exc),
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _audit(self, event: str, payload: dict[str, Any]) -> None:
        logger = self.audit_logger
        if logger is None:
            return
        try:
            logger.log_event(event, payload)
        except Exception:
            # Audit logging must never break reconciliation.
            pass

    async def _release_reservation_if_present(
        self,
        record: dict[str, Any],
        *,
        reason: str,
    ) -> None:
        """Release a reservation when the record carries reservation_id.

        Coordinator-path orders populate this either at the top level of
        ``request`` or inside ``request.recovery_metadata`` (PR6). Falls back
        to the top-level ``record["reservation_id"]`` if neither is present.
        Safe no-op when ``reservation_service`` is unset.
        """
        if self.reservation_service is None:
            return
        request = record.get("request") if isinstance(record, dict) else None
        reservation_id: Any = None
        reservation_source: str | None = None
        if isinstance(request, dict):
            reservation_id = request.get("reservation_id")
            if reservation_id:
                reservation_source = "request"
            if not reservation_id:
                recovery = request.get("recovery_metadata")
                if isinstance(recovery, dict):
                    reservation_id = recovery.get("reservation_id")
                    if reservation_id:
                        reservation_source = "request.recovery_metadata"
        if not reservation_id:
            reservation_id = record.get("reservation_id")
            if reservation_id:
                reservation_source = "record"
        if not reservation_id:
            order_id = str(record.get("order_id") or "").strip()
            getter = getattr(self.orders_repo, "get_by_order_id", None)
            if order_id and callable(getter):
                try:
                    order_row = await getter(order_id)
                except Exception:
                    order_row = None
                if isinstance(order_row, dict):
                    order_request = order_row.get("request")
                    if isinstance(order_request, dict):
                        reservation_id = order_request.get("reservation_id")
                        if reservation_id:
                            reservation_source = "orders.request"
                        if not reservation_id:
                            recovery = order_request.get("recovery_metadata")
                            if isinstance(recovery, dict):
                                reservation_id = recovery.get("reservation_id")
                                if reservation_id:
                                    reservation_source = "orders.request.recovery_metadata"
                    if not reservation_id:
                        reservation_id = order_row.get("reservation_id")
                        if reservation_id:
                            reservation_source = "orders.row"
        if not reservation_id:
            reservation_id = await self._lookup_active_reservation_id_for_order(
                record.get("order_id")
            )
            if reservation_id:
                reservation_source = "order_reservations.order_id"
        if not reservation_id:
            return
        try:
            _log.info(
                "reconciliation_reservation_release | reservation_id=%s reason=%s source=%s order_id=%s",
                reservation_id,
                reason,
                reservation_source or "unknown",
                record.get("order_id"),
            )
            self._audit(
                "reconciliation_reservation_release",
                {
                    "reservation_id": str(reservation_id),
                    "reason": reason,
                    "source": reservation_source or "unknown",
                    "order_id": record.get("order_id"),
                },
            )
            await self.reservation_service.release_terminal(str(reservation_id), reason=reason)
        except Exception as exc:
            _log.warning(
                "reconciliation_reservation_release_failed | reservation_id=%s reason=%s error=%r",
                reservation_id,
                reason,
                exc,
            )

    async def _lookup_active_reservation_id_for_order(self, order_id: Any) -> str | None:
        repo = getattr(self.reservation_service, "reservations_repo", None)
        lookup = getattr(repo, "get_active_by_order_id", None)
        if not callable(lookup):
            return None
        for candidate in _order_id_candidates(order_id):
            try:
                reservation = await lookup(candidate)
            except Exception:
                continue
            reservation_id = getattr(reservation, "id", None)
            if reservation_id:
                return str(reservation_id)
        return None

    def _emit_lifecycle(
        self,
        *,
        status: str,
        side: str,
        record: dict[str, Any],
        order_id: str | None,
        venue_status: str | None = None,
        size: Any = None,
        price: Any = None,
        usdc_value: Any = None,
        signal_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Emit an order_lifecycle log via the canonical helper.

        Best-effort: never breaks reconciliation. The helper deduplicates by
        (order_id|signal_id|condition_id, status), so repeated reconciliation
        passes never re-render the same panel.
        """
        try:
            from utils.logger import log_order_lifecycle as _log_lc

            _log_lc(
                _log,
                status=status,
                side=side,
                condition_id=record.get("condition_id") or record.get("conditionId"),
                asset=record.get("asset") or record.get("token_id"),
                order_id=order_id,
                size=float(size) if size is not None else None,
                price=float(price) if price is not None else None,
                usdc_value=float(usdc_value) if usdc_value is not None else None,
                venue_status=venue_status,
                signal_id=signal_id or record.get("trade_key") or record.get("signal_id"),
                meta=meta,
            )
        except Exception:
            pass

    def _should_skip_for_delay(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
        last_known_venue_status: str | None = None,
    ) -> bool:
        """Return True if the reconciler should defer polling for this record.

        Only applies before the configured ``initial_delay_seconds`` elapses
        AND while no fill has been recorded locally. Once a venue status has
        been observed at least once (i.e. the record carries a non-empty
        response.status that's not the pending placeholder) the delay no
        longer applies.
        """
        if order_confirmation_cfg is None:
            return False

        created_at = _record_created_at(record)
        if created_at is None:
            return False

        token_id = str(record.get("asset") or record.get("token_id") or "")
        condition_id = str(record.get("condition_id") or record.get("conditionId") or "")
        delay = order_confirmation_cfg.resolve_initial_delay(
            token_id=token_id,
            condition_id=condition_id,
            venue_status=last_known_venue_status,
        )
        # Treat any existing matched-total > 0 as "already observed", skip the delay.
        previous_matched = _matched_amount_from_response(record.get("response"))
        if previous_matched > 0:
            return False
        elapsed = time.time() - created_at
        return elapsed < delay

    def _is_past_ambiguous_timeout(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        if order_confirmation_cfg is None:
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        return (time.time() - created_at) >= float(order_confirmation_cfg.max_wait_seconds)

    def _is_past_auto_release_grace(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        if order_confirmation_cfg is None:
            return False
        if not getattr(order_confirmation_cfg, "auto_release_ambiguous", False):
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        threshold = float(order_confirmation_cfg.max_wait_seconds) + float(
            order_confirmation_cfg.auto_release_grace_seconds
        )
        return (time.time() - created_at) >= threshold

    @staticmethod
    def _classify_probe_exception(exc: Exception) -> str:
        """Classify an auto-release probe exception.

        ``clean``: the venue affirmatively knows nothing about the order (404).
        ``inconclusive``: transient venue trouble (5xx/rate-limit) — we cannot
        know right now; release stays blocked, but this is expected during an
        outage and must not count as a hard probe error.
        ``error``: anything else (fail-closed).
        """
        try:
            from integrations.polymarket.errors import (
                VenueOrderNotFoundError,
                VenueRateLimitError,
                VenueUnavailableError,
            )
        except Exception:
            return "error"
        if isinstance(exc, VenueOrderNotFoundError):
            return "clean"
        if isinstance(exc, (VenueRateLimitError, VenueUnavailableError)):
            return "inconclusive"
        return "error"

    async def _run_auto_release_probes(
        self,
        *,
        order_id: str,
        venue_order_id: str | None,
        market_id: str | None,
        trade_ids: list[str],
        user_id: str,
        condition_id: str,
        token_id: str,
        expected_size: Decimal,
    ) -> dict[str, Any]:
        """Run every independent safety probe and classify its outcome.

        Each probe is classified as clean/inconclusive/error. The caller
        decides whether to auto-release based on the configured policy.
        """
        result: dict[str, Any] = {
            "errors": 0,
            "inconclusive": 0,
            "clean": 0,
            "details": {},
        }

        gw = self.clob_gateway
        if gw is None:
            result["errors"] += 1
            result["details"]["clob_gateway_missing"] = True
            return result

        # A backed-off venue source cannot produce an all-clean probe battery
        # anyway; defer without touching the gateway so the auto-release path
        # stays silent (instead of spamming errors) during an outage.
        for source in ("order", "open_orders", "trade_fills", "account_fills"):
            backed_off, reason = self._source_backed_off(source)
            if backed_off:
                result["deferred"] = True
                result["details"]["deferred_source"] = source
                result["details"]["deferred_reason"] = reason
                return result

        # Probe 1: get_order
        try:
            venue_state = await gw.get_order(venue_order_id or order_id)
            status = _normalize_venue_status(
                getattr(venue_state, "status", None)
                if not isinstance(venue_state, dict)
                else venue_state.get("status")
            )
            if status in _TERMINAL_NO_FILL_STATUSES or status in {
                "",
                "absent",
                "not_found",
            }:
                result["clean"] += 1
                result["details"]["get_order"] = status or "absent"
            else:
                result["inconclusive"] += 1
                result["details"]["get_order"] = status
        except Exception as exc:
            kind = self._classify_probe_exception(exc)
            if kind == "clean":
                result["clean"] += 1
                result["details"]["get_order"] = "absent"
            elif kind == "inconclusive":
                self._record_poll_failure("order", order_id, exc)
                result["inconclusive"] += 1
                result["details"]["get_order"] = "venue_unavailable"
            else:
                result["errors"] += 1
                result["details"]["get_order_error"] = redact_error_detail(exc)

        # Probe 2: exact authenticated open-order lookup.
        list_orders = getattr(gw, "list_open_orders", None)
        if callable(list_orders):
            try:
                try:
                    orders = await list_orders(
                        order_id=venue_order_id or order_id,
                        market=market_id,
                    )
                except TypeError:
                    orders = await list_orders()
                ids = {
                    str(
                        o.get("order_id") or o.get("id")
                        if isinstance(o, dict)
                        else getattr(o, "order_id", None) or getattr(o, "id", "")
                    )
                    for o in orders or []
                }
                if order_id in ids or (venue_order_id and venue_order_id in ids):
                    result["inconclusive"] += 1
                    result["details"]["list_open_orders"] = "order_present"
                else:
                    result["clean"] += 1
                    result["details"]["list_open_orders"] = "absent"
            except Exception as exc:
                kind = self._classify_probe_exception(exc)
                if kind == "clean":
                    result["clean"] += 1
                    result["details"]["list_open_orders"] = "absent"
                elif kind == "inconclusive":
                    self._record_poll_failure("open_orders", order_id, exc)
                    result["inconclusive"] += 1
                    result["details"]["list_open_orders"] = "venue_unavailable"
                else:
                    result["errors"] += 1
                    result["details"]["list_open_orders_error"] = redact_error_detail(exc)
        else:
            result["inconclusive"] += 1
            result["details"]["list_open_orders"] = "not_supported"

        # Probe 3: each persisted trade ID. Any returned trade means settlement
        # evidence still exists and release is unsafe, regardless of its status.
        for trade_id in trade_ids:
            try:
                trade_fills = await gw.get_fills(
                    market=market_id,
                    trade_id=trade_id,
                    order_id=venue_order_id or order_id,
                )
                if trade_fills:
                    result["inconclusive"] += 1
                    result["details"][f"trade:{trade_id}"] = "present"
                else:
                    result["clean"] += 1
                    result["details"][f"trade:{trade_id}"] = "absent"
            except Exception as exc:
                kind = self._classify_probe_exception(exc)
                if kind == "clean":
                    result["clean"] += 1
                    result["details"][f"trade:{trade_id}"] = "absent"
                elif kind == "inconclusive":
                    self._record_poll_failure("trade_fills", order_id, exc)
                    result["inconclusive"] += 1
                    result["details"][f"trade:{trade_id}"] = "venue_unavailable"
                else:
                    result["errors"] += 1
                    result["details"][f"trade:{trade_id}:error"] = redact_error_detail(exc)

        # Probe 4: exact account fills for the order.
        try:
            try:
                fills = await gw.get_fills(
                    market=market_id,
                    order_id=venue_order_id or order_id,
                )
            except TypeError:
                fills = await gw.get_fills(market=market_id)
            related = [
                f
                for f in fills or []
                if str(f.get("order_id") if isinstance(f, dict) else getattr(f, "order_id", ""))
                in {order_id, venue_order_id}
            ]
            if related:
                result["inconclusive"] += 1
                result["details"]["get_fills"] = f"fills_present:{len(related)}"
            else:
                result["clean"] += 1
                result["details"]["get_fills"] = "absent"
        except Exception as exc:
            kind = self._classify_probe_exception(exc)
            if kind == "clean":
                result["clean"] += 1
                result["details"]["get_fills"] = "absent"
            elif kind == "inconclusive":
                self._record_poll_failure("account_fills", order_id, exc)
                result["inconclusive"] += 1
                result["details"]["get_fills"] = "venue_unavailable"
            else:
                result["errors"] += 1
                result["details"]["get_fills_error"] = redact_error_detail(exc)

        # Probe 5: local position delta.
        try:
            pos = await self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
            local_size = Decimal(str(getattr(pos, "size_tokens", 0) or 0))
            if local_size >= expected_size > 0:
                # Position already absorbed at least the expected size — order
                # may have filled. Conservative: inconclusive.
                result["inconclusive"] += 1
                result["details"]["position_delta"] = f"local_size={local_size}"
            else:
                result["clean"] += 1
                result["details"]["position_delta"] = f"local_size={local_size}"
        except Exception as exc:
            result["errors"] += 1
            result["details"]["position_delta_error"] = redact_error_detail(exc)

        return result

    def _clear_auto_release_state(self, order_id: str) -> None:
        self._auto_release_next_probe_at.pop(order_id, None)
        self._auto_release_abort_counts.pop(order_id, None)
        self._ambiguous_stalled_last_audit.pop(order_id, None)

    def _maybe_emit_ambiguous_stalled(self, order_id: str, record: dict[str, Any]) -> None:
        """Surface a stuck ambiguous order at most once per hour.

        The per-attempt abort logs are throttled; this is the operator-facing
        heartbeat that a reservation is still parked behind an unresolvable
        order.
        """
        now = time.monotonic()
        last = self._ambiguous_stalled_last_audit.get(order_id)
        if last is not None and (now - last) < 3600.0:
            return
        self._ambiguous_stalled_last_audit[order_id] = now
        _log.warning(
            "reconciliation_ambiguous_stalled | order_id=%s status=%s "
            "reservation_id=%s abort_count=%d",
            order_id,
            record.get("status"),
            record.get("reservation_id"),
            self._auto_release_abort_counts.get(order_id, 0),
        )
        self._audit(
            "reconciliation_ambiguous_stalled",
            {
                "order_id": order_id,
                "status": record.get("status"),
                "reservation_id": record.get("reservation_id"),
                "abort_count": self._auto_release_abort_counts.get(order_id, 0),
            },
        )

    def _log_throttled_auto_release_abort(
        self,
        *,
        order_id: str,
        reason: str,
        probe: dict[str, Any],
        level: int,
        message: str,
    ) -> None:
        """Emit the abort log/audit on the 1st and every 10th streak occurrence.

        A stuck ambiguous order re-aborts on every probe attempt; unthrottled
        that floods the log and audit trail with identical lines.
        """
        count = self._auto_release_abort_counts.get(order_id, 0) + 1
        self._auto_release_abort_counts[order_id] = count
        emit = count == 1 or count % 10 == 0
        if not emit:
            level = logging.DEBUG
        _log.log(
            level,
            "%s | order_id=%s errors=%d abort_count=%d details=%s",
            message,
            order_id,
            probe["errors"],
            count,
            probe["details"],
        )
        if emit:
            self._audit(
                "reconciliation_auto_release_aborted",
                {
                    "order_id": order_id,
                    "reason": reason,
                    "probe": probe,
                    "abort_count": count,
                },
            )

    async def _maybe_auto_release_ambiguous(
        self,
        record: dict[str, Any],
        *,
        side: str,
        order_confirmation_cfg: Any,
    ) -> bool:
        """Run safety probes and auto-release the reservation when clean.

        Returns True if the auto-release fired.
        """
        order_id = str(record.get("order_id") or "")
        if not order_id:
            return False

        request = record.get("request") or {}
        venue_order_id = str(record.get("venue_order_id") or order_id)
        market_id = str(request.get("market") or request.get("market_id") or "") or None
        user_id = str(record.get("user_id") or request.get("user_id") or "")
        condition_id = str(record.get("condition_id") or request.get("condition_id") or "")
        token_id = str(record.get("asset") or request.get("token_id") or "")
        try:
            expected_size = Decimal(str(request.get("amount") or request.get("size") or "0"))
        except Exception:
            expected_size = Decimal("0")
        trade_ids, _ = self._submission_sequences(record)

        probe = await self._run_auto_release_probes(
            order_id=order_id,
            venue_order_id=venue_order_id,
            market_id=market_id,
            trade_ids=trade_ids,
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            expected_size=expected_size,
        )

        if probe.get("deferred"):
            _log.debug(
                "auto_release_deferred_backoff | order_id=%s source=%s reason=%s",
                order_id,
                probe["details"].get("deferred_source"),
                probe["details"].get("deferred_reason"),
            )
            return False

        max_errors = int(order_confirmation_cfg.auto_release_max_probe_errors)
        require_clean = bool(order_confirmation_cfg.auto_release_require_all_probes_clean)
        if probe["errors"] > max_errors:
            self._log_throttled_auto_release_abort(
                order_id=order_id,
                reason="probe_errors",
                probe=probe,
                level=logging.WARNING,
                message="auto_release_aborted_probe_errors",
            )
            return False
        if require_clean and probe["inconclusive"] > 0:
            self._log_throttled_auto_release_abort(
                order_id=order_id,
                reason="inconclusive",
                probe=probe,
                level=logging.INFO,
                message="auto_release_aborted_inconclusive",
            )
            return False
        self._auto_release_abort_counts.pop(order_id, None)

        # All probes clean — release.
        try:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status="auto_released_ambiguous",
                response_patch={"probe": probe},
            )
        except Exception:
            pass

        await self._release_reservation_if_present(
            record,
            reason="auto_release_after_ambiguous_timeout",
        )

        self._emit_lifecycle(
            status="auto_released_ambiguous",
            side=side,
            record=record,
            order_id=order_id,
            venue_status="ambiguous_timeout",
        )
        self._audit(
            "reconciliation_auto_release_ambiguous",
            {"order_id": order_id, "probe": probe},
        )
        try:
            from services.metrics import (
                METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS,
                labels={"side": (side or "unknown").upper()},
            )
        except Exception:
            pass
        return True

    async def _handle_unresolved_confirmation_timeout(
        self,
        record: dict[str, Any],
        *,
        side: str,
        fill_info: dict[str, Any],
    ) -> tuple[bool, bool]:
        """Persist timeout state and optionally run strict auto-release.

        Returns ``(timed_out, released)``. A timeout never implies a fill or a
        cancellation; reconciliation continues unless every configured safety
        probe is clean after the grace period.
        """
        cfg = self.order_confirmation_cfg
        if cfg is None:
            return False, False
        order_id = str(record.get("order_id") or "")
        hydrated = await self._hydrate_order_record(order_id)
        combined = {**record, **hydrated}
        for key in ("trade_key", "asset", "condition_id", "user_id", "reservation_id"):
            if not combined.get(key) and record.get(key):
                combined[key] = record[key]
        if not self._is_past_ambiguous_timeout(
            combined,
            order_confirmation_cfg=cfg,
        ):
            return False, False

        if str(combined.get("status") or "") != "ambiguous_timeout":
            try:
                await self.orders_repo.update_submission_status(
                    order_id=order_id,
                    status="ambiguous_timeout",
                    response_patch={
                        "last_confirmation_status": fill_info.get("status"),
                        "ambiguous_since": time.time(),
                    },
                )
            except Exception as exc:
                _log.warning(
                    "ambiguous_timeout_persist_failed | order_id=%s error=%s",
                    order_id,
                    redact_error_detail(exc),
                )
            combined["status"] = "ambiguous_timeout"
            self._emit_lifecycle(
                status="ambiguous_timeout",
                side=side,
                record=combined,
                order_id=order_id,
                venue_status=_normalize_venue_status(fill_info.get("status")),
            )

        if not self._is_past_auto_release_grace(
            combined,
            order_confirmation_cfg=cfg,
        ):
            return True, False

        now = time.monotonic()
        schedule = self._auto_release_next_probe_at.get(order_id)
        if schedule is not None and now < schedule[0]:
            # Escalating per-order pacing: a stuck ambiguous order is re-probed
            # on a widening schedule instead of every reconcile tick.
            self._maybe_emit_ambiguous_stalled(order_id, combined)
            return True, False

        released = await self._maybe_auto_release_ambiguous(
            combined,
            side=side,
            order_confirmation_cfg=cfg,
        )
        if released:
            self._clear_auto_release_state(order_id)
        else:
            base = float(getattr(cfg, "auto_release_retry_backoff_seconds", 30.0))
            cap = float(getattr(cfg, "auto_release_retry_backoff_max_seconds", 900.0))
            interval = schedule[1] * 2 if schedule is not None else base
            interval = min(max(interval, base), cap)
            self._auto_release_next_probe_at[order_id] = (now + interval, interval)
            self._maybe_emit_ambiguous_stalled(order_id, combined)
        if released and combined.get("trade_key"):
            try:
                await self.orders_repo.delete_processed_trade(combined["trade_key"])
            except Exception as exc:
                _log.warning(
                    "auto_release_processed_trade_unlock_failed | order_id=%s error=%s",
                    order_id,
                    redact_error_detail(exc),
                )
        return True, released

    def _record_negative_delta_anomaly(
        self,
        *,
        side: str,
        order_id: str,
        current_matched: Decimal,
        previous_matched: Decimal,
        logger: Any = None,
    ) -> None:
        """Surface a venue-state regression.

        ``current_matched < previous_matched`` means the venue reported a
        cumulative matched size smaller than what we have already applied
        locally. This should never happen in normal operation — Polymarket
        only increments cumulative matched. Clamp to zero (caller does that)
        but make the anomaly observable so operators can investigate.
        """
        emit_logger = logger if logger is not None else _log
        try:
            emit_logger.warning(
                "reconciliation_negative_delta_anomaly | order_id=%s side=%s "
                "current_matched=%s previous_matched=%s",
                order_id,
                side,
                str(current_matched),
                str(previous_matched),
                extra={
                    "type": "reconciliation_negative_delta_anomaly",
                    "order_id": order_id,
                    "side": side,
                    "current_matched": str(current_matched),
                    "previous_matched": str(previous_matched),
                },
            )
        except Exception:
            pass
        self._audit(
            "reconciliation_negative_delta_anomaly",
            {
                "order_id": order_id,
                "side": side,
                "current_matched": str(current_matched),
                "previous_matched": str(previous_matched),
            },
        )
        try:
            from services.metrics import (
                METRIC_RECONCILIATION_NEGATIVE_DELTA,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_NEGATIVE_DELTA,
                labels={"side": str(side or "unknown")},
            )
        except Exception:
            pass

    @staticmethod
    async def _previous_matched_for_order(orders_repo: Any, order_id: str) -> Decimal:
        if not order_id:
            return Decimal("0")
        getter = getattr(orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            existing = await getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(existing, dict):
            return Decimal("0")
        return _matched_amount_from_response(existing.get("response"))

    async def _resolve_buy_requested_amount(self, order_id: str) -> Decimal:
        """Return the requested BUY notional from the persisted orders row.

        Used by the terminal-no-fill branch to release the eagerly recorded
        daily-cap spend back to the regime ledger. Reads ``request_json.amount``
        (falling back to ``size``) on the local orders row.
        """
        if not order_id:
            return Decimal("0")
        getter = getattr(self.orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            row = await getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(row, dict):
            return Decimal("0")
        request = row.get("request") or {}
        if not isinstance(request, dict):
            return Decimal("0")
        raw = request.get("amount") if request.get("amount") is not None else request.get("size")
        try:
            return Decimal(str(raw or 0))
        except Exception:
            return Decimal("0")

    @staticmethod
    def _avg_price_from_fill(fill_info: dict[str, Any]) -> Decimal:
        raw = (
            fill_info.get("avg_price")
            or fill_info.get("avg_fill_price")
            or fill_info.get("avgFillPrice")
            or fill_info.get("price")
            or fill_info.get("average_price")
            or fill_info.get("averagePrice")
            or 0
        )
        try:
            return Decimal(str(raw))
        except Exception:
            return Decimal("0")

    # ------------------------------------------------------------------
    # Protocol-based path (kept for unit tests + V2 gateway)
    # ------------------------------------------------------------------
    async def reconcile_order(self, order_id: str) -> ReconciliationResult:
        order = await self.orders_repo.get(order_id)

        if not order.venue_order_id:
            await self.orders_repo.mark_reconciliation_required(order.id)
            return ReconciliationResult.unresolved(
                order.id,
                OrderLifecycleStatus.RECONCILIATION_REQUIRED,
                "no_venue_order_id",
            )

        try:
            venue_order = await self.clob_gateway.get_order(order.venue_order_id)
            venue_fills = await self.clob_gateway.get_fills(market=order.market_id)
        except Exception as exc:
            self._audit("reconciliation_unreachable", {"order_id": order.id, "error": str(exc)})
            return ReconciliationResult.unresolved(
                order.id, OrderLifecycleStatus.UNKNOWN_UNCONFIRMED, str(exc)
            )

        if venue_fills:
            # Issue #97: filter fills to the exact venue_order_id before
            # applying. get_fills(market=...) returns ALL fills for the
            # market — applying every one to the target order corrupts
            # position state and can trigger premature reservation release.
            # Invariant: every applied fill MUST carry an order_id that
            # matches order.venue_order_id. Fills missing an order_id are
            # inconclusive and must NOT be attributed to this order.
            matching_fills = [
                f
                for f in venue_fills
                if (
                    f.get("order_id") or f.get("orderID")
                    if isinstance(f, dict)
                    else getattr(f, "order_id", None)
                )
                == order.venue_order_id
            ]
            if matching_fills:
                # PR #108 finding: the previous ``async with self.db.transaction()``
                # was a no-op — production callers never pass ``db=``, and
                # ``asyncpg.Pool`` does not expose ``.transaction()``. Each
                # repo call already commits on its own pool connection, so
                # we issue them in dependency order without a doomed outer
                # wrap. If true cross-repo atomicity is needed here it
                # requires a connection-aware repo path (out of scope).
                for venue_fill in matching_fills:
                    fill = await self.fills_repo.upsert_venue_fill(order, venue_fill)
                    await self.positions_repo.upsert_position(fill)

                await self.orders_repo.mark_filled_or_partially_filled(order.id)
                if self.reservation_service is not None:
                    await self.reservation_service.release_terminal(
                        order.reservation_id,
                        reason="venue_fill_confirmed",
                    )
                self._audit("reconciliation_resolved_filled", {"order_id": order.id})
                return ReconciliationResult.resolved(order.id, OrderLifecycleStatus.FILLED)
            # venue_fills was non-empty but none matched this order — treat
            # as inconclusive (fills belong to a different order in the same
            # market, or lack order_id and cannot be safely attributed).
            self._audit(
                "reconciliation_no_matching_fill",
                {"order_id": order.id, "total_fills": len(venue_fills)},
            )

        if venue_order and venue_order.status.lower() in {"live", "open", "pending"}:
            await self.orders_repo.mark_submitted_unconfirmed(order.id)
            return ReconciliationResult.pending(
                order.id, OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
            )

        if venue_order and venue_order.status.lower() in {
            "cancelled",
            "expired",
            "rejected",
        }:
            # PR #108 finding: same as the matching_fills branch — the
            # outer ``self.db.transaction()`` was a no-op in production.
            await self.orders_repo.mark_terminal(order.id, venue_order.status.lower())
            if self.reservation_service is not None:
                await self.reservation_service.release_terminal(
                    order.reservation_id,
                    reason=venue_order.status.lower(),
                )
            self._audit(
                "reconciliation_resolved_terminal",
                {"order_id": order.id, "status": venue_order.status.lower()},
            )
            try:
                status = OrderLifecycleStatus(venue_order.status.lower())
            except ValueError:
                status = OrderLifecycleStatus.TERMINAL_FAILED
            return ReconciliationResult.resolved(order.id, status)

        await self.orders_repo.mark_reconciliation_required(order.id)
        return ReconciliationResult.unresolved(
            order.id,
            OrderLifecycleStatus.RECONCILIATION_REQUIRED,
            f"unknown_venue_status:{venue_order.status if venue_order else 'None'}",
        )

    # ------------------------------------------------------------------
    # Trade BUY/SELL processed_trade reconciliation
    # ------------------------------------------------------------------
    async def reconcile_trade_record(
        self,
        record: dict[str, Any],
        side: str,
        *,
        confirm_fill: ConfirmFillCallable,
        pnl_tracker: Any | None = None,
        get_position: Callable[..., Any] | None = None,
        logger: Any | None = None,
        balance_regime: Any | None = None,
    ) -> bool:
        """Reconcile one BUY/SELL processed_trade order.

        Returns True if the order was reconciled (fill applied or retry
        unlocked); False otherwise. Logs go through ``logger`` if supplied so
        callers retain their existing trace tags.
        """
        order_id = record.get("order_id")
        if not order_id:
            return False

        # Fail closed on malformed rows. Terminal branches dereference these
        # keys with bracket access — a missing key would raise KeyError inside
        # the outer try/except and the caller would silently retry forever.
        required_keys = ("user_id", "trade_key", "asset", "condition_id")
        missing_keys = [k for k in required_keys if not record.get(k)]
        if missing_keys:
            if logger is not None:
                logger.error(
                    "reconcile_trade_record_malformed | order_id=%s missing=%s keys=%s",
                    order_id,
                    missing_keys,
                    list(record.keys()),
                )
            return False

        try:
            fill_info = await confirm_fill(order_id)
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_poll_error | side=%s order_id=%s error=%s",
                    side,
                    order_id,
                    repr(exc),
                )
            return False

        order_status = _normalize_venue_status(fill_info.get("status", ""))
        current_matched = _matched_amount_from_response(fill_info)
        if not order_status and current_matched <= 0:
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_retryable_missing_status | side=%s order_id=%s cond=%s",
                    side,
                    order_id,
                    str(record.get("condition_id", ""))[:12],
                    extra={
                        "type": "reconcile_unconfirmed_missing_status",
                        "order_id": order_id,
                        "condition_id": record.get("condition_id"),
                        "user_id": record["user_id"],
                        "side": side,
                    },
                )
            return False

        unresolved_status = (
            order_status not in _FINAL_FILL_STATUSES_TRADE
            and order_status not in _TERMINAL_NO_FILL_STATUSES
            and current_matched <= 0
        )
        if unresolved_status:
            timed_out, released = await self._handle_unresolved_confirmation_timeout(
                record,
                side=side,
                fill_info=fill_info,
            )
            if released:
                return True
            if timed_out:
                return False

        previous_matched = await self._previous_matched_for_order(self.orders_repo, order_id)
        delta_matched = current_matched - previous_matched

        if current_matched < previous_matched:
            self._record_negative_delta_anomaly(
                side=side,
                order_id=order_id,
                current_matched=current_matched,
                previous_matched=previous_matched,
                logger=logger,
            )

        # Non-terminal intermediate status (live, delayed, matched, mined,
        # retrying). Persist the mapped local status and emit a single
        # lifecycle log per transition. Reservation stays active. No state
        # mutation beyond status — the existing terminal branches do the
        # fill/release work.
        if order_status in _VENUE_NON_TERMINAL and delta_matched <= 0:
            mapped = _VENUE_TO_LIFECYCLE.get(order_status)
            if mapped is not None:
                local_status, lifecycle_status = mapped
                # Idempotency: if the row already carries this status, skip.
                current_local = str(record.get("status") or "")
                if current_local != local_status:
                    try:
                        await self.orders_repo.update_submission_status(
                            order_id=order_id,
                            status=local_status,
                            response_patch=fill_info,
                        )
                    except Exception:
                        pass
                self._emit_lifecycle(
                    status=lifecycle_status,
                    side=side,
                    record=record,
                    order_id=order_id,
                    venue_status=order_status,
                )
            return False

        # Terminal no-fill — unlock the trade key for retry.
        if current_matched <= 0 and order_status in _TERMINAL_NO_FILL_STATUSES:
            if logger is not None:
                logger.info(
                    "reconcile_%s_no_fill_unlock_retry | order_id=%s cond=%s",
                    side.lower(),
                    order_id,
                    str(record.get("condition_id", ""))[:12],
                    extra={
                        "type": f"reconcile_{side.lower()}_no_fill_unlock",
                        "order_id": order_id,
                        "condition_id": record.get("condition_id"),
                        "user_id": record["user_id"],
                        "order_status": order_status,
                    },
                )
            # Phase 4 — release the BUY's reserved spend on the rolling
            # daily-cap ledger before we drop the processed-trade row. The
            # BUY was recorded eagerly at submit time (see
            # ``orchestrators.trade_pipeline``); a terminal no-fill cancel
            # means the venue is returning the budget, so we must too.
            # SELLs do not participate in the daily-spend ledger.
            if (
                balance_regime is not None
                and str(side).upper() == "BUY"
                and hasattr(balance_regime, "record_cancel")
            ):
                cancel_amt = await self._resolve_buy_requested_amount(order_id)
                if cancel_amt > 0:
                    try:
                        balance_regime.record_cancel(
                            cancel_amt,
                            order_id=order_id,
                        )
                    except TypeError:
                        # Back-compat: older signature without order_id.
                        try:
                            balance_regime.record_cancel(cancel_amt)
                        except Exception:  # pragma: no cover — defensive
                            pass
                    except Exception:  # pragma: no cover — defensive
                        pass
            await self.orders_repo.delete_processed_trade(record["trade_key"])
            self._audit(
                "reconciliation_no_fill_unlock",
                {"order_id": order_id, "side": side, "venue_status": order_status},
            )
            # Release reservation if the record carries one (coordinator path).
            await self._release_reservation_if_present(record, reason=order_status)
            # Lifecycle log — render the canonical terminal-failure panel.
            mapped = _VENUE_TO_LIFECYCLE.get(order_status, ("failed", "failed"))
            self._emit_lifecycle(
                status=mapped[1],
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
            )
            return True

        if delta_matched < 0:
            delta_matched = Decimal("0")

        is_final_fill = order_status in _FINAL_FILL_STATUSES_TRADE or (
            order_status in _TERMINAL_NO_FILL_STATUSES and current_matched > 0
        )

        if delta_matched <= 0:
            if is_final_fill:
                # Detect "already settled": either the in-memory record
                # already shows the terminal state, or the orders_repo row
                # was updated by a previous pass.
                stored_row = None
                try:
                    stored_row = await self.orders_repo.get_by_order_id(order_id)
                except Exception:
                    stored_row = None
                stored_status = ""
                if isinstance(stored_row, dict):
                    stored_status = str(stored_row.get("status") or "")
                already_terminal = (
                    str(record.get("status") or "") == "filled_reconciled"
                    or stored_status == "filled_reconciled"
                )
                # Cumulative matched already accounted for — persist terminal
                # state without double-applying the fill.
                if not already_terminal:
                    try:
                        await self.orders_repo.update_submission_status(
                            order_id=order_id,
                            status="filled_reconciled",
                            response_patch=fill_info,
                        )
                    except Exception:
                        pass
                    await self.orders_repo.mark_processed_trade(
                        trade_key=record["trade_key"],
                        user_id=record["user_id"],
                        leader_id=record.get("leader_id"),
                        tx_hash=record.get("tx_hash"),
                        side=side,
                        asset=record["asset"],
                        condition_id=record["condition_id"],
                        status="filled_reconciled",
                        order_id=order_id,
                    )
                    self._audit(
                        "reconciliation_terminal_no_new_delta",
                        {
                            "order_id": order_id,
                            "side": side,
                            "matched_total": str(current_matched),
                        },
                    )
                    await self._release_reservation_if_present(record, reason="confirmed")
                    await self._record_funder_activity(
                        record=record,
                        side=side,
                        order_id=order_id,
                        size=current_matched,
                        price=None,
                    )
                # Lifecycle log fires once via the dedup map — idempotent.
                self._emit_lifecycle(
                    status="confirmed",
                    side=side,
                    record=record,
                    order_id=order_id,
                    venue_status=order_status,
                    size=current_matched,
                )
                return True
            return False

        avg_price = self._avg_price_from_fill(fill_info)
        if avg_price <= 0:
            return False

        # P0-1: read entry_price BEFORE the atomic settlement. A SELL apply does
        # not change avg_entry, but keep the ordering explicit.
        entry_price: float | None = None
        if side == "SELL" and get_position is not None:
            pos = await get_position(
                record["user_id"], record["condition_id"], token_id=record["asset"]
            )
            if pos is not None:
                entry_raw = getattr(pos, "avg_entry_price", None)
                if entry_raw is not None:
                    try:
                        entry_price = float(entry_raw)
                    except (TypeError, ValueError):
                        entry_price = None

        try:
            # P0-1: append the fill and apply the position delta atomically in a
            # single transaction, gated on the fill actually inserting and keyed
            # on a STABLE dedup_key = the cumulative matched total. A re-apply of
            # the same settlement (after a crash, or a swallowed watermark error
            # left the reconciler recomputing the same delta) conflicts on
            # idx_fills_dedup_v2 and returns (0, None), so the delta is applied
            # exactly once — no more double-apply across a wall-clock second.
            fill_id, _applied = await self.fills_repo.append_fill_and_apply(
                FillRecord(
                    user_id=record["user_id"],
                    leader_id=record.get("leader_id"),
                    order_id=order_id,
                    condition_id=record["condition_id"],
                    token_id=record["asset"],
                    side=side,
                    size_tokens=delta_matched,
                    price=avg_price,
                    fee=Decimal("0"),
                    fill_ts=int(time.time()),
                    dedup_key=str(current_matched),
                ),
                self.positions_repo,
                raw_payload={
                    "reconciled_from": "submitted_unconfirmed",
                    "copy_order_id": order_id,
                    "status": "reconciled",
                    "side": side,
                },
            )
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_apply_error | side=%s order_id=%s error=%s",
                    side,
                    order_id,
                    repr(exc),
                )
            return False

        if fill_id == 0:
            # Duplicate settlement — a prior pass already appended this fill and
            # applied the delta. Do not re-apply or re-record PnL; fall through
            # so a prior pass that crashed before the status/terminal write still
            # completes that (idempotent) handling.
            if logger is not None:
                logger.info(
                    "reconcile_duplicate_settlement_skipped | order_id=%s cumulative=%s",
                    order_id,
                    current_matched,
                )
        elif side == "SELL" and pnl_tracker is not None:
            realized_source = "entry_price"
            if entry_price is None:
                if logger is not None:
                    logger.warning(
                        "entry_price missing, using fill price for realized PnL | trade_key=%s cond=%s asset=%s",
                        record.get("trade_key"),
                        record.get("condition_id"),
                        record.get("asset"),
                    )
                entry_price = float(avg_price)
                realized_source = "fill_price_fallback"
            # Compute realized PnL in Decimal to avoid IEEE-754 drift on
            # tick-granular subtractions; cast at the tracker boundary only.
            entry_d = Decimal(str(entry_price))
            realized_d = (avg_price - entry_d) * delta_matched
            pnl_tracker.record_sell(
                condition_id=record["condition_id"],
                price=float(avg_price),
                size_tokens=float(delta_matched),
                usdc_value=float(delta_matched * avg_price),
                entry_price=entry_price,
                realized_pnl=float(realized_d),
                exit_reason="reconciled_sell",
                leader_wallet=record.get("leader_id", ""),
                event_slug="",
                title=f"realized_source={realized_source}",
            )

        if is_final_fill:
            await self.orders_repo.mark_processed_trade(
                trade_key=record["trade_key"],
                user_id=record["user_id"],
                leader_id=record.get("leader_id"),
                tx_hash=record.get("tx_hash"),
                side=side,
                asset=record["asset"],
                condition_id=record["condition_id"],
                status="filled_reconciled",
                order_id=order_id,
            )
            submission_status = "filled_reconciled"
            await self._release_reservation_if_present(record, reason="confirmed")
            await self._record_funder_activity(
                record=record,
                side=side,
                order_id=order_id,
                size=current_matched,
                price=avg_price,
            )
            self._emit_lifecycle(
                status="confirmed",
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
                size=current_matched,
                price=avg_price,
                usdc_value=float(current_matched * avg_price)
                if current_matched and avg_price
                else None,
            )
        else:
            # Non-terminal partial fill — keep the order row alive in
            # ``submitted_unconfirmed`` so duplicate-blocking and the next
            # reconcile pass continue to fire.
            submission_status = "submitted_unconfirmed"
            self._emit_lifecycle(
                status="matched",
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
                size=current_matched,
                price=avg_price,
            )

        try:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status=submission_status,
                response_patch=fill_info,
            )
        except Exception as exc:
            # P0-1: surface a failed watermark write instead of swallowing it.
            # idx_fills_dedup_v2 (keyed on cumulative matched) is now the durable
            # idempotency guard, so a stale watermark no longer double-applies —
            # but a persistent write failure is a real signal worth logging.
            if logger is not None:
                logger.warning(
                    "reconcile_watermark_write_failed | order_id=%s status=%s error=%s",
                    order_id,
                    submission_status,
                    repr(exc),
                )

        if logger is not None:
            logger.info(
                "reconcile_unconfirmed_filled | side=%s order_id=%s cond=%s size=%.4f price=%.4f",
                side,
                order_id,
                str(record.get("condition_id", ""))[:12],
                float(delta_matched),
                float(avg_price),
                extra={
                    "type": "reconcile_unconfirmed_filled",
                    "side": side,
                    "order_id": order_id,
                    "condition_id": record.get("condition_id"),
                    "user_id": record["user_id"],
                    "filled_amount": str(delta_matched),
                    "avg_price": str(avg_price),
                    "venue_status": order_status,
                    "matched_total": str(current_matched),
                    "matched_previous": str(previous_matched),
                },
            )

        self._audit(
            "reconciliation_applied_fill",
            {
                "order_id": order_id,
                "side": side,
                "delta": str(delta_matched),
                "matched_total": str(current_matched),
                "is_final": is_final_fill,
            },
        )
        try:
            from services.metrics import (
                METRIC_RECONCILIATION_DELTA_APPLIED,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_DELTA_APPLIED,
                value=float(delta_matched),
                labels={"side": str(side or "unknown")},
            )
        except Exception:
            pass
        return True

    # ------------------------------------------------------------------
    # Risk SELL reconciliation
    # ------------------------------------------------------------------
    async def reconcile_risk_record(
        self,
        record: dict[str, Any],
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        confirm_fill: ConfirmFillCallable,
        pnl_tracker: Any | None = None,
        get_position: Callable[..., Any] | None = None,
        logger: Any | None = None,
    ) -> bool:
        """Reconcile one risk-monitor-originated SELL order.

        Returns True when reconciliation produced any state convergence.
        """
        order_id = str(record.get("order_id") or "").strip()
        if not order_id:
            return False

        request = record.get("request") or {}

        try:
            fill_info = await confirm_fill(order_id)
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "risk_unconfirmed_reconcile_poll_error | user=%s order_id=%s error=%s",
                    user_id,
                    order_id,
                    repr(exc),
                )
            return False

        venue_status = _normalize_venue_status(fill_info.get("status", ""))
        current_matched = _matched_amount_from_response(fill_info)
        unresolved_status = (
            venue_status not in _FINAL_FILL_STATUSES_RISK
            and venue_status not in _TERMINAL_NO_FILL_STATUSES
            and current_matched <= 0
        )
        if unresolved_status:
            timed_out, released = await self._handle_unresolved_confirmation_timeout(
                {
                    **record,
                    "user_id": user_id,
                    "condition_id": condition_id,
                    "asset": token_id,
                },
                side="SELL",
                fill_info=fill_info,
            )
            if released:
                return True
            if timed_out:
                return False
        # CRITICAL: read previous matched from the persisted orders row,
        # not the in-memory ``record["response"]``. Pre-fix, on restart
        # the in-memory record was rebuilt without the prior fill
        # response and ``previous_matched`` would silently fall back to
        # zero — re-applying the entire cumulative venue size as a
        # fresh delta and double-counting every risk SELL fill that was
        # in-flight across the restart.
        previous_matched = await self._previous_matched_for_order(self.orders_repo, order_id)
        delta_matched = current_matched - previous_matched
        if current_matched < previous_matched:
            self._record_negative_delta_anomaly(
                side="SELL",
                order_id=order_id,
                current_matched=current_matched,
                previous_matched=previous_matched,
                logger=logger,
            )
        if delta_matched < 0:
            delta_matched = Decimal("0")

        # Non-terminal intermediate status (live, delayed, mined, retrying).
        # Persist mapped status; reservation stays active; emit lifecycle log.
        if venue_status in _VENUE_NON_TERMINAL and delta_matched <= 0:
            mapped = _VENUE_TO_LIFECYCLE.get(venue_status)
            if mapped is not None:
                local_status, lifecycle_status = mapped
                try:
                    await self.orders_repo.update_submission_status(
                        order_id=order_id,
                        status=local_status,
                        response_patch=fill_info,
                    )
                except Exception:
                    pass
                # Risk SELL log uses synthetic record shape — pass user/cond/token.
                synthetic_record = {
                    **record,
                    "condition_id": condition_id,
                    "asset": token_id,
                }
                self._emit_lifecycle(
                    status=lifecycle_status,
                    side="SELL",
                    record=synthetic_record,
                    order_id=order_id,
                    venue_status=venue_status,
                )
            return False

        # Terminal no-fill: persist status, drop the in-flight reservation.
        if current_matched <= 0 and venue_status in _TERMINAL_NO_FILL_STATUSES:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status=venue_status,
                response_patch=fill_info,
            )
            self._audit(
                "reconciliation_risk_no_fill",
                {"order_id": order_id, "venue_status": venue_status},
            )
            if self.risk_triggers_repo is not None:
                try:
                    recovery_metadata = request.get("recovery_metadata")
                    trigger_meta = (
                        dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
                    )
                    trigger_meta["venue_status"] = venue_status
                    await self.risk_triggers_repo.record_trigger(
                        user_id=user_id,
                        condition_id=condition_id,
                        token_id=token_id,
                        reason=str(request.get("reason") or "risk_exit"),
                        source="risk_reconciliation",
                        fill_status="rejected",
                        fill_order_id=order_id,
                        metadata=trigger_meta,
                    )
                except Exception:
                    _log.debug("risk_trigger_record_error order_id=%s", order_id, exc_info=True)
            await self._release_reservation_if_present(record, reason=venue_status)
            mapped = _VENUE_TO_LIFECYCLE.get(venue_status, ("failed", "failed"))
            self._emit_lifecycle(
                status=mapped[1],
                side="SELL",
                record={**record, "condition_id": condition_id, "asset": token_id},
                order_id=order_id,
                venue_status=venue_status,
            )
            return True

        is_final_fill = venue_status in _FINAL_FILL_STATUSES_RISK or (
            venue_status in _TERMINAL_NO_FILL_STATUSES and current_matched > 0
        )

        if delta_matched <= 0 and not is_final_fill:
            return False

        if delta_matched <= 0 and is_final_fill:
            already_terminal = str(record.get("status") or "") == "filled_reconciled"
            if not already_terminal:
                await self.orders_repo.update_submission_status(
                    order_id=order_id,
                    status="filled_reconciled",
                    response_patch={
                        **fill_info,
                        "reconciled_from": "risk_submitted_unconfirmed",
                        "matched_previous": str(previous_matched),
                        "matched_delta": str(delta_matched),
                    },
                )
                self._audit(
                    "reconciliation_risk_terminal_no_new_delta",
                    {"order_id": order_id, "matched_total": str(current_matched)},
                )
                await self._release_reservation_if_present(record, reason="confirmed")
            self._emit_lifecycle(
                status="confirmed",
                side="SELL",
                record={**record, "condition_id": condition_id, "asset": token_id},
                order_id=order_id,
                venue_status=venue_status,
                size=current_matched,
            )
            return True

        avg_price = self._avg_price_from_fill(fill_info)
        if avg_price <= 0:
            return False

        reason = str(request.get("reason") or "risk_exit")
        leader_id = str(request.get("leader_id") or "risk-monitor")
        existing_pos = None
        if get_position is not None:
            try:
                existing_pos = await get_position(user_id, condition_id, token_id=token_id)
            except Exception:
                existing_pos = None
        try:
            entry_price = (
                float(getattr(existing_pos, "avg_entry_price", avg_price))
                if existing_pos
                else float(avg_price)
            )
        except (TypeError, ValueError):
            entry_price = float(avg_price)

        try:
            # P0-1: atomic fill+position gated on insert, stable dedup_key = the
            # cumulative matched total (same idempotency guard as the trade
            # path). A re-apply of the same settlement returns (0, None) and is
            # not applied twice.
            fill_id, updated_pos = await self.fills_repo.append_fill_and_apply(
                FillRecord(
                    user_id=user_id,
                    leader_id=leader_id,
                    order_id=order_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    side="SELL",
                    size_tokens=delta_matched,
                    price=avg_price,
                    fee=Decimal("0"),
                    fill_ts=int(time.time()),
                    dedup_key=str(current_matched),
                ),
                self.positions_repo,
                raw_payload={
                    "reconciled_from": "risk_submitted_unconfirmed",
                    "copy_order_id": order_id,
                    "status": "reconciled",
                    "side": "SELL",
                },
            )
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "risk_unconfirmed_reconcile_apply_error | user=%s order_id=%s error=%s",
                    user_id,
                    order_id,
                    repr(exc),
                )
            return False

        reapplied_duplicate = fill_id == 0
        if reapplied_duplicate:
            # Duplicate settlement — a prior pass already applied this SELL delta.
            # Re-derive the live position so the re-entry-block decision below is
            # made on the true closure state (a prior pass may have crashed
            # before marking the block; mark_risk_block is idempotent). The PnL
            # re-record further down is skipped so realized PnL is not double
            # counted.
            updated_pos = None
            if get_position is not None:
                try:
                    updated_pos = await get_position(user_id, condition_id, token_id=token_id)
                except Exception:
                    updated_pos = None
            if logger is not None:
                logger.info(
                    "risk_reconcile_duplicate_settlement_skipped | user=%s order_id=%s cumulative=%s",
                    user_id,
                    order_id,
                    current_matched,
                )

        # Persist a re-entry block only after a confirmed full close.
        recovery_metadata = request.get("recovery_metadata")
        block_metadata = dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
        block_metadata["fill_price"] = float(avg_price)
        block_metadata["fill_size_tokens"] = float(delta_matched)
        block_metadata["fill_order_id"] = order_id
        if is_final_fill and updated_pos is None and self.risk_blocks_repo is not None:
            await self.risk_blocks_repo.mark_risk_block(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                reason=reason,
                source="risk_monitor",
                leader_anchor=leader_id,
                metadata=block_metadata,
            )
        elif logger is not None and not (is_final_fill and updated_pos is None):
            logger.info(
                "risk_unconfirmed_partial_fill_no_block | user=%s order_id=%s condition=%s token=%s remaining=%s",
                user_id,
                order_id,
                condition_id,
                token_id,
                getattr(updated_pos, "size_tokens", None),
            )

        if not reapplied_duplicate and self.risk_triggers_repo is not None:
            try:
                trigger_fill_status = (
                    "filled" if (is_final_fill and updated_pos is None) else "partial"
                )
                await self.risk_triggers_repo.record_trigger(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    reason=reason,
                    source="risk_reconciliation",
                    fill_status=trigger_fill_status,
                    fill_order_id=order_id,
                    metadata=block_metadata,
                )
            except Exception:
                _log.debug("risk_trigger_record_error order_id=%s", order_id, exc_info=True)

        if (
            not reapplied_duplicate
            and pnl_tracker is not None
            and hasattr(pnl_tracker, "record_sell")
        ):
            # Realized PnL in Decimal to avoid float subtraction drift; cast
            # at the tracker boundary only.
            entry_d = Decimal(str(entry_price))
            realized_d = (avg_price - entry_d) * delta_matched
            pnl_tracker.record_sell(
                condition_id=condition_id,
                price=float(avg_price),
                size_tokens=float(delta_matched),
                usdc_value=float(delta_matched * avg_price),
                entry_price=entry_price,
                realized_pnl=float(realized_d),
                exit_reason=reason,
                leader_wallet=leader_id,
            )

        submission_status = "filled_reconciled" if is_final_fill else "submitted_unconfirmed"
        await self.orders_repo.update_submission_status(
            order_id=order_id,
            status=submission_status,
            response_patch={
                **fill_info,
                "reconciled_from": "risk_submitted_unconfirmed",
                "matched_previous": str(previous_matched),
                "matched_delta": str(delta_matched),
            },
        )
        self._audit(
            "reconciliation_risk_applied_fill",
            {
                "order_id": order_id,
                "delta": str(delta_matched),
                "matched_total": str(current_matched),
                "is_final": is_final_fill,
            },
        )
        if is_final_fill:
            await self._release_reservation_if_present(record, reason="confirmed")
        self._emit_lifecycle(
            status="confirmed" if is_final_fill else "matched",
            side="SELL",
            record={**record, "condition_id": condition_id, "asset": token_id},
            order_id=order_id,
            venue_status=venue_status,
            size=current_matched,
            price=avg_price,
            usdc_value=float(current_matched * avg_price)
            if current_matched and avg_price
            else None,
        )
        return True

    # ------------------------------------------------------------------
    # Manual / off-bot position drift reconciliation (Issue #74)
    # ------------------------------------------------------------------
    async def reconcile_position_drift(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        leader_id: str,
        live_position_fetcher: Callable[..., Awaitable[Decimal | None]],
        reservation_service: Any | None = None,
    ) -> dict[str, Any]:
        """Reconcile local position against live venue holdings.

        Called when a SELL fails with ``insufficient_balance`` / ``not enough
        balance`` / ``allowance`` — strong indicators that the true position
        has drifted from our local state due to manual or off-bot trades.

        Returns a dict with keys:
          - ``reconciled``: bool — whether drift was detected and corrected.
          - ``live_size``: Decimal — the actual live position size.
          - ``action``: str — ``"deleted"`` | ``"shrunk"`` | ``"no_drift"`` | ``"fetch_failed"``.
        """
        result: dict[str, Any] = {
            "reconciled": False,
            "live_size": Decimal("0"),
            "action": "fetch_failed",
        }

        try:
            live_size = await live_position_fetcher()
        except Exception as exc:
            _log.warning(
                "reconcile_position_drift_fetch_failed | user=%s cond=%s token=%s error=%s",
                user_id,
                condition_id,
                token_id,
                repr(exc),
            )
            self._audit(
                "manual_position_drift_fetch_failed",
                {
                    "user_id": user_id,
                    "condition_id": condition_id,
                    "token_id": token_id,
                    "error": str(exc),
                },
            )
            return result

        result["live_size"] = live_size if live_size is not None else Decimal("0")

        if live_size is None:
            return result

        existing = await self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
        local_size = getattr(existing, "size_tokens", Decimal("0")) if existing else Decimal("0")

        if local_size <= Decimal("1e-12") and live_size <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        if abs(local_size - live_size) <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        avg_entry_price = (
            getattr(existing, "avg_entry_price", Decimal("0")) if existing else Decimal("0")
        )

        if live_size <= Decimal("1e-12"):
            await self.positions_repo.delete_position(user_id, condition_id, token_id=token_id)
            result["action"] = "deleted"
            result["reconciled"] = True

            if reservation_service is not None:
                try:
                    reservations_repo = getattr(reservation_service, "reservations_repo", None)
                    if reservations_repo is not None:
                        active_reservations = await reservations_repo.list_active_for_scope(
                            user_id=user_id,
                            condition_id=condition_id,
                            token_id=token_id,
                        )
                        for res in active_reservations:
                            if res.side.value == "SELL":
                                await reservation_service.release_terminal(
                                    res.id, reason="manual_position_drift_deleted"
                                )
                except Exception:
                    _log.debug("drift_reservation_cleanup_failed", exc_info=True)
        else:
            from storage.postgres.positions_repo import PositionRecord

            await self.positions_repo.upsert_position(
                PositionRecord(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    leader_id=leader_id,
                    size_tokens=live_size,
                    avg_entry_price=Decimal(str(avg_entry_price or 0)),
                )
            )
            result["action"] = "shrunk"
            result["reconciled"] = True

            if reservation_service is not None:
                try:
                    reservations_repo = getattr(reservation_service, "reservations_repo", None)
                    if reservations_repo is not None:
                        active_reservations = await reservations_repo.list_active_for_scope(
                            user_id=user_id,
                            condition_id=condition_id,
                            token_id=token_id,
                        )
                        for res in active_reservations:
                            if res.side.value == "SELL" and res.reserved_size > live_size:
                                await reservation_service.release_terminal(
                                    res.id, reason="manual_position_drift_shrunk"
                                )
                except Exception:
                    _log.debug("drift_reservation_cleanup_failed", exc_info=True)

        self._audit(
            "manual_position_drift_reconciled",
            {
                "user_id": user_id,
                "leader_id": leader_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "previous_local_size": str(local_size),
                "live_size": str(result["live_size"]),
                "action": result["action"],
                "source": "reconciliation_service",
            },
        )
        _log.info(
            "manual_position_drift_reconciled | user=%s cond=%s token=%s local=%s live=%s action=%s",
            user_id,
            condition_id,
            token_id,
            str(local_size),
            str(result["live_size"]),
            result["action"],
        )
        return result
