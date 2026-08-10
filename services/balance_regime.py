"""Phase 4 — Balance regime gate.

Enforces three independent absolute-USDC caps before a BUY is sent to the
execution layer:

* ``MAX_POSITION_SIZE_USDC`` — absolute cap on the notional of any single new
  BUY (not a percentage of equity; that is handled by ``MAX_POSITION_PCT``).
* ``MAX_DAILY_SPEND_USDC``  — rolling 24h ceiling on cumulative BUY notional
  across all markets and leaders.
* ``MIN_BALANCE_HEADROOM_USDC`` — refuse to execute if running the BUY would
  bring free cash below the configured floor.

Each knob is **independent and additive**; a value of ``0`` disables the
corresponding check (so the gate is a no-op when none are configured). Even
when ``MIN_BALANCE_HEADROOM_USDC`` is zero, the gate still hard-rejects any
BUY that would push free cash strictly below zero — a config slip can never
authorise negative cash.

Reserve/commit lifecycle
------------------------
Concurrent BUY signals can otherwise observe the same ``spent_today`` /
``free_cash`` snapshot and race past the daily ceiling. To prevent this the
gate exposes an atomic ``reserve()`` → ``commit()`` / ``release()`` pattern:

* ``reserve(order_id, notional, free_cash)`` — checks the three caps **and**
  records the submitted spend in one critical section, returning a
  :class:`BalanceDecision`. Held headroom is tracked by ``order_id``.
* ``commit(order_id, actual_filled_usdc=None)`` — confirms a reservation when
  fills land. If the actual fill is smaller than the reserved amount the
  ledger is adjusted down by the unfilled remainder.
* ``release(order_id)`` — rolls the full reservation back when the order is
  cancelled, rejected, or replaced.

Synchronous ``record_buy`` / ``record_cancel`` / ``record_partial_fill``
helpers remain available for code paths that do not use the reserve
lifecycle (e.g. the existing trade pipeline submit hook).
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable

from utils.logger import get_logger

logger = get_logger("BalanceRegime")

_DAILY_WINDOW_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class BalanceDecision:
    """Outcome of a balance-regime check for a single proposed BUY."""

    allow: bool
    reason: str
    policy_name: str
    requested_usdc: Decimal
    daily_spent_usdc: Decimal
    daily_remaining_usdc: Decimal
    headroom_usdc: Decimal

    @property
    def skip_code(self) -> str:
        return self.reason


def _to_decimal(value: object, default: str = "0") -> Decimal:
    """Coerce ``value`` to ``Decimal`` with explicit handling of bad input.

    Catches only the parsing/arithmetic errors that ``Decimal(str(...))`` can
    raise — broad ``Exception`` swallowing previously masked real bugs and
    silently produced ``Decimal(0)``. NaN and Infinity are rejected as
    finance-unsafe inputs even though ``Decimal`` will happily parse them.
    """
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError, ArithmeticError) as exc:
        logger.warning(
            "balance_regime_to_decimal_invalid_input",
            extra={"value": repr(value), "error": str(exc), "default": default},
        )
        return Decimal(default)
    if not result.is_finite():
        logger.warning(
            "balance_regime_to_decimal_non_finite",
            extra={"value": repr(value), "parsed": str(result), "default": default},
        )
        return Decimal(default)
    return result


class _DailySpendLedger:
    """Thread-safe rolling-24h spend ledger.

    Records ``(timestamp, signed_notional_usdc)`` for every submitted BUY
    notional (positive) and cancellation/over-recorded delta (negative). On
    query we evict entries older than 24h, sum the remainder, and floor at
    zero so an over-eager cancel can never produce a negative total.

    The lock is :mod:`threading`-based because the ledger is also touched
    from blocking executor threads (reconciliation runs venue calls via
    ``run_in_executor``). No critical section in this class spans an
    ``await``, so there is no deadlock risk against the asyncio event
    loop. Async coordination of the multi-call reserve/commit lifecycle
    happens at the :class:`BalanceRegime` layer with :class:`asyncio.Lock`.
    """

    def __init__(self, window_seconds: float = _DAILY_WINDOW_SECONDS) -> None:
        self._window = float(window_seconds)
        self._entries: deque[tuple[float, Decimal]] = deque()
        self._lock = threading.Lock()

    def record(self, notional_usdc: Decimal, *, now: float | None = None) -> None:
        if notional_usdc <= 0:
            return
        ts = float(now) if now is not None else time.time()
        with self._lock:
            self._entries.append((ts, notional_usdc))
            self._evict(ts)

    def adjust(self, delta_usdc: Decimal, *, now: float | None = None) -> None:
        """Apply a signed adjustment (used by cancel/partial-fill rollbacks)."""
        if delta_usdc == 0:
            return
        ts = float(now) if now is not None else time.time()
        with self._lock:
            self._entries.append((ts, delta_usdc))
            self._evict(ts)

    def total(self, *, now: float | None = None) -> Decimal:
        ts = float(now) if now is not None else time.time()
        with self._lock:
            self._evict(ts)
            running = sum((amt for _, amt in self._entries), Decimal("0"))
        # Floor at zero — over-eager cancels must never produce negative spend.
        return running if running > 0 else Decimal("0")

    def reset(self) -> None:
        with self._lock:
            self._entries.clear()

    def _evict(self, now: float) -> None:
        cutoff = now - self._window
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.popleft()


class BalanceRegime:
    """Phase 4 balance gate.

    The gate is constructed once at runtime bootstrap with an
    ``ExecutionConfig`` slice. Each ``check_buy`` call evaluates the proposed
    BUY against the three knobs and returns a :class:`BalanceDecision`.

    Successful BUYs must be reported back via :meth:`record_buy` (or the
    atomic :meth:`reserve` / :meth:`commit` lifecycle) so the rolling
    daily-spend ledger stays accurate. Cancellations/rejections must call
    :meth:`record_cancel` (or :meth:`release`) to roll the ledger back.
    """

    def __init__(
        self,
        *,
        max_position_size_usdc: Decimal | float | int = 0,
        max_daily_spend_usdc: Decimal | float | int = 0,
        min_balance_headroom_usdc: Decimal | float | int = 0,
        ledger: _DailySpendLedger | None = None,
    ) -> None:
        self._max_position = _to_decimal(max_position_size_usdc)
        self._max_daily = _to_decimal(max_daily_spend_usdc)
        self._min_headroom = _to_decimal(min_balance_headroom_usdc)
        self._ledger = ledger or _DailySpendLedger()
        # Held headroom by order id — populated by reserve(), consumed by
        # commit() / release().
        self._reservations: dict[str, Decimal] = {}
        self._cancelled_ids: set[str] = set()
        self._reservations_guard = threading.Lock()
        # Async coordination for the reserve→commit/release lifecycle so
        # concurrent BUY signals cannot race past the cap.
        self._async_lock = asyncio.Lock()

    # ------------------------------------------------------------------ API
    @classmethod
    def from_config(cls, execution_cfg: object) -> "BalanceRegime":
        return cls(
            max_position_size_usdc=getattr(execution_cfg, "max_position_size_usdc", 0),
            max_daily_spend_usdc=getattr(execution_cfg, "max_daily_spend_usdc", 0),
            min_balance_headroom_usdc=getattr(execution_cfg, "min_balance_headroom_usdc", 0),
        )

    @property
    def daily_spent_usdc(self) -> Decimal:
        return self._ledger.total()

    def record_buy(
        self,
        notional_usdc: Decimal | float | int,
        *,
        order_id: str | None = None,
    ) -> None:
        """Record a submitted BUY against the rolling daily-spend ledger.

        Called from both the immediately-filled and submitted-unconfirmed
        BUY paths in the trade pipeline — the ledger tracks *submitted*
        spend, not *unfilled* spend, so every accepted BUY must hit it
        exactly once.
        """
        amt = _to_decimal(notional_usdc)
        if amt <= 0:
            return
        self._ledger.record(amt)
        if order_id:
            with self._reservations_guard:
                self._reservations[order_id] = self._reservations.get(order_id, Decimal("0")) + amt

    def record_cancel(
        self,
        notional_usdc: Decimal | float | int,
        *,
        order_id: str | None = None,
    ) -> None:
        """Roll back a previously recorded BUY (cancellation/rejection).

        If ``order_id`` is supplied and a matching reservation exists, the
        recorded amount is used as the source of truth instead of
        ``notional_usdc`` — this avoids drift when the upstream caller no
        longer has the exact submitted notional.
        """
        recorded: Decimal | None = None
        if order_id:
            with self._reservations_guard:
                if order_id in self._cancelled_ids:
                    return  # already cancelled — idempotent
                recorded = self._reservations.pop(order_id, None)
                if recorded is not None:
                    self._cancelled_ids.add(order_id)
        amt = recorded if recorded is not None else _to_decimal(notional_usdc)
        if amt <= 0:
            return
        self._ledger.adjust(-amt)

    def record_partial_fill(
        self,
        *,
        recorded_usdc: Decimal | float | int,
        actual_usdc: Decimal | float | int,
        order_id: str | None = None,
    ) -> None:
        """Reduce the ledger when the actual fill is smaller than recorded.

        ``record_buy`` records at submission time using the *requested*
        notional. If the venue fills less than requested (partial fill that
        terminally cancels the remainder) the unfilled remainder must come
        back to the daily budget.
        """
        recorded = _to_decimal(recorded_usdc)
        actual = _to_decimal(actual_usdc)
        if actual < 0:
            actual = Decimal("0")
        delta = recorded - actual
        if delta <= 0:
            return
        self._ledger.adjust(-delta)
        if order_id:
            with self._reservations_guard:
                remaining = self._reservations.get(order_id)
                if remaining is not None:
                    new_remaining = remaining - delta
                    if new_remaining <= 0:
                        self._reservations.pop(order_id, None)
                    else:
                        self._reservations[order_id] = new_remaining

    def reset_daily_ledger(self) -> None:
        self._ledger.reset()
        with self._reservations_guard:
            self._reservations.clear()

    def check_buy(
        self,
        *,
        notional_usdc: Decimal | float | int,
        free_cash_usdc: Decimal | float | int,
    ) -> BalanceDecision:
        """Evaluate a proposed BUY against the three regime knobs.

        Order of evaluation is fixed so the *first* tripped rule wins — this
        keeps log messages and skip codes deterministic when an operator
        triples up tight thresholds.
        """
        requested = _to_decimal(notional_usdc)
        free_cash = _to_decimal(free_cash_usdc)
        spent = self._ledger.total()
        daily_remaining = (self._max_daily - spent) if self._max_daily > 0 else Decimal("0")
        headroom_after = free_cash - requested

        if requested <= 0:
            return BalanceDecision(
                allow=False,
                reason="balance_regime_zero_amount",
                policy_name="balance_regime",
                requested_usdc=requested,
                daily_spent_usdc=spent,
                daily_remaining_usdc=daily_remaining,
                headroom_usdc=headroom_after,
            )

        # 1. Per-position absolute cap.
        if self._max_position > 0 and requested > self._max_position:
            return BalanceDecision(
                allow=False,
                reason="balance_regime_position_size_exceeded",
                policy_name="max_position_size_usdc",
                requested_usdc=requested,
                daily_spent_usdc=spent,
                daily_remaining_usdc=daily_remaining,
                headroom_usdc=headroom_after,
            )

        # 2. Rolling daily-spend ceiling.
        if self._max_daily > 0 and (spent + requested) > self._max_daily:
            return BalanceDecision(
                allow=False,
                reason="balance_regime_daily_spend_exceeded",
                policy_name="max_daily_spend_usdc",
                requested_usdc=requested,
                daily_spent_usdc=spent,
                daily_remaining_usdc=daily_remaining,
                headroom_usdc=headroom_after,
            )

        # 3a. Hard floor: never allow free cash to go strictly negative,
        # regardless of the configured headroom knob. A config slip
        # (``MIN_BALANCE_HEADROOM_USDC=0``) must not authorise a BUY that
        # overdraws the wallet.
        if headroom_after < 0:
            return BalanceDecision(
                allow=False,
                reason="balance_regime_insufficient_free_cash",
                policy_name="min_balance_headroom_usdc",
                requested_usdc=requested,
                daily_spent_usdc=spent,
                daily_remaining_usdc=daily_remaining,
                headroom_usdc=headroom_after,
            )

        # 3b. Configured free-cash headroom floor.
        if self._min_headroom > 0 and headroom_after < self._min_headroom:
            return BalanceDecision(
                allow=False,
                reason="balance_regime_headroom_floor",
                policy_name="min_balance_headroom_usdc",
                requested_usdc=requested,
                daily_spent_usdc=spent,
                daily_remaining_usdc=daily_remaining,
                headroom_usdc=headroom_after,
            )

        return BalanceDecision(
            allow=True,
            reason="ok",
            policy_name="balance_regime",
            requested_usdc=requested,
            daily_spent_usdc=spent,
            daily_remaining_usdc=daily_remaining,
            headroom_usdc=headroom_after,
        )

    # ------------------------------------------------ reserve/commit/release
    async def reserve(
        self,
        *,
        order_id: str,
        notional_usdc: Decimal | float | int,
        free_cash_usdc: Decimal | float | int,
    ) -> BalanceDecision:
        """Atomically check-and-record a BUY against the daily ceiling.

        Two concurrent BUY signals cannot both pass the cap check on the
        same snapshot — the async lock serialises the (read spent → compare
        cap → record) sequence so subsequent reservations observe earlier
        ones.
        """
        if not order_id:
            raise ValueError("reserve requires a non-empty order_id")
        async with self._async_lock:
            decision = self.check_buy(
                notional_usdc=notional_usdc,
                free_cash_usdc=free_cash_usdc,
            )
            if decision.allow:
                self.record_buy(decision.requested_usdc, order_id=order_id)
            return decision

    async def commit(
        self,
        *,
        order_id: str,
        actual_filled_usdc: Decimal | float | int | None = None,
    ) -> None:
        """Confirm a reservation as filled.

        When ``actual_filled_usdc`` is supplied and smaller than the held
        reservation the unfilled remainder is returned to the daily budget
        via :meth:`record_partial_fill`. The reservation handle is then
        discarded — the ledger entry persists for the 24h window.
        """
        async with self._async_lock:
            with self._reservations_guard:
                reserved = self._reservations.pop(order_id, None)
            if reserved is None:
                return
            if actual_filled_usdc is None:
                return
            actual = _to_decimal(actual_filled_usdc)
            if actual < 0:
                actual = Decimal("0")
            unfilled = reserved - actual
            if unfilled > 0:
                self._ledger.adjust(-unfilled)

    async def release(self, *, order_id: str) -> None:
        """Roll a reservation back in full (cancel/reject/replace)."""
        async with self._async_lock:
            with self._reservations_guard:
                reserved = self._reservations.pop(order_id, None)
            if reserved is None or reserved <= 0:
                return
            self._ledger.adjust(-reserved)

    def reserved_total_usdc(self) -> Decimal:
        with self._reservations_guard:
            return sum(self._reservations.values(), Decimal("0"))

    # ---------------------------------------------------- diagnostic helpers
    def thresholds(self) -> dict[str, Decimal]:
        return {
            "max_position_size_usdc": self._max_position,
            "max_daily_spend_usdc": self._max_daily,
            "min_balance_headroom_usdc": self._min_headroom,
        }

    def hydrate(self, fills: Iterable[tuple[float, Decimal | float | int]]) -> None:
        """Seed the daily ledger from external history (e.g. orders repo).

        ``fills`` is an iterable of ``(timestamp_seconds, notional_usdc)``.
        Entries older than the rolling window are dropped automatically by
        the ledger.
        """
        for ts, amt in fills:
            self._ledger.record(_to_decimal(amt), now=ts)
