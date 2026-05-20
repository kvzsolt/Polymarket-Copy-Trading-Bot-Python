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
corresponding check (so the gate is a no-op when none are configured).

The daily-spend ledger is in-memory (per-process). It is intentionally
conservative: the bot is single-process today, and the ledger resets on
restart, which only ever *over*-allows after a crash — never silently
under-allows after one. A future enhancement can hydrate from
``OrderRepository`` if longer-than-process retention is required.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
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
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


class _DailySpendLedger:
    """Thread-safe rolling-24h spend ledger.

    Records ``(timestamp, notional_usdc)`` for every confirmed BUY notional.
    On query we evict entries older than 24h and sum the remainder.
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

    def total(self, *, now: float | None = None) -> Decimal:
        ts = float(now) if now is not None else time.time()
        with self._lock:
            self._evict(ts)
            return sum((amt for _, amt in self._entries), Decimal("0"))

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

    Successful BUYs must be reported back via :meth:`record_buy` so the
    rolling daily-spend ledger stays accurate.
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

    def record_buy(self, notional_usdc: Decimal | float | int) -> None:
        amt = _to_decimal(notional_usdc)
        self._ledger.record(amt)

    def reset_daily_ledger(self) -> None:
        self._ledger.reset()

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
        daily_remaining = (
            (self._max_daily - spent) if self._max_daily > 0 else Decimal("0")
        )
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

        # 3. Free-cash headroom floor.
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
