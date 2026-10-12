"""Deterministic per-fill stop-loss and take-profit decisions.

The engine is pure: it evaluates one immutable lot against one immutable
protective policy and returns the highest-priority protective decision. It
performs no I/O and places no orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

_ZERO = Decimal("0")
_HUNDRED = Decimal("100")


def _decimal(value: object, *, name: str, minimum: Decimal = _ZERO) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite() or result < minimum:
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return result


@dataclass(frozen=True, slots=True)
class LayeredLevel:
    """One percentage trigger and fraction of remaining shares to exit."""

    trigger_percent: Decimal
    exit_fraction: Decimal

    def __post_init__(self) -> None:
        trigger = _decimal(self.trigger_percent, name="trigger_percent")
        fraction = _decimal(self.exit_fraction, name="exit_fraction")
        if fraction <= 0 or fraction > 1:
            raise ValueError("exit_fraction must be in (0, 1]")
        object.__setattr__(self, "trigger_percent", trigger)
        object.__setattr__(self, "exit_fraction", fraction)


@dataclass(frozen=True, slots=True)
class ProtectiveRiskPolicy:
    """Per-fill protective exit settings captured when a fill opens."""

    enabled: bool = True
    stop_loss_percent: Decimal | None = None
    stop_loss_price: Decimal | None = None
    take_profit_percent: Decimal | None = None
    take_profit_price: Decimal | None = None
    trailing_stop_percent: Decimal | None = None
    layered_stop_loss: tuple[LayeredLevel, ...] = ()
    layered_take_profit: tuple[LayeredLevel, ...] = ()

    @property
    def requires_monitoring(self) -> bool:
        """Return whether an enabled rule needs continuing runtime management."""
        return self.enabled and (
            bool(self.layered_stop_loss or self.layered_take_profit)
            or any(
                value is not None
                for value in (
                    self.stop_loss_percent,
                    self.stop_loss_price,
                    self.take_profit_percent,
                    self.take_profit_price,
                    self.trailing_stop_percent,
                )
            )
        )

    def __post_init__(self) -> None:
        for name in (
            "stop_loss_percent",
            "stop_loss_price",
            "take_profit_percent",
            "take_profit_price",
            "trailing_stop_percent",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name=name))


@dataclass(frozen=True, slots=True)
class PendingLayerExit:
    """Confirmed partial execution leaves this exact layer target outstanding."""

    level_id: str
    remaining_shares: Decimal

    def __post_init__(self) -> None:
        if not self.level_id or not isinstance(self.remaining_shares, Decimal):
            raise ValueError("Pending layer requires an identity and exact shares")
        if not self.remaining_shares.is_finite() or self.remaining_shares <= 0:
            raise ValueError("Pending layer shares must be finite and positive")


@dataclass(frozen=True, slots=True)
class FillRiskLot:
    """Follower fill lot with its own entry and protective state."""

    lot_id: str
    task_id: str
    market_id: str
    outcome_id: str
    entry_price: Decimal
    original_shares: Decimal
    remaining_shares: Decimal
    opened_at: datetime
    peak_price: Decimal
    triggered_levels: tuple[str, ...] = ()
    pending_exits: tuple[PendingLayerExit, ...] = ()

    def __post_init__(self) -> None:
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        for name in ("lot_id", "task_id", "market_id", "outcome_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        for name in ("entry_price", "original_shares", "remaining_shares", "peak_price"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name=name))
        if self.entry_price <= 0 or self.entry_price > 1:
            raise ValueError("entry_price must be in (0, 1]")
        if self.remaining_shares > self.original_shares:
            raise ValueError("remaining_shares cannot exceed original_shares")


@dataclass(frozen=True, slots=True)
class ProtectiveExitDecision:
    """Pure protective action for one lot at one current price."""

    should_exit: bool
    reason: str
    shares_to_exit: Decimal
    terminal: bool
    level_id: str | None = None


class ProtectiveExitEngine:
    """Evaluate explicit protective rules without placing an order."""

    def evaluate(
        self,
        lot: FillRiskLot,
        policy: ProtectiveRiskPolicy,
        *,
        current_price: Decimal,
    ) -> ProtectiveExitDecision:
        """Return the highest-priority protective decision for one lot.

        Args:
            lot: Immutable follower fill lot.
            policy: Immutable protective risk policy.
            current_price: Current market price for the lot outcome.

        Returns:
            Immutable protective decision with exact shares to exit.
        """

        price = _decimal(current_price, name="current_price")
        if not policy.enabled or lot.remaining_shares <= 0:
            return ProtectiveExitDecision(False, "protective_exit_disabled", _ZERO, False)

        layered_stop = self._layered_stop(lot, policy, price)
        if layered_stop is not None:
            return layered_stop
        if policy.stop_loss_price is not None and price <= policy.stop_loss_price:
            return self._full(lot, "stop_loss")
        if policy.stop_loss_percent is not None:
            threshold = lot.entry_price * (_HUNDRED - policy.stop_loss_percent) / _HUNDRED
            if price <= threshold:
                return self._full(lot, "stop_loss")
        if policy.trailing_stop_percent is not None:
            threshold = lot.peak_price * (_HUNDRED - policy.trailing_stop_percent) / _HUNDRED
            if price <= threshold:
                return self._full(lot, "trailing_stop")

        layered_take = self._layered_take(lot, policy, price)
        if layered_take is not None:
            return layered_take
        if policy.take_profit_price is not None and price >= policy.take_profit_price:
            return self._full(lot, "take_profit")
        if policy.take_profit_percent is not None:
            threshold = lot.entry_price * (_HUNDRED + policy.take_profit_percent) / _HUNDRED
            if price >= threshold:
                return self._full(lot, "take_profit")
        return ProtectiveExitDecision(False, "no_protective_trigger", _ZERO, False)

    @staticmethod
    def _full(lot: FillRiskLot, reason: str) -> ProtectiveExitDecision:
        return ProtectiveExitDecision(True, reason, lot.remaining_shares, True)

    def _layered_stop(
        self,
        lot: FillRiskLot,
        policy: ProtectiveRiskPolicy,
        price: Decimal,
    ) -> ProtectiveExitDecision | None:
        pending = self._pending(lot, "sl:", "layered_stop_loss")
        if pending is not None:
            return pending
        for index, level in enumerate(policy.layered_stop_loss):
            level_id = f"sl:{index}"
            if level_id in lot.triggered_levels:
                continue
            threshold = lot.entry_price * (_HUNDRED - level.trigger_percent) / _HUNDRED
            if price <= threshold:
                return self._fraction(lot, "layered_stop_loss", level, level_id)
        return None

    def _layered_take(
        self,
        lot: FillRiskLot,
        policy: ProtectiveRiskPolicy,
        price: Decimal,
    ) -> ProtectiveExitDecision | None:
        pending = self._pending(lot, "tp:", "layered_take_profit")
        if pending is not None:
            return pending
        for index, level in enumerate(policy.layered_take_profit):
            level_id = f"tp:{index}"
            if level_id in lot.triggered_levels:
                continue
            threshold = lot.entry_price * (_HUNDRED + level.trigger_percent) / _HUNDRED
            if price >= threshold:
                return self._fraction(lot, "layered_take_profit", level, level_id)
        return None

    @staticmethod
    def _pending(lot: FillRiskLot, prefix: str, reason: str) -> ProtectiveExitDecision | None:
        for pending in lot.pending_exits:
            if pending.level_id.startswith(prefix):
                shares = min(pending.remaining_shares, lot.remaining_shares)
                return ProtectiveExitDecision(
                    True,
                    reason,
                    shares,
                    shares == lot.remaining_shares,
                    pending.level_id,
                )
        return None

    @staticmethod
    def _fraction(
        lot: FillRiskLot,
        reason: str,
        level: LayeredLevel,
        level_id: str,
    ) -> ProtectiveExitDecision:
        shares = lot.remaining_shares * level.exit_fraction
        return ProtectiveExitDecision(
            True,
            reason,
            shares,
            shares >= lot.remaining_shares,
            level_id,
        )


__all__ = [
    "FillRiskLot",
    "LayeredLevel",
    "ProtectiveExitDecision",
    "ProtectiveExitEngine",
    "ProtectiveRiskPolicy",
]
