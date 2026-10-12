"""Task-specific copied-price execution policy.

The service translates immutable copy-task settings into the shared entry
admission engine. It owns no market data or venue I/O; the caller supplies an
authoritative reference price and the venue adapter performs final tick-aware
quantization immediately before signing.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    PriceOffsetDirection,
    PriceOffsetMode,
)
from pmcopy.services.execution.entry_admission import (
    EntryAdmissionEngine,
    EntryPolicy,
    EntryPriceBandPolicy,
    OffsetDirection,
    OffsetMode,
    TraderPriceOffsetPolicy,
)

_ZERO = Decimal("0")
_HUNDRED = Decimal("100")


@dataclass(frozen=True, slots=True)
class TaskEntryPriceDecision:
    """Explicit marketable cap/floor derived from one task policy."""

    allowed: bool
    reason: str
    order_type: str = "FAK"
    price_limit: Decimal | None = None
    reference_price: Decimal | None = None


class CopyTaskEntryPriceService:
    """Derive one explicit task-specific FAK cap or floor."""

    def evaluate(
        self,
        *,
        task: CopyTask,
        follower_action: str,
        source_price: Decimal,
        execution_reference_price: Decimal | None = None,
        counter_trade: bool = False,
    ) -> TaskEntryPriceDecision:
        """Return an execution bound without performing any I/O.

        Args:
            task: Immutable task whose price policy controls the decision.
            follower_action: Follower ``BUY`` or ``SELL`` action.
            source_price: Price observed on the source trader's outcome.
            execution_reference_price: Optional authoritative price for the
                follower outcome. It is mandatory for counter trades because
                the opposite outcome cannot safely be priced as ``1 - price``.
            counter_trade: Whether the follower acts on the opposite outcome.

        Returns:
            An immutable decision with an explicit FAK cap/floor when allowed.
        """

        action = str(follower_action).strip().upper()
        if action not in {"BUY", "SELL"}:
            return self._reject("invalid_follower_action")

        try:
            source = self._decimal(source_price, name="source_price")
        except ValueError:
            return self._reject("invalid_source_price")

        if counter_trade and execution_reference_price is None:
            return self._reject("counter_trade_execution_price_missing")
        try:
            reference = self._decimal(
                execution_reference_price if execution_reference_price is not None else source,
                name="execution_reference_price",
            )
        except ValueError:
            return self._reject("invalid_execution_reference_price")

        policy = task.price
        if not policy.enabled:
            if not _ZERO < reference < Decimal("1"):
                return self._reject("invalid_execution_reference_price")
            return TaskEntryPriceDecision(
                allowed=True,
                reason="task_price_policy_disabled",
                price_limit=reference,
                reference_price=reference,
            )

        engine_policy = self._engine_policy(task)
        decision = EntryAdmissionEngine(engine_policy).evaluate(
            side=action,
            copied_trader_price=reference,
        )
        if not decision.allowed:
            return self._reject(decision.reason, reference=reference)

        if self._offset_exceeds_slippage(
            task=task,
            reference=reference,
            limit=decision.price_limit,
        ):
            return self._reject(
                "task_price_offset_exceeds_slippage",
                reference=reference,
            )
        return TaskEntryPriceDecision(
            allowed=True,
            reason="task_entry_price_allowed",
            order_type=decision.order_type,
            price_limit=decision.price_limit,
            reference_price=reference,
        )

    @staticmethod
    def _engine_policy(task: CopyTask) -> EntryPolicy:
        """Translate one task policy into the shared entry engine policy."""

        price = task.price
        if price.offset_mode is PriceOffsetMode.DISABLED:
            offset = TraderPriceOffsetPolicy(
                enabled=price.max_slippage_percent > 0,
                mode=OffsetMode.PERCENTAGE,
                direction=OffsetDirection.MARKETABLE,
                value=price.max_slippage_percent,
            )
        else:
            offset = TraderPriceOffsetPolicy(
                enabled=True,
                mode=(
                    OffsetMode.PERCENTAGE
                    if price.offset_mode is PriceOffsetMode.PERCENTAGE
                    else OffsetMode.FIXED
                ),
                direction=(
                    OffsetDirection.ABOVE
                    if price.offset_direction is PriceOffsetDirection.ABOVE
                    else OffsetDirection.BELOW
                ),
                value=price.offset,
            )
        return EntryPolicy(
            enabled=True,
            price_band=EntryPriceBandPolicy(
                enabled=False,
                minimum=price.min_price,
                maximum=price.max_price,
            ),
            offset=offset,
        )

    @staticmethod
    def _offset_exceeds_slippage(
        *,
        task: CopyTask,
        reference: Decimal,
        limit: Decimal | None,
    ) -> bool:
        """Return whether an explicit price-match offset exceeds its safety ceiling."""

        if task.price.offset_mode is PriceOffsetMode.DISABLED:
            return False
        ceiling = task.price.max_slippage_percent
        if ceiling <= 0 or limit is None:
            return False
        deviation = abs(limit - reference) / reference * _HUNDRED
        return deviation > ceiling

    @staticmethod
    def _decimal(value: object, *, name: str) -> Decimal:
        """Convert one boundary value to a finite probability price."""

        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be decimal-like") from exc
        if not result.is_finite() or not _ZERO < result < Decimal("1"):
            raise ValueError(f"{name} must be in (0, 1)")
        return result

    @staticmethod
    def _reject(
        reason: str,
        *,
        reference: Decimal | None = None,
    ) -> TaskEntryPriceDecision:
        """Create one fail-closed price decision."""

        return TaskEntryPriceDecision(
            allowed=False,
            reason=reason,
            price_limit=None,
            reference_price=reference,
        )


__all__ = ["CopyTaskEntryPriceService", "TaskEntryPriceDecision"]
