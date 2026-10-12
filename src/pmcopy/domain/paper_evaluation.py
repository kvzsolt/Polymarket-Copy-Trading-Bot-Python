"""Exact, immutable paper execution and source-isolated episode accounting."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

from pmcopy.domain.auto_trader import AutoTraderValidation, CopyPerformance

ZERO = Decimal(0)


@dataclass(frozen=True, slots=True)
class PaperBook:
    """Validated current depth and required venue execution metadata."""

    asks: tuple[tuple[Decimal, Decimal], ...]
    bids: tuple[tuple[Decimal, Decimal], ...]
    tick: Decimal
    minimum: Decimal
    fee_rate: Decimal

    def __post_init__(self) -> None:
        """Reject missing metadata and malformed price levels."""
        if any(not x.is_finite() for x in (self.tick, self.minimum, self.fee_rate)):
            raise ValueError("non-finite paper metadata")
        if not 0 < self.tick < 1 or self.minimum <= 0 or not 0 <= self.fee_rate <= 1:
            raise ValueError("invalid paper metadata")
        if any(
            not p.is_finite() or not q.is_finite() or not 0 < p < 1 or q <= 0
            for p, q in (*self.asks, *self.bids)
        ):
            raise ValueError("invalid paper depth")


@dataclass(frozen=True, slots=True)
class PaperFill:
    """Aggregate depth-consumed quantity, gross value, and USDC fee."""

    shares: Decimal = ZERO
    notional: Decimal = ZERO
    fee: Decimal = ZERO

    def __post_init__(self) -> None:
        """Reject invalid financial evidence."""
        if any(not x.is_finite() or x < 0 for x in (self.shares, self.notional, self.fee)):
            raise ValueError("invalid paper fill")
        if bool(self.shares) != bool(self.notional):
            raise ValueError("paper fill requires known price")


class PaperSimulation:
    """Consume executable depth under an explicit cap with FAK semantics."""

    def fill(self, book: PaperBook, *, side: str, size: Decimal, limit: Decimal) -> PaperFill:
        """Bound BUY cash including costs, or SELL shares, without private I/O."""
        if side not in {"BUY", "SELL"} or not size.is_finite() or size <= 0:
            raise ValueError("invalid paper order")
        if not limit.is_finite() or not 0 < limit < 1 or limit % book.tick != 0:
            raise ValueError("paper limit must align with current tick")
        if (size / limit if side == "BUY" else size) < book.minimum:
            return PaperFill()
        remaining = size
        shares = notional = fees = ZERO
        levels = sorted(book.asks if side == "BUY" else book.bids, reverse=side == "SELL")
        for price, available in levels:
            if (side == "BUY" and price > limit) or (side == "SELL" and price < limit):
                break
            fee_per_share = book.fee_rate * price * (1 - price)
            amount = min(
                available, remaining / (price + fee_per_share) if side == "BUY" else remaining
            )
            amount = amount.quantize(Decimal(".000001"), rounding=ROUND_DOWN)
            fee = (amount * fee_per_share).quantize(Decimal(".00001"), rounding=ROUND_HALF_UP)
            if side == "BUY" and amount * price + fee > remaining:
                amount = max(ZERO, amount - Decimal(".00002") / price).quantize(
                    Decimal(".000001"), rounding=ROUND_DOWN
                )
                fee = (amount * fee_per_share).quantize(Decimal(".00001"), rounding=ROUND_HALF_UP)
            if amount <= 0:
                continue
            shares += amount
            notional += amount * price
            fees += fee
            remaining -= amount * price + fee if side == "BUY" else amount
            if remaining <= 0:
                break
        return PaperFill(shares, notional, fees)


@dataclass(frozen=True, slots=True)
class PaperPosition:
    """One open flat-to-flat episode including prior partial-sale cash flows."""

    token: str
    condition: str
    shares: Decimal
    net_cost: Decimal
    gross_cost: Decimal


@dataclass(frozen=True, slots=True)
class PaperLedger:
    """Durable virtual cash, inventory, costs, and bounded closed-episode evidence."""

    initial_cash: Decimal
    cash: Decimal
    started_at: datetime
    positions: tuple[PaperPosition, ...] = ()
    closed_episodes: int = 0
    realized_pnl: Decimal = ZERO
    recent_pnls: tuple[Decimal, ...] = ()
    equity: Decimal = ZERO
    peak_equity: Decimal = ZERO
    max_drawdown: Decimal = ZERO
    valuation_complete: bool = True
    evidence_complete: bool = True
    watermark: str = "initial"
    revision: int = 0

    @classmethod
    def start(cls, cash: Decimal, now: datetime) -> "PaperLedger":
        """Seed virtual funds once from an explicit recorded account allocation."""
        AutoTraderValidation.instant(now)
        if not cash.is_finite() or cash <= 0:
            raise ValueError("paper allocation must be positive")
        return cls(initial_cash=cash, cash=cash, started_at=now, peak_equity=cash, equity=cash)

    def apply(
        self, *, token: str, condition: str, side: str, fill: PaperFill, event: str
    ) -> "PaperLedger":
        """Apply one deduplicated fill; only a fully closed episode adds a sample."""
        if not token or not condition or not event or side not in {"BUY", "SELL"}:
            raise ValueError("invalid paper fill identity")
        if fill.shares == 0:
            return self
        position = next(
            (p for p in self.positions if p.token == token),
            PaperPosition(token, condition, ZERO, ZERO, ZERO),
        )
        if position.condition != condition:
            raise ValueError("paper token condition mismatch")
        if side == "SELL" and fill.shares > position.shares:
            raise ValueError("paper sell exceeds owned shares")
        value = fill.notional + fill.fee if side == "BUY" else -fill.notional + fill.fee
        cash = self.cash - value
        if cash < 0:
            raise ValueError("paper cash exhausted")
        updated = replace(
            position,
            shares=position.shares + (fill.shares if side == "BUY" else -fill.shares),
            net_cost=position.net_cost + value,
            gross_cost=(
                position.gross_cost + value
                if side == "BUY"
                else position.gross_cost * (position.shares - fill.shares) / position.shares
            ),
        )
        positions = tuple(p for p in self.positions if p.token != token)
        result = replace(
            self,
            cash=cash,
            positions=positions + ((updated,) if updated.shares else ()),
            valuation_complete=False,
        )
        if updated.shares == 0:
            pnl = -updated.net_cost
            result = replace(
                result,
                closed_episodes=self.closed_episodes + 1,
                realized_pnl=self.realized_pnl + pnl,
                recent_pnls=(*self.recent_pnls, pnl)[-20:],
                watermark=event,
            )
        if not result.positions:
            result = result.mark({})
        return result

    def mark(self, prices: Mapping[str, Decimal]) -> "PaperLedger":
        """Mark all inventory at executable liquidation values, or flag incomplete."""
        if any(
            p.token not in prices
            or not prices[p.token].is_finite()
            or not 0 <= prices[p.token] <= 1
            for p in self.positions
        ):
            return replace(self, valuation_complete=False)
        equity = self.cash + sum((p.shares * prices[p.token] for p in self.positions), ZERO)
        peak = max(self.peak_equity, equity)
        drawdown = (peak - equity) / peak if peak else ZERO
        return replace(
            self,
            equity=equity,
            peak_equity=peak,
            max_drawdown=max(self.max_drawdown, drawdown),
            valuation_complete=True,
        )

    def performance(self, now: datetime, evaluation_id: str) -> CopyPerformance:
        """Expose source-tagged evidence without mixing paper and live fills."""
        return CopyPerformance(
            evaluation_id,
            now,
            self.started_at,
            self.closed_episodes,
            self.realized_pnl,
            sum(self.recent_pnls, ZERO),
            self.max_drawdown,
            "paper:" + self.watermark,
            self.evidence_complete and self.valuation_complete,
        )
