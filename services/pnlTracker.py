"""P&L tracker — logs every trade and computes running metrics.

All money/size/price values are kept as ``decimal.Decimal`` end-to-end.
The public API accepts ``float`` for legacy callers but coerces via
``Decimal(str(x))`` at the boundary so the IEEE-754 representation of
e.g. ``0.1`` never lands in the ledger. Per CLAUDE.md non-negotiable:
no floats in money/size/price/notional/PnL arithmetic.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional, Union

from utils.logger import get_logger

logger = get_logger("PnLTracker")

# Accept anything an external caller might still pass — int/float/str all
# round-trip safely through ``Decimal(str(x))``.
Number = Union[Decimal, float, int, str]

_ZERO = Decimal("0")


def _to_decimal(value: Optional[Number]) -> Decimal:
    """Coerce *value* to ``Decimal`` losslessly.

    ``None`` collapses to zero so legacy callers passing missing fields
    do not crash the ledger. Floats round-trip via ``str()`` so
    ``Decimal(0.1)`` precision traps do not contaminate the running totals.
    """
    if value is None:
        return _ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass
class TradeRecord:
    timestamp: float
    condition_id: str
    side: str  # BUY or SELL
    price: Decimal
    size_tokens: Decimal
    usdc_value: Decimal
    leader_wallet: str = ""
    event_slug: str = ""
    title: str = ""
    # For sells
    entry_price: Optional[Decimal] = None
    realized_pnl: Optional[Decimal] = None
    exit_reason: Optional[str] = None


class PnLTracker:
    """Append-only P&L ledger with running metrics (all Decimal)."""

    def __init__(self, log_path: Optional[str] = None):
        self._log_path = Path(log_path or "log/pnl.jsonl")
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        # The ledger is in-memory, so every metric it reports is scoped to this
        # object's lifetime. Recording that instant lets an operator read state
        # "since when" instead of implying an all-time total.
        self._started_at = datetime.now(tz=timezone.utc)
        self._records: list[TradeRecord] = []
        self._total_invested: Decimal = _ZERO
        self._total_returned: Decimal = _ZERO
        self._peak_value: Decimal = _ZERO
        self._max_drawdown: Decimal = _ZERO

    @property
    def started_at(self) -> datetime:
        """Return when this in-memory ledger began recording."""
        return self._started_at

    def record_buy(
        self,
        condition_id: str,
        price: Number,
        size_tokens: Number,
        usdc_value: Number,
        leader_wallet: str = "",
        event_slug: str = "",
        title: str = "",
    ):
        usdc = _to_decimal(usdc_value)
        rec = TradeRecord(
            timestamp=time.time(),
            condition_id=condition_id,
            side="BUY",
            price=_to_decimal(price),
            size_tokens=_to_decimal(size_tokens),
            usdc_value=usdc,
            leader_wallet=leader_wallet,
            event_slug=event_slug,
            title=title,
        )
        self._records.append(rec)
        self._total_invested += usdc
        self._write(rec)

    def record_sell(
        self,
        condition_id: str,
        price: Number,
        size_tokens: Number,
        usdc_value: Number,
        entry_price: Number = 0,
        realized_pnl: Number = 0,
        exit_reason: str = "",
        leader_wallet: str = "",
        event_slug: str = "",
        title: str = "",
    ):
        usdc = _to_decimal(usdc_value)
        rec = TradeRecord(
            timestamp=time.time(),
            condition_id=condition_id,
            side="SELL",
            price=_to_decimal(price),
            size_tokens=_to_decimal(size_tokens),
            usdc_value=usdc,
            entry_price=_to_decimal(entry_price),
            realized_pnl=_to_decimal(realized_pnl),
            exit_reason=exit_reason,
            leader_wallet=leader_wallet,
            event_slug=event_slug,
            title=title,
        )
        self._records.append(rec)
        self._total_returned += usdc
        self._update_drawdown()
        self._write(rec)

    def _update_drawdown(self):
        net = self._total_returned - self._total_invested
        if net > self._peak_value:
            self._peak_value = net
        drawdown = self._peak_value - net
        if drawdown > self._max_drawdown:
            self._max_drawdown = drawdown

    def _write(self, rec: TradeRecord):
        try:
            with open(self._log_path, "a") as f:
                # default=str renders Decimal as its exact string form so
                # JSON consumers see "0.10" instead of the float repr.
                f.write(json.dumps(asdict(rec), default=str) + "\n")
        except Exception as e:
            logger.error("Failed to write P&L record: %r", e)

    def metrics(self) -> dict:
        sells = [r for r in self._records if r.side == "SELL" and r.realized_pnl is not None]
        wins = [r for r in sells if (r.realized_pnl or _ZERO) > _ZERO]
        losses = [r for r in sells if (r.realized_pnl or _ZERO) <= _ZERO]
        total_pnl = sum((r.realized_pnl or _ZERO for r in sells), start=_ZERO)

        # Round display values to 2dp but the underlying accumulators
        # stay at full Decimal precision for the next call.
        two = Decimal("0.01")
        return {
            "total_trades": len(self._records),
            "total_buys": sum(1 for r in self._records if r.side == "BUY"),
            "total_sells": len(sells),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": float(
                (Decimal(len(wins)) / Decimal(max(len(sells), 1)) * Decimal(100)).quantize(
                    Decimal("0.1")
                )
            ),
            "total_invested": self._total_invested.quantize(two),
            "total_returned": self._total_returned.quantize(two),
            "net_pnl": total_pnl.quantize(two),
            "max_drawdown": self._max_drawdown.quantize(two),
        }

    def print_summary(self):
        m = self.metrics()
        logger.info(
            "📊 P&L Summary: trades=%s wins=%s losses=%s win_rate=%s%% "
            "net_pnl=$%s max_drawdown=$%s",
            m["total_trades"],
            m["wins"],
            m["losses"],
            m["win_rate"],
            m["net_pnl"],
            m["max_drawdown"],
        )
