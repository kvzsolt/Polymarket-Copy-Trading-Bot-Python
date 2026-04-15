"""P&L tracker — logs every trade and computes running metrics."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from utils.logger import get_logger

logger = get_logger("PnLTracker")


@dataclass
class TradeRecord:
    timestamp: float
    condition_id: str
    side: str              # BUY or SELL
    price: float
    size_tokens: float
    usdc_value: float
    leader_wallet: str = ""
    event_slug: str = ""
    title: str = ""
    # For sells
    entry_price: Optional[float] = None
    realized_pnl: Optional[float] = None
    exit_reason: Optional[str] = None


class PnLTracker:
    """Append-only P&L ledger with running metrics."""

    def __init__(self, log_path: Optional[str] = None):
        self._log_path = Path(log_path or "log/pnl.jsonl")
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._records: list[TradeRecord] = []
        self._total_invested: float = 0.0
        self._total_returned: float = 0.0
        self._peak_value: float = 0.0
        self._max_drawdown: float = 0.0

    def record_buy(
        self,
        condition_id: str,
        price: float,
        size_tokens: float,
        usdc_value: float,
        leader_wallet: str = "",
        event_slug: str = "",
        title: str = "",
    ):
        rec = TradeRecord(
            timestamp=time.time(),
            condition_id=condition_id,
            side="BUY",
            price=price,
            size_tokens=size_tokens,
            usdc_value=usdc_value,
            leader_wallet=leader_wallet,
            event_slug=event_slug,
            title=title,
        )
        self._records.append(rec)
        self._total_invested += usdc_value
        self._write(rec)

    def record_sell(
        self,
        condition_id: str,
        price: float,
        size_tokens: float,
        usdc_value: float,
        entry_price: float = 0.0,
        realized_pnl: float = 0.0,
        exit_reason: str = "",
        leader_wallet: str = "",
        event_slug: str = "",
        title: str = "",
    ):
        rec = TradeRecord(
            timestamp=time.time(),
            condition_id=condition_id,
            side="SELL",
            price=price,
            size_tokens=size_tokens,
            usdc_value=usdc_value,
            entry_price=entry_price,
            realized_pnl=realized_pnl,
            exit_reason=exit_reason,
            leader_wallet=leader_wallet,
            event_slug=event_slug,
            title=title,
        )
        self._records.append(rec)
        self._total_returned += usdc_value
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
                f.write(json.dumps(asdict(rec), default=str) + "\n")
        except Exception as e:
            logger.error(f"Failed to write P&L record: {e}")

    def metrics(self) -> dict:
        sells = [r for r in self._records if r.side == "SELL" and r.realized_pnl is not None]
        wins = [r for r in sells if (r.realized_pnl or 0) > 0]
        losses = [r for r in sells if (r.realized_pnl or 0) <= 0]
        total_pnl = sum(r.realized_pnl or 0 for r in sells)

        return {
            "total_trades": len(self._records),
            "total_buys": sum(1 for r in self._records if r.side == "BUY"),
            "total_sells": len(sells),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / max(len(sells), 1) * 100, 1),
            "total_invested": round(self._total_invested, 2),
            "total_returned": round(self._total_returned, 2),
            "net_pnl": round(total_pnl, 2),
            "max_drawdown": round(self._max_drawdown, 2),
        }

    def print_summary(self):
        m = self.metrics()
        logger.info(
            f"📊 P&L Summary: trades={m['total_trades']} "
            f"wins={m['wins']} losses={m['losses']} "
            f"win_rate={m['win_rate']}% "
            f"net_pnl=${m['net_pnl']:.2f} "
            f"max_drawdown=${m['max_drawdown']:.2f}"
        )
