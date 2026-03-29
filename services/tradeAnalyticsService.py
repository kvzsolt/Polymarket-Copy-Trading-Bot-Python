from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from services.metrics import metrics
from utils.logger import get_logger

logger = get_logger("TradeAnalyticsService")


@dataclass
class TradeAnalyticsSnapshot:
    total_signals: int = 0
    approved_signals: int = 0
    executed_trades: int = 0
    skipped_signals: int = 0
    avg_quality_score: float = 0.0
    avg_slippage_bps: float = 0.0
    by_reason: dict[str, int] = field(default_factory=dict)


class TradeAnalyticsService:
    """Collect post-decision/post-trade quality analytics for copied signals."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._quality_sum = 0.0
        self._slippage_sum = 0.0
        self._snapshot = TradeAnalyticsSnapshot()

    async def record_decision(self, *, decision, trade: dict) -> None:
        async with self._lock:
            self._snapshot.total_signals += 1
            if decision.allow:
                self._snapshot.approved_signals += 1
            else:
                self._snapshot.skipped_signals += 1
                self._snapshot.by_reason[decision.skip_code] = (
                    self._snapshot.by_reason.get(decision.skip_code, 0) + 1
                )

            self._quality_sum += float(decision.decision_score or 0.0)
            self._snapshot.avg_quality_score = self._quality_sum / max(self._snapshot.total_signals, 1)

        await metrics.set_gauge("copy_quality_avg_score", self._snapshot.avg_quality_score)

        logger.info(
            "ANALYTICS decision | allow=%s | code=%s | score=%.4f | side=%s",
            decision.allow,
            decision.skip_code,
            float(decision.decision_score or 0.0),
            trade.get("side"),
            extra={
                "type": "trade_quality_decision",
                "allow": bool(decision.allow),
                "skipCode": str(decision.skip_code),
                "decisionScore": float(decision.decision_score or 0.0),
                "leaderScore": float(decision.leader_score or 0.0),
                "tradeScore": float(decision.trade_score or 0.0),
                "conditionId": trade.get("conditionId"),
                "asset": trade.get("asset") or trade.get("tokenID") or trade.get("token_id"),
            },
        )

    async def record_execution(
        self,
        *,
        decision,
        trade: dict,
        fill_price: float,
        result: str,
    ) -> None:
        side = str(trade.get("side", "")).upper()
        ref_price = float(trade.get("price", 0) or 0.0)

        slippage_bps = 0.0
        if ref_price > 0 and fill_price > 0:
            raw = (fill_price - ref_price) / ref_price
            # For SELL, lower copy fill is worse, so invert sign for "cost" semantics.
            if side == "SELL":
                raw = -raw
            slippage_bps = raw * 10_000.0

        async with self._lock:
            if result == "executed":
                self._snapshot.executed_trades += 1
            self._slippage_sum += slippage_bps
            denom = max(self._snapshot.executed_trades, 1)
            self._snapshot.avg_slippage_bps = self._slippage_sum / denom
            if result != "executed":
                self._snapshot.by_reason[result] = self._snapshot.by_reason.get(result, 0) + 1

        await metrics.set_gauge("copy_quality_avg_slippage_bps", self._snapshot.avg_slippage_bps)

        logger.info(
            "ANALYTICS execution | result=%s | slippage_bps=%.2f | side=%s",
            result,
            slippage_bps,
            side,
            extra={
                "type": "trade_quality_execution",
                "result": result,
                "slippageBps": slippage_bps,
                "decisionScore": float(decision.decision_score or 0.0),
                "leaderScore": float(decision.leader_score or 0.0),
                "tradeScore": float(decision.trade_score or 0.0),
                "conditionId": trade.get("conditionId"),
                "asset": trade.get("asset") or trade.get("tokenID") or trade.get("token_id"),
            },
        )

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "total_signals": self._snapshot.total_signals,
                "approved_signals": self._snapshot.approved_signals,
                "executed_trades": self._snapshot.executed_trades,
                "skipped_signals": self._snapshot.skipped_signals,
                "avg_quality_score": self._snapshot.avg_quality_score,
                "avg_slippage_bps": self._snapshot.avg_slippage_bps,
                "by_reason": dict(self._snapshot.by_reason),
            }

