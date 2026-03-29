from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable


@dataclass(frozen=True)
class BacktestConfig:
    initial_equity: float
    max_trade_usdc: float
    slippage_bps: int
    fee_bps: int
    latency_ms: int


@dataclass(frozen=True)
class BacktestPoint:
    ts: float
    equity: float
    cash: float
    position_tokens: float
    price: float


class BacktestEngine:
    """Simple deterministic replay engine for copy-trade signals."""

    def run(self, signals: Iterable[dict], market_data: dict[str, float], config: BacktestConfig):
        cash = Decimal(str(config.initial_equity))
        position = Decimal("0")
        equity_curve: list[BacktestPoint] = []
        fee = Decimal(config.fee_bps) / Decimal(10_000)
        slippage = Decimal(config.slippage_bps) / Decimal(10_000)

        for signal in sorted(signals, key=lambda s: float(s.get("timestamp", 0))):
            token_id = str(signal.get("token_id") or signal.get("asset") or "")
            side = str(signal.get("side") or "").upper()
            price = Decimal(str(market_data.get(token_id, signal.get("price", 0) or 0)))
            if price <= 0:
                continue

            if side == "BUY":
                requested = Decimal(str(signal.get("usdc_size", signal.get("amount", 0)) or 0))
                requested = min(requested, Decimal(str(config.max_trade_usdc)), cash)
                if requested <= 0:
                    continue
                fill_price = price * (Decimal("1") + slippage)
                tokens = requested / fill_price
                fees = requested * fee
                total_cost = requested + fees
                if total_cost > cash:
                    continue
                cash -= total_cost
                position += tokens
            elif side == "SELL":
                requested_tokens = Decimal(str(signal.get("size", signal.get("amount", 0)) or 0))
                if requested_tokens <= 0:
                    requested_tokens = position
                sell_tokens = min(requested_tokens, position)
                if sell_tokens <= 0:
                    continue
                fill_price = price * (Decimal("1") - slippage)
                proceeds = sell_tokens * fill_price
                fees = proceeds * fee
                cash += proceeds - fees
                position -= sell_tokens

            mtm_equity = cash + (position * price)
            equity_curve.append(
                BacktestPoint(
                    ts=float(signal.get("timestamp", 0)),
                    equity=float(mtm_equity),
                    cash=float(cash),
                    position_tokens=float(position),
                    price=float(price),
                )
            )

        final_equity = equity_curve[-1].equity if equity_curve else float(config.initial_equity)
        pnl = final_equity - float(config.initial_equity)
        return {
            "initial_equity": float(config.initial_equity),
            "final_equity": final_equity,
            "pnl": pnl,
            "return_pct": (pnl / float(config.initial_equity) * 100.0) if config.initial_equity else 0.0,
            "num_points": len(equity_curve),
            "equity_curve": equity_curve,
        }
