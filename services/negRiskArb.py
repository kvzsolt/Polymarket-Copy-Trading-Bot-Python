"""Neg-risk arbitrage scanner and executor — with full basket lifecycle.

Scans Polymarket markets for neg-risk arbitrage opportunities where the sum of
best ask prices for all outcomes is < 1.00 (minus fees). Buying all outcomes
locks in a guaranteed profit at resolution.

Lifecycle:
  1. SCAN → detect candidate markets with sum(asks) + fees < 1.00
  2. VALIDATE → depth-walk, fee-model, worst-price check
  3. EXECUTE → buy all legs (taker-only, concurrent)
  4. RECONCILE → check which legs filled, compute complete basket units
  5. INCOMPLETE HANDLING → cleanup partial baskets (sell back filled legs)
  6. HOLD → persist complete baskets, track to resolution
  7. SETTLE → detect resolution, redeem, record realized PnL

Basket state is persisted to SQLite so in-flight arbs survive restarts.
"""

from __future__ import annotations

import asyncio
import enum
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any, Optional

from config.env import ENV
from services.executionService import ExecutionRequest, ExecutionService
from services.feeService import FeeService
from services.marketMetaService import MarketMetaService
from services.orderRepository import OrderRepository
from utils.logger import get_logger

logger = get_logger("NegRiskArb")


# ── Basket lifecycle states ──────────────────────────────────────────────────

class BasketState(str, enum.Enum):
    PLANNED = "planned"
    EXECUTING = "executing"
    COMPLETE = "complete"             # all legs filled, basket is locked
    INCOMPLETE = "incomplete"         # some legs failed
    CLEANUP_IN_PROGRESS = "cleanup"   # selling back partial fills
    HOLDING = "holding"               # complete basket waiting for resolution
    RESOLVED = "resolved"             # market resolved, ready to redeem
    REDEEMED = "redeemed"             # redeemed, PnL realized
    ABANDONED = "abandoned"           # gave up (cleanup done or loss accepted)
    FAILED = "failed"                 # hard failure


@dataclass
class BasketLegResult:
    """Result of executing one leg of a basket."""
    token_id: str
    outcome: str
    side: str  # BUY or SELL
    accepted: bool = False
    filled_amount: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    fees_paid: Decimal = Decimal("0")
    order_id: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "token_id": self.token_id,
            "outcome": self.outcome,
            "side": self.side,
            "accepted": self.accepted,
            "filled_amount": float(self.filled_amount),
            "avg_price": float(self.avg_price) if self.avg_price else None,
            "fees_paid": float(self.fees_paid),
            "order_id": self.order_id,
            "error": self.error,
        }


@dataclass
class ArbBasket:
    """A tracked neg-risk arb basket with full lifecycle state."""
    basket_id: str                        # unique ID: f"arb:{condition_id}:{timestamp}"
    condition_id: str
    market_slug: str
    question: str
    state: BasketState = BasketState.PLANNED
    neg_risk: bool = True

    # Planning
    outcomes: list[dict] = field(default_factory=list)
    target_size_tokens: Decimal = Decimal("0")
    planned_cost_usdc: Decimal = Decimal("0")
    expected_profit_usdc: Decimal = Decimal("0")
    sum_asks: Decimal = Decimal("0")

    # Execution results per leg
    leg_results: list[BasketLegResult] = field(default_factory=list)

    # Reconciliation
    complete_units: Decimal = Decimal("0")  # min(filled across all legs)
    residuals: dict[str, Decimal] = field(default_factory=dict)  # leg_token -> excess
    total_cost_usdc: Decimal = Decimal("0")
    total_fees_usdc: Decimal = Decimal("0")

    # Cleanup
    cleanup_results: list[BasketLegResult] = field(default_factory=list)
    cleanup_proceeds_usdc: Decimal = Decimal("0")
    cleanup_loss_usdc: Decimal = Decimal("0")

    # Settlement
    resolved_at: Optional[float] = None
    winning_token_id: Optional[str] = None
    redemption_value_usdc: Decimal = Decimal("0")
    realized_pnl_usdc: Decimal = Decimal("0")

    # Metadata
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    retries_used: int = 0

    def to_dict(self) -> dict:
        return {
            "basket_id": self.basket_id,
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "question": self.question,
            "state": self.state.value,
            "neg_risk": self.neg_risk,
            "target_size_tokens": float(self.target_size_tokens),
            "planned_cost_usdc": float(self.planned_cost_usdc),
            "expected_profit_usdc": float(self.expected_profit_usdc),
            "sum_asks": float(self.sum_asks),
            "leg_results": [lr.to_dict() for lr in self.leg_results],
            "complete_units": float(self.complete_units),
            "residuals": {k: float(v) for k, v in self.residuals.items()},
            "total_cost_usdc": float(self.total_cost_usdc),
            "total_fees_usdc": float(self.total_fees_usdc),
            "cleanup_results": [cr.to_dict() for cr in self.cleanup_results],
            "cleanup_proceeds_usdc": float(self.cleanup_proceeds_usdc),
            "cleanup_loss_usdc": float(self.cleanup_loss_usdc),
            "resolved_at": self.resolved_at,
            "winning_token_id": self.winning_token_id,
            "redemption_value_usdc": float(self.redemption_value_usdc),
            "realized_pnl_usdc": float(self.realized_pnl_usdc),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "retries_used": self.retries_used,
        }


class ArbBasketStore:
    """Persist arb baskets to SQLite for crash recovery."""

    def __init__(self):
        self._in_memory: dict[str, ArbBasket] = {}

    def save(self, basket: ArbBasket) -> None:
        basket.updated_at = time.time()
        self._in_memory[basket.basket_id] = basket
        try:
            from config.db import get_database
            with get_database() as conn:
                payload = basket.to_dict()
                conn.execute(
                    """
                    INSERT INTO arb_baskets (basket_id, condition_id, state, data_json, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(basket_id) DO UPDATE SET
                      condition_id = excluded.condition_id,
                      state = excluded.state,
                      data_json = excluded.data_json,
                      updated_at = excluded.updated_at
                    """,
                    (
                        basket.basket_id,
                        basket.condition_id,
                        basket.state.value,
                        json.dumps(payload, separators=(",", ":"), default=str),
                        basket.updated_at,
                    ),
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to persist basket {basket.basket_id}: {e}")

    def load_non_terminal(self) -> list[ArbBasket]:
        """Load all baskets that are not in a terminal state (for recovery)."""
        terminal = {BasketState.REDEEMED, BasketState.ABANDONED, BasketState.FAILED}
        try:
            from config.db import get_database
            with get_database() as conn:
                rows = conn.execute(
                    f"""
                    SELECT data_json
                    FROM arb_baskets
                    WHERE state NOT IN ({",".join("?" for _ in terminal)})
                    """,
                    tuple(s.value for s in terminal),
                ).fetchall()
            baskets = []
            for row in rows:
                doc = json.loads(row["data_json"])
                b = ArbBasket(
                    basket_id=doc["basket_id"],
                    condition_id=doc["condition_id"],
                    market_slug=doc.get("market_slug", ""),
                    question=doc.get("question", ""),
                    state=BasketState(doc.get("state", "failed")),
                    neg_risk=doc.get("neg_risk", True),
                    target_size_tokens=Decimal(str(doc.get("target_size_tokens", 0))),
                    planned_cost_usdc=Decimal(str(doc.get("planned_cost_usdc", 0))),
                    expected_profit_usdc=Decimal(str(doc.get("expected_profit_usdc", 0))),
                    complete_units=Decimal(str(doc.get("complete_units", 0))),
                    total_cost_usdc=Decimal(str(doc.get("total_cost_usdc", 0))),
                    created_at=doc.get("created_at", 0),
                    updated_at=doc.get("updated_at", 0),
                )
                baskets.append(b)
                self._in_memory[b.basket_id] = b
            if baskets:
                return baskets
        except Exception as e:
            logger.warning(f"Failed to load baskets from DB: {e}")
        # Fallback to in-memory
        return [b for b in self._in_memory.values() if b.state not in terminal]

    def get_active_condition_ids(self) -> set[str]:
        terminal = {BasketState.REDEEMED, BasketState.ABANDONED, BasketState.FAILED}
        return {
            b.condition_id for b in self._in_memory.values()
            if b.state not in terminal
        }


# Global basket store
_basket_store = ArbBasketStore()


@dataclass(frozen=True)
class ArbOpportunity:
    """A detected neg-risk arbitrage opportunity."""
    condition_id: str
    market_slug: str
    question: str
    outcomes: list[dict]  # [{token_id, outcome, best_ask, ask_depth}]
    sum_asks: Decimal
    fee_cost: Decimal
    net_profit_per_token: Decimal
    optimal_size_tokens: Decimal
    total_cost_usdc: Decimal
    expected_profit_usdc: Decimal
    neg_risk: bool


@dataclass
class ArbConfig:
    """Configuration for the arb scanner."""
    min_profit_bps: int = 30  # Minimum profit in basis points (0.30%)
    max_cost_usdc: Decimal = Decimal("10")  # Max USDC per arb trade
    min_ask_depth_tokens: Decimal = Decimal("20")  # Min liquidity on each outcome
    scan_interval_seconds: int = 30
    fee_bps: Decimal = Decimal("20")  # Taker fee in bps (both sides)
    max_concurrent_arbs: int = 2
    enabled: bool = True


def _load_arb_config() -> ArbConfig:
    return ArbConfig(
        min_profit_bps=int(ENV.get("ARB_MIN_PROFIT_BPS", 30)),
        max_cost_usdc=Decimal(str(ENV.get("ARB_MAX_COST_USDC", 10))),
        min_ask_depth_tokens=Decimal(str(ENV.get("ARB_MIN_ASK_DEPTH", 20))),
        scan_interval_seconds=int(ENV.get("ARB_SCAN_INTERVAL", 30)),
        fee_bps=Decimal(str(ENV.get("TRANSACTION_FEE_BPS", 20))),
        max_concurrent_arbs=int(ENV.get("ARB_MAX_CONCURRENT", 2)),
        enabled=str(ENV.get("ENABLE_NEG_RISK_ARB", "false")).lower() in ("1", "true", "yes"),
    )


def _best_ask(order_book) -> tuple[Decimal, Decimal]:
    """Extract best ask price and total ask depth from an order book.

    Returns (best_ask_price, total_ask_depth_tokens).
    """
    asks = getattr(order_book, "asks", None) or []
    if not asks:
        return Decimal("0"), Decimal("0")

    best_price = Decimal("2")  # impossibly high
    total_depth = Decimal("0")

    for ask in asks:
        price = Decimal(str(ask.price if hasattr(ask, "price") else ask.get("price", "0")))
        size = Decimal(str(ask.size if hasattr(ask, "size") else ask.get("size", "0")))
        if price < best_price and price > 0:
            best_price = price
        total_depth += size

    return (best_price if best_price < 2 else Decimal("0")), total_depth


def _depth_within_price(asks: list[Any], price_limit: Decimal) -> Decimal:
    """Return cumulative ask depth available at prices <= price_limit."""
    depth = Decimal("0")
    for ask in asks or []:
        price = Decimal(str(ask.price if hasattr(ask, "price") else ask.get("price", "0")))
        size = Decimal(str(ask.size if hasattr(ask, "size") else ask.get("size", "0")))
        if price > 0 and price <= price_limit:
            depth += size
    return depth


def _ceil_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    units = (price / tick).to_integral_value(rounding=ROUND_CEILING)
    return units * tick


async def scan_market_for_arb(
    clob_client,
    condition_id: str,
    config: ArbConfig,
) -> ArbOpportunity | None:
    """Check a single market for neg-risk arb opportunity.
    
    Returns an ArbOpportunity if one exists, None otherwise.
    """
    loop = asyncio.get_running_loop()

    try:
        market = await loop.run_in_executor(
            None, lambda: clob_client.client.get_market(condition_id)
        )
    except Exception as e:
        logger.debug(f"Failed to fetch market {condition_id[:12]}…: {e}")
        return None

    if not market or not isinstance(market, dict):
        return None

    tokens = market.get("tokens", [])
    if len(tokens) < 2:
        return None

    neg_risk = bool(market.get("neg_risk", False))
    question = str(market.get("question", ""))
    market_slug = str(market.get("market_slug", ""))

    # Fetch order books for all outcomes
    outcomes = []
    sum_asks = Decimal("0")
    min_depth = Decimal("999999")

    for token in tokens:
        token_id = str(token.get("token_id", ""))
        outcome_name = str(token.get("outcome", ""))
        if not token_id:
            return None

        try:
            book = await loop.run_in_executor(
                None, lambda tid=token_id: clob_client.client.get_order_book(tid)
            )
        except Exception as e:
            logger.debug(f"Failed to fetch book for {outcome_name}: {e}")
            return None

        best_ask, depth = _best_ask(book)
        if best_ask <= 0:
            return None  # No asks = can't arb

        outcomes.append({
            "token_id": token_id,
            "outcome": outcome_name,
            "best_ask": best_ask,
            "ask_depth": depth,
        })
        sum_asks += best_ask
        if depth < min_depth:
            min_depth = depth

    if min_depth < config.min_ask_depth_tokens:
        return None  # Not enough liquidity

    # Calculate arb profitability using slippage-protected worst prices.
    # Revenue per token set at resolution: $1.00 (exactly one outcome wins).
    # Fees: modeled as fee_bps applied to total notional on entry (sum across legs).

    slippage_pct = Decimal(str(ENV.get("SLIPPAGE_TOLERANCE", 2.0) or 2.0)) / Decimal("100")
    slippage_pct = max(Decimal("0"), slippage_pct)
    fee_pct = (config.fee_bps / Decimal("10000"))

    sum_worst = Decimal("0")
    for o in outcomes:
        tick = Decimal(str(market.get("tick_size") or "0.01"))
        worst = _ceil_to_tick(o["best_ask"] * (Decimal("1") + slippage_pct), tick)
        o["worst_ask"] = worst
        sum_worst += worst

    total_cost_per_set = sum_worst * (Decimal("1") + fee_pct)
    net_profit_per_token = Decimal("1") - total_cost_per_set

    min_profit_threshold = Decimal(str(config.min_profit_bps)) / Decimal("10000")
    if net_profit_per_token < min_profit_threshold:
        return None  # Not profitable enough

    # Optimal size: min of (max_cost / cost_per_set, depth available within slippage band)
    if total_cost_per_set <= 0:
        return None

    max_sets_by_capital = config.max_cost_usdc / total_cost_per_set

    # Re-check depth within slippage-protected band for each leg.
    min_depth_within_band = Decimal("999999")
    for token in tokens:
        token_id = str(token.get("token_id", ""))
        try:
            book = await loop.run_in_executor(None, lambda tid=token_id: clob_client.client.get_order_book(tid))
        except Exception:
            return None
        asks = list(getattr(book, "asks", []) or [])
        # Use the precomputed worst ask for this token
        worst_ask = next((o["worst_ask"] for o in outcomes if o["token_id"] == token_id), None)
        if worst_ask is None:
            return None
        depth_band = _depth_within_price(asks, worst_ask)
        if depth_band < min_depth_within_band:
            min_depth_within_band = depth_band

    if min_depth_within_band < config.min_ask_depth_tokens:
        return None

    optimal_size = min(max_sets_by_capital, min_depth_within_band)
    # Round down to 2 decimals
    optimal_size = optimal_size.quantize(Decimal("0.01"))

    if optimal_size <= 0:
        return None

    total_cost = optimal_size * total_cost_per_set
    expected_profit = optimal_size * net_profit_per_token

    return ArbOpportunity(
        condition_id=condition_id,
        market_slug=market_slug,
        question=question,
        outcomes=outcomes,
        sum_asks=sum_asks,
        fee_cost=sum_worst * fee_pct * optimal_size,
        net_profit_per_token=net_profit_per_token,
        optimal_size_tokens=optimal_size,
        total_cost_usdc=total_cost,
        expected_profit_usdc=expected_profit,
        neg_risk=neg_risk,
    )


async def execute_arb(
    clob_client,
    opp: ArbOpportunity,
    execution_service: ExecutionService,
    slippage_bps: int = 100,
    *,
    store: ArbBasketStore | None = None,
) -> dict[str, Any]:
    """Execute a neg-risk arb with full basket lifecycle tracking.

    Creates a tracked ArbBasket, executes all legs, reconciles fills,
    handles incomplete baskets with cleanup, and persists state.
    """
    store = store or _basket_store
    user_id = str(ENV.get("FUNDER_ADDRESS") or "")
    if not user_id:
        raise RuntimeError("FUNDER_ADDRESS required for arb execution")

    # Create tracked basket
    basket = ArbBasket(
        basket_id=f"arb:{opp.condition_id}:{int(time.time())}",
        condition_id=opp.condition_id,
        market_slug=opp.market_slug,
        question=opp.question,
        state=BasketState.PLANNED,
        neg_risk=opp.neg_risk,
        outcomes=opp.outcomes,
        target_size_tokens=opp.optimal_size_tokens,
        planned_cost_usdc=opp.total_cost_usdc,
        expected_profit_usdc=opp.expected_profit_usdc,
        sum_asks=opp.sum_asks,
    )
    store.save(basket)

    # ── Execute all legs concurrently ──
    basket.state = BasketState.EXECUTING
    store.save(basket)

    leg_reqs: list[tuple[str, str, Decimal]] = []
    for outcome in opp.outcomes:
        token_id = outcome["token_id"]
        outcome_name = outcome["outcome"]
        worst_ask = Decimal(str(outcome.get("worst_ask") or outcome.get("best_ask")))
        notional = (opp.optimal_size_tokens * worst_ask).quantize(Decimal("0.0001"))
        leg_reqs.append((token_id, outcome_name, notional))

    async def _buy_leg(token_id: str, outcome_name: str, notional: Decimal):
        req = ExecutionRequest(
            user_id=user_id,
            leader_id="neg-risk-arb",
            token_id=token_id,
            side="BUY",
            amount=notional,
            max_slippage_bps=slippage_bps,
            reason="neg_risk_arb",
            signal_id=f"arb:{opp.condition_id}:{token_id}:{int(time.time())}",
        )
        return token_id, outcome_name, await execution_service.execute(req)

    raw_results = await asyncio.gather(
        *[_buy_leg(*r) for r in leg_reqs], return_exceptions=True
    )

    # ── Reconcile fills ──
    for item in raw_results:
        if isinstance(item, Exception):
            basket.leg_results.append(BasketLegResult(
                token_id="unknown", outcome="unknown", side="BUY",
                accepted=False, error=str(item),
            ))
            continue
        token_id, outcome_name, result = item
        lr = BasketLegResult(
            token_id=token_id,
            outcome=outcome_name,
            side="BUY",
            accepted=getattr(result, "accepted", False),
            filled_amount=Decimal(str(getattr(result, "filled_amount", 0) or 0)),
            avg_price=Decimal(str(result.avg_price)) if getattr(result, "avg_price", None) else None,
            fees_paid=Decimal(str(getattr(result, "fees_paid", 0) or 0)),
            order_id=getattr(result, "order_id", None),
            error=getattr(result, "error", None),
        )
        basket.leg_results.append(lr)

    # Compute complete basket units = min(filled across all legs)
    filled_amounts = []
    for lr in basket.leg_results:
        if lr.accepted and lr.filled_amount > 0:
            filled_amounts.append(lr.filled_amount)
        else:
            filled_amounts.append(Decimal("0"))

    if filled_amounts:
        basket.complete_units = min(filled_amounts)
    else:
        basket.complete_units = Decimal("0")

    # Compute residuals (excess per leg above complete_units)
    basket.residuals = {}
    for lr in basket.leg_results:
        excess = lr.filled_amount - basket.complete_units
        if excess > Decimal("0.001"):
            basket.residuals[lr.token_id] = excess

    # Total cost and fees
    basket.total_cost_usdc = sum(
        (lr.filled_amount * lr.avg_price if lr.avg_price else Decimal("0"))
        for lr in basket.leg_results
    )
    basket.total_fees_usdc = sum(lr.fees_paid for lr in basket.leg_results)

    all_legs_filled = all(
        lr.accepted and lr.filled_amount > 0 for lr in basket.leg_results
    ) and len(basket.leg_results) == len(opp.outcomes)

    # ── Determine basket state ──
    if all_legs_filled and basket.complete_units > 0:
        basket.state = BasketState.COMPLETE
        basket.realized_pnl_usdc = basket.complete_units - basket.total_cost_usdc - basket.total_fees_usdc
        logger.info(
            f"✅ ARB BASKET COMPLETE: {opp.market_slug} | "
            f"units={basket.complete_units} | cost=${basket.total_cost_usdc:.4f} | "
            f"expected_pnl=${basket.realized_pnl_usdc:.4f}"
        )
        # Transition to holding
        basket.state = BasketState.HOLDING
        store.save(basket)
    else:
        basket.state = BasketState.INCOMPLETE
        store.save(basket)

        logger.warning(
            f"⚠️ INCOMPLETE BASKET: {opp.market_slug} | "
            f"complete_units={basket.complete_units} | "
            f"filled_legs={sum(1 for lr in basket.leg_results if lr.filled_amount > 0)}/{len(opp.outcomes)}"
        )

        # ── Cleanup: sell back filled legs to flatten directional exposure ──
        cleanup_enabled = str(ENV.get("ARB_CLEANUP_ON_PARTIAL", "true")).lower() in ("1", "true", "yes")
        max_cleanup_loss_bps = int(ENV.get("ARB_MAX_CLEANUP_LOSS_BPS", 50))

        if cleanup_enabled:
            basket.state = BasketState.CLEANUP_IN_PROGRESS
            store.save(basket)

            filled_legs = [
                lr for lr in basket.leg_results
                if lr.accepted and lr.filled_amount > 0
            ]

            async def _sell_leg(lr: BasketLegResult):
                req = ExecutionRequest(
                    user_id=user_id,
                    leader_id="neg-risk-arb",
                    token_id=lr.token_id,
                    side="SELL",
                    amount=lr.filled_amount,
                    max_slippage_bps=slippage_bps,
                    reason="neg_risk_arb_cleanup",
                    signal_id=f"arb-cleanup:{opp.condition_id}:{lr.token_id}:{int(time.time())}",
                )
                return lr.token_id, lr.outcome, await execution_service.execute(req)

            cleanup_raw = await asyncio.gather(
                *[_sell_leg(lr) for lr in filled_legs], return_exceptions=True
            )

            for item in cleanup_raw:
                if isinstance(item, Exception):
                    basket.cleanup_results.append(BasketLegResult(
                        token_id="unknown", outcome="unknown", side="SELL",
                        accepted=False, error=str(item),
                    ))
                    continue
                token_id, outcome_name, result = item
                cr = BasketLegResult(
                    token_id=token_id,
                    outcome=outcome_name,
                    side="SELL",
                    accepted=getattr(result, "accepted", False),
                    filled_amount=Decimal(str(getattr(result, "filled_amount", 0) or 0)),
                    avg_price=Decimal(str(result.avg_price)) if getattr(result, "avg_price", None) else None,
                    fees_paid=Decimal(str(getattr(result, "fees_paid", 0) or 0)),
                    order_id=getattr(result, "order_id", None),
                    error=getattr(result, "error", None),
                )
                basket.cleanup_results.append(cr)
                if cr.accepted and cr.avg_price:
                    basket.cleanup_proceeds_usdc += cr.filled_amount * cr.avg_price - cr.fees_paid

            basket.cleanup_loss_usdc = basket.total_cost_usdc - basket.cleanup_proceeds_usdc
            basket.state = BasketState.ABANDONED
            store.save(basket)

            logger.warning(
                f"🧹 CLEANUP DONE: {opp.market_slug} | "
                f"cost=${basket.total_cost_usdc:.4f} | "
                f"recovered=${basket.cleanup_proceeds_usdc:.4f} | "
                f"loss=${basket.cleanup_loss_usdc:.4f}"
            )
        else:
            basket.state = BasketState.ABANDONED
            store.save(basket)

    # Build legacy-compatible summary
    summary = {
        "basket_id": basket.basket_id,
        "condition_id": opp.condition_id,
        "market_slug": opp.market_slug,
        "state": basket.state.value,
        "all_filled": all_legs_filled,
        "complete_units": float(basket.complete_units),
        "total_cost_usdc": float(basket.total_cost_usdc),
        "expected_profit_usdc": float(opp.expected_profit_usdc),
        "realized_pnl_usdc": float(basket.realized_pnl_usdc),
        "sum_asks": float(opp.sum_asks),
        "legs": {lr.outcome: lr.to_dict() for lr in basket.leg_results},
        "residuals": {k: float(v) for k, v in basket.residuals.items()},
    }

    return summary


async def arb_scanner_loop(
    clob_client,
    bot_params: dict,
    *,
    store: ArbBasketStore | None = None,
    config=None,
):
    """Background loop that scans for neg-risk arb opportunities.

    Also monitors held baskets for resolution and triggers settlement.
    """
    store = store or _basket_store
    if config is None:
        config = _load_arb_config()
    if not config.enabled:
        logger.info("Neg-risk arb scanner disabled (ENABLE_NEG_RISK_ARB=false)")
        return

    execution_service = ExecutionService(
        clob_client=clob_client,
        market_meta_service=MarketMetaService(clob_client),
        fee_service=FeeService(clob_client),
        order_repo=OrderRepository(),
        config=config,
    )
    slippage_bps = int(float(ENV.get("SLIPPAGE_TOLERANCE", 2.0)) * 100)

    logger.info(
        f"🔍 Neg-risk arb scanner started: "
        f"min_profit={config.min_profit_bps}bps, "
        f"max_cost=${config.max_cost_usdc}, "
        f"interval={config.scan_interval_seconds}s"
    )

    # Recovery: load non-terminal baskets from persistence
    recovered = store.load_non_terminal()
    if recovered:
        logger.info(f"🔄 Recovered {len(recovered)} in-flight arb baskets from persistence")
        for b in recovered:
            logger.info(f"  → {b.basket_id} state={b.state.value} units={b.complete_units}")

    while True:
        try:
            loop = asyncio.get_running_loop()

            # ── Check held baskets for resolution ──
            await _check_held_baskets_for_resolution(clob_client, store)

            # ── Try to redeem resolved baskets ──
            await _try_redeem_resolved_baskets(clob_client, store, config=config)

            # ── Scan for new opportunities ──
            active_conditions = store.get_active_condition_ids()

            try:
                markets_resp = await loop.run_in_executor(
                    None, lambda: clob_client.client.get_sampling_markets(next_cursor="")
                )
            except Exception as e:
                logger.warning(f"Failed to fetch markets: {e}")
                await asyncio.sleep(config.scan_interval_seconds)
                continue

            markets = []
            if isinstance(markets_resp, dict):
                markets = markets_resp.get("data", []) or []
            elif isinstance(markets_resp, list):
                markets = markets_resp

            scanned = 0
            found = 0

            for market in markets:
                cid = str(market.get("condition_id", ""))
                if not cid or cid in active_conditions:
                    continue

                if len(active_conditions) >= config.max_concurrent_arbs:
                    break

                opp = await scan_market_for_arb(clob_client, cid, config)
                scanned += 1

                if opp:
                    found += 1
                    logger.info(
                        f"🎯 ARB FOUND: {opp.market_slug} | "
                        f"sum_asks={opp.sum_asks:.4f} | "
                        f"profit/token=${opp.net_profit_per_token:.4f} | "
                        f"size={opp.optimal_size_tokens} | "
                        f"expected=${opp.expected_profit_usdc:.4f}"
                    )

                    try:
                        summary = await execute_arb(
                            clob_client, opp, execution_service, slippage_bps
                        )
                        if summary.get("state") == BasketState.HOLDING.value:
                            active_conditions.add(cid)
                    except Exception as e:
                        logger.error(f"Arb execution error: {e}")

                await asyncio.sleep(0.2)

            if scanned > 0:
                logger.debug(f"Arb scan: {scanned} markets checked, {found} opportunities found")

        except Exception as e:
            logger.error(f"Arb scanner loop error: {e}")

        await asyncio.sleep(config.scan_interval_seconds)


async def _check_held_baskets_for_resolution(clob_client, store: ArbBasketStore) -> None:
    """Check if any HOLDING baskets have resolved and can be settled."""
    held = [b for b in store._in_memory.values() if b.state == BasketState.HOLDING]
    if not held:
        return

    loop = asyncio.get_running_loop()

    for basket in held:
        try:
            market = await loop.run_in_executor(
                None, lambda cid=basket.condition_id: clob_client.client.get_market(cid)
            )
            if not market or not isinstance(market, dict):
                continue

            # Check if market is resolved
            is_resolved = (
                str(market.get("closed", "")).lower() == "true"
                or str(market.get("resolved", "")).lower() == "true"
                or market.get("end_date_iso") is not None
            )

            if is_resolved:
                basket.state = BasketState.RESOLVED
                basket.resolved_at = time.time()

                # Try to identify winning token
                tokens = market.get("tokens", [])
                for token in tokens:
                    winner = token.get("winner", False)
                    if winner:
                        basket.winning_token_id = str(token.get("token_id", ""))
                        break

                # Calculate realized PnL
                if basket.complete_units > 0:
                    basket.redemption_value_usdc = basket.complete_units  # $1 per unit
                    basket.realized_pnl_usdc = (
                        basket.redemption_value_usdc
                        - basket.total_cost_usdc
                        - basket.total_fees_usdc
                    )

                store.save(basket)
                logger.info(
                    f"🏁 BASKET RESOLVED: {basket.market_slug} | "
                    f"winner={basket.winning_token_id} | "
                    f"redemption=${basket.redemption_value_usdc:.4f} | "
                    f"realized_pnl=${basket.realized_pnl_usdc:.4f}"
                )

        except Exception as e:
            logger.debug(f"Resolution check failed for {basket.basket_id}: {e}")


async def _try_redeem_resolved_baskets(
    clob_client,
    store: ArbBasketStore,
    redemption_service=None,
    *,
    config=None,
) -> None:
    """Attempt onchain CTF redemption for resolved baskets.

    Uses the existing RedemptionService infrastructure.
    Handles the critical nuance: redeemPositions() burns the ENTIRE condition
    balance — so if multiple baskets hold the same condition, we must do
    pro-rata lot allocation of the redeemed USDC.
    """
    resolved = [b for b in store._in_memory.values() if b.state == BasketState.RESOLVED]
    if not resolved:
        return

    # Lazy-load redemption service if not provided
    if redemption_service is None:
        try:
            from services.redemptionService import RedemptionService
            redemption_service = RedemptionService(clob_client, config=config)
        except Exception as e:
            logger.warning(f"Cannot load RedemptionService: {e}")
            return

    user_id = str(ENV.get("FUNDER_ADDRESS") or "")
    if not user_id:
        return

    # ── Cross-basket lot accounting ──
    # Group resolved baskets by condition_id, since redeemPositions() burns
    # the ENTIRE balance for a condition — not per-basket.
    condition_baskets: dict[str, list[ArbBasket]] = {}
    for basket in resolved:
        condition_baskets.setdefault(basket.condition_id, []).append(basket)

    for condition_id, baskets in condition_baskets.items():
        # Total complete units across all baskets for this condition
        total_units = sum(b.complete_units for b in baskets)
        if total_units <= 0:
            continue

        try:
            # Scan for redeemable positions for this user
            redeemables = await redemption_service.scan_redeemables(user_id)
            matching = [
                r for r in redeemables
                if r.condition_id == condition_id
            ]

            if not matching:
                logger.debug(f"No redeemable positions found for condition {condition_id[:12]}…")
                continue

            # Submit redemption (burns entire condition balance)
            for pos in matching:
                result = await redemption_service.redeem(pos)

                if result.submitted and result.status in ("submitted", "confirmed"):
                    redeemed_usdc = result.redeemed_amount

                    logger.info(
                        f"💰 CTF REDEEMED: condition={condition_id[:12]}… | "
                        f"amount=${redeemed_usdc:.4f} | tx={result.tx_hash}"
                    )

                    # Pro-rata allocation across baskets sharing this condition
                    for basket in baskets:
                        if total_units > 0:
                            basket_share = basket.complete_units / total_units
                        else:
                            basket_share = Decimal("0")

                        basket_redeemed = redeemed_usdc * basket_share
                        basket.redemption_value_usdc = basket_redeemed
                        basket.realized_pnl_usdc = (
                            basket_redeemed
                            - basket.total_cost_usdc
                            - basket.total_fees_usdc
                        )
                        basket.state = BasketState.REDEEMED
                        store.save(basket)

                        logger.info(
                            f"📊 BASKET SETTLED: {basket.basket_id} | "
                            f"share={float(basket_share)*100:.1f}% | "
                            f"redeemed=${basket_redeemed:.4f} | "
                            f"pnl=${basket.realized_pnl_usdc:.4f}"
                        )

                    if len(baskets) > 1:
                        logger.info(
                            f"🔄 CROSS-BASKET LOT ALLOCATION: condition={condition_id[:12]}… | "
                            f"total_redeemed=${redeemed_usdc:.4f} | "
                            f"allocated_across={len(baskets)} baskets"
                        )

                elif result.error:
                    logger.warning(
                        f"Redemption failed: condition={condition_id[:12]}… | "
                        f"error={result.error}"
                    )

        except Exception as e:
            logger.error(f"Redemption attempt failed for condition {condition_id[:12]}…: {e}")
