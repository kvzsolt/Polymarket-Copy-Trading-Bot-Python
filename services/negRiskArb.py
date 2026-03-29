"""Neg-risk arbitrage scanner and executor.

Scans Polymarket markets for neg-risk arbitrage opportunities where the sum of
best ask prices for all outcomes is < 1.00 (minus fees). Buying all outcomes
locks in a guaranteed profit at resolution.

How it works:
- For each market, fetch the order book for every outcome token
- If sum(best_ask_prices) + total_fees < 1.00, that's an arb
- Buy the minimum viable size of each outcome
- At resolution, exactly one outcome pays $1.00 per token → guaranteed profit

Requirements:
- Works best on neg-risk markets (Polymarket handles complement pricing)
- Also works on standard binary markets (YES + NO)
- Fee-aware: accounts for maker/taker fees on both entry and redemption
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING
from typing import Any

from config.env import ENV
from services.executionService import ExecutionRequest, ExecutionService
from services.feeService import FeeService
from services.marketMetaService import MarketMetaService
from services.orderRepository import OrderRepository
from utils.logger import get_logger

logger = get_logger("NegRiskArb")


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

    # Calculate arb profitability
    # Cost per token set: sum of all outcome asks
    # Revenue per token set at resolution: $1.00 (one outcome wins)
    # Fees: taker fee on each buy leg + no fee on redemption
    num_legs = len(outcomes)
    fee_per_leg = config.fee_bps / Decimal("10000")
    total_fee_pct = fee_per_leg * num_legs  # Fee on buying all legs
    
    # Net profit per token set = 1.00 - sum_asks - (sum_asks * total_fee_pct)
    total_cost_per_set = sum_asks * (Decimal("1") + total_fee_pct)
    net_profit_per_token = Decimal("1") - total_cost_per_set

    min_profit_threshold = Decimal(str(config.min_profit_bps)) / Decimal("10000")
    if net_profit_per_token < min_profit_threshold:
        return None  # Not profitable enough

    # Optimal size: min of (max_cost / cost_per_set, min_depth across all books)
    if total_cost_per_set > 0:
        max_sets_by_capital = config.max_cost_usdc / total_cost_per_set
    else:
        return None

    optimal_size = min(max_sets_by_capital, min_depth)
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
        fee_cost=sum_asks * total_fee_pct * optimal_size,
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
) -> dict[str, Any]:
    """Execute a neg-risk arb by buying all outcomes simultaneously.
    
    Returns a summary dict with results per leg.
    """
    results = {}
    total_cost = Decimal("0")
    all_filled = True

    for outcome in opp.outcomes:
        token_id = outcome["token_id"]
        outcome_name = outcome["outcome"]
        # Notional per leg = optimal_size * best_ask (we're buying tokens)
        notional = opp.optimal_size_tokens * outcome["best_ask"]
        
        req = ExecutionRequest(
            user_id=str(ENV.get("FUNDER_ADDRESS") or ENV.get("PROXY_WALLET") or ""),
            leader_id="neg-risk-arb",
            token_id=token_id,
            side="BUY",
            amount=notional,
            max_slippage_bps=slippage_bps,
            reason="neg_risk_arb",
            signal_id=f"arb:{opp.condition_id}:{token_id}:{int(time.time())}",
        )

        result = await execution_service.execute(req)
        results[outcome_name] = {
            "token_id": token_id,
            "accepted": result.accepted,
            "status": result.status,
            "filled_amount": float(result.filled_amount),
            "avg_price": float(result.avg_price) if result.avg_price else None,
            "fees_paid": float(result.fees_paid),
            "error": result.error,
        }

        if result.accepted and result.filled_amount > 0:
            total_cost += result.filled_amount * (result.avg_price or Decimal("0")) + result.fees_paid
        else:
            all_filled = False
            logger.warning(
                f"Arb leg FAILED for {outcome_name}: {result.error or result.status}"
            )

    summary = {
        "condition_id": opp.condition_id,
        "market_slug": opp.market_slug,
        "all_filled": all_filled,
        "total_cost_usdc": float(total_cost),
        "expected_profit_usdc": float(opp.expected_profit_usdc),
        "sum_asks": float(opp.sum_asks),
        "legs": results,
    }

    if all_filled:
        logger.info(
            f"✅ ARB EXECUTED: {opp.market_slug} | "
            f"cost=${total_cost:.4f} | expected_profit=${opp.expected_profit_usdc:.4f} | "
            f"sum_asks={opp.sum_asks:.4f}"
        )
    else:
        logger.warning(
            f"⚠️ PARTIAL ARB: {opp.market_slug} — some legs failed. "
            f"This creates RISK (unhedged exposure)."
        )

    return summary


async def arb_scanner_loop(clob_client, bot_params: dict):
    """Background loop that scans for neg-risk arb opportunities.
    
    Fetches active markets, checks each for arb, and executes if profitable.
    """
    config = _load_arb_config()
    if not config.enabled:
        logger.info("Neg-risk arb scanner disabled (ENABLE_NEG_RISK_ARB=false)")
        return

    execution_service = ExecutionService(
        clob_client=clob_client,
        market_meta_service=MarketMetaService(clob_client),
        fee_service=FeeService(clob_client),
        order_repo=OrderRepository(),
    )
    slippage_bps = int(float(ENV.get("SLIPPAGE_TOLERANCE", 2.0)) * 100)

    logger.info(
        f"🔍 Neg-risk arb scanner started: "
        f"min_profit={config.min_profit_bps}bps, "
        f"max_cost=${config.max_cost_usdc}, "
        f"interval={config.scan_interval_seconds}s"
    )

    # Track active arbs to avoid double-entry
    active_arbs: set[str] = set()

    while True:
        try:
            loop = asyncio.get_running_loop()

            # Fetch active markets from CLOB
            try:
                markets_resp = await loop.run_in_executor(
                    None, lambda: clob_client.client.get_sampling_markets(next_cursor="")
                )
            except Exception as e:
                logger.warning(f"Failed to fetch markets: {e}")
                await asyncio.sleep(config.scan_interval_seconds)
                continue

            # Parse markets list
            markets = []
            if isinstance(markets_resp, dict):
                markets = markets_resp.get("data", []) or []
            elif isinstance(markets_resp, list):
                markets = markets_resp

            scanned = 0
            found = 0

            for market in markets:
                cid = str(market.get("condition_id", ""))
                if not cid or cid in active_arbs:
                    continue

                if len(active_arbs) >= config.max_concurrent_arbs:
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

                    active_arbs.add(cid)
                    try:
                        summary = await execute_arb(
                            clob_client, opp, execution_service, slippage_bps
                        )
                        if not summary.get("all_filled"):
                            # Remove from active so we don't block re-attempts
                            active_arbs.discard(cid)
                    except Exception as e:
                        logger.error(f"Arb execution error: {e}")
                        active_arbs.discard(cid)

                # Small delay between market checks to avoid rate limits
                await asyncio.sleep(0.2)

            if scanned > 0:
                logger.debug(f"Arb scan: {scanned} markets checked, {found} opportunities found")

        except Exception as e:
            logger.error(f"Arb scanner loop error: {e}")

        await asyncio.sleep(config.scan_interval_seconds)
