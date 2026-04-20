"""Legacy trade executor helpers retained for backward-compatible tests.

These functions are **not** used by the active pipeline (see
``orchestrators/trade_pipeline.py`` and ``services/copyStrategy.py``).
They are kept solely so the existing unit-test suite continues to pass.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from config.env import ENV
from utils.getMyBalance import get_my_balance
from utils.fetchData import fetch_data
from utils.logger import log_trade_execution, log_trade_skip
from services.copyDecisionService import CopyDecisionService

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Position-manager stub (only needed for adaptive sizing in tests)
# ---------------------------------------------------------------------------
_position_manager = None


def get_position_manager():
    return _position_manager


# ---------------------------------------------------------------------------
# _calc_buy_amount
# ---------------------------------------------------------------------------
def _calc_buy_amount(
    trade: dict,
    ratio: float,
    buy_min: float,
    buy_max: float,
) -> float:
    """Compute the buy amount in USDC.

    ``ratio`` is the copy percentage expressed as a fraction (e.g. 0.05 = 5%).
    """
    sizing_mode = ENV.get("SIZING_MODE", "proportional")
    fixed_amount = float(ENV.get("FIXED_AMOUNT", 10))

    # --- Determine leader notional -----------------------------------------
    raw = trade.get("usdcSize")
    try:
        leader_notional = float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        leader_notional = 0.0

    if leader_notional <= 0:
        size = float(trade.get("size", 0) or 0)
        price = float(trade.get("price", 0) or 0)
        leader_notional = size * price

    # --- Strategy dispatch -------------------------------------------------
    if sizing_mode == "fixed":
        proxy = fixed_amount
    elif sizing_mode == "adaptive":
        pm = get_position_manager()
        summary = pm.summary() if pm else {}
        win_rate = float(summary.get("win_rate", 50)) / 100.0
        multiplier = 0.5 + win_rate * 1.5
        proxy = leader_notional * ratio * multiplier
    else:
        # proportional (default)
        proxy = leader_notional * ratio

    # --- Tiered multiplier (optional) -------------------------------------
    tiered_raw = ENV.get("TIERED_TRADE_MULTIPLIERS", "")
    if tiered_raw:
        from services.copyStrategy import parse_tiered_multipliers, get_trade_multiplier
        tiers = parse_tiered_multipliers(tiered_raw)
        mult = get_trade_multiplier(leader_notional_usdc=leader_notional, tiered_multipliers=tiers)
        proxy *= float(mult)

    # --- Clamp ------------------------------------------------------------
    effective_min = max(buy_min, 1.0)  # $1 absolute floor
    proxy = max(proxy, effective_min)
    proxy = min(proxy, buy_max)
    return proxy


# ---------------------------------------------------------------------------
# _apply_balance_cap
# ---------------------------------------------------------------------------
def _apply_balance_cap(amount: float, balance: float) -> float:
    """Cap *amount* to ``MAX_POSITION_PCT`` of *balance*.

    If the resulting cap < $1 the original amount is returned (small-balance
    passthrough).
    """
    max_pct = float(ENV.get("MAX_POSITION_PCT", 0.25))
    cap = balance * max_pct
    if cap < 1.0:
        return amount
    return min(amount, cap)


# ---------------------------------------------------------------------------
# _min_valid_buy_notional_usdc
# ---------------------------------------------------------------------------
def _min_valid_buy_notional_usdc(
    live_price: float,
    market_min_size: float = 0.0,
) -> float:
    """Smallest notional that can produce a valid buy order.

    Takes the larger of 5 tokens or *market_min_size*, then prices it at the
    worst-case slippage-adjusted price.
    """
    slippage_pct = float(ENV.get("SLIPPAGE_TOLERANCE", 2.0))
    worst_price = live_price * (1.0 + slippage_pct / 100.0)
    min_tokens = max(5.0, market_min_size)
    return min_tokens * worst_price


# ---------------------------------------------------------------------------
# _maybe_boost_tiny_leader_buy_cap
# ---------------------------------------------------------------------------
def _maybe_boost_tiny_leader_buy_cap(
    *,
    leader_notional: float,
    min_leader_trade_notional: float,
    decision_score: float,
    raw_amount: float,
    shadow_cash: float,
    live_price: float,
    market_min_size: float = 0.0,
    high_confidence_score: float = 0.75,
) -> float:
    """If the leader trade is tiny *and* our confidence is high, boost the
    order to the minimum valid buy notional so it actually fills."""
    if leader_notional >= min_leader_trade_notional:
        return raw_amount
    if decision_score < high_confidence_score:
        return raw_amount

    min_notional = _min_valid_buy_notional_usdc(live_price, market_min_size)
    max_pct = float(ENV.get("MAX_POSITION_PCT", 0.25))
    cap = shadow_cash * max_pct
    boosted = min(min_notional, cap)
    return max(boosted, raw_amount)


# ---------------------------------------------------------------------------
# Module-level singletons (patched in tests)
# ---------------------------------------------------------------------------
_copy_decision = CopyDecisionService()
_execution_service = None


def _get_execution_service():
    return _execution_service


# ---------------------------------------------------------------------------
# execute_trades — legacy trade executor (full pipeline in one function)
# ---------------------------------------------------------------------------
async def execute_trades(
    clob_client: Any,
    bot_params: dict,
    trades: list[dict],
) -> None:
    """Execute a batch of copy trades.

    This is the old monolithic executor retained for backward-compatible tests.
    The production pipeline lives in ``orchestrators/trade_pipeline.py``.
    """
    if not trades:
        return

    wallet_raw = ENV.get("FUNDER_ADDRESS", "")
    wallet = wallet_raw if isinstance(wallet_raw, str) else ""
    if not wallet.strip():
        logger.error("legacy_executor_missing_funder_address", extra={"env_key": "FUNDER_ADDRESS"})
        raise ValueError("FUNDER_ADDRESS is required for legacy trade execution")

    balance = await get_my_balance(wallet)
    if balance is None:
        logger.error(
            "legacy_executor_balance_lookup_failed",
            extra={"env_key": "FUNDER_ADDRESS", "wallet": wallet},
        )
        raise ValueError("Unable to fetch balance for FUNDER_ADDRESS")

    ratio = bot_params.get("tradeRate", 5) / 100.0
    buy_min = bot_params.get("buyMinAmount", 1)
    buy_max = bot_params.get("buyMaxAmount", 50)

    exec_svc = _get_execution_service()

    for trade in trades:
        side = trade.get("side", "").upper()
        if side not in ("BUY", "SELL"):
            continue

        if side == "BUY":
            # --- live price check ---
            price_resp = await clob_client.get_price(trade.get("asset", ""))
            live_price = float(price_resp.get("price", 0))
            min_share_price = float(ENV.get("MIN_SHARE_PRICE", 0))
            if min_share_price > 0 and live_price < min_share_price:
                continue

            # --- sizing ---
            amount = _calc_buy_amount(trade, ratio, buy_min, buy_max)
            amount = _apply_balance_cap(amount, balance)
            ref_price = float(trade.get("price", live_price) or live_price)
            target_size = amount / live_price if live_price > 0 else 0.0

            # --- optional decision gate ---
            decision = None
            try:
                from services.copyDecisionService import LeaderScore
                ls = LeaderScore(score=0.5, tier="live", leader_wallet=bot_params.get("leaderWallet", ""))
                decision = _copy_decision.decide_buy(
                    trade=trade,
                    leader_score=ls,
                    live_price=live_price,
                    base_notional_usdc=amount,
                    max_notional_usdc=amount,
                    market_min_size=0.0,
                )
                if not decision.allow:
                    continue
                target_size = decision.target_size_tokens
                amount = decision.target_notional_usdc
            except Exception:
                # Fallback: legacy path without decision gate
                pass

            # --- execute ---
            req = {
                "token_id": trade.get("asset", ""),
                "side": "BUY",
                "amount": amount,
                "price": live_price,
            }
            result = await exec_svc.execute(req)

            # --- log ---
            filled = float(result.filled_amount) if result.filled_amount else 0.0
            if filled > 0:
                log_trade_execution(
                    _logger,
                    side="BUY",
                    condition_id=trade.get("conditionId"),
                    asset=trade.get("asset"),
                    size=filled,
                    price=live_price,
                    usdc_value=amount,
                    meta={
                        "copyOrderId": result.order_id,
                        "leaderTx": trade.get("transactionHash"),
                    },
                )
            else:
                log_trade_skip(
                    _logger,
                    side="BUY",
                    condition_id=trade.get("conditionId"),
                    asset=trade.get("asset"),
                    size=0.0,
                    price=live_price,
                    usdc_value=amount,
                    skip_code="zero_fill",
                    reason=f"zero_fill:{result.status}",
                    meta={
                        "copyOrderId": result.order_id,
                        "leaderTx": trade.get("transactionHash"),
                    },
                )
