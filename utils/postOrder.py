"""DEPRECATED — Legacy order execution module.

All runtime order execution now goes through services.executionService.ExecutionService.
This file is kept only for backward compatibility with old tests.
Do NOT use post_order() or post_risk_sell() in new code.
"""

import asyncio
import math
import warnings
from typing import Optional, Tuple
from config.env import ENV
from utils.clob_v1_compat import OrderArgs, BUY, SELL
from utils.logger import (
    get_logger,
    print_trade_result,
    print_skip_reason,
    print_api_error,
    print_api_response,
    print_order_skip,
)

MIN_ORDER_SIZE = ENV.get('MIN_ORDER_SIZE')
MIN_SHARE_PRICE = float(ENV.get('MIN_SHARE_PRICE', 0) or 0)
MAX_RETRIES = ENV.get('MAX_RETRIES', 3)
SLIPPAGE_TOLERANCE = ENV.get('SLIPPAGE_TOLERANCE', 2.0) / 100.0  # Convert to decimal
RETRY_BACKOFF_BASE = ENV.get('RETRY_BACKOFF_BASE', 1.0)
MARKET_URL = 'https://polymarket.com/event/'

logger = get_logger("PostOrder")


def _reason(reason: str, detail: str = ''):
    print_skip_reason(reason, detail)
    logger.warning(f"Order skipped: {reason}" + (f" | {detail}" if detail else ""))


def _mark_bot_done(trade: dict, user_activity_coll, **set_fields):
    if user_activity_coll is None or trade.get('_id') is None:
        return
    try:
        user_activity_coll.update_one({'_id': trade['_id']}, {'$set': set_fields})
    except Exception:
        pass


def _user_activity(leader_wallet: Optional[str] = None):
    try:
        from models.userHistory import get_user_activity_model
        wallet = leader_wallet or ENV.get('USER_ADDRESS')
        return get_user_activity_model(wallet)
    except Exception:
        return None


def _market_link(trade: dict) -> str:
    slug = trade.get('eventSlug') or trade.get('slug') or ''
    if slug:
        return f"{MARKET_URL}{slug}"
    cid = trade.get('conditionId') or ''
    if cid:
        return f"https://polymarket.com/market/{cid}"
    return ''


async def _execute_order_loop(
    clob_client,
    trade,
    asset,
    side_str,
    side_const,
    remaining,
    size_is_tokens=False,
    reference_price: float = 0.0,
):
    """Execute an order with slippage check and exponential backoff on retries.

    Args:
        reference_price: The leader's trade price. If > 0, slippage is checked
            against this before submitting.
    """
    retry = 0
    price = None
    resp = None
    last_resp = None
    filled_tokens = 0.0
    filled_usdc = 0.0

    market_min = await clob_client.get_min_order_size(asset)

    while remaining > 0 and retry < MAX_RETRIES:
        price_data = await clob_client.get_price(asset, side_str)
        if not price_data or price_data.get('price') is None:
            return None, None, None, retry, f'no price from CLOB for asset|asset={asset}', 0.0, 0.0

        try:
            price = float(str(price_data['price']).strip())
        except (TypeError, ValueError):
            return (
                None, None, None, retry,
                f'invalid price from CLOB for asset|asset={asset}, price={price_data.get("price")}',
                0.0,
                0.0,
            )

        if price <= 0:
            return (
                None, None, None, retry,
                f'invalid non-positive price from CLOB for asset|asset={asset}, price={price_data.get("price")}',
                0.0,
                0.0,
            )

        if side_str == 'BUY' and MIN_SHARE_PRICE > 0 and price < MIN_SHARE_PRICE:
            return (
                None, None, None, retry,
                f'price below min_share_price|asset={asset}, price={price:.4f}, min={MIN_SHARE_PRICE:.4f}',
                0.0,
                0.0,
            )

        # --- Slippage check ---
        if reference_price > 0 and SLIPPAGE_TOLERANCE > 0:
            if side_str == 'BUY':
                slippage = (price - reference_price) / reference_price
                if slippage > SLIPPAGE_TOLERANCE:
                    return (
                        None, None, None, retry,
                        f'slippage too high for BUY|'
                        f'current={price:.4f} ref={reference_price:.4f} '
                        f'slippage={slippage*100:.2f}% > tolerance={SLIPPAGE_TOLERANCE*100:.1f}%',
                        0.0,
                        0.0,
                    )
            elif side_str == 'SELL':
                slippage = (reference_price - price) / reference_price
                if slippage > SLIPPAGE_TOLERANCE:
                    return (
                        None, None, None, retry,
                        f'slippage too high for SELL|'
                        f'current={price:.4f} ref={reference_price:.4f} '
                        f'slippage={slippage*100:.2f}% > tolerance={SLIPPAGE_TOLERANCE*100:.1f}%',
                        0.0,
                        0.0,
                    )

        if size_is_tokens:
            order_size = remaining
        else:
            order_size = math.ceil(remaining / price * 100) / 100

        # Enforce per-market minimum token size
        if market_min > 0 and order_size < market_min:
            order_size = market_min

        # Enforce $1 minimum order value for BUY orders
        if not size_is_tokens and order_size * price < 1.0:
            order_size = math.ceil(1.0 / price * 100) / 100

        if order_size <= 0:
            return None, None, None, retry, f'order size is 0|remaining={remaining:.6f}', 0.0, 0.0


        order_args = OrderArgs(
            side=side_const,
            token_id=asset,
            size=order_size,
            price=price,
        )
        resp = await clob_client.post_order(order_args, trade.get('conditionId'))

        if resp.get('success') is True:
            copy_tx = _extract_copy_tx(resp)
            market = _market_link(trade)
            filled_tokens = order_size
            filled_usdc = order_size * price
            logger.info(
                f"✅ {side_str} OK: size={filled_tokens:.4f} @ {price:.4f} = ${filled_usdc:.2f}"
                + (f" | tx={copy_tx}" if copy_tx != 'N/A' else "")
                + (f" | market={market}" if market else "")
            )
            if size_is_tokens:
                remaining -= order_size
            else:
                remaining -= order_size * price
            retry = 0
            break

        logger.warning(
            f"Order REJECTED: {side_str} | attempt={retry + 1}/{MAX_RETRIES}"
        )
        last_resp = resp
        retry += 1

        # Exponential backoff before next retry
        if retry < MAX_RETRIES:
            backoff = RETRY_BACKOFF_BASE * (2 ** (retry - 1))
            logger.info(f"Retrying in {backoff:.1f}s...")
            await asyncio.sleep(backoff)

    return price, resp, last_resp, retry, None, filled_tokens, filled_usdc


def _handle_retry_failure(side_str, last_resp, retry, trade, asset, user_activity):
    api_err = (last_resp or {}).get('error') if last_resp else None
    if api_err:
        print_api_error(
            side=side_str,
            error=api_err,
            condition_id=trade.get('conditionId'),
            asset=asset,
            retry=retry,
            max_retries=MAX_RETRIES,
        )
    else:
        print_order_skip(
            side=side_str,
            reason=f'{side_str} order failed after retries',
            condition_id=trade.get('conditionId'),
            asset=asset,
            detail=f'retries={retry}',
        )
    if last_resp and not last_resp.get('success'):
        print_api_response(
            side=side_str,
            response=last_resp,
            condition_id=trade.get('conditionId'),
        )
    logger.error(f"Order FAILED after {retry} retries: {side_str} | conditionId={trade.get('conditionId')}")
    _mark_bot_done(trade, user_activity, bot=True, botExcutedTime=retry)


def _extract_copy_tx(resp):
    data = resp.get('data') or {}
    return str(
        data.get('transactionHash')
        or data.get('orderID')
        or resp.get('orderID')
        or 'N/A'
    )


async def post_order(
    clob_client,
    condition: str,
    my_position: Optional[dict],
    user_position: Optional[dict],
    trade: dict,
    my_balance: float,
    user_balance: float,
    bot_params: dict
) -> Tuple[str, Optional[dict]]:
    """Returns (result, fill) where result is 'executed', 'skipped', or 'failed'."""
    if condition == 'merge':
        condition = 'buy'
    leader_wallet = bot_params.get('leaderWallet') if isinstance(bot_params, dict) else None
    UserActivity = _user_activity(leader_wallet)
    tx_hash = trade.get('transactionHash') or 'N/A'
    market = _market_link(trade)
    cid = trade.get('conditionId')

    # Reference price from leader's trade for slippage check
    ref_price = float(trade.get('price', 0) or 0)

    if condition == 'buy':
        asset = trade.get('asset')
        if not asset:
            _reason('trade has no asset', f'conditionId={cid}')
            _mark_bot_done(trade, UserActivity, bot=True)
            return 'skipped', None

        remaining = float(trade.get('clampedAmount') or 0)
        if remaining <= 0:
            leader_usd = float(trade.get('usdcSize', 0) or 0)
            if leader_usd <= 0:
                leader_usd = float(trade.get('size', 0) or 0) * float(trade.get('price', 0) or 0)
            ratio = float(bot_params.get('tradeRate', 0)) / 100
            buy_min = float(bot_params.get('buyMinAmount', 0) or 0)
            buy_max = float(bot_params.get('buyMaxAmount', 0) or 0)
            proxy_usd = leader_usd * ratio
            if proxy_usd > buy_max:
                remaining = buy_max
            elif proxy_usd < buy_min:
                remaining = buy_min
            else:
                remaining = proxy_usd
            # Enforce $1 minimum
            if remaining < 1.0:
                remaining = 1.0
            if my_balance < remaining:
                remaining = my_balance

        if remaining <= 0:
            _reason('BUY amount is 0 or negative', f'clampedAmount/derived={remaining}')
            _mark_bot_done(trade, UserActivity, bot=True)
            return 'skipped', None
        else:
            price, resp, last_resp, retry, err_msg, filled_tokens, filled_usdc = await _execute_order_loop(
                clob_client, trade, asset, 'BUY', BUY, remaining,
                size_is_tokens=False, reference_price=ref_price,
            )
            if err_msg:
                parts = err_msg.split('|', 1)
                _reason(parts[0], parts[1] if len(parts) > 1 else '')
                _mark_bot_done(trade, UserActivity, bot=True)
                return 'skipped', None
            elif retry != 0:
                _handle_retry_failure('BUY', last_resp, retry, trade, asset, UserActivity)
                return 'failed', None
            else:
                _mark_bot_done(trade, UserActivity, bot=True)
                if price and resp and resp.get('success') is True:
                    copy_tx = _extract_copy_tx(resp)
                    print_trade_result(
                        success=True, side='BUY',
                        leader_tx=tx_hash, copy_tx=copy_tx,
                        amount=filled_usdc, price=price,
                    )
                    return 'executed', {
                        'price': price,
                        'size_tokens': filled_tokens,
                        'usdc_value': filled_usdc,
                        'copy_tx': copy_tx,
                    }
                return 'failed', None

    elif condition == 'sell':
        if not my_position:
            _reason('proxy has no position in this market', f'conditionId={cid}')
            _mark_bot_done(trade, UserActivity, bot=True)
            return 'skipped', None

        asset = trade.get('asset') or my_position.get('asset')
        if not asset:
            _reason('SELL has no asset', f'conditionId={cid}')
            _mark_bot_done(trade, UserActivity, bot=True)
            return 'skipped', None

        my_size = float(my_position.get('size', 0) or 0)
        leader_sold = float(trade.get('size', 0) or 0)
        leader_size_after = float(user_position.get('size', 0) or 0) if user_position else 0
        leader_size_before = leader_size_after + leader_sold
        leader_sell_ratio = leader_sold / leader_size_before if leader_size_before > 0 else 1.0
        remaining = my_size * leader_sell_ratio

        min_sz = float(MIN_ORDER_SIZE or 0)
        if min_sz > 0 and 0 < remaining < min_sz:
            remaining = min(my_size, min_sz)

        if remaining <= 0:
            _reason('SELL size is 0', f'my_size={my_size}, leader_sell_ratio={leader_sell_ratio:.4f}')
            _mark_bot_done(trade, UserActivity, bot=True)
            return 'skipped', None

        logger.info(f"Executing SELL: {remaining:.4f} tokens | conditionId={cid}")
        price, resp, last_resp, retry, err_msg, filled_tokens, filled_usdc = await _execute_order_loop(
            clob_client, trade, asset, 'SELL', SELL, remaining,
            size_is_tokens=True, reference_price=ref_price,
        )
        if err_msg:
            parts = err_msg.split('|', 1)
            _reason(parts[0], parts[1] if len(parts) > 1 else '')
            _mark_bot_done(trade, UserActivity, bot=True)
            return 'skipped', None
        elif retry != 0:
            _handle_retry_failure('SELL', last_resp, retry, trade, asset, UserActivity)
            return 'failed', None
        else:
            _mark_bot_done(trade, UserActivity, bot=True)
            if price and resp and resp.get('success') is True:
                copy_tx = _extract_copy_tx(resp)
                print_trade_result(
                    success=True, side='SELL',
                    leader_tx=tx_hash, copy_tx=copy_tx,
                    amount=filled_usdc, price=price,
                )
                return 'executed', {
                    'price': price,
                    'size_tokens': filled_tokens,
                    'usdc_value': filled_usdc,
                    'copy_tx': copy_tx,
                }
            return 'failed', None

    return 'skipped', None


async def post_risk_sell(clob_client, position, current_price: float, reason: str) -> str:
    """Execute a risk-triggered sell (stop-loss or take-profit).

    Called by the risk monitor, not by the normal trade flow.
    """
    asset = position.asset
    cid = position.condition_id
    remaining = position.size_tokens

    if remaining <= 0:
        return 'skipped'

    logger.info(
        f"Risk {reason}: selling {remaining:.4f} tokens of {cid[:12]}… "
        f"@ ~{current_price:.4f}"
    )

    trade_stub = {'conditionId': cid, 'asset': asset}
    price, resp, last_resp, retry, err_msg, _, _ = await _execute_order_loop(
        clob_client, trade_stub, asset, 'SELL', SELL, remaining,
        size_is_tokens=True, reference_price=0.0,  # No slippage check for risk sells
    )
    if err_msg:
        logger.error(f"Risk sell failed: {err_msg}")
        return 'failed'
    elif retry != 0:
        logger.error(f"Risk sell exhausted retries for {cid[:12]}…")
        return 'failed'
    else:
        logger.info(f"✅ Risk {reason} executed for {cid[:12]}…")
        return 'executed'
