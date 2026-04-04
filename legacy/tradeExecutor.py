"""Legacy trade execution path (compatibility only).

Active production runtime now routes through:
  - orchestrators/trade_pipeline.py
  - domain/trading_context.py

This module remains for backward-compatible tests and external imports.
"""

import asyncio
import inspect
import time
from decimal import Decimal
from typing import Optional

from config.env import ENV, get_funder_address
from utils.fetchData import fetch_data
from utils.getMyBalance import get_my_balance
from utils.idempotency import mark_processed_all_tx_hashes
from services.executionService import ExecutionRequest, ExecutionService
from services.feeService import FeeService
from services.marketMetaService import MarketMetaService
from services.metrics import metrics
from services.orderRepository import OrderRepository
from services.leaderScoringService import LeaderScoringService
from services.tradeScoringService import TradeScoringService
from services.copyDecisionService import CopyDecisionService, CopyDecision
from services.copyStrategy import calculate_copy_notional
from services.tradeAnalyticsService import TradeAnalyticsService
from services.accountingService import AccountingService, PositionLot
from services.positionManager import get_position_manager
from services.pnlTracker import get_pnl_tracker
from services.riskEngine import (
    PortfolioSnapshot,
    ProposedTrade,
    RiskEngine,
    policy_from_env,
    regime_for_equity,
    get_regime,
    compute_soft_cap,
    headroom_for_buy,
)
from utils.dayEquity import get_day_start_equity, get_daily_loss
from utils.logger import (
    get_logger,
    print_capital_info,
    print_executing_strategy,
    print_balance_update,
    print_skip_reason,
    print_info,
    log_trade_execution,
    log_trade_submitted,
    log_trade_decision,
    log_trade_skip,
)

POSITIONS_URL = 'https://data-api.polymarket.com/positions?user='
MARKET_URL = 'https://polymarket.com/event/'

logger = get_logger("TradeExecutor")

_execution_services: dict[int, ExecutionService] = {}
_risk_engine = RiskEngine()
_accounting = AccountingService()
_leader_scorer = LeaderScoringService()
_trade_scorer = TradeScoringService()
_copy_decision = CopyDecisionService(
    leader_scorer=_leader_scorer,
    trade_scorer=_trade_scorer,
)
_trade_analytics = TradeAnalyticsService()


def _get_execution_service(clob_client) -> ExecutionService:
    key = id(clob_client)
    svc = _execution_services.get(key)
    if svc is None:
        svc = ExecutionService(
            clob_client=clob_client,
            market_meta_service=MarketMetaService(clob_client),
            fee_service=FeeService(clob_client),
            order_repo=OrderRepository(),
        )
        _execution_services[key] = svc
    return svc


def _record_trading_result_sync(leader_wallet: str, trade: dict, result: str, reason: Optional[str] = None):
    try:
        from models.userHistory import get_trading_collection
        doc = {
            'leaderWallet': leader_wallet,
            'transactionHash': trade.get('transactionHash'),
            'conditionId': trade.get('conditionId'),
            'side': trade.get('side'),
            'title': trade.get('title'),
            'outcome': trade.get('outcome'),
            'price': trade.get('price'),
            'size': trade.get('size'),
            'usdcSize': trade.get('usdcSize'),
            'timestamp': trade.get('timestamp'),
            'result': result,
            'recordedAt': time.time(),
        }
        if reason:
            doc['reason'] = reason
        get_trading_collection().insert_one(doc)
    except Exception as e:
        logger.error(f'Failed to record trade result to DB: {e}')


def _record_trading_result(leader_wallet: str, trade: dict, result: str, reason: Optional[str] = None):
    try:
        _record_trading_result_sync(leader_wallet, trade, result, reason)
    except Exception:
        pass


def _calc_buy_amount(trade: dict, ratio: float, buy_min: float, buy_max: float) -> float:
    """Calculate copy amount based on the active sizing mode."""
    sizing_mode = ENV.get('SIZING_MODE', 'proportional')

    leader_bought = float(trade.get('usdcSize', 0) or 0)
    if leader_bought <= 0:
        size_val = float(trade.get('size', 0) or 0)
        price_val = float(trade.get('price', 0) or 0)
        if size_val and price_val:
            leader_bought = size_val * price_val

    adaptive_basis = str(ENV.get('ADAPTIVE_SIZING_BASIS', 'performance') or 'performance')
    performance_win_rate = None
    if sizing_mode == 'adaptive' and adaptive_basis.strip().lower() == 'performance':
        pm = get_position_manager()
        summary = pm.summary()
        performance_win_rate = summary.get('win_rate', 50)

    result = calculate_copy_notional(
        strategy=sizing_mode,
        leader_notional_usdc=leader_bought,
        copy_percentage=ratio * 100.0,
        fixed_amount=ENV.get('FIXED_AMOUNT', 10),
        min_order_usdc=buy_min,
        max_order_usdc=buy_max,
        trade_multiplier=ENV.get('TRADE_MULTIPLIER', 1.0),
        tiered_multipliers=ENV.get('TIERED_TRADE_MULTIPLIERS', ''),
        adaptive_basis=adaptive_basis,
        adaptive_min_percent=ENV.get('ADAPTIVE_MIN_PERCENT', 0),
        adaptive_max_percent=ENV.get('ADAPTIVE_MAX_PERCENT', 0),
        adaptive_threshold_usdc=ENV.get('ADAPTIVE_THRESHOLD_USDC', 500),
        performance_win_rate=performance_win_rate,
    )
    return float(result.final_notional_usdc)


def _apply_balance_cap(amount: float, balance: float) -> float:
    """Cap trade amount by MAX_POSITION_PCT of our balance.

    NOTE: This is the legacy static cap. When ENABLE_BALANCE_TIERED_POLICY is
    true, headroom_for_buy() provides the authoritative cap and this function
    is only used as a coarse pre-filter.
    """
    max_pct = ENV.get('MAX_POSITION_PCT', 0.25)
    max_allowed = balance * max_pct
    if max_allowed < 1.0:
        max_allowed = balance  # If balance is tiny, allow full balance
    return min(amount, max_allowed, balance)


def _compute_sell_tokens(my_position: dict | None, user_position: dict | None, trade: dict) -> float:
    if not my_position:
        return 0.0
    my_size = float(my_position.get("size", 0) or 0)
    leader_sold = float(trade.get("size", 0) or 0)
    leader_size_after = float(user_position.get("size", 0) or 0) if user_position else 0.0
    leader_size_before = leader_size_after + leader_sold
    leader_sell_ratio = leader_sold / leader_size_before if leader_size_before > 0 else 1.0
    sell_tokens = my_size * leader_sell_ratio

    min_sz = float(ENV.get("MIN_ORDER_SIZE", 0) or 0)
    if min_sz > 0 and 0 < sell_tokens < min_sz:
        sell_tokens = min(my_size, min_sz)
    return max(0.0, sell_tokens)


def _build_portfolio_snapshot(
    *,
    my_positions: list[dict],
    my_balance: float,
    leader_wallet: str,
) -> PortfolioSnapshot:
    market_exposures: dict[str, Decimal] = {}
    category_exposures: dict[str, Decimal] = {}
    leader_exposures: dict[str, Decimal] = {}
    total_exposure = Decimal("0")

    for pos in my_positions:
        condition_id = str(pos.get("conditionId") or "")
        category = str(pos.get("category") or "unknown")
        size = Decimal(str(pos.get("size", 0) or 0))
        price = Decimal(str(pos.get("avgPrice", pos.get("curPrice", 0)) or 0))
        notional = size * price
        total_exposure += notional
        market_exposures[condition_id] = market_exposures.get(condition_id, Decimal("0")) + notional
        category_exposures[category] = category_exposures.get(category, Decimal("0")) + notional

    leader_exposures[str(leader_wallet)] = total_exposure
    equity = Decimal(str(my_balance)) + total_exposure
    return PortfolioSnapshot(
        equity_usdc=equity,
        daily_drawdown_pct=Decimal("0"),
        open_positions_count=len(my_positions),
        market_exposures=market_exposures,
        category_exposures=category_exposures,
        leader_exposures=leader_exposures,
    )


def _market_link(trade: dict) -> str:
    slug = trade.get('eventSlug') or trade.get('slug') or ''
    if slug:
        return f"{MARKET_URL}{slug}"
    cid = trade.get('conditionId') or ''
    if cid:
        return f"https://polymarket.com/market/{cid}"
    return ''


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _min_valid_buy_notional_usdc(*, live_price: float, market_min_size: float) -> float:
    """Minimum BUY notional needed to produce a valid copy order.

    This must be consistent with ExecutionService semantics:
    - BUY request amount is USDC notional
    - min_order_size is enforced using worst_price (slippage-protected)

    We therefore compute the minimum *worst-case* notional required to satisfy the
    token minimum, and also enforce the $1 Polymarket minimum.
    """
    if live_price <= 0:
        return 1.0

    min_shares_cfg = float(ENV.get("COPY_MIN_SHARES", 5.0) or 5.0)
    min_shares = max(5.0, min_shares_cfg)
    min_tokens = max(min_shares, float(market_min_size or 0.0))

    slippage_raw = ENV.get("SLIPPAGE_TOLERANCE", 2.0)
    # IMPORTANT: don't use `or` here; 0.0 is a valid value.
    if slippage_raw is None or slippage_raw == "":
        slippage_raw = 2.0
    slippage_pct = max(0.0, float(slippage_raw)) / 100.0
    worst_price = live_price * (1.0 + slippage_pct)

    notional_from_tokens = worst_price * min_tokens
    return max(1.0, float(notional_from_tokens))


def _maybe_boost_tiny_leader_buy_cap(
    *,
    leader_notional: float,
    min_leader_trade_notional: float,
    decision_score: float,
    raw_amount: float,
    shadow_cash: float,
    live_price: float,
    market_min_size: float,
    high_confidence_score: float,
) -> float:
    """Return an upsized BUY notional for tiny leader trades when confidence is high.

    This only lifts copy sizing to the minimum executable order. Hard balance/risk checks
    still happen later in the pipeline.
    """
    if leader_notional <= 0 or leader_notional >= min_leader_trade_notional:
        return raw_amount
    if decision_score < high_confidence_score:
        return raw_amount

    min_required = _min_valid_buy_notional_usdc(
        live_price=live_price,
        market_min_size=market_min_size,
    )
    return _apply_balance_cap(max(raw_amount, min_required), shadow_cash)


def _estimate_buy_edge_and_cost(
    *,
    reference_price: float,
    current_price: float,
    notional_usdc: float,
) -> dict[str, float]:
    """Estimate expected edge vs transaction cost for a BUY copy trade."""
    fee_pct = float(ENV.get('TRANSACTION_FEE_BPS', 20)) / 10_000.0
    gas_usdc = float(ENV.get('GAS_COST_USDC', 0.03))
    risk_buffer_pct = float(ENV.get('RISK_BUFFER_PCT', 0.5)) / 100.0

    if current_price <= 0:
        return {
            'expected_edge_pct': 0.0,
            'execution_cost_pct': 1.0,
            'risk_buffer_pct': risk_buffer_pct,
        }

    quote_drift = 0.0
    if reference_price > 0:
        quote_drift = max(0.0, current_price - reference_price)

    # Leader-implied edge for a binary token is remaining payout space from leader entry.
    leader_implied_edge = 0.0
    if reference_price > 0:
        leader_implied_edge = max(0.0, 1.0 - reference_price)

    edge_left = max(0.0, leader_implied_edge - quote_drift)
    expected_edge_pct = edge_left / current_price

    quote_drift_pct = quote_drift / current_price
    gas_pct = (gas_usdc / notional_usdc) if notional_usdc > 0 else 0.0
    execution_cost_pct = quote_drift_pct + fee_pct + gas_pct

    return {
        'expected_edge_pct': expected_edge_pct,
        'execution_cost_pct': execution_cost_pct,
        'risk_buffer_pct': risk_buffer_pct,
    }


async def _passes_buy_gate(clob_client, trade: dict, notional_usdc: float) -> tuple[bool, str]:
    asset = trade.get('asset')
    if not asset:
        return False, 'missing asset for BUY gate'

    ref_price = float(trade.get('price', 0) or 0)
    if ref_price <= 0:
        return False, 'invalid leader reference price'

    quote = await clob_client.get_price(asset, 'BUY')
    if not quote or quote.get('price') is None:
        return False, 'no live quote for BUY gate'

    try:
        current_price = float(str(quote['price']).strip())
    except (TypeError, ValueError):
        return False, 'invalid live quote in BUY gate'

    if current_price <= 0:
        return False, 'non-positive live quote in BUY gate'

    min_share_price = float(ENV.get('MIN_SHARE_PRICE', 0) or 0)
    if min_share_price > 0 and current_price < min_share_price:
        return False, (
            f'min share price gate: live={current_price:.4f} < min={min_share_price:.4f}'
        )

    metrics = _estimate_buy_edge_and_cost(
        reference_price=ref_price,
        current_price=current_price,
        notional_usdc=notional_usdc,
    )
    edge = metrics['expected_edge_pct']
    cost = metrics['execution_cost_pct']
    buffer_pct = metrics['risk_buffer_pct']

    if edge > cost + buffer_pct:
        return True, (
            f'edge={edge*100:.2f}% cost={cost*100:.2f}% buffer={buffer_pct*100:.2f}% '
            f'ref={ref_price:.4f} live={current_price:.4f}'
        )
    return False, (
        f'edge too low: edge={edge*100:.2f}% <= cost+buffer={(cost+buffer_pct)*100:.2f}% '
        f'(cost={cost*100:.2f}%, buffer={buffer_pct*100:.2f}%, ref={ref_price:.4f}, live={current_price:.4f})'
    )


async def execute_trades(clob_client, bot_params: dict, trades: list[dict]):
    """Execute a list of trades immediately. Called directly by the monitor — no queue."""
    leader_wallet = bot_params['leaderWallet']
    proxy_wallet = bot_params.get('proxyWallet') or get_funder_address()
    if not proxy_wallet:
        logger.warning('Skipping trade execution because PROXY_WALLET is not configured')
        return

    my_balance, user_balance, my_positions, user_positions = await asyncio.gather(
        get_my_balance(proxy_wallet),
        get_my_balance(leader_wallet),
        fetch_data(f'{POSITIONS_URL}{proxy_wallet}', silent=True),
        fetch_data(f'{POSITIONS_URL}{leader_wallet}', silent=True),
    )
    if my_balance is None or user_balance is None:
        logger.warning('Skipping trade execution because wallet balances could not be loaded')
        return

    my_positions = my_positions or []
    user_positions = user_positions or []
    my_by_cid = {p.get('conditionId'): p for p in my_positions}
    user_by_cid = {p.get('conditionId'): p for p in user_positions}

    ratio = float(bot_params.get('tradeRate', 0)) / 100
    buy_min, buy_max = bot_params['buyMinAmount'], bot_params['buyMaxAmount']
    position_threshold = float(bot_params.get('positionThreshold', 0) or 0)

    pm = get_position_manager()
    pnl = get_pnl_tracker()

    print_capital_info(
        my_balance=float(my_balance),
        leader_balance=float(user_balance),
        leader_addr=leader_wallet,
    )
    execution_service = _get_execution_service(clob_client)
    risk_policy = policy_from_env()
    slippage_bps = int(float(ENV.get("SLIPPAGE_TOLERANCE", 2.0)) * 100)
    pm_summary = pm.summary()
    pnl_metrics = pnl.metrics()
    leader_score = _leader_scorer.score_leader(
        leader_wallet,
        leader_balance=float(user_balance),
        analytics={
            "sample_size": int(pm_summary.get("closed_positions", 0) or 0),
            "win_rate": float(pm_summary.get("win_rate", 50.0) or 50.0),
            "net_pnl": float(pnl_metrics.get("net_pnl", 0.0) or 0.0),
        },
        recent_trade_count=len(trades),
    )

    min_leader_trade_notional = float(ENV.get('MIN_LEADER_TRADE_NOTIONAL_USDC', 8.0) or 8.0)
    tiny_leader_high_confidence_score = float(
        ENV.get('TINY_LEADER_HIGH_CONFIDENCE_SCORE', 0.75) or 0.75
    )

    # --- Shadow cash: track balance depletion within a single batch ---
    enable_tiered = str(ENV.get("ENABLE_BALANCE_TIERED_POLICY", "false")).lower() in ("1", "true", "yes")
    shadow_cash = float(my_balance)
    shadow_open_count = len(my_positions)

    for trade in trades:
        def _status_token(value: str) -> str:
            token = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value).lower())
            token = token.strip("_")
            return token[:80] if token else "unknown"

        def _mark_processed(status: str):
            try:
                mark_processed_all_tx_hashes(leader_wallet, trade, status=status)
            except Exception as e:
                logger.warning(f'Failed to persist idempotency marker: {e}')

        side = (trade.get('side') or '').strip().upper()
        cid = trade.get('conditionId')
        title = trade.get('title', 'N/A')
        market = _market_link(trade)
        leader_tx = trade.get('transactionHash') or ''

        if position_threshold > 0:
            leader_size = float(trade.get('size', 0) or 0)
            if leader_size >= position_threshold:
                print_skip_reason(
                    'leader trade size >= position threshold',
                    f'size={leader_size}, threshold={position_threshold}',
                )
                _record_trading_result(leader_wallet, trade, 'skipped', reason='position threshold')
                _mark_processed('skipped_threshold')
                continue

        leader_notional = float(trade.get('usdcSize', 0) or 0)
        if leader_notional <= 0:
            leader_notional = float(trade.get('size', 0) or 0) * float(trade.get('price', 0) or 0)
        tiny_leader_buy = side == 'BUY' and 0 < leader_notional < min_leader_trade_notional

        if not trade.get('asset'):
            trade['asset'] = trade.get('tokenID') or trade.get('token_id')
        if not trade.get('asset'):
            pos = user_by_cid.get(cid) or my_by_cid.get(cid)
            if pos and pos.get('asset'):
                trade['asset'] = pos['asset']

        my_position = my_by_cid.get(cid)
        user_position = user_by_cid.get(cid)
        decision: CopyDecision
        live_price = float(trade.get("price", 0) or 0.0)

        # Use per-market minimum order size from market metadata when available.
        # Falling back to ENV MIN_ORDER_SIZE is unsafe: exchange min size varies by market.
        market_min_size = float(ENV.get("MIN_ORDER_SIZE", 0.00001) or 0.00001)
        try:
            asset_for_meta = str(trade.get("asset") or trade.get("tokenID") or trade.get("token_id") or "")
            if asset_for_meta:
                meta = await execution_service.market_meta_service.get(asset_for_meta)
                market_min_size = max(market_min_size, float(getattr(meta, "min_order_size", 0) or 0.0))
        except Exception:
            pass

        if side == 'BUY':
            raw_amount = _calc_buy_amount(trade, ratio, buy_min, buy_max)
            capped_amount = _apply_balance_cap(raw_amount, shadow_cash)

            # --- Regime-aware headroom cap (overrides static cap when enabled) ---
            regime_cap = capped_amount
            regime_name = "legacy"
            if enable_tiered:
                portfolio = _build_portfolio_snapshot(
                    my_positions=my_positions,
                    my_balance=shadow_cash,
                    leader_wallet=leader_wallet,
                )
                headroom = headroom_for_buy(
                    equity_usdc=portfolio.equity_usdc,
                    free_cash_usdc=Decimal(str(shadow_cash)),
                    open_positions_count=shadow_open_count,
                    market_exposure_usdc=portfolio.market_exposure_usdc(str(cid or "")),
                    category_exposure_usdc=portfolio.category_exposure_usdc(str(trade.get("category") or "unknown")),
                    leader_exposure_usdc=portfolio.leader_exposure_usdc(leader_wallet),
                    daily_loss_usdc=get_daily_loss(shadow_cash),
                    day_start_equity_usdc=get_day_start_equity(shadow_cash),
                )
                regime_name = headroom.regime
                if not headroom.allow:
                    print_skip_reason(f"regime [{regime_name}] headroom blocked BUY", headroom.reason)
                    log_trade_skip(
                        logger,
                        side=side,
                        condition_id=cid,
                        asset=trade.get("asset"),
                        size=0.0,
                        price=live_price,
                        usdc_value=capped_amount,
                        skip_code=headroom.reason,
                        reason=f"regime [{regime_name}] headroom: {headroom.reason}",
                        status="skipped",
                        event_slug=trade.get('eventSlug') or trade.get('slug'),
                        meta={"leaderTx": leader_tx or None, "title": title, "market": market},
                    )
                    _record_trading_result(leader_wallet, trade, 'skipped', reason=headroom.reason)
                    _mark_processed(f"skipped_{_status_token(headroom.reason)}")
                    continue

                # Apply score-weighted soft cap
                regime_obj = get_regime(regime_name)
                hard_cap_usdc = float(headroom.cap_notional_usdc)
                # Use decision_score (computed later) for soft cap — for now use 0.5 as placeholder,
                # we'll re-cap after decision scoring below
                regime_cap = min(capped_amount, hard_cap_usdc)

            trade['clampedAmount'] = regime_cap
            if not trade.get("asset"):
                decision = CopyDecision(
                    allow=False,
                    skip_code="missing_asset",
                    reason="missing asset for BUY",
                    side="BUY",
                    decision_score=0.0,
                    leader_score=leader_score.score,
                    trade_score=0.0,
                    live_price=live_price,
                    reference_price=float(trade.get("price", 0) or 0.0),
                    target_notional_usdc=0.0,
                    target_size_tokens=0.0,
                    price_drift_pct=0.0,
                    edge_decay_pct=0.0,
                )
            else:
                quote = await clob_client.get_price(trade["asset"], "BUY")
                if quote and quote.get("price") is not None:
                    try:
                        live_price = float(str(quote["price"]).strip())
                    except (TypeError, ValueError):
                        live_price = 0.0
                min_share_price = float(ENV.get("MIN_SHARE_PRICE", 0) or 0)
                if min_share_price > 0 and live_price > 0 and live_price < min_share_price:
                    decision = CopyDecision(
                        allow=False,
                        skip_code="min_share_price_gate",
                        reason=(
                            f"min share price gate: live={live_price:.4f} < "
                            f"min={min_share_price:.4f}"
                        ),
                        side="BUY",
                        decision_score=0.0,
                        leader_score=leader_score.score,
                        trade_score=0.0,
                        live_price=live_price,
                        reference_price=float(trade.get("price", 0) or 0.0),
                        target_notional_usdc=0.0,
                        target_size_tokens=0.0,
                        price_drift_pct=0.0,
                        edge_decay_pct=0.0,
                    )
                    log_trade_decision(
                        logger,
                        side=side or "?",
                        condition_id=cid,
                        asset=trade.get("asset"),
                        decision_score=decision.decision_score,
                        allow=decision.allow,
                        reason=decision.reason,
                        event_slug=trade.get('eventSlug') or trade.get('slug'),
                        meta={
                            "leaderTx": leader_tx or None,
                            "leaderScore": decision.leader_score,
                            "tradeScore": decision.trade_score,
                            "priceDriftPct": decision.price_drift_pct,
                            "edgeDecayPct": decision.edge_decay_pct,
                        },
                    )
                    await _trade_analytics.record_decision(decision=decision, trade=trade)
                    print_skip_reason("min share price gate rejected BUY", decision.reason)
                    log_trade_skip(
                        logger,
                        side=side or "?",
                        condition_id=cid,
                        asset=trade.get("asset"),
                        size=float(trade.get("size", 0) or 0.0),
                        price=float(trade.get("price", 0) or 0.0),
                        usdc_value=float(trade.get("usdcSize", 0) or 0.0),
                        skip_code=decision.skip_code,
                        reason=decision.reason,
                        status="skipped",
                        event_slug=trade.get('eventSlug') or trade.get('slug'),
                        meta={"leaderTx": leader_tx or None, "title": title, "market": market},
                    )
                    _record_trading_result(leader_wallet, trade, 'skipped', reason=decision.skip_code)
                    _mark_processed(f"skipped_{_status_token(decision.skip_code)}")
                    await _trade_analytics.record_execution(
                        decision=decision,
                        trade=trade,
                        fill_price=live_price,
                        result="skipped",
                    )
                    continue
                try:
                    market_meta = await _maybe_await(
                        execution_service.market_meta_service.get(str(trade["asset"]))
                    )
                    market_min_size = max(market_min_size, float(market_meta.min_order_size or 0.0))
                except Exception as exc:
                    logger.warning("Failed to fetch market meta for decision gate: %s", exc)
                decision = _copy_decision.decide_buy(
                    trade=trade,
                    leader_score=leader_score,
                    live_price=live_price,
                    base_notional_usdc=float(regime_cap),
                    max_notional_usdc=float(regime_cap),
                    market_min_size=market_min_size,
                )
                if tiny_leader_buy and decision.decision_score >= tiny_leader_high_confidence_score:
                    boosted_cap = _maybe_boost_tiny_leader_buy_cap(
                        leader_notional=leader_notional,
                        min_leader_trade_notional=min_leader_trade_notional,
                        decision_score=decision.decision_score,
                        raw_amount=float(raw_amount),
                        shadow_cash=shadow_cash,
                        live_price=live_price,
                        market_min_size=market_min_size,
                        high_confidence_score=tiny_leader_high_confidence_score,
                    )
                    if boosted_cap > float(regime_cap) + 1e-9:
                        print_info(
                            f"Tiny leader BUY override: boosting cap from ${float(regime_cap):.4f} "
                            f"to ${boosted_cap:.4f} (leader=${leader_notional:.2f}, score={decision.decision_score:.3f})"
                        )
                        decision = _copy_decision.decide_buy(
                            trade=trade,
                            leader_score=leader_score,
                            live_price=live_price,
                            base_notional_usdc=boosted_cap,
                            max_notional_usdc=boosted_cap,
                            market_min_size=market_min_size,
                        )
                        trade['clampedAmount'] = boosted_cap
        elif side == 'SELL':
            sell_tokens = _compute_sell_tokens(my_position, user_position, trade)
            if trade.get("asset"):
                quote = await clob_client.get_price(trade["asset"], "SELL")
                if quote and quote.get("price") is not None:
                    try:
                        live_price = float(str(quote["price"]).strip())
                    except (TypeError, ValueError):
                        pass
                try:
                    market_meta = await _maybe_await(
                        execution_service.market_meta_service.get(str(trade["asset"]))
                    )
                    market_min_size = max(market_min_size, float(market_meta.min_order_size or 0.0))
                except Exception as exc:
                    logger.warning("Failed to fetch market meta for SELL decision: %s", exc)
            decision = _copy_decision.decide_sell(
                trade=trade,
                sell_tokens=sell_tokens,
                live_price=live_price,
                market_min_size=market_min_size,
                max_sell_tokens=float((my_position or {}).get("size", 0.0) or 0.0),
            )
        else:
            decision = CopyDecision(
                allow=False,
                skip_code="unknown_side",
                reason=f'unknown side "{side}"',
                side=side or "?",
                decision_score=0.0,
                leader_score=leader_score.score,
                trade_score=0.0,
                live_price=live_price,
                reference_price=float(trade.get("price", 0) or 0.0),
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=0.0,
                edge_decay_pct=0.0,
            )

        log_trade_decision(
            logger,
            side=side or "?",
            condition_id=cid,
            asset=trade.get("asset"),
            decision_score=decision.decision_score,
            allow=decision.allow,
            reason=decision.reason,
            event_slug=trade.get('eventSlug') or trade.get('slug'),
            meta={
                "leaderTx": leader_tx or None,
                "leaderScore": decision.leader_score,
                "tradeScore": decision.trade_score,
                "priceDriftPct": decision.price_drift_pct,
                "edgeDecayPct": decision.edge_decay_pct,
            },
        )
        await _trade_analytics.record_decision(decision=decision, trade=trade)

        if tiny_leader_buy and decision.allow and decision.decision_score < tiny_leader_high_confidence_score:
            decision = CopyDecision(
                allow=False,
                skip_code="tiny_leader_low_confidence",
                reason=(
                    f"leader BUY notional too small ({leader_notional:.2f} < "
                    f"{min_leader_trade_notional:.2f} USDC) and score {decision.decision_score:.3f} "
                    f"< high-confidence threshold {tiny_leader_high_confidence_score:.3f}"
                ),
                side=decision.side,
                decision_score=decision.decision_score,
                leader_score=decision.leader_score,
                trade_score=decision.trade_score,
                live_price=decision.live_price,
                reference_price=decision.reference_price,
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=decision.price_drift_pct,
                edge_decay_pct=decision.edge_decay_pct,
                details=dict(decision.details),
            )

        if not decision.allow:
            print_skip_reason("decision gate rejected", decision.reason)
            log_trade_skip(
                logger,
                side=side or "?",
                condition_id=cid,
                asset=trade.get("asset"),
                size=float(trade.get("size", 0) or 0.0),
                price=float(trade.get("price", 0) or 0.0),
                usdc_value=float(trade.get("usdcSize", 0) or 0.0),
                skip_code=decision.skip_code,
                reason=decision.reason,
                status="skipped",
                event_slug=trade.get('eventSlug') or trade.get('slug'),
                meta={"leaderTx": leader_tx or None, "title": title, "market": market},
            )
            _record_trading_result(leader_wallet, trade, 'skipped', reason=decision.skip_code)
            _mark_processed(f"skipped_{_status_token(decision.skip_code)}")
            await _trade_analytics.record_execution(
                decision=decision,
                trade=trade,
                fill_price=live_price,
                result="skipped",
            )
            continue

        print_executing_strategy(side)
        result = "skipped"
        result_reason = "unknown"
        fill = None

        # Build signal_id for idempotency
        _signal_id = str(trade.get("transactionHash") or trade.get("txHash") or f"{leader_wallet}:{cid}:{side}:{time.time()}")

        if side == 'BUY':
            # --- Apply score-weighted soft cap (now that we have decision_score) ---
            effective_cap = float(regime_cap if enable_tiered else capped_amount)
            if enable_tiered and decision.allow:
                regime_obj = get_regime(regime_name)
                soft = float(compute_soft_cap(
                    Decimal(str(hard_cap_usdc)),
                    Decimal(str(decision.decision_score)),
                    regime_obj.base_min_score,
                ))
                effective_cap = min(effective_cap, soft)
                if decision.target_notional_usdc > effective_cap + 0.001:
                    print_info(
                        f"Soft-capping BUY from ${decision.target_notional_usdc:.4f} "
                        f"to ${effective_cap:.4f} (regime={regime_name}, score={decision.decision_score:.3f})"
                    )

            portfolio = _build_portfolio_snapshot(
                my_positions=my_positions,
                my_balance=float(my_balance),
                leader_wallet=leader_wallet,
            )
            proposed = ProposedTrade(
                user_id=proxy_wallet,
                leader_id=leader_wallet,
                market_id=str(cid or ""),
                category=str(trade.get("category") or "unknown"),
                token_id=str(trade.get("asset") or ""),
                side="BUY",
                notional_usdc=Decimal(str(decision.target_notional_usdc)),
            )
            risk_decision = _risk_engine.evaluate(portfolio, risk_policy, proposed)
            if not risk_decision.allow:
                print_skip_reason("risk gate rejected BUY", risk_decision.reason)
                await metrics.inc("risk_rejections_total")
                log_trade_skip(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    skip_code=risk_decision.reason,
                    reason="risk gate rejected BUY",
                    status="skipped",
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={"leaderTx": leader_tx or None, "title": title, "market": market},
                )
                _record_trading_result(leader_wallet, trade, 'skipped', reason=risk_decision.reason)
                _mark_processed(f"skipped_{_status_token(risk_decision.reason)}")
                await _trade_analytics.record_execution(
                    decision=decision,
                    trade=trade,
                    fill_price=live_price,
                    result="skipped",
                )
                continue

            exec_req = ExecutionRequest(
                user_id=proxy_wallet,
                leader_id=leader_wallet,
                token_id=str(trade.get("asset") or ""),
                side="BUY",
                amount=Decimal(str(min(decision.target_notional_usdc, effective_cap))),
                max_slippage_bps=slippage_bps,
                reason="copy_trade",
                signal_id=_signal_id,
            )
            exec_result = await execution_service.execute(exec_req)
            if exec_result.accepted:
                await metrics.inc("orders_submitted_total")
                log_trade_submitted(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={
                        "copyOrderId": exec_result.order_id,
                        "leaderTx": leader_tx or None,
                        "title": title,
                        "market": market,
                    },
                )
            if exec_result.accepted and exec_result.filled_amount > 0:
                await metrics.inc("orders_filled_total")
                result = "executed"
                result_reason = "filled"
                fill = {
                    "price": float(exec_result.avg_price or 0),
                    "size_tokens": float(exec_result.filled_amount),
                    "usdc_value": float((exec_result.filled_amount * (exec_result.avg_price or Decimal("0")))),
                    "copy_tx": exec_result.order_id,
                }
                log_trade_execution(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset"),
                    size=float(fill.get("size_tokens") or 0.0),
                    price=float(fill.get("price") or 0.0),
                    usdc_value=float(fill.get("usdc_value") or 0.0),
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={
                        "copyOrderId": exec_result.order_id,
                        "leaderTx": leader_tx or None,
                        "title": title,
                        "market": market,
                        "result": result,
                    },
                )
            elif exec_result.status == "duplicate_ignored":
                result = "skipped"
                result_reason = "duplicate_ignored"
                fill = None
                log_trade_skip(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    skip_code="duplicate_ignored",
                    reason="execution duplicate ignored",
                    status="skipped",
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={
                        "copyOrderId": exec_result.order_id,
                        "leaderTx": leader_tx or None,
                        "title": title,
                        "market": market,
                    },
                )
            else:
                await metrics.inc("orders_rejected_total")
                result = "failed"
                result_reason = exec_result.error or "execution_failed"
                fill = None
                if exec_result.error:
                    print_skip_reason("buy execution failed", exec_result.error)
                log_trade_skip(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    skip_code="execution_failed",
                    reason=result_reason,
                    status="failed",
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={"leaderTx": leader_tx or None, "title": title, "market": market},
                )

            # Track position for risk management
            if result == 'executed' and fill:
                exec_price = float(fill.get('price') or 0)
                tokens = float(fill.get('size_tokens') or 0)
                cost = float(fill.get('usdc_value') or 0)
                if exec_price <= 0 or tokens <= 0 or cost <= 0:
                    _record_trading_result(leader_wallet, trade, 'failed', reason='invalid fill')
                    _mark_processed('failed_invalid_fill')
                    continue
                pm.track_buy(
                    condition_id=cid,
                    asset=trade.get('asset', ''),
                    entry_price=exec_price,
                    size_tokens=tokens,
                    usdc_cost=cost,
                    leader_wallet=leader_wallet,
                    event_slug=trade.get('eventSlug') or trade.get('slug') or '',
                    title=title,
                    category=str(trade.get("category") or "unknown"),
                )
                # Shadow cash: deduct cost from batch-local balance tracker
                shadow_cash = max(0.0, shadow_cash - cost)
                shadow_open_count += 1
                pnl.record_buy(
                    condition_id=cid,
                    price=exec_price,
                    size_tokens=tokens,
                    usdc_value=cost,
                    leader_wallet=leader_wallet,
                    event_slug=trade.get('eventSlug') or trade.get('slug') or '',
                    title=title,
                )
                _accounting.book_fill(
                    user_id=proxy_wallet,
                    fill_doc={
                        "order_id": fill.get("copy_tx"),
                        "condition_id": cid,
                        "token_id": trade.get("asset"),
                        "side": "BUY",
                        "size_tokens": str(tokens),
                        "price": str(exec_price),
                        "usdc_value": str(cost),
                        "leader_id": leader_wallet,
                        "category": str(trade.get("category") or "unknown"),
                        "reason": "copy_trade",
                    },
                )
                _accounting.upsert_position(
                    PositionLot(
                        user_id=proxy_wallet,
                        condition_id=str(cid or ""),
                        token_id=str(trade.get("asset") or ""),
                        size_tokens=Decimal(str(tokens)),
                        avg_entry_price=Decimal(str(exec_price)),
                        leader_id=leader_wallet,
                        category=str(trade.get("category") or "unknown"),
                    )
                )

        elif side == 'SELL':
            portfolio = _build_portfolio_snapshot(
                my_positions=my_positions,
                my_balance=float(my_balance),
                leader_wallet=leader_wallet,
            )
            proposed = ProposedTrade(
                user_id=proxy_wallet,
                leader_id=leader_wallet,
                market_id=str(cid or ""),
                category=str(trade.get("category") or "unknown"),
                token_id=str(trade.get("asset") or (my_position or {}).get("asset") or ""),
                side="SELL",
                notional_usdc=Decimal(str(decision.target_notional_usdc)),
            )
            risk_decision = _risk_engine.evaluate(portfolio, risk_policy, proposed)
            if not risk_decision.allow:
                print_skip_reason("risk gate rejected SELL", risk_decision.reason)
                await metrics.inc("risk_rejections_total")
                log_trade_skip(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset") or (my_position or {}).get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    skip_code=risk_decision.reason,
                    reason="risk gate rejected SELL",
                    status="skipped",
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={"leaderTx": leader_tx or None, "title": title, "market": market},
                )
                _record_trading_result(leader_wallet, trade, "skipped", reason=risk_decision.reason)
                _mark_processed(f"skipped_{_status_token(risk_decision.reason)}")
                await _trade_analytics.record_execution(
                    decision=decision,
                    trade=trade,
                    fill_price=live_price,
                    result="skipped",
                )
                continue

            exec_req = ExecutionRequest(
                user_id=proxy_wallet,
                leader_id=leader_wallet,
                token_id=str(trade.get("asset") or (my_position or {}).get("asset") or ""),
                side="SELL",
                amount=Decimal(str(decision.target_size_tokens)),
                max_slippage_bps=slippage_bps,
                reason="leader_sell",
                signal_id=_signal_id,
            )
            exec_result = await execution_service.execute(exec_req)
            if exec_result.accepted:
                await metrics.inc("orders_submitted_total")
                log_trade_submitted(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset") or (my_position or {}).get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={
                        "copyOrderId": exec_result.order_id,
                        "leaderTx": leader_tx or None,
                        "title": title,
                        "market": market,
                    },
                )
            if exec_result.accepted and exec_result.filled_amount > 0:
                await metrics.inc("orders_filled_total")
                result = "executed"
                result_reason = "filled"
                fill = {
                    "price": float(exec_result.avg_price or 0),
                    "size_tokens": float(exec_result.filled_amount),
                    "usdc_value": float((exec_result.filled_amount * (exec_result.avg_price or Decimal("0")))),
                    "copy_tx": exec_result.order_id,
                }
                log_trade_execution(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset") or (my_position or {}).get("asset"),
                    size=float(fill.get("size_tokens") or 0.0),
                    price=float(fill.get("price") or 0.0),
                    usdc_value=float(fill.get("usdc_value") or 0.0),
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={
                        "copyOrderId": exec_result.order_id,
                        "leaderTx": leader_tx or None,
                        "title": title,
                        "market": market,
                        "result": result,
                    },
                )
            elif exec_result.status == "duplicate_ignored":
                result = "skipped"
                result_reason = "duplicate_ignored"
                fill = None
                log_trade_skip(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset") or (my_position or {}).get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    skip_code="duplicate_ignored",
                    reason="execution duplicate ignored",
                    status="skipped",
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={"copyOrderId": exec_result.order_id, "leaderTx": leader_tx or None},
                )
            else:
                await metrics.inc("orders_rejected_total")
                result = "failed"
                result_reason = exec_result.error or "execution_failed"
                fill = None
                if exec_result.error:
                    print_skip_reason("sell execution failed", exec_result.error)
                log_trade_skip(
                    logger,
                    side=side,
                    condition_id=cid,
                    asset=trade.get("asset") or (my_position or {}).get("asset"),
                    size=decision.target_size_tokens,
                    price=live_price,
                    usdc_value=decision.target_notional_usdc,
                    skip_code="execution_failed",
                    reason=result_reason,
                    status="failed",
                    event_slug=trade.get('eventSlug') or trade.get('slug'),
                    meta={"leaderTx": leader_tx or None, "title": title, "market": market},
                )

            # Track sell for P&L
            if result == 'executed' and fill:
                exec_price = float(fill.get('price') or 0)
                sell_tokens = float(fill.get('size_tokens') or 0)
                usdc_value = float(fill.get('usdc_value') or 0)
                if exec_price <= 0 or sell_tokens <= 0 or usdc_value <= 0:
                    _record_trading_result(leader_wallet, trade, 'failed', reason='invalid fill')
                    _mark_processed('failed_invalid_fill')
                    continue
                tracked = pm.get_position(cid)
                entry_p = tracked.entry_price if tracked else 0
                realized = (exec_price - entry_p) * sell_tokens if entry_p > 0 else 0
                pm.track_sell(cid, exec_price, sell_tokens, reason='leader_sell')
                pnl.record_sell(
                    condition_id=cid,
                    price=exec_price,
                    size_tokens=sell_tokens,
                    usdc_value=usdc_value,
                    entry_price=entry_p,
                    realized_pnl=realized,
                    exit_reason='leader_sell',
                    leader_wallet=leader_wallet,
                    event_slug=trade.get('eventSlug') or trade.get('slug') or '',
                    title=title,
                )
                _accounting.book_fill(
                    user_id=proxy_wallet,
                    fill_doc={
                        "order_id": fill.get("copy_tx"),
                        "condition_id": cid,
                        "token_id": trade.get("asset") or (my_position or {}).get("asset"),
                        "side": "SELL",
                        "size_tokens": str(sell_tokens),
                        "price": str(exec_price),
                        "usdc_value": str(usdc_value),
                        "leader_id": leader_wallet,
                        "category": str(trade.get("category") or "unknown"),
                        "reason": "leader_sell",
                    },
                )
                remaining = pm.get_position(cid)
                if remaining is None:
                    _accounting.close_position(user_id=proxy_wallet, condition_id=str(cid or ""))
                else:
                    _accounting.upsert_position(
                        PositionLot(
                            user_id=proxy_wallet,
                            condition_id=str(cid or ""),
                            token_id=str(trade.get("asset") or (my_position or {}).get("asset") or ""),
                            size_tokens=Decimal(str(remaining.size_tokens)),
                            avg_entry_price=Decimal(str(remaining.entry_price)),
                            leader_id=leader_wallet,
                            category=str(trade.get("category") or "unknown"),
                        )
                    )

        if result == "executed":
            _record_trading_result(leader_wallet, trade, result)
            _mark_processed(result)
            await _trade_analytics.record_execution(
                decision=decision,
                trade=trade,
                fill_price=float(fill.get("price") or live_price) if fill else live_price,
                result=result,
            )
        else:
            _record_trading_result(leader_wallet, trade, result, reason=result_reason)
            _mark_processed(f"{_status_token(result)}_{_status_token(result_reason)}")
            await _trade_analytics.record_execution(
                decision=decision,
                trade=trade,
                fill_price=live_price,
                result=result,
            )

    try:
        pnl_metrics = pnl.metrics()
        await metrics.set_gauge("pnl_realized_usdc", float(pnl_metrics.get("net_pnl", 0.0)))
        await metrics.set_gauge(
            "pnl_unrealized_usdc",
            float(pm.summary().get("total_invested", 0.0)),
        )
        _accounting.snapshot_daily_equity(
            user_id=proxy_wallet,
            equity_usdc=Decimal(str(my_balance)),
            free_cash_usdc=Decimal(str(my_balance)),
        )
    except Exception as e:
        logger.warning(f"Failed to persist equity snapshot: {e}")

    print_balance_update(float(my_balance))
