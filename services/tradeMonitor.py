import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from config.env import ENV, get_funder_address
from services.leaderCursorStore import LeaderCursorStore
from services.healthServer import update_readiness
from services.metrics import metrics
from storage.sqlite.cursors_repo import TradeCursor
from utils.fetchData import fetch_data
from utils.logger import (
    get_logger,
    monitor_heartbeat_tick,
    print_new_trade_detected,
    print_first_run_init,
    print_historical_done,
    print_monitor_start,
    redact_error_detail,
    shorten_addr,
    log_trade_detected,
)

logger = get_logger("TradeMonitor")

# Position save interval (seconds) — avoid saving on every loop
_POSITION_SAVE_INTERVAL = 60


@dataclass
class LeaderMonitorState:
    recent_activities: list[dict] = field(default_factory=list)
    seen_hashes: set[str] = field(default_factory=set)
    last_position_save: float = 0.0
    last_processed_ts: float = 0.0
    last_cursor: TradeCursor = field(default_factory=lambda: TradeCursor(timestamp=0, tx_hash="", trade_id=None))


def _to_seconds(ts) -> float:
    try:
        value = float(ts or 0)
    except (TypeError, ValueError):
        return 0.0
    if value > 1e12:
        return value / 1000.0
    return value


def _trade_ordering_key(trade: dict) -> tuple[int, str, str]:
    ts = int(_to_seconds(trade.get("timestamp")))
    tx_hash = str(trade.get("transactionHash") or trade.get("txHash") or "").strip().lower()
    trade_id = str(trade.get("id") or trade.get("tradeID") or trade.get("trade_id") or "").strip().lower()
    return ts, tx_hash, trade_id


async def _run_research_capture(
    research_capture_service: Any,
    raw_trades: list[dict],
) -> None:
    try:
        await research_capture_service.capture_observed_trades(raw_trades)
    except Exception as exc:
        logger.warning(
            "research_trade_capture_failed",
            extra={"error": redact_error_detail(exc)},
        )


async def _run_position_capture(
    research_capture_service: Any,
    leader_wallet: str,
    positions: list[dict],
) -> None:
    try:
        await research_capture_service.capture_position_snapshots(
            leader_wallet,
            positions,
        )
    except Exception as exc:
        logger.warning(
            "research_position_capture_failed",
            extra={"leader_wallet": leader_wallet, "error": redact_error_detail(exc)},
        )


async def init(user_address: str, state: LeaderMonitorState):
    if not user_address or len(user_address) != 42:
        logger.error(f'Invalid wallet address "{user_address}" — must be 42-char hex (0x + 40 digits)')
        state.recent_activities = []
        state.seen_hashes.clear()
        return
    try:
        user_activities = await fetch_data(
            f'https://data-api.polymarket.com/activity?user={user_address}&limit=50&offset=0',
            silent=True,
        )
        normalized = _normalize_activities(user_activities)
        state.recent_activities = normalized[-200:]
        state.seen_hashes = {
            str(t.get('transactionHash') or '').lower()
            for t in state.recent_activities
            if t.get('transactionHash')
        }
    except Exception as e:
        logger.error(f'Init activity failed: {e}')
        state.recent_activities = []
        state.seen_hashes.clear()


def _normalize_activities(raw):
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return raw.get('data', raw.get('activities', raw.get('results', []))) or []
    return []


def _trade_usdc_value(trade: dict) -> float:
    usdc = float(trade.get('usdcSize') or 0)
    if usdc <= 0:
        usdc = float(trade.get('size', 0) or 0) * float(trade.get('price', 0) or 0)
    return max(0.0, usdc)


def _aggregate_trades(trades: list[dict]) -> list[dict]:
    """Merge bursty same-market fills into a single signal.

    Groups by leader + condition + asset + side inside a short time window.
    This reduces noisy micro-fill spam from the Polymarket activity feed.
    """
    if not trades:
        return []

    window_s = float(ENV.get('AGGREGATE_SIGNAL_WINDOW_SECONDS', 2.0) or 2.0)
    ordered = sorted(trades, key=lambda x: (_to_seconds(x.get('timestamp')), str(x.get('transactionHash') or '')))
    aggregated: list[dict] = []

    for trade in ordered:
        ts = _to_seconds(trade.get('timestamp'))
        asset = trade.get('asset') or trade.get('tokenID') or trade.get('token_id')
        if not asset:
            aggregated.append(trade)
            continue

        key = (
            str(trade.get('proxyWallet') or trade.get('user') or '').lower(),
            str(trade.get('conditionId') or ''),
            str(asset),
            str(trade.get('side') or '').upper(),
        )

        last = aggregated[-1] if aggregated else None
        if last:
            last_asset = last.get('asset') or last.get('tokenID') or last.get('token_id')
            last_key = (
                str(last.get('proxyWallet') or last.get('user') or '').lower(),
                str(last.get('conditionId') or ''),
                str(last_asset),
                str(last.get('side') or '').upper(),
            )
            last_ts = _to_seconds(last.get('timestamp'))
        else:
            last_key = None
            last_ts = 0.0

        if last and key == last_key and (ts - last_ts) <= window_s:
            last_size = float(last.get('size', 0) or 0)
            cur_size = float(trade.get('size', 0) or 0)
            last_usdc = _trade_usdc_value(last)
            cur_usdc = _trade_usdc_value(trade)
            total_size = last_size + cur_size
            total_usdc = last_usdc + cur_usdc
            weighted_price = (total_usdc / total_size) if total_size > 0 else float(last.get('price', 0) or 0)
            last['size'] = total_size
            last['usdcSize'] = total_usdc
            last['price'] = weighted_price
            tx_hashes = list(last.get('_txHashes') or [])
            tx_hash = trade.get('transactionHash')
            if tx_hash:
                tx_hashes.append(tx_hash)
            last['_txHashes'] = tx_hashes
            last['_aggregatedCount'] = int(last.get('_aggregatedCount') or 1) + 1
            # Keep the newest timestamp so cursor progression remains monotonic.
            last['timestamp'] = max(last_ts, ts)
        else:
            clone = dict(trade)
            if clone.get('transactionHash'):
                clone['_txHashes'] = [clone['transactionHash']]
            clone['_aggregatedCount'] = 1
            aggregated.append(clone)

    return aggregated


async def fetch_trade_data(
    user_address: str,
    on_trade_callback,
    state: LeaderMonitorState,
    research_capture_service: Any | None = None,
):
    """Fetch new trades and call *on_trade_callback(trade)* immediately for each."""
    try:
        user_activities = await fetch_data(
            f'https://data-api.polymarket.com/activity?user={user_address}&limit=50&offset=0',
            silent=True,
        )
    except Exception as e:
        logger.error(f'Fetch activity failed: {e}')
        return
    user_activities = _normalize_activities(user_activities)
    if not user_activities:
        update_readiness(last_trade_poll_ts=int(time.time()))
        return
    try:
        current_timestamp = int(time.time())
        update_readiness(last_trade_poll_ts=current_timestamp)
        cursor_key = (
            int(state.last_cursor.timestamp or 0),
            str(state.last_cursor.tx_hash or ""),
            str(state.last_cursor.trade_id or ""),
        )

        new_trades = [
            a for a in user_activities
            if _trade_ordering_key(a) > cursor_key
        ]
        if new_trades:
            newest = max(_to_seconds(t.get("timestamp")) for t in new_trades)
            ingest_lag = max(0.0, float(current_timestamp) - newest)
            await metrics.set_gauge("ingest_lag_seconds", ingest_lag)
        for t in new_trades:
            t['bot'] = False
            t['botExcutedTime'] = 0
            t['proxyWallet'] = t.get('proxyWallet') or user_address
        new_trades.sort(key=lambda x: x.get('timestamp', 0))
        state.recent_activities.extend(new_trades)
        if len(state.recent_activities) > 200:
            state.recent_activities = state.recent_activities[-200:]
        state.seen_hashes = {
            str(t.get('transactionHash') or '').lower()
            for t in state.recent_activities
            if t.get('transactionHash')
        }

        trade_candidates = []
        max_seen_ts = state.last_processed_ts
        for trade in new_trades:
            if str(trade.get('type', '')).upper() != 'TRADE':
                continue
            await metrics.inc("signals_seen_total")

            if not trade.get('asset'):
                trade['asset'] = trade.get('tokenID') or trade.get('token_id')

            trade_candidates.append(trade)
            ts = _to_seconds(trade.get("timestamp"))
            if ts > max_seen_ts:
                max_seen_ts = ts

        trade_list = _aggregate_trades(trade_candidates)
        if trade_candidates and research_capture_service is not None:
            asyncio.create_task(
                _run_research_capture(research_capture_service, list(trade_candidates))
            )
        for trade in trade_list:
            # --- Rich console panel for detected trade ---
            print_new_trade_detected(trade)

            # --- Structured JSONL log with full trade details ---
            usdc = _trade_usdc_value(trade)
            leader_tx = trade.get('transactionHash', '')
            event_slug = trade.get('eventSlug') or trade.get('slug') or ''
            log_trade_detected(
                logger,
                side=trade.get('side', '?'),
                condition_id=trade.get('conditionId'),
                asset=trade.get('asset') or trade.get('tokenID'),
                size=float(trade.get('size', 0) or 0),
                price=float(trade.get('price', 0) or 0),
                usdc_value=usdc,
                tx_hash=leader_tx,
                event_slug=event_slug,
                meta={
                    'title': trade.get('title', 'N/A'),
                    'outcome': trade.get('outcome'),
                    'leader': trade.get('proxyWallet') or user_address,
                    'aggregatedCount': int(trade.get('_aggregatedCount') or 1),
                    'txHashes': trade.get('_txHashes') or None,
                },
            )

        # Execute all detected trades immediately — no queue
        if trade_list:
            await on_trade_callback(trade_list)
            state.last_processed_ts = max_seen_ts

        # Save to DB only for new trades
        if new_trades:
            max_cursor_trade = max(new_trades, key=_trade_ordering_key)
            ts, tx_hash, trade_id = _trade_ordering_key(max_cursor_trade)
            state.last_cursor = TradeCursor(timestamp=ts, tx_hash=tx_hash, trade_id=trade_id or None)
            state.last_processed_ts = float(ts)
            try:
                from models.userHistory import get_user_activity_model
                get_user_activity_model(user_address).insert_many(new_trades)
            except Exception as e:
                logger.error(f'DB save activities: {e}')
    except Exception as e:
        logger.error(f'Error fetch_trade_data: {e}')


async def fetch_and_save_positions(
    leader_wallet: str,
    proxy_wallet: str,
    research_capture_service: Any | None = None,
):
    if not bool(ENV.get("ENABLE_LEGACY_POSITION_SNAPSHOT", False)):
        return
    for wallet in (leader_wallet, proxy_wallet):
        if not wallet:
            continue
        try:
            positions = await fetch_data(
                f'https://data-api.polymarket.com/positions?user={wallet}',
                silent=True,
            )
            positions = positions or []
            if wallet == leader_wallet and research_capture_service is not None:
                asyncio.create_task(
                    _run_position_capture(research_capture_service, leader_wallet, list(positions))
                )
            docs = [dict(p, proxyWallet=p.get('proxyWallet') or wallet) for p in positions]
            from models.userHistory import get_user_position_model
            coll = get_user_position_model(wallet)
            coll.delete_many({})
            if docs:
                coll.insert_many(docs)
        except Exception as e:
            logger.error(f'DB save positions {shorten_addr(wallet)}: {e}')


async def trade_monitor(
    bot_params: dict,
    on_trade_callback,
    research_capture_service: Any | None = None,
):
    """Main monitor loop. Calls *on_trade_callback(trades_list)* immediately on detection."""
    fetch_interval = int(bot_params.get('FETCH_INTERVAL', ENV.get('FETCH_INTERVAL', 1)))
    state = LeaderMonitorState()
    cursor_store = LeaderCursorStore()
    leader_wallet = bot_params['leaderWallet']
    state.last_cursor = cursor_store.get_cursor(leader_wallet)
    state.last_processed_ts = float(state.last_cursor.timestamp)

    print_monitor_start(trader_count=1, interval_s=fetch_interval)
    print_first_run_init()
    await init(leader_wallet, state)
    print_historical_done()
    proxy_wallet = get_funder_address()
    while True:
        previous_cursor = state.last_cursor
        await fetch_trade_data(
            leader_wallet,
            on_trade_callback,
            state,
            research_capture_service=research_capture_service,
        )
        if (
            state.last_cursor.timestamp != previous_cursor.timestamp
            or state.last_cursor.tx_hash != previous_cursor.tx_hash
            or (state.last_cursor.trade_id or "") != (previous_cursor.trade_id or "")
        ):
            cursor_store.set_cursor(leader_wallet, state.last_cursor)

        # Save positions periodically, not every loop
        now = time.time()
        if now - state.last_position_save >= _POSITION_SAVE_INTERVAL:
            state.last_position_save = now
            asyncio.create_task(
                fetch_and_save_positions(
                    bot_params['leaderWallet'],
                    proxy_wallet,
                    research_capture_service=research_capture_service,
                )
            )

        monitor_heartbeat_tick(trader_count=1, checked_activities=len(state.recent_activities))
        await asyncio.sleep(fetch_interval)
