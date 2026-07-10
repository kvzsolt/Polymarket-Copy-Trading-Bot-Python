from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from services.leaderCursorStore import LeaderCursorStore
from services.metrics import metrics
from storage.postgres.cursors_repo import TradeCursor
from utils.fetchData import fetch_data

if TYPE_CHECKING:
    from config.app_config import AppConfig
    from services.healthServer import ReadinessTracker
from utils.logger import (
    get_logger,
    log_trade_detected,
    redact_error_detail,
    log_trade_batch_summary,
    monitor_heartbeat_tick,
    print_new_trade_detected,
)

logger = get_logger("TradeMonitor")

_LEADER_FETCH_LOCKS: dict[tuple[str, str], asyncio.Lock] = {}
_LEADER_FETCH_LAST_TS: dict[tuple[str, str], float] = {}
_LEADER_FETCH_INFLIGHT: dict[tuple[str, str], asyncio.Task] = {}

# Set by trade_monitor() at startup; read by _leader_fetch_spacing_seconds()
_fetch_spacing_config: Any = None


def _leader_fetch_key(kind: str, leader_wallet: str) -> tuple[str, str]:
    return kind, str(leader_wallet or "").strip().lower()



def _leader_fetch_lock(kind: str, leader_wallet: str) -> asyncio.Lock:
    key = _leader_fetch_key(kind, leader_wallet)
    lock = _LEADER_FETCH_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _LEADER_FETCH_LOCKS[key] = lock
    return lock



def _leader_fetch_spacing_seconds(kind: str) -> float:
    global _fetch_spacing_config
    if _fetch_spacing_config is not None:
        ms = (
            _fetch_spacing_config.polling.leader_positions_min_fetch_spacing_ms
            if kind == "positions"
            else _fetch_spacing_config.polling.leader_activity_min_fetch_spacing_ms
        )
        return max(0.0, ms / 1000.0)

    # No typed config injected — fail-closed default mirroring the
    # previous ``ENV.get(LEADER_*_MIN_FETCH_SPACING_MS, 250)`` defaults.
    # Production calls `configure_fetch_spacing(config)` from bootstrap.
    return 0.25


async def _guarded_leader_fetch(kind: str, leader_wallet: str, url: str):
    key = _leader_fetch_key(kind, leader_wallet)
    existing = _LEADER_FETCH_INFLIGHT.get(key)
    if existing is not None and not existing.done():
        return await asyncio.shield(existing)

    lock = _leader_fetch_lock(kind, leader_wallet)
    async with lock:
        existing = _LEADER_FETCH_INFLIGHT.get(key)
        if existing is not None and not existing.done():
            return await asyncio.shield(existing)

        async def _runner():
            spacing = _leader_fetch_spacing_seconds(kind)
            last_ts = _LEADER_FETCH_LAST_TS.get(key, 0.0)
            now = time.monotonic()
            wait_for = spacing - (now - last_ts)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            try:
                return await fetch_data(url, silent=True)
            finally:
                _LEADER_FETCH_LAST_TS[key] = time.monotonic()

        task = asyncio.create_task(_runner())
        _LEADER_FETCH_INFLIGHT[key] = task

    try:
        return await asyncio.shield(task)
    finally:
        current = _LEADER_FETCH_INFLIGHT.get(key)
        if current is task and task.done():
            _LEADER_FETCH_INFLIGHT.pop(key, None)


# Position save interval (seconds) — avoid saving on every loop
@dataclass
class LeaderMonitorState:
    recent_activities: list[dict] = field(default_factory=list)
    seen_hashes: set[str] = field(default_factory=set)
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


def _cursor_key(cursor: TradeCursor) -> tuple[int, str, str]:
    return (
        int(cursor.timestamp or 0),
        str(cursor.tx_hash or "").strip().lower(),
        str(cursor.trade_id or "").strip().lower(),
    )


def _normalize_cursor(cursor: TradeCursor) -> TradeCursor:
    return TradeCursor(
        timestamp=int(cursor.timestamp or 0),
        tx_hash=str(cursor.tx_hash or "").strip().lower(),
        trade_id=str(cursor.trade_id or "").strip().lower() or None,
    )


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


# Strong references for fire-and-forget research-capture tasks. Without
# this set the asyncio event loop holds only a weak reference and the
# task can be garbage-collected mid-execution, surfacing as
# "Task was destroyed but it is pending!" warnings and silently
# dropping the capture. Tasks remove themselves on completion via
# the done_callback registered in _spawn_research_capture.
_research_capture_tasks: set[asyncio.Task] = set()


def _spawn_research_capture(
    research_capture_service: Any,
    raw_trades: list[dict],
) -> asyncio.Task:
    """Fire-and-forget the research-capture coroutine with a strong ref
    + a done-callback that logs any unhandled exception so the task is
    never silently lost."""
    task = asyncio.create_task(
        _run_research_capture(research_capture_service, raw_trades)
    )
    _research_capture_tasks.add(task)

    def _on_done(t: asyncio.Task) -> None:
        _research_capture_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.warning(
                "research_trade_capture_task_crashed",
                extra={"error": redact_error_detail(exc)},
            )

    task.add_done_callback(_on_done)
    return task





async def init(user_address: str, state: LeaderMonitorState):
    if not user_address or len(user_address) != 42:
        logger.error(f'Invalid wallet address "{user_address}" — must be 42-char hex (0x + 40 digits)')
        state.recent_activities = []
        state.seen_hashes.clear()
        return
    try:
        state.last_cursor = _normalize_cursor(state.last_cursor)
        state.last_processed_ts = max(state.last_processed_ts, float(state.last_cursor.timestamp))
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
        if normalized:
            latest_activity = max(normalized, key=_trade_ordering_key)
            ts, tx_hash, trade_id = _trade_ordering_key(latest_activity)
            latest_cursor = TradeCursor(timestamp=ts, tx_hash=tx_hash, trade_id=trade_id or None)
            if _cursor_key(latest_cursor) > _cursor_key(state.last_cursor):
                state.last_cursor = latest_cursor
                state.last_processed_ts = float(ts)
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


def _position_lookup_keys(position: dict[str, Any]) -> list[tuple[str, str]]:
    condition_id = str(position.get("conditionId") or position.get("condition_id") or "")
    token_ids = [
        str(position.get("tokenID") or ""),
        str(position.get("token_id") or ""),
        str(position.get("asset") or ""),
    ]
    if not condition_id:
        return []
    return [(condition_id, token_id) for token_id in token_ids if token_id]


async def _annotate_sell_fractions(trades: list[dict], leader_wallet: str) -> None:
    sell_trades = [trade for trade in trades if str(trade.get("side") or "").upper() == "SELL"]
    if not sell_trades:
        return

    try:
        positions = await _guarded_leader_fetch(
            "positions",
            leader_wallet,
            f"https://data-api.polymarket.com/positions?user={leader_wallet}",
        )
    except Exception as exc:
        logger.warning(
            "leader_positions_fetch_failed",
            extra={
                "leader_wallet": leader_wallet,
                "error": redact_error_detail(exc),
            },
        )
        return

    if not isinstance(positions, list):
        return

    remaining_by_key: dict[tuple[str, str], float] = {}
    for position in positions:
        try:
            size = float(position.get("size") or 0)
        except (TypeError, ValueError):
            continue
        if size < 0:
            continue
        for key in _position_lookup_keys(position):
            remaining_by_key[key] = size

    sell_trades_by_key: dict[tuple[str, str], list[dict]] = {}
    for trade in sell_trades:
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or "")
        token_id = str(trade.get("tokenID") or trade.get("token_id") or trade.get("asset") or "")
        try:
            sold_size = float(trade.get("size") or 0)
        except (TypeError, ValueError):
            sold_size = 0.0
        if not condition_id or not token_id or sold_size <= 0:
            continue
        sell_trades_by_key.setdefault((condition_id, token_id), []).append(trade)

    for key, trades_for_key in sell_trades_by_key.items():
        condition_id, token_id = key
        trades_for_key.sort(key=lambda trade: _to_seconds(trade.get("timestamp")))
        total_sold_per_key = 0.0
        for trade in trades_for_key:
            try:
                total_sold_per_key += float(trade.get("size") or 0)
            except (TypeError, ValueError):
                continue
        if total_sold_per_key <= 0:
            continue
        remaining_by_key[key] = max(0.0, remaining_by_key.get(key, 0.0))
        remaining_by_key[key] += total_sold_per_key

        for trade in trades_for_key:
            try:
                sold_size = float(trade.get("size") or 0)
            except (TypeError, ValueError):
                sold_size = 0.0
            if sold_size <= 0:
                continue

            remaining_size = max(0.0, remaining_by_key.get(key, 0.0))
            leader_size_before = remaining_size
            if leader_size_before <= 0:
                continue

            sell_fraction = min(1.0, max(0.0, sold_size / leader_size_before))
            trade["_leader_position_size_after"] = max(0.0, leader_size_before - sold_size)
            trade["_leader_position_size_before"] = leader_size_before
            trade["_leader_sell_fraction"] = sell_fraction
            remaining_by_key[key] = max(0.0, remaining_size - sold_size)


def _aggregate_trades(trades: list[dict], window_s: float = 2.0) -> list[dict]:
    """Merge bursty same-market fills into a single signal.

    Groups by leader + condition + asset + side inside a short time window.
    This reduces noisy micro-fill spam from the Polymarket activity feed.
    """
    if not trades:
        return []

    ordered = sorted(trades, key=lambda x: (_to_seconds(x.get('timestamp')), str(x.get('transactionHash') or '')))
    aggregated: list[dict] = []

    for trade in ordered:
        ts = _to_seconds(trade.get('timestamp'))
        asset = trade.get('tokenID') or trade.get('token_id') or trade.get('asset')
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
            last_asset = last.get('tokenID') or last.get('token_id') or last.get('asset')
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


async def _persist_cursor(cursor_store: Any, leader_wallet: str, cursor: TradeCursor) -> bool:
    """Persist *cursor* for *leader_wallet*, swallowing transient store errors.

    P2-h: ``cursor_store.set_cursor`` is a DB round-trip. Pre-fix an unguarded
    ``await`` here propagated a transient failure (PG blip, pool exhaustion) out
    of ``trade_monitor`` → ``run_leader_monitor`` and crashed the whole leader
    monitor task. The app supervisor logs-and-drops a crashed leader monitor
    (it is not a restartable ``_Supervised`` spec), so a single transient write
    error permanently stopped copying that leader until a full restart.

    A failed persist is non-fatal: the cursor lives in ``state.last_cursor`` and
    the next loop iteration retries the write. The only cost of a missed persist
    is re-reading already-seen activity on a *process* restart, which the
    cursor-comparison de-dup (``_cursor_key``) already tolerates — so dropping a
    write never re-executes a trade. Returns True on success, False on failure.
    """
    try:
        await cursor_store.set_cursor(leader_wallet, cursor)
        return True
    except asyncio.CancelledError:
        # Cooperative shutdown — must propagate so the task can unwind.
        raise
    except Exception as exc:
        metrics.inc("leader_cursor_persist_failed_total")
        logger.error(
            "leader_cursor_persist_failed | leader=%s error=%s",
            leader_wallet,
            redact_error_detail(exc),
        )
        return False


def _monitor_loop_healthy(*, fetch_ok: bool, fetch_interval_s: int, readiness_tracker: ReadinessTracker) -> bool:
    if not fetch_ok:
        return False
    readiness = readiness_tracker.get_all()
    if not bool(readiness.get("db_ok")):
        return False
    if not bool(readiness.get("api_ok", True)):
        return False
    last_poll_ts = int(readiness.get("last_trade_poll_ts") or 0)
    if last_poll_ts <= 0:
        return False
    max_poll_lag_s = max(30, int(fetch_interval_s) * 3)
    return (int(time.time()) - last_poll_ts) <= max_poll_lag_s


def _log_detected_trade(leader_wallet: str, trade: dict) -> None:
    submarket = (
        trade.get("submarket")
        or trade.get("subMarket")
        or trade.get("sub_market")
        or trade.get("marketSubtype")
        or trade.get("marketType")
        or trade.get("lineType")
        or trade.get("betType")
        or trade.get("groupItemTitle")
        or trade.get("groupTitle")
    )
    outcome = (
        trade.get("outcome")
        or trade.get("outcomeName")
        or trade.get("outcome_name")
        or trade.get("tokenOutcome")
        or trade.get("selection")
        or trade.get("selectionName")
        or trade.get("choice")
    )
    print_new_trade_detected(trade)
    log_trade_detected(
        logger,
        side=str(trade.get("side") or "").upper(),
        condition_id=trade.get("conditionId"),
        asset=trade.get("asset") or trade.get("tokenID") or trade.get("token_id"),
        size=float(trade.get("size", 0) or 0),
        price=float(trade.get("price", 0) or 0),
        usdc_value=_trade_usdc_value(trade),
        tx_hash=trade.get("transactionHash"),
        event_slug=trade.get("eventSlug") or trade.get("slug"),
        meta={
            "leaderWallet": leader_wallet,
            "proxyWallet": trade.get("proxyWallet") or trade.get("user"),
            "title": trade.get("title"),
            "submarket": submarket,
            "outcome": outcome,
            "aggregatedCount": int(trade.get("_aggregatedCount") or 1),
            "txHashes": list(trade.get("_txHashes") or []),
        },
    )


async def fetch_trade_data(
    user_address: str,
    on_trade_callback,
    state: LeaderMonitorState,
    readiness_tracker: ReadinessTracker,
    config: AppConfig,
    research_capture_service: Any | None = None,
):
    """Fetch new trades and call *on_trade_callback(trade)* immediately for each."""
    checked_activities = 0
    try:
        user_activities = await _guarded_leader_fetch(
            "activity",
            user_address,
            f'https://data-api.polymarket.com/activity?user={user_address}&limit=50&offset=0',
        )
    except Exception as e:
        logger.error(f'Fetch activity failed: {e}')
        return False, checked_activities
    user_activities = _normalize_activities(user_activities)
    checked_activities = len(user_activities)
    if not user_activities:
        readiness_tracker.update(last_trade_poll_ts=int(time.time()))
        return True, checked_activities
    try:
        current_timestamp = int(time.time())
        readiness_tracker.update(last_trade_poll_ts=current_timestamp)
        state.last_cursor = _normalize_cursor(state.last_cursor)
        cursor_key = _cursor_key(state.last_cursor)

        new_trades = [
            a for a in user_activities
            if _trade_ordering_key(a) > cursor_key
        ]
        if new_trades:
            newest = max(_to_seconds(t.get("timestamp")) for t in new_trades)
            ingest_lag = max(0.0, float(current_timestamp) - newest)
            metrics.set_gauge("ingest_lag_seconds", ingest_lag)
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
            metrics.inc("signals_seen_total")

            if not trade.get('asset'):
                trade['asset'] = trade.get('tokenID') or trade.get('token_id')

            trade_candidates.append(trade)
            ts = _to_seconds(trade.get("timestamp"))
            if ts > max_seen_ts:
                max_seen_ts = ts

        trade_list = _aggregate_trades(
            trade_candidates,
            window_s=config.polling.aggregate_signal_window_seconds
        )
        if trade_candidates and research_capture_service is not None:
            _spawn_research_capture(research_capture_service, list(trade_candidates))
        if trade_list:
            await _annotate_sell_fractions(trade_list, user_address)
        # Execute all detected trades immediately — no queue
        if trade_list:
            for trade in trade_list:
                _log_detected_trade(user_address, trade)
            log_trade_batch_summary(logger, leader_wallet=user_address, trades=trade_list)
            await on_trade_callback(trade_list)
            state.last_processed_ts = max_seen_ts

        # Save to DB only for new trades
        if new_trades:
            max_cursor_trade = max(new_trades, key=_trade_ordering_key)
            ts, tx_hash, trade_id = _trade_ordering_key(max_cursor_trade)
            next_cursor = TradeCursor(timestamp=ts, tx_hash=tx_hash, trade_id=trade_id or None)
            if _cursor_key(next_cursor) > _cursor_key(state.last_cursor):
                state.last_cursor = next_cursor
            state.last_processed_ts = max(state.last_processed_ts, float(ts))
            try:
                from models.userHistory import get_user_activity_model
                await get_user_activity_model(user_address).insert_many(new_trades)
            except Exception as e:
                logger.error(f'DB save activities: {e}')
    except Exception as e:
        logger.error(f'Error fetch_trade_data: {e}')
        return False, checked_activities
    return True, checked_activities


async def trade_monitor(
    bot_params: dict,
    on_trade_callback,
    readiness_tracker: ReadinessTracker,
    config: AppConfig,
    research_capture_service: Any | None = None,
):
    """Main monitor loop. Calls *on_trade_callback(trades_list)* immediately on detection."""
    global _fetch_spacing_config
    _fetch_spacing_config = config
    fetch_interval = float(bot_params.get('FETCH_INTERVAL', config.polling.fetch_interval))
    state = LeaderMonitorState()
    cursor_store = await LeaderCursorStore.create()
    leader_wallet = bot_params['leaderWallet']
    state.last_cursor = _normalize_cursor(await cursor_store.get_cursor(leader_wallet))
    state.last_processed_ts = float(state.last_cursor.timestamp)

    await init(leader_wallet, state)
    await _persist_cursor(cursor_store, leader_wallet, state.last_cursor)
    monitored_leader_count = int(bot_params.get('MONITORED_LEADER_COUNT', 1) or 1)
    while True:
        previous_cursor = state.last_cursor
        fetch_ok, checked_activities = await fetch_trade_data(
            leader_wallet,
            on_trade_callback,
            state,
            readiness_tracker=readiness_tracker,
            config=config,
            research_capture_service=research_capture_service,
        )
        if (
            state.last_cursor.timestamp != previous_cursor.timestamp
            or state.last_cursor.tx_hash != previous_cursor.tx_hash
            or (state.last_cursor.trade_id or "") != (previous_cursor.trade_id or "")
        ):
            await _persist_cursor(cursor_store, leader_wallet, state.last_cursor)

        if _monitor_loop_healthy(fetch_ok=fetch_ok, fetch_interval_s=fetch_interval, readiness_tracker=readiness_tracker):
            monitor_heartbeat_tick(
                trader_count=monitored_leader_count,
                checked_activities=checked_activities,
                leader_wallet=leader_wallet,
                monitored_leader_count=monitored_leader_count,
                symbol="🟢",
            )

        await asyncio.sleep(fetch_interval)
