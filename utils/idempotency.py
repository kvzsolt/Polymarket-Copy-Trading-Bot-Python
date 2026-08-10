"""Idempotency helpers — async PG (PG-A5 sweep).

``build_trade_key`` is pure (no IO) and is also re-exported from
``utils/__init__.py`` for the release-gate suite.

The IO-touching helpers (``has_processed``, ``mark_processed``,
``mark_processed_all_tx_hashes``, ``purge_stale_records``) now route
through the canonical async ``storage.postgres.orders_repo.OrdersRepo``
against the singleton asyncpg pool from
``storage.postgres.connection.get_pool``. Callers must ``await`` them.
"""
from __future__ import annotations

from typing import Any

from utils.logger import get_logger

logger = get_logger("Idempotency")


def _normalize(value: str | None) -> str:
    return str(value or '').strip().lower()


def build_trade_key(leader_wallet: str, trade: dict) -> str:
    """Pure helper — namespace + canonicalise trade identity into a
    stable string key. Pre-PG-A5 behaviour preserved."""
    tx_hash = trade.get('transactionHash') or trade.get('txHash') or ''
    side = str(trade.get('side') or '').strip().upper()
    asset = trade.get('asset') or trade.get('tokenID') or trade.get('token_id') or ''
    user_id = trade.get('user_id') or trade.get('userId') or ''
    user_part = _normalize(user_id)
    if user_part:
        return f'{user_part}|{_normalize(leader_wallet)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}'
    return f'{_normalize(leader_wallet)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}'


async def _repo():
    """Return an async PG ``OrdersRepo`` bound to the live pool.

    Fails closed when the pool is uninit — callers must ensure
    ``await connect_db()`` ran during boot.
    """
    from storage.postgres.connection import get_pool
    from storage.postgres.orders_repo import OrdersRepo

    pool = await get_pool()
    return OrdersRepo(pool)


async def has_processed(trade_key: str) -> bool:
    repo = await _repo()
    return await repo.has_processed_trade(trade_key)


async def mark_processed(
    leader_wallet: str, trade: dict, *, status: str = 'processed'
) -> str:
    trade_key = build_trade_key(leader_wallet, trade)
    if not trade_key:
        return ''

    repo = await _repo()
    await repo.mark_processed_trade(
        trade_key=trade_key,
        user_id=str(trade.get('user_id') or trade.get('userId') or ''),
        leader_id=_normalize(leader_wallet),
        tx_hash=str(trade.get('transactionHash') or trade.get('txHash') or ''),
        side=str(trade.get('side') or '').strip().upper(),
        asset=str(trade.get('asset') or trade.get('tokenID') or trade.get('token_id') or ''),
        condition_id=str(trade.get('conditionId') or trade.get('condition_id') or ''),
        status=status,
    )
    return trade_key


async def mark_processed_all_tx_hashes(
    leader_wallet: str, trade: dict, *, status: str = 'processed'
) -> list[str]:
    keys: list[str] = []
    primary = await mark_processed(leader_wallet, trade, status=status)
    if primary:
        keys.append(primary)

    tx_hashes = trade.get('_txHashes') or trade.get('txHashes') or []
    if isinstance(tx_hashes, (list, tuple)):
        for txh in tx_hashes:
            if not txh:
                continue
            t2: dict[str, Any] = {
                'user_id': trade.get('user_id') or trade.get('userId') or '',
                'transactionHash': txh,
                'txHash': txh,
                'side': trade.get('side'),
                'asset': trade.get('asset') or trade.get('tokenID') or trade.get('token_id'),
                'conditionId': trade.get('conditionId') or trade.get('condition_id'),
            }
            k = await mark_processed(leader_wallet, t2, status=status)
            if k and k not in keys:
                keys.append(k)
    return keys


async def purge_stale_records(max_age_seconds: float | None = None) -> int:
    """Delete old processed_trades. Returns total rows deleted."""
    if max_age_seconds is not None:
        age = float(max_age_seconds)
    else:
        # Fail-closed default mirrors the previous
        # ``ENV.get("RETENTION_MAX_AGE_SECONDS", 30*86400)`` read. Callers
        # that need a different retention pass it explicitly (e.g. app.py
        # reads ``application.config.app.retention_max_age_seconds``).
        age = float(30 * 86400)
    repo = await _repo()
    n = await repo.purge_old_processed_trades(age)
    if n:
        logger.info(f"Purged {n} stale processed_trades (age > {age:.0f}s)")
    return n
