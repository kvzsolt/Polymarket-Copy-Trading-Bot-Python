from __future__ import annotations

import threading
from pathlib import Path

from config.env import ENV
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from utils.logger import get_logger

logger = get_logger("Idempotency")

# Schema-init guard. Pre-fix the check-then-add was unlocked, so two
# concurrent callers could both observe ``db_path not in _SCHEMA_READY``
# and both run ``apply_migrations`` against the same SQLite file. With
# the wrapping BEGIN IMMEDIATE landed in this branch the second migrate
# would block on the writer lock and re-run idempotently, but the lock
# below removes the racy double-init at the Python layer regardless.
_SCHEMA_READY: set[str] = set()
_SCHEMA_LOCK = threading.Lock()


def _current_db_path() -> Path:
    """Resolve the SQLite path on every call.

    Pre-fix this was a module-level constant resolved at import time. Test
    harnesses that point ``BOT_STATE_DB`` at a tmp file AFTER import (or
    multi-user configs that swap the path mid-run) silently kept the
    first-imported path, so the second user's processed_trades landed in
    the first user's DB. Re-resolve from ENV on every call.
    """
    return Path(str(ENV.get("BOT_STATE_DB", "data/bot_state.db")))



def _normalize(value: str | None) -> str:
    return str(value or '').strip().lower()



def build_trade_key(leader_wallet: str, trade: dict) -> str:
    tx_hash = trade.get('transactionHash') or trade.get('txHash') or ''
    side = str(trade.get('side') or '').strip().upper()
    asset = trade.get('asset') or trade.get('tokenID') or trade.get('token_id') or ''
    user_id = trade.get('user_id') or trade.get('userId') or ''
    user_part = _normalize(user_id)
    if user_part:
        return f'{user_part}|{_normalize(leader_wallet)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}'
    return f'{_normalize(leader_wallet)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}'



def _repo() -> OrdersRepo:
    db_path = str(_current_db_path())
    # Double-checked locking: cheap read first, then the (mutating) init
    # under the lock with a second read to handle a racing first init.
    if db_path not in _SCHEMA_READY:
        with _SCHEMA_LOCK:
            if db_path not in _SCHEMA_READY:
                with create_connection(db_path) as conn:
                    apply_migrations(conn)
                    conn.commit()
                _SCHEMA_READY.add(db_path)
    return OrdersRepo(db_path)



def has_processed(trade_key: str) -> bool:
    return _repo().has_processed_trade(trade_key)



def mark_processed(leader_wallet: str, trade: dict, *, status: str = 'processed') -> str:
    trade_key = build_trade_key(leader_wallet, trade)
    if not trade_key:
        return ''

    _repo().mark_processed_trade(
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



def mark_processed_all_tx_hashes(leader_wallet: str, trade: dict, *, status: str = 'processed') -> list[str]:
    keys: list[str] = []
    primary = mark_processed(leader_wallet, trade, status=status)
    if primary:
        keys.append(primary)

    tx_hashes = trade.get('_txHashes') or trade.get('txHashes') or []
    if isinstance(tx_hashes, (list, tuple)):
        for txh in tx_hashes:
            if not txh:
                continue
            t2 = {
                'user_id': trade.get('user_id') or trade.get('userId') or '',
                'transactionHash': txh,
                'txHash': txh,
                'side': trade.get('side'),
                'asset': trade.get('asset') or trade.get('tokenID') or trade.get('token_id'),
                'conditionId': trade.get('conditionId') or trade.get('condition_id'),
            }
            k = mark_processed(leader_wallet, t2, status=status)
            if k and k not in keys:
                keys.append(k)
    return keys



def purge_stale_records(max_age_seconds: float | None = None) -> int:
    """Delete old processed_trades. Returns total rows deleted."""
    age = max_age_seconds or float(ENV.get("RETENTION_MAX_AGE_SECONDS", 30 * 86400))
    repo = _repo()
    n = repo.purge_old_processed_trades(age)
    if n:
        logger.info(f"Purged {n} stale processed_trades (age > {age:.0f}s)")
    return n


def reset_cache_for_tests() -> None:
    with _SCHEMA_LOCK:
        _SCHEMA_READY.clear()
