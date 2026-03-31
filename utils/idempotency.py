import sqlite3
import threading
import time
from pathlib import Path

from config.env import ENV
from utils.logger import get_logger

logger = get_logger("Idempotency")

_LOCK = threading.Lock()
_INITIALIZED = False
_DB_PATH = Path(ENV.get('BOT_STATE_DB', 'data/bot_state.db'))


def _normalize(value: str | None) -> str:
    return str(value or '').strip().lower()


def build_trade_key(leader_wallet: str, trade: dict) -> str:
    tx_hash = trade.get('transactionHash') or trade.get('txHash') or ''
    side = str(trade.get('side') or '').strip().upper()
    asset = trade.get('asset') or trade.get('tokenID') or trade.get('token_id') or ''
    return f'{_normalize(leader_wallet)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}'


def _connect() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(_DB_PATH)


def _init_table() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    with _connect() as conn:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS processed_trades (
                trade_key TEXT PRIMARY KEY,
                leader_wallet TEXT NOT NULL,
                tx_hash TEXT,
                side TEXT,
                asset TEXT,
                condition_id TEXT,
                status TEXT NOT NULL,
                processed_at REAL NOT NULL
            )
            '''
        )
        conn.commit()
    _INITIALIZED = True


def has_processed(trade_key: str) -> bool:
    if not trade_key:
        return False
    with _LOCK:
        _init_table()
        with _connect() as conn:
            row = conn.execute(
                'SELECT 1 FROM processed_trades WHERE trade_key = ? LIMIT 1',
                (trade_key,),
            ).fetchone()
    return row is not None


def mark_processed(leader_wallet: str, trade: dict, *, status: str = 'processed') -> str:
    trade_key = build_trade_key(leader_wallet, trade)
    if not trade_key:
        return ''

    tx_hash = trade.get('transactionHash') or trade.get('txHash') or ''
    side = str(trade.get('side') or '').strip().upper()
    asset = trade.get('asset') or trade.get('tokenID') or trade.get('token_id') or ''
    condition_id = trade.get('conditionId') or ''

    with _LOCK:
        _init_table()
        with _connect() as conn:
            conn.execute(
                '''
                INSERT OR IGNORE INTO processed_trades
                (trade_key, leader_wallet, tx_hash, side, asset, condition_id, status, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    trade_key,
                    _normalize(leader_wallet),
                    str(tx_hash),
                    side,
                    str(asset),
                    str(condition_id),
                    status,
                    time.time(),
                ),
            )
            conn.commit()
    return trade_key


def mark_processed_all_tx_hashes(leader_wallet: str, trade: dict, *, status: str = 'processed') -> list[str]:
    """Mark the trade as processed, including any aggregated tx hashes.

    Trade monitor aggregation can merge multiple fills into one signal and attach
    `_txHashes`. Without marking each underlying tx hash, the bot can re-process
    earlier fills after restarts or cursor lag.

    Returns list of trade_keys that were marked (may be empty).
    """
    keys: list[str] = []
    # Mark primary trade
    primary = mark_processed(leader_wallet, trade, status=status)
    if primary:
        keys.append(primary)

    tx_hashes = trade.get('_txHashes') or trade.get('txHashes') or []
    if isinstance(tx_hashes, (list, tuple)):
        for txh in tx_hashes:
            if not txh:
                continue
            # Create a minimal trade dict carrying this tx hash.
            t2 = {
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


def reset_cache_for_tests() -> None:
    """Test helper to force table re-init if env/path changes."""
    global _INITIALIZED
    _INITIALIZED = False

