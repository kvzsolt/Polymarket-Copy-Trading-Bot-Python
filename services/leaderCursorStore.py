from __future__ import annotations

from config.db import get_db_path
from storage.sqlite.cursors_repo import CursorsRepo, TradeCursor
from utils.logger import get_logger

logger = get_logger("LeaderCursorStore")


class LeaderCursorStore:
    def __init__(self, db_path: str | None = None):
        self._repo = CursorsRepo(db_path or get_db_path())

    def get_last_timestamp(self, leader_wallet: str) -> float:
        try:
            return float(self._repo.get_cursor(leader_wallet).timestamp)
        except Exception as exc:
            logger.warning(f"Failed reading cursor for {leader_wallet[:10]}…: {exc}")
            return 0.0

    def set_last_timestamp(self, leader_wallet: str, ts: float) -> None:
        try:
            current = self._repo.get_cursor(leader_wallet)
            self._repo.set_cursor(
                leader_wallet,
                TradeCursor(
                    timestamp=int(ts),
                    tx_hash=current.tx_hash,
                    trade_id=current.trade_id,
                ),
            )
        except Exception as exc:
            logger.warning(f"Failed writing cursor for {leader_wallet[:10]}…: {exc}")

    def get_cursor(self, leader_wallet: str) -> TradeCursor:
        return self._repo.get_cursor(leader_wallet)

    def set_cursor(self, leader_wallet: str, cursor: TradeCursor) -> None:
        self._repo.set_cursor(leader_wallet, cursor)
