from __future__ import annotations

import time
from dataclasses import dataclass

from storage.sqlite.connection import create_connection


@dataclass
class LeaderCandidateRecord:
    leader_id: str
    score: float
    recent_trades: int
    active: bool
    rejection_reason: str | None = None
    source: str | None = None


class LeadersRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def upsert_candidates(self, records: list[LeaderCandidateRecord]) -> None:
        if not records:
            return

        with create_connection(self.db_path) as conn:
            for rec in records:
                conn.execute(
                    """
                    INSERT INTO discovered_leaders (
                        wallet, composite_score, total_positions, active, rejection_reason, source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(wallet) DO UPDATE SET
                        composite_score = excluded.composite_score,
                        total_positions = excluded.total_positions,
                        active = excluded.active,
                        rejection_reason = excluded.rejection_reason,
                        source = excluded.source,
                        updated_at = excluded.updated_at
                    """,
                    (
                        rec.leader_id.lower(),
                        float(rec.score),
                        int(rec.recent_trades),
                        1 if rec.active else 0,
                        rec.rejection_reason,
                        rec.source,
                        time.time(),
                    ),
                )
            conn.commit()

    def list_active(self) -> list[str]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT wallet
                FROM discovered_leaders
                WHERE active = 1
                ORDER BY composite_score DESC, wallet ASC
                """
            ).fetchall()
        return [str(r["wallet"]) for r in rows]
