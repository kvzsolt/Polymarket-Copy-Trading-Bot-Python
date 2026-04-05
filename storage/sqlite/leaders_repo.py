from __future__ import annotations

import sqlite3
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


@dataclass
class LeaderTierRecord:
    wallet: str
    follow_tier: str
    tier_changed_at: float | None


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

    def get_tier(self, wallet: str) -> LeaderTierRecord:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT wallet, follow_tier, tier_changed_at FROM discovered_leaders WHERE wallet = ?",
                (wallet.lower(),),
            ).fetchone()
        if row is None:
            return LeaderTierRecord(wallet=wallet.lower(), follow_tier="watchlist", tier_changed_at=None)
        return LeaderTierRecord(
            wallet=str(row["wallet"]),
            follow_tier=str(row["follow_tier"]) if row["follow_tier"] else "watchlist",
            tier_changed_at=float(row["tier_changed_at"]) if row["tier_changed_at"] else None,
        )

    def update_tier(self, wallet: str, follow_tier: str, tier_changed_at: float) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE discovered_leaders
                SET follow_tier = ?, tier_changed_at = ?, updated_at = ?
                WHERE wallet = ?
                """,
                (follow_tier, tier_changed_at, time.time(), wallet.lower()),
            )
            conn.commit()

    def get_tier_for_leader(self, wallet: str) -> str:
        """Return the follow tier string for a leader. Defaults to 'watchlist'."""
        rec = self.get_tier(wallet)
        return rec.follow_tier

    def get_leaders_by_tier(self, tier: str) -> list[str]:
        """Return wallet addresses for all leaders at a given tier."""
        with create_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT wallet FROM discovered_leaders WHERE follow_tier = ?",
                (tier,),
            ).fetchall()
        return [row["wallet"] for row in rows]
