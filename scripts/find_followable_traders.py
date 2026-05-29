"""Write a snapshot of Falcon/Polymarket discovery candidates.

This script is intentionally read-only with respect to runtime leadership:
it never edits LEADER_WALLETS and never starts monitors. Use
`python bot.py rotate-traders` for operator-confirmed rotation.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from config.app_config import load_config
from config.db import connect_db
from config.env import rehydrate_env
from config.runtime_env import load_runtime_env
from storage.postgres.connection import get_pool
from storage.postgres.discovery_repo import DiscoveryRepo
from utils.logger import get_logger


logger = get_logger("TraderFinder")
OUTPUT_JSON = Path("data/followable_traders.json")


def _row(row) -> dict:
    return {
        "wallet": row.wallet,
        "status": row.status,
        "final_score": row.final_score,
        "falcon_quality_score": row.falcon_quality_score,
        "copyability_score": row.copyability_score,
        "copyable_signal_ratio": row.copyable_signal_ratio,
        "copyable_notional_ratio": row.copyable_notional_ratio,
        "sports_esports_ratio": row.sports_esports_ratio,
        "fast_crypto_signal_ratio": row.fast_crypto_signal_ratio,
        "fast_crypto_notional_ratio": row.fast_crypto_notional_ratio,
        "pnl_7d": str(row.pnl_7d),
        "pnl_3d": str(row.pnl_3d),
        "rejection_reason": row.rejection_reason,
        "eligible_live": row.eligible_live,
        "eligible_shadow": row.eligible_shadow,
        "updated_at": row.updated_at,
    }


async def main() -> None:
    load_runtime_env(override_secrets=True)
    rehydrate_env(load_dotenv_file=False)
    await connect_db()
    config = load_config()
    pool = await get_pool()
    repo = DiscoveryRepo(pool)
    rows = await repo.list_evaluations(statuses=["live_eligible", "shadow"], limit=100)

    payload = {
        "generated_at": time.time(),
        "mode": "db_only",
        "rotation_command": "python bot.py rotate-traders",
        "thresholds": {
            "min_copyable_grouped_signal_ratio": (
                config.discovery.discovery_min_copyable_grouped_signal_ratio
            ),
            "min_copyable_notional_ratio": (
                config.discovery.discovery_min_copyable_notional_ratio
            ),
        },
        "candidate_count": len(rows),
        "candidates": [_row(row) for row in rows],
    }
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("wrote %s with %d candidates", OUTPUT_JSON, len(rows))


if __name__ == "__main__":
    asyncio.run(main())

