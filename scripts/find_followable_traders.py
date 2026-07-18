"""Export the latest single-generation discovery audit snapshot."""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import asyncio
import json
import time
from pathlib import Path

OUTPUT_JSON = Path("data/followable_traders.json")


async def main() -> None:
    from bootstrap import build_app
    from storage.postgres.connection import shutdown_pool

    application = await build_app()
    try:
        rows = await application.discovery_repo.list_snapshots(limit=100)
        active = await application.leaders_repo.list_active(
            config_hash=application.discovery_service.config_hash
        )
        payload = {
            "generated_at": time.time(),
            "candidate_count": len(rows),
            "active_wallets": active,
            "candidates": rows,
        }
        OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_JSON.write_text(
            json.dumps(payload, indent=2, default=str),
            encoding="utf-8",
        )
    finally:
        await application.aclose()
        await shutdown_pool()


if __name__ == "__main__":
    from utils.event_loop import new_event_loop

    with asyncio.Runner(loop_factory=new_event_loop) as runner:
        runner.run(main())
