#!/usr/bin/env python3
"""Run or inspect trader discovery without starting trading runtimes."""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list-only", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--json-out", default="")
    parser.add_argument("--require-active-candidates", action="store_true")
    return parser.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    from pmcopy.bootstrap import build_app
    from pmcopy.storage.postgres.connection import shutdown_pool

    application = await build_app()
    try:
        candidates = []
        if not args.list_only:
            candidates = await application.discovery_service.run()
            await application.registry.reconcile_candidates(candidates)
        snapshots = await application.discovery_repo.list_snapshots(limit=max(1, args.limit))
        roster = await application.leaders_repo.list_roster()
        active = await application.leaders_repo.list_active(
            config_hash=application.discovery_service.config_hash
        )
        payload = {
            "discovery_ran": not args.list_only,
            "accepted_count": len(candidates),
            "active_wallets": active,
            "roster": [
                {
                    "wallet": entry.wallet,
                    "tier": entry.tier,
                    "score": entry.score,
                    "cycles_in_tier": entry.cycles_in_tier,
                    "promote_streak": entry.promote_streak,
                    "demote_streak": entry.demote_streak,
                }
                for entry in roster[: max(1, args.limit)]
            ],
            "snapshots": snapshots,
        }
        rendered = json.dumps(payload, indent=2, default=str)
        print(rendered)
        if args.json_out:
            output = Path(args.json_out)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
        return 1 if args.require_active_candidates and not active else 0
    finally:
        await application.aclose()
        await shutdown_pool()


def main(argv: list[str] | None = None) -> int:
    from pmcopy.utils.event_loop import new_event_loop

    with asyncio.Runner(loop_factory=new_event_loop) as runner:
        return runner.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
