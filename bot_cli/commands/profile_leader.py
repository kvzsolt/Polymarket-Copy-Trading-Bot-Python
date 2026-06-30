"""``bot.py profile-leader`` — profile a leader wallet and emit a JSON profile (PR3)."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path


def register_profile_leader_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "profile-leader",
        help="Profile a leader wallet's trading strategy and emit a JSON profile",
    )
    parser.add_argument("--wallet", required=True, help="Leader wallet address (0x...)")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--output", default=None, help="Output JSON path (default: stdout)")


async def _run_profile_leader_async(args: argparse.Namespace) -> int:
    from services.strategy_profile.leader_history_loader import LeaderHistoryLoader
    from services.strategy_profile.strategy_profiler import profile_leader

    loader = LeaderHistoryLoader(lookback_days=args.lookback_days)
    history = await loader.load(args.wallet)
    if not history.wallet:
        print("profile-leader: --wallet is required", flush=True)
        return 2
    profile = profile_leader(history)
    payload = {
        "wallet": profile.wallet,
        "lookback_days": profile.lookback_days,
        "archetype": str(profile.archetype),
        "archetype_name": profile.archetype.value,
        "copyable": profile.copyable,
        "computed_at": profile.computed_at,
        "notes": profile.notes,
        "features": {
            k: (v if v is not None else None) for k, v in profile.features.items()
        },
        "history_counts": {
            "activity": len(history.activity),
            "trades": len(history.trades),
            "closed_positions": len(history.closed_positions),
        },
    }
    text = json.dumps(payload, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
        print(f"profile-leader: wrote {args.output}", flush=True)
    else:
        print(text, flush=True)
    return 0


def run_profile_leader(args: argparse.Namespace) -> int:
    return asyncio.run(_run_profile_leader_async(args))