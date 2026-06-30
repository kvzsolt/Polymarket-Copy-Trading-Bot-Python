"""``bot.py tune-strategy`` — tune env params for a leader (dry-run by default) (PR3)."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path


def register_tune_strategy_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "tune-strategy",
        help="Tune copy-strategy env params for a leader (dry-run by default)",
    )
    parser.add_argument("--wallet", required=True, help="Leader wallet address (0x...)")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute patches and print the explanation without writing (default)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the patches to --target (requires --target)",
    )
    parser.add_argument(
        "--target",
        default="config/runtime.local.env",
        help="Env file to patch (default: config/runtime.local.env)",
    )
    parser.add_argument(
        "--patch-out",
        default=None,
        help="Optional path to write the patch table as a text file",
    )
    parser.add_argument(
        "--allow-leader-rewrite",
        action="store_true",
        help="Allow rewriting LEADER_WALLETS (off by default; requires --apply)",
    )


async def _run_tune_strategy_async(args: argparse.Namespace) -> int:
    from services.strategy_profile.env_patch_writer import EnvPatchWriter
    from services.strategy_profile.leader_history_loader import LeaderHistoryLoader
    from services.strategy_profile.strategy_env_tuner import tune_strategy
    from services.strategy_profile.strategy_profiler import profile_leader

    if args.apply and not args.target:
        print("tune-strategy: --apply requires --target", flush=True)
        return 2

    loader = LeaderHistoryLoader(lookback_days=args.lookback_days)
    history = await loader.load(args.wallet)
    if not history.wallet:
        print("tune-strategy: --wallet is required", flush=True)
        return 2

    profile = profile_leader(history)
    patches = tune_strategy(profile)

    writer = EnvPatchWriter(args.target)
    result = writer.apply(
        patches,
        apply=bool(args.apply),
        allow_leader_rewrite=bool(args.allow_leader_rewrite),
    )

    print(result.explanation, flush=True)
    if not profile.copyable:
        print(
            f"\nNote: archetype={profile.archetype.value} is not copyable. "
            "No env changes are recommended.",
            flush=True,
        )

    if args.patch_out:
        Path(args.patch_out).write_text(result.explanation + "\n", encoding="utf-8")
        print(f"tune-strategy: patch table written to {args.patch_out}", flush=True)

    if result.applied and result.backup_path:
        print(f"tune-strategy: backup written to {result.backup_path}", flush=True)

    return 0


def run_tune_strategy(args: argparse.Namespace) -> int:
    return asyncio.run(_run_tune_strategy_async(args))