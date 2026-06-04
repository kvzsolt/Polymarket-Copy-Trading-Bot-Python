#!/usr/bin/env python3
"""Run Falcon/Polymarket discovery only.

This script intentionally does NOT start the copy-trading bot. It does not
create CLOB clients, market-data runtimes, user-data runtimes, leader monitors,
risk monitors, or write env files.

Usage:
    python scripts/run_discovery_only.py --once
    python scripts/run_discovery_only.py --list-only
    python scripts/run_discovery_only.py --once --require-live-candidates
    python scripts/run_discovery_only.py --json-out data/discovery_snapshot.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Falcon/Polymarket trader discovery without running the bot."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one discovery pass before listing DB evaluations.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only list existing DB evaluations; do not call discovery.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum evaluations to print.",
    )
    parser.add_argument(
        "--eval-limit",
        type=int,
        default=0,
        help="Maximum Falcon candidates to evaluate in this run. 0 means no explicit cap.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional path to write the summary JSON.",
    )
    parser.add_argument(
        "--require-live-candidates",
        action="store_true",
        help="Exit 1 if no live-eligible candidates are found.",
    )
    return parser.parse_args(argv)


def _evaluation_to_dict(e: Any) -> dict[str, Any]:
    uncopyable_reason_counts = _uncopyable_reason_counts(e)
    return {
        "wallet": getattr(e, "wallet", ""),
        "status": getattr(e, "status", ""),
        "eligible_live": bool(getattr(e, "eligible_live", False)),
        "eligible_shadow": bool(getattr(e, "eligible_shadow", False)),
        "final_score": float(getattr(e, "final_score", 0.0) or 0.0),
        "falcon_quality_score": float(getattr(e, "falcon_quality_score", 0.0) or 0.0),
        "copyability_score": float(getattr(e, "copyability_score", 0.0) or 0.0),
        "recent_state_score": float(getattr(e, "recent_state_score", 0.0) or 0.0),
        "market_fit_score": float(getattr(e, "market_fit_score", 0.0) or 0.0),
        "copyable_signal_ratio": float(getattr(e, "copyable_signal_ratio", 0.0) or 0.0),
        "copyable_notional_ratio": float(getattr(e, "copyable_notional_ratio", 0.0) or 0.0),
        "meaningful_signals": int(getattr(e, "meaningful_signals", 0) or 0),
        "copyable_signals": int(getattr(e, "copyable_signals", 0) or 0),
        "sports_esports_ratio": float(getattr(e, "sports_esports_ratio", 0.0) or 0.0),
        "fast_crypto_signal_ratio": float(getattr(e, "fast_crypto_signal_ratio", 0.0) or 0.0),
        "fast_crypto_notional_ratio": float(getattr(e, "fast_crypto_notional_ratio", 0.0) or 0.0),
        "pnl_7d": str(getattr(e, "pnl_7d", "0")),
        "pnl_3d": str(getattr(e, "pnl_3d", "0")),
        "rejection_reason": getattr(e, "rejection_reason", None),
        "uncopyable_reason_counts": dict(sorted(uncopyable_reason_counts.items())),
        "updated_at": float(getattr(e, "updated_at", 0.0) or 0.0),
    }


def _uncopyable_reason_counts(e: Any) -> Counter:
    replay_json = getattr(e, "replay_json", {}) or {}
    if not isinstance(replay_json, dict):
        return Counter()
    counts: Counter = Counter()
    for item in replay_json.get("signal_results", ()) or ():
        if not isinstance(item, dict):
            continue
        if bool(item.get("copyable")):
            continue
        reason = str(item.get("reason") or "").strip()
        if reason and reason != "ok":
            counts[reason] += 1
    return counts


async def _amain(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if args.once and args.list_only:
        print("--once and --list-only are mutually exclusive", file=sys.stderr)
        return 2

    # Import project modules after argument parsing so --help works even if
    # local runtime dependencies are not fully configured.
    from config.runtime_env import load_runtime_env
    from config.env import rehydrate_env, validate_runtime_env
    from config.app_config import EnvValidationError, load_config
    from bootstrap import build_app
    from utils.logger import configure_logging, get_logger, redact_error_detail

    logger = get_logger("DiscoveryOnly")
    configure_logging(level="INFO")

    load_runtime_env(override_secrets=True)
    rehydrate_env(load_dotenv_file=False)

    try:
        validate_runtime_env()
        _startup_config = load_config()
    except EnvValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        app = await build_app()

        logger.info(
            "discovery_only_started",
            extra={
                "once": bool(args.once),
                "list_only": bool(args.list_only),
                "limit": int(args.limit),
                "eval_limit": int(args.eval_limit),
                "require_live_candidates": bool(args.require_live_candidates),
                "falcon_configured": bool(app.config.discovery.falcon_api_token),
                "lookback_days": app.config.discovery.polymarket_history_lookback_days,
                "falcon_page_size": app.config.discovery.falcon_page_size,
                "falcon_max_pages": app.config.discovery.falcon_max_pages,
            },
        )

        profiles = []
        if not args.list_only:
            profiles = await app.discovery_service.discover_candidates(
                limit=args.eval_limit or None
            )
            logger.info(
                "discovery_only_pass_complete",
                extra={"profile_count": len(profiles)},
            )

        evaluations = await app.discovery_repo.list_evaluations(limit=int(args.limit))
        live = [e for e in evaluations if bool(getattr(e, "eligible_live", False))]
        shadow = [
            e
            for e in evaluations
            if bool(getattr(e, "eligible_shadow", False))
            and not bool(getattr(e, "eligible_live", False))
        ]
        rejected = [
            e
            for e in evaluations
            if not bool(getattr(e, "eligible_live", False))
            and not bool(getattr(e, "eligible_shadow", False))
        ]

        rejection_counts = Counter(
            str(getattr(e, "rejection_reason", None) or "none")
            for e in evaluations
        )
        global_uncopyable_reason_counts: Counter = Counter()
        for e in evaluations:
            global_uncopyable_reason_counts.update(_uncopyable_reason_counts(e))

        payload = {
            "discovery_ran": not args.list_only,
            "profiles_returned": len(profiles),
            "total_listed": len(evaluations),
            "live_eligible_count": len(live),
            "shadow_count": len(shadow),
            "rejected_count": len(rejected),
            "rejection_counts": dict(sorted(rejection_counts.items())),
            "global_uncopyable_reason_counts": dict(
                sorted(global_uncopyable_reason_counts.items())
            ),
            "top": [_evaluation_to_dict(e) for e in evaluations[: int(args.limit)]],
        }

        print(json.dumps(payload, indent=2, default=str))

        if args.json_out:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(payload, indent=2, default=str),
                encoding="utf-8",
            )
            logger.info("discovery_only_json_written", extra={"path": str(out_path)})

        if args.require_live_candidates and not live:
            logger.error(
                "discovery_only_no_live_candidates",
                extra={"listed_count": len(evaluations)},
            )
            return 1

        logger.info(
            "discovery_only_finished",
            extra={
                "live_eligible_count": len(live),
                "shadow_count": len(shadow),
                "rejected_count": len(rejected),
            },
        )
        return 0

    except Exception as exc:
        logger.error(
            "discovery_only_failed",
            exc_info=True,
            extra={
                "error_type": type(exc).__name__,
                "error": redact_error_detail(exc),
            },
        )
        return 1
    finally:
        try:
            from storage.postgres.connection import shutdown_pool

            await shutdown_pool()
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
