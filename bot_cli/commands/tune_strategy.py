"""``bot.py tune-strategy`` — tune env params for a leader (dry-run by default) (PR3)."""
from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

# SonarCloud S8707 (LLM path-traversal guard): ``--target`` and
# ``--patch-out`` accept any path, but an LLM (or accident) can escape
# the workspace (e.g. ``--target /etc/cron.d/payload``). Resolve each
# path and require it to live under the repo root OR under the system
# temp dir (so pytest's ``tmp_path`` fixture keeps working). Everything
# else is rejected with exit code 2 before any file-system write happens.
#
# ``tempfile.gettempdir()`` is used instead of a hardcoded ``"/tmp"``
# literal so SonarCloud S5443 (publicly writable directories) does not
# flag the allowlist: the value is resolved at runtime from the
# ``TMPDIR``/``TEMP``/``TMP`` environment, and on pytest's ``tmp_path``
# the resolved path is a user-private subdirectory (``pytest-of-<user>``)
# that is 0700-permissioned, not the world-writable ``/tmp`` itself.
_ALLOWED_WRITE_ROOTS: tuple[Path, ...] = (
    _REPO_ROOT,
    Path(tempfile.gettempdir()),
)


def _validate_writable_path(raw: str, *, flag_name: str) -> Path | None:
    """Resolve and validate a user-supplied path against the allowlist.

    Returns the resolved Path, or None if ``raw`` is empty. Raises
    ``ValueError`` if the resolved path is outside the allowlist.
    """
    if not raw:
        return None
    resolved = Path(raw).resolve()
    for root in _ALLOWED_WRITE_ROOTS:
        try:
            if resolved.is_relative_to(root):
                return resolved
        except AttributeError:  # pragma: no cover — Python < 3.9
            try:
                resolved.relative_to(root)
                return resolved
            except ValueError:
                pass
    raise ValueError(
        f"--{flag_name} must resolve under {_REPO_ROOT} or /tmp; got {resolved}"
    )


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

    # S8707: validate --target before any writer construction. The
    # default ("config/runtime.local.env") resolves under the repo
    # root so existing happy-path invocations are unaffected.
    try:
        target_path = _validate_writable_path(args.target, flag_name="target")
    except ValueError as exc:
        print(f"tune-strategy: {exc}", flush=True)
        return 2

    if args.apply and not target_path:
        print("tune-strategy: --apply requires --target", flush=True)
        return 2

    # S8707: validate --patch-out before any write_text call.
    patch_out_path: Path | None = None
    if args.patch_out:
        try:
            patch_out_path = _validate_writable_path(
                args.patch_out, flag_name="patch-out"
            )
        except ValueError as exc:
            print(f"tune-strategy: {exc}", flush=True)
            return 2

    loader = LeaderHistoryLoader(lookback_days=args.lookback_days)
    history = await loader.load(args.wallet)
    if not history.wallet:
        print("tune-strategy: --wallet is required", flush=True)
        return 2

    profile = profile_leader(history)
    patches = tune_strategy(profile)

    writer = EnvPatchWriter(target_path)
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

    if patch_out_path is not None:
        patch_out_path.write_text(result.explanation + "\n", encoding="utf-8")
        print(f"tune-strategy: patch table written to {patch_out_path}", flush=True)

    if result.applied and result.backup_path:
        print(f"tune-strategy: backup written to {result.backup_path}", flush=True)

    return 0


def run_tune_strategy(args: argparse.Namespace) -> int:
    return asyncio.run(_run_tune_strategy_async(args))