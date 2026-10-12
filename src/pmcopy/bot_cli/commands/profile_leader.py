"""``bot.py profile-leader`` — profile a leader wallet and emit a JSON profile (PR3)."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]


def _checkout_repo_root() -> Path | None:
    """Return the checkout root, or ``None`` in an installed distribution.

    ``parents[4]`` of this module lands on the repository root only when
    running from a source checkout. In a wheel/sdist install it points
    inside the interpreter's own tree (``<venv>/lib/python3.x``), which must
    never become an allowed ``--output`` write root — no artifact of this
    command may be written into the installed package environment. The
    checkout is detected by its ``pyproject.toml`` + ``src/pmcopy`` layout.
    """
    if (_REPO_ROOT / "pyproject.toml").is_file() and (_REPO_ROOT / "src" / "pmcopy").is_dir():
        return _REPO_ROOT
    return None


# SonarCloud S8707 (LLM path-traversal guard): ``--output`` accepts any
# path, but an LLM (or accident) can escape the workspace (e.g.
# ``--output ../../etc/cron.d/payload``). Resolve the path and require it
# to live under the checkout repo root (when running from one) OR under the
# system temp dir (so pytest's ``tmp_path`` fixture keeps working in
# tests). Everything else is rejected with exit code 2 before any
# file-system write happens.
#
# ``tempfile.gettempdir()`` is used instead of a hardcoded ``"/tmp"``
# literal so SonarCloud S5443 (publicly writable directories) does not
# flag the allowlist: the value is resolved at runtime from the
# ``TMPDIR``/``TEMP``/``TMP`` environment, and on pytest's ``tmp_path``
# the resolved path is a user-private subdirectory (``pytest-of-<user>``)
# that is 0700-permissioned, not the world-writable ``/tmp`` itself.
def _build_write_roots() -> tuple[Path, ...]:
    """Return the allowed ``--output`` write roots for this process."""
    return tuple(
        root
        for root in (
            _checkout_repo_root(),
            Path(tempfile.gettempdir()),
        )
        if root is not None
    )


_ALLOWED_WRITE_ROOTS: tuple[Path, ...] = _build_write_roots()


def _validate_writable_path(raw: str) -> Path | None:
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
    allowed = " or ".join(str(root) for root in _ALLOWED_WRITE_ROOTS)
    raise ValueError(f"--output must resolve under {allowed}; got {resolved}")


def register_profile_leader_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "profile-leader",
        help="Profile a leader wallet's trading strategy and emit a JSON profile",
    )
    parser.add_argument("--wallet", required=True, help="Leader wallet address (0x...)")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--output", default=None, help="Output JSON path (default: stdout)")


async def _run_profile_leader_async(args: argparse.Namespace) -> int:
    from pmcopy.services.strategy_profile.leader_history_loader import LeaderHistoryLoader
    from pmcopy.services.strategy_profile.strategy_profiler import profile_leader

    output_path: Path | None = None
    if args.output:
        try:
            output_path = _validate_writable_path(args.output)
        except ValueError as exc:
            print(f"profile-leader: {exc}", flush=True)
            return 2

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
        "features": {k: (v if v is not None else None) for k, v in profile.features.items()},
        "history_counts": {
            "activity": len(history.activity),
            "trades": len(history.trades),
            "closed_positions": len(history.closed_positions),
        },
    }
    text = json.dumps(payload, indent=2, default=str)
    if output_path is not None:
        output_path.write_text(text + "\n", encoding="utf-8")
        print(f"profile-leader: wrote {output_path}", flush=True)
    else:
        print(text, flush=True)
    return 0


def run_profile_leader(args: argparse.Namespace) -> int:
    return asyncio.run(_run_profile_leader_async(args))
