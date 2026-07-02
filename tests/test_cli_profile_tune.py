"""Tests for the ``profile-leader`` and ``tune-strategy`` CLI commands (PR3)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from bot_cli.commands.profile_leader import (
    register_profile_leader_command,
    run_profile_leader,
)
from bot_cli.commands.tune_strategy import (
    register_tune_strategy_command,
    run_tune_strategy,
)
from models.strategy_profile import StrategyArchetype, StrategyProfile
from services.strategy_profile.leader_history_loader import LoadedHistory


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    register_profile_leader_command(sub)
    register_tune_strategy_command(sub)
    return parser


def test_profile_leader_command_registered() -> None:
    parser = _build_parser()
    # Parsing --wallet works.
    args = parser.parse_args(["profile-leader", "--wallet", "0x" + "ab" * 20])
    assert args.wallet == "0x" + "ab" * 20
    assert args.lookback_days == 30


def test_tune_strategy_command_registered() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        ["tune-strategy", "--wallet", "0x" + "ab" * 20, "--dry-run"]
    )
    assert args.wallet == "0x" + "ab" * 20
    assert args.dry_run is True
    assert args.apply is False
    assert args.target == "config/runtime.local.env"


def test_profile_leader_writes_json_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    parser = _build_parser()
    out = tmp_path / "profile.json"
    args = parser.parse_args(
        ["profile-leader", "--wallet", "0x" + "ab" * 20, "--output", str(out)]
    )

    fake_history = LoadedHistory(
        wallet="0x" + "ab" * 20,
        activity=[{"side": "BUY", "price": "0.5", "size": "10", "timestamp": 1700000000000}],
        closed_positions=[],
    )

    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=fake_history,
    ):
        rc = run_profile_leader(args)

    assert rc == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["wallet"] == "0x" + "ab" * 20
    assert "archetype" in payload
    assert "features" in payload


def test_tune_strategy_dry_run_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("BUY_MAX_AMOUNT=10\n", encoding="utf-8")
    parser = _build_parser()
    args = parser.parse_args(
        [
            "tune-strategy",
            "--wallet",
            "0x" + "ab" * 20,
            "--dry-run",
            "--target",
            str(target),
        ]
    )

    fake_profile = StrategyProfile(
        wallet="0x" + "ab" * 20,
        lookback_days=30,
        features={"trade_count_30d": 50.0, "trade_count_7d": 20.0,
                  "avg_hold_time_minutes": 10.0, "crypto_fast_signal_ratio": 0.6,
                  "grouped_signal_ratio": 0.5, "market_concentration_hhi": 0.2},
        archetype=StrategyArchetype.FAST_CRYPTO_SCALPER,
        copyable=True,
        computed_at=1700000000,
    )

    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ), patch(
        "services.strategy_profile.strategy_profiler.profile_leader",
        return_value=fake_profile,
    ):
        rc = run_tune_strategy(args)

    assert rc == 0
    # Dry-run -> file unchanged.
    assert target.read_text(encoding="utf-8") == "BUY_MAX_AMOUNT=10\n"
    out = capsys.readouterr().out
    assert "DRY-RUN" in out


def test_tune_strategy_apply_writes_to_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("BUY_MAX_AMOUNT=10\n", encoding="utf-8")
    parser = _build_parser()
    args = parser.parse_args(
        [
            "tune-strategy",
            "--wallet",
            "0x" + "ab" * 20,
            "--apply",
            "--target",
            str(target),
        ]
    )

    fake_profile = StrategyProfile(
        wallet="0x" + "ab" * 20,
        lookback_days=30,
        features={"trade_count_30d": 50.0, "trade_count_7d": 20.0,
                  "avg_hold_time_minutes": 10.0, "crypto_fast_signal_ratio": 0.6,
                  "grouped_signal_ratio": 0.5, "market_concentration_hhi": 0.2},
        archetype=StrategyArchetype.FAST_CRYPTO_SCALPER,
        copyable=True,
        computed_at=1700000000,
    )

    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ), patch(
        "services.strategy_profile.strategy_profiler.profile_leader",
        return_value=fake_profile,
    ):
        rc = run_tune_strategy(args)

    assert rc == 0
    text = target.read_text(encoding="utf-8")
    # Fast crypto scalper sets BUY_MAX_AMOUNT=3.
    assert "BUY_MAX_AMOUNT=3" in text
    # Existing unrelated keys preserved.
    assert "ENABLE_ONCHAIN_PENDING_DETECTION=true" in text
    out = capsys.readouterr().out
    assert "APPLY" in out


def test_tune_strategy_apply_without_target_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The CLI default for --target is config/runtime.local.env, so to test the
    # missing-target guard we'd need to override the default. Instead, verify
    # the guard logic directly by calling with an empty target via a patch.
    parser = _build_parser()
    args = parser.parse_args(
        ["tune-strategy", "--wallet", "0x" + "ab" * 20, "--apply"]
    )
    # Force target empty to exercise the guard.
    args.target = ""
    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ):
        rc = run_tune_strategy(args)
    assert rc == 2
    out = capsys.readouterr().out
    assert "--apply requires --target" in out


def test_tune_strategy_noisy_uncopyable_emits_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("BUY_MAX_AMOUNT=10\n", encoding="utf-8")
    parser = _build_parser()
    args = parser.parse_args(
        ["tune-strategy", "--wallet", "0x" + "ab" * 20, "--dry-run", "--target", str(target)]
    )

    fake_profile = StrategyProfile(
        wallet="0x" + "ab" * 20,
        lookback_days=30,
        features={"trade_count_30d": 2.0},
        archetype=StrategyArchetype.NOISY_UNCOPYABLE,
        copyable=False,
        computed_at=1700000000,
    )

    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ), patch(
        "services.strategy_profile.strategy_profiler.profile_leader",
        return_value=fake_profile,
    ):
        rc = run_tune_strategy(args)

    assert rc == 0
    out = capsys.readouterr().out
    assert "not copyable" in out.lower()


# --- S8707 path-traversal rejection (SonarCloud LLM-escape guard) -----------

def test_profile_leader_rejects_path_traversal_in_output(capsys: pytest.CaptureFixture[str]) -> None:
    """``--output ../../etc/cron.d/payload`` must be rejected with exit 2
    before any file-system write happens (SonarCloud S8707)."""
    parser = _build_parser()
    args = parser.parse_args(
        ["profile-leader", "--wallet", "0x" + "ab" * 20, "--output", "../../etc/cron.d/payload"]
    )
    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ):
        rc = run_profile_leader(args)
    assert rc == 2
    out = capsys.readouterr().out
    assert "must resolve under" in out
    assert "--output" in out


def test_profile_leader_rejects_absolute_path_outside_workspace(capsys: pytest.CaptureFixture[str]) -> None:
    """``--output /etc/passwd`` (absolute path outside repo + /tmp) is rejected."""
    parser = _build_parser()
    args = parser.parse_args(
        ["profile-leader", "--wallet", "0x" + "ab" * 20, "--output", "/etc/passwd"]
    )
    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ):
        rc = run_profile_leader(args)
    assert rc == 2
    assert "must resolve under" in capsys.readouterr().out


def test_profile_leader_accepts_tmp_path_output(tmp_path: Path) -> None:
    """pytest's ``tmp_path`` lives under /tmp and must pass validation
    (so existing tests + operator scratch-dir invocations still work)."""
    from bot_cli.commands.profile_leader import _validate_writable_path
    out = tmp_path / "profile.json"
    resolved = _validate_writable_path(str(out))
    assert resolved is not None
    assert resolved.name == "profile.json"


def test_tune_strategy_rejects_path_traversal_in_target(capsys: pytest.CaptureFixture[str]) -> None:
    """``--target ../../etc/cron.d/payload`` is rejected before EnvPatchWriter
    is constructed (S8707)."""
    parser = _build_parser()
    args = parser.parse_args(
        ["tune-strategy", "--wallet", "0x" + "ab" * 20, "--dry-run", "--target", "../../etc/cron.d/payload"]
    )
    with patch(
        "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
        new_callable=AsyncMock,
        return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
    ):
        rc = run_tune_strategy(args)
    assert rc == 2
    out = capsys.readouterr().out
    assert "must resolve under" in out
    assert "--target" in out


def test_tune_strategy_rejects_path_traversal_in_patch_out(capsys: pytest.CaptureFixture[str]) -> None:
    """``--patch-out /etc/cron.d/payload`` is rejected before write_text."""
    parser = _build_parser()
    # Use a tmp_path target so --target validation passes; the rejection
    # is specifically for --patch-out.
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".env", delete=False) as tf:
        target = tf.name
    try:
        args = parser.parse_args(
            [
                "tune-strategy",
                "--wallet",
                "0x" + "ab" * 20,
                "--dry-run",
                "--target",
                target,
                "--patch-out",
                "/etc/cron.d/payload",
            ]
        )
        with patch(
            "services.strategy_profile.leader_history_loader.LeaderHistoryLoader.load",
            new_callable=AsyncMock,
            return_value=LoadedHistory(wallet="0x" + "ab" * 20, activity=[{}], closed_positions=[]),
        ), patch(
            "services.strategy_profile.strategy_profiler.profile_leader",
            return_value=StrategyProfile(
                wallet="0x" + "ab" * 20,
                lookback_days=30,
                features={"trade_count_30d": 2.0},
                archetype=StrategyArchetype.NOISY_UNCOPYABLE,
                copyable=False,
                computed_at=1700000000,
            ),
        ):
            rc = run_tune_strategy(args)
        assert rc == 2
        out = capsys.readouterr().out
        assert "must resolve under" in out
        assert "--patch-out" in out
    finally:
        import os
        os.unlink(target)


def test_tune_strategy_default_target_passes_validation() -> None:
    """The default --target (config/runtime.local.env) resolves under the
    repo root, so existing happy-path invocations are unaffected."""
    from bot_cli.commands.tune_strategy import _validate_writable_path
    resolved = _validate_writable_path("config/runtime.local.env", flag_name="target")
    assert resolved is not None
    assert resolved.name == "runtime.local.env"