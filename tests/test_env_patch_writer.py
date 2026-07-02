"""Tests for ``services/strategy_profile/env_patch_writer.py`` (PR3)."""
from __future__ import annotations

from pathlib import Path

import pytest

from models.env_patch import EnvPatch
from services.strategy_profile.env_patch_writer import (
    EnvPatchWriter,
    is_protected_key,
)

_PROTECTED = "0x" + "11" * 20


def test_is_protected_key_exact() -> None:
    assert is_protected_key("PRIVATE_KEY")
    assert is_protected_key("CLOB_SECRET")
    assert is_protected_key("CLOB_PASSPHRASE")
    assert is_protected_key("RPC_URL")
    assert is_protected_key("FUNDER_ADDRESS")
    assert is_protected_key("LEADER_WALLETS")


def test_is_protected_key_substring() -> None:
    assert is_protected_key("MY_API_KEY")
    assert is_protected_key("SOME_TOKEN")
    assert is_protected_key("DB_PASSWORD")
    assert is_protected_key("WALLET_SECRET")


def test_is_protected_key_non_protected() -> None:
    assert not is_protected_key("BUY_MAX_AMOUNT")
    assert not is_protected_key("MAX_POSITION_PCT")
    assert not is_protected_key("ENABLE_NEAR_MAX_EXIT")
    assert not is_protected_key("MARKET_DATA_PRIMARY_SOURCE")


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("BUY_MAX_AMOUNT=10\n", encoding="utf-8")
    writer = EnvPatchWriter(target)
    patches = [EnvPatch("BUY_MAX_AMOUNT", None, "5", "test", 0.9)]
    result = writer.apply(patches, apply=False)
    assert result.applied is False
    assert result.backup_path is None
    # File unchanged.
    assert target.read_text(encoding="utf-8") == "BUY_MAX_AMOUNT=10\n"
    # Explanation mentions DRY-RUN.
    assert "DRY-RUN" in result.explanation
    assert "BUY_MAX_AMOUNT" in result.explanation


def test_apply_writes_and_creates_backup(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("BUY_MAX_AMOUNT=10\nMAX_POSITION_PCT=0.10\n", encoding="utf-8")
    writer = EnvPatchWriter(target)
    patches = [
        EnvPatch("BUY_MAX_AMOUNT", None, "5", "scalper clip", 0.9),
        EnvPatch("MAX_POSITION_PCT", None, "0.05", "tight cap", 0.8),
    ]
    result = writer.apply(patches, apply=True)
    assert result.applied is True
    assert result.backup_path is not None
    assert Path(result.backup_path).exists()
    text = target.read_text(encoding="utf-8")
    assert "BUY_MAX_AMOUNT=5" in text
    assert "MAX_POSITION_PCT=0.05" in text


def test_apply_appends_new_keys(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("BUY_MAX_AMOUNT=10\n", encoding="utf-8")
    writer = EnvPatchWriter(target)
    patches = [EnvPatch("ENABLE_NEAR_MAX_EXIT", None, "true", "new flag", 0.7)]
    result = writer.apply(patches, apply=True)
    assert result.applied is True
    text = target.read_text(encoding="utf-8")
    assert "ENABLE_NEAR_MAX_EXIT=true" in text
    assert "BUY_MAX_AMOUNT=10" in text  # existing key preserved


def test_protected_keys_rejected(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("PRIVATE_KEY=0x" + "1" * 64 + "\n", encoding="utf-8")
    writer = EnvPatchWriter(target)
    patches = [EnvPatch("PRIVATE_KEY", None, "0x" + "2" * 64, "should be refused", 0.9)]
    result = writer.apply(patches, apply=True)
    assert result.applied is False
    assert len(result.rejected) == 1
    assert result.rejected[0].key == "PRIVATE_KEY"
    # File unchanged.
    assert "0x" + "1" * 64 in target.read_text(encoding="utf-8")


def test_leader_wallets_refused_unless_explicit(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("LEADER_WALLETS=0xabc\n", encoding="utf-8")
    writer = EnvPatchWriter(target)
    patches = [EnvPatch("LEADER_WALLETS", None, "0xdef,0x123", "rotate", 0.5)]
    # Without allow_leader_rewrite -> rejected.
    result = writer.apply(patches, apply=True)
    assert result.applied is False
    assert len(result.rejected) == 1
    # With allow_leader_rewrite -> accepted.
    result2 = writer.apply(patches, apply=True, allow_leader_rewrite=True)
    assert result2.applied is True
    assert "LEADER_WALLETS=0xdef,0x123" in target.read_text(encoding="utf-8")


def test_apply_creates_missing_target(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    assert not target.exists()
    writer = EnvPatchWriter(target)
    patches = [EnvPatch("BUY_MAX_AMOUNT", None, "5", "test", 0.9)]
    result = writer.apply(patches, apply=True)
    assert result.applied is True
    assert target.exists()
    assert "BUY_MAX_AMOUNT=5" in target.read_text(encoding="utf-8")


def test_explanation_redacts_protected_old_values(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    target.write_text("PRIVATE_KEY=0x" + "1" * 64 + "\n", encoding="utf-8")
    writer = EnvPatchWriter(target)
    # Even though the patch is rejected, the plan path reads current values;
    # ensure no secret leaks into the explanation.
    patches = [EnvPatch("PRIVATE_KEY", None, "0x" + "2" * 64, "refused", 0.1)]
    result = writer.apply(patches, apply=False)
    assert "0x" + "1" * 64 not in result.explanation
    assert "0x" + "2" * 64 not in result.explanation


def test_apply_preserves_comments_and_unrelated_keys(tmp_path: Path) -> None:
    target = tmp_path / "runtime.local.env"
    content = "# my comment\nBUY_MAX_AMOUNT=10\nMAX_POSITION_PCT=0.10 # inline\n"
    target.write_text(content, encoding="utf-8")
    writer = EnvPatchWriter(target)
    patches = [EnvPatch("BUY_MAX_AMOUNT", None, "5", "test", 0.9)]
    result = writer.apply(patches, apply=True)
    assert result.applied is True
    text = target.read_text(encoding="utf-8")
    assert "# my comment" in text
    assert "BUY_MAX_AMOUNT=5" in text
    # MAX_POSITION_PCT line preserved (the inline comment is dropped by the
    # line replacement for the *patched* key only; unrelated keys keep their
    # full original line).
    assert "MAX_POSITION_PCT=0.10 # inline" in text


def test_envpatch_validates_confidence_range() -> None:
    with pytest.raises(ValueError):
        EnvPatch("KEY", None, "x", "r", 1.5)
    with pytest.raises(ValueError):
        EnvPatch("KEY", None, "x", "r", -0.1)


def test_envpatch_validates_nonempty_key() -> None:
    with pytest.raises(ValueError):
        EnvPatch("", None, "x", "r", 0.5)