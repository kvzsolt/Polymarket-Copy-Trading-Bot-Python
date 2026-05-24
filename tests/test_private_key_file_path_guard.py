"""Regression: PRIVATE_KEY_FILE path is resolved + confined to safe roots.

Pre-fix: only ``Path(key_file).expanduser()`` + 0600 stat check on the
named path. A symlink pointing at ``/etc/passwd`` could pass the mode
check on the link itself; a path like ``../../etc/passwd`` would
silently resolve and read arbitrary files.

Post-fix:
  * resolve() the path so the 0600 check applies to the target.
  * Confine to ``$HOME`` or working dir — paths outside fail closed.
"""
from __future__ import annotations

import os

import pytest

from config.app_config import _Parser


def _build(env: dict) -> _Parser:
    return _Parser(env)


def test_path_outside_home_or_cwd_is_rejected(tmp_path):
    # /etc is outside both $HOME and cwd → reject regardless of mode.
    target = "/etc/hostname"
    if not os.path.exists(target):
        pytest.skip("no /etc/hostname on this platform")
    p = _build({"PRIVATE_KEY_FILE": target})
    p.private_key()
    assert any(
        "under $HOME or the working dir" in e for e in p.errors
    ), f"path outside safe roots must be rejected; got {p.errors}"


def test_relative_traversal_resolves_and_is_caught(tmp_path):
    # ../../etc/hostname relative to a cwd inside tmp_path.
    if not os.path.exists("/etc/hostname"):
        pytest.skip("no /etc/hostname on this platform")
    cwd = os.getcwd()
    try:
        sub = tmp_path / "sub"
        sub.mkdir()
        os.chdir(sub)
        p = _build({"PRIVATE_KEY_FILE": "../../../../etc/hostname"})
        p.private_key()
        assert any(
            "under $HOME or the working dir" in e for e in p.errors
        ), f"path traversal must be caught after resolve(); got {p.errors}"
    finally:
        os.chdir(cwd)


def test_file_under_cwd_with_correct_mode_accepted(tmp_path, monkeypatch):
    key_file = tmp_path / "key"
    key_file.write_text("0x" + "1" * 64)
    os.chmod(key_file, 0o600)
    monkeypatch.chdir(tmp_path)
    p = _build({"PRIVATE_KEY_FILE": str(key_file)})
    result = p.private_key()
    assert result == "0x" + "1" * 64
    assert p.errors == []


def test_file_under_cwd_with_wrong_mode_rejected(tmp_path, monkeypatch):
    key_file = tmp_path / "key"
    key_file.write_text("0x" + "1" * 64)
    os.chmod(key_file, 0o644)  # world-readable — must reject
    monkeypatch.chdir(tmp_path)
    p = _build({"PRIVATE_KEY_FILE": str(key_file)})
    p.private_key()
    assert any("0600" in e for e in p.errors), p.errors
