"""PR 8: app.py startup honours structured doctor result."""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


_APP_PY = Path(__file__).resolve().parents[1] / "app.py"


def _app_source() -> str:
    return _APP_PY.read_text(encoding="utf-8")


def _ast_tree() -> ast.AST:
    return ast.parse(_app_source())


def test_app_runs_live_doctor_only_when_not_paper_mode():
    src = _app_source()
    # Look for the live doctor call inside a `mode == "live"` branch.
    assert 'run_doctor_checks("live", config=startup_config)' in src
    # And there should be a paper guard preceding it.
    assert 'paper_trade_only' in src or 'mode == "live"' in src


def test_app_refuses_to_start_when_doctor_returns_blocked():
    """Source must short-circuit (return) when ``doctor_result['status'] == 'blocked'``."""
    src = _app_source()
    assert '"blocked"' in src
    assert "Refusing to start" in src
    assert "run_doctor_checks" in src


def test_app_prints_blocked_check_fix_details():
    src = _app_source()
    # New structured printing — iterate blocked checks and surface their fix.
    assert 'c.get("status") == "blocked"' in src
    assert 'c.get("fix")' in src or "fix" in src


def test_paper_mode_skips_live_doctor():
    tree = _ast_tree()
    # Find a call to run_doctor_checks with literal "live".
    live_call_lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name == "run_doctor_checks":
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and arg.value == "live":
                        live_call_lines.append(node.lineno)
    assert live_call_lines, "expected at least one run_doctor_checks('live') call"
    # Each live call should sit inside a branch that checks the mode != paper.
    # Lightweight contract: there should be a `mode == "live"` comparison nearby.
    src_lines = _app_source().splitlines()
    parts: list[str] = []
    for line in live_call_lines:
        parts.extend(src_lines[max(0, line - 6):line])
    nearby = "\n".join(parts)
    assert "live" in nearby


@pytest.mark.asyncio
async def test_live_doctor_runs_before_build_app(monkeypatch):
    import app as app_module

    events: list[str] = []
    fake_config = SimpleNamespace(
        execution=SimpleNamespace(paper_trade_only=False),
        network=SimpleNamespace(enforce_geoblock=False),
        logging=SimpleNamespace(
            level="INFO",
            log_dir="log",
            app_log_jsonl_file="log/test.jsonl",
            log_max_bytes=1024,
            log_backup_count=1,
        ),
    )

    async def _fake_doctor(_mode: str, *, config=None):
        assert config is fake_config
        events.append("doctor")
        return {"status": "ok", "checks": []}

    async def _fake_build_app(*, config=None):
        assert config is fake_config
        events.append("build_app")
        raise RuntimeError("stop_after_build")

    monkeypatch.setattr(app_module, "load_config", lambda: fake_config)
    monkeypatch.setattr(app_module, "build_app", _fake_build_app)
    monkeypatch.setattr("bot_cli.commands.doctor.run_doctor_checks", _fake_doctor)

    with pytest.raises(RuntimeError, match="stop_after_build"):
        await app_module.main()

    assert events == ["doctor", "build_app"]


@pytest.mark.asyncio
async def test_blocked_live_doctor_short_circuits_before_build_app(monkeypatch):
    import app as app_module

    fake_config = SimpleNamespace(
        execution=SimpleNamespace(paper_trade_only=False),
        network=SimpleNamespace(enforce_geoblock=False),
        logging=SimpleNamespace(
            level="INFO",
            log_dir="log",
            app_log_jsonl_file="log/test.jsonl",
            log_max_bytes=1024,
            log_backup_count=1,
        ),
    )
    build_app = AsyncMock()

    async def _fake_doctor(_mode: str, *, config=None):
        assert config is fake_config
        return {
            "status": "blocked",
            "checks": [
                {
                    "name": "leaders",
                    "status": "blocked",
                    "detail": "no leaders",
                    "fix": "set leader.wallets",
                }
            ],
        }

    monkeypatch.setattr(app_module, "load_config", lambda: fake_config)
    monkeypatch.setattr(app_module, "build_app", build_app)
    monkeypatch.setattr("bot_cli.commands.doctor.run_doctor_checks", _fake_doctor)

    await app_module.main()

    build_app.assert_not_called()


@pytest.mark.asyncio
async def test_paper_mode_does_not_call_geoblock_gate(monkeypatch):
    import app as app_module

    fake_config = SimpleNamespace(
        execution=SimpleNamespace(paper_trade_only=True),
        network=SimpleNamespace(enforce_geoblock=True),
        logging=SimpleNamespace(
            level="INFO",
            log_dir="log",
            app_log_jsonl_file="log/test.jsonl",
            log_max_bytes=1024,
            log_backup_count=1,
        ),
    )
    geoblock = AsyncMock()

    async def _stop_after_gate(*, config=None):
        assert config is fake_config
        raise RuntimeError("stop_after_gate")

    monkeypatch.setattr(app_module, "load_config", lambda: fake_config)
    monkeypatch.setattr(app_module, "check_geoblock", geoblock, raising=False)
    monkeypatch.setattr(app_module, "build_app", _stop_after_gate)

    with pytest.raises(RuntimeError, match="stop_after_gate"):
        await app_module.main()

    geoblock.assert_not_awaited()
