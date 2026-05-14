"""PR 8: structured doctor live readiness gate."""
from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pytest

import config.app_config as app_config_mod


_BASE_LIVE_ENV = {
    "PRIVATE_KEY": "0x" + "a" * 64,
    "FUNDER_ADDRESS": "0x" + "b" * 40,
    "USER_ADDRESS": "0x" + "c" * 40,
    "LEADER_WALLETS": "0x" + "d" * 40,
    "RPC_URL": "https://polygon-rpc.example.com",
    "CHAIN_ID": "137",
    "PAPER_TRADE_ONLY": "false",
    "CLOB_V2_READY": "true",
    "KILL_SWITCH": "false",
    "LIVE_CANARY_MODE": "true",
    "LIVE_CANARY_MAX_NOTIONAL_USDC": "1.00",
    "LIVE_CANARY_MAX_ORDERS": "1",
    "POSITION_DRIFT_CHECK_ON_STARTUP": "true",
    "ENFORCE_GEOBLOCK": "false",
}


def _patch_env(env_overrides: dict[str, str] | None = None):
    env = {**_BASE_LIVE_ENV, **(env_overrides or {})}

    def _load():
        with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
            return app_config_mod.load_config(env)

    return env, _load


async def _run_doctor(mode: str, env_overrides: dict[str, str] | None = None, *, tmp_log: Path | None = None):
    env, loader = _patch_env(env_overrides)
    from bot_cli.commands import doctor as doctor_mod
    import orchestrators.startup_health as startup_health_mod
    import services.compliance as compliance_mod

    async def _fake_rpc(_url):
        from types import SimpleNamespace
        return SimpleNamespace(status="ok", details="stub")

    async def _fake_geoblock():
        from types import SimpleNamespace
        return SimpleNamespace(allowed=True, reason="stub")

    async def _fake_wallet_sig(_wallet, **_kw):
        from config.env import SignatureType
        return (SignatureType.EOA, "EOA", "0x" + "a" * 40)

    patches = [
        patch.object(doctor_mod, "load_config", new=lambda *a, **kw: loader()),
        patch.dict("config.env.ENV", env, clear=False),
        patch.object(doctor_mod, "_check_wss_endpoint", return_value=(True, "ok")),
        patch.object(startup_health_mod, "_check_rpc", new=_fake_rpc),
        patch.object(compliance_mod, "check_geoblock", new=_fake_geoblock),
        patch.object(doctor_mod, "detect_wallet_signature_type", new=_fake_wallet_sig),
    ]
    if tmp_log is not None:
        patches.append(patch.object(doctor_mod, "_resolve_log_dir", return_value=tmp_log))
        patches.append(patch.object(doctor_mod, "_resolve_jsonl_path", return_value=tmp_log / "app.jsonl"))

    from contextlib import ExitStack

    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        return await doctor_mod.run_doctor_checks(mode)


def _checks_by_name(result: dict) -> dict[str, dict]:
    return {c["name"]: c for c in result["checks"]}


@pytest.mark.asyncio
async def test_doctor_paper_returns_structured_list(tmp_path):
    out = await _run_doctor("paper", tmp_log=tmp_path)
    assert isinstance(out["checks"], list)
    assert out["mode"] == "paper"
    for c in out["checks"]:
        assert {"name", "status", "detail", "fix"}.issubset(c.keys())


@pytest.mark.asyncio
async def test_doctor_live_blocked_when_clob_v2_ready_missing(tmp_path):
    out = await _run_doctor("live", {"CLOB_V2_READY": "false"}, tmp_log=tmp_path)
    assert out["status"] == "blocked"
    rec = _checks_by_name(out)["clob_v2_ready"]
    assert rec["status"] == "blocked"
    assert "CLOB_V2_READY" in rec["fix"]


@pytest.mark.asyncio
async def test_doctor_live_blocked_when_paper_trade_only_true(tmp_path):
    out = await _run_doctor("live", {"PAPER_TRADE_ONLY": "true"}, tmp_log=tmp_path)
    rec = _checks_by_name(out)["paper_trade_only_false"]
    assert rec["status"] == "blocked"
    assert out["status"] == "blocked"


@pytest.mark.asyncio
async def test_doctor_live_blocked_when_kill_switch_on(tmp_path):
    out = await _run_doctor("live", {"KILL_SWITCH": "true"}, tmp_log=tmp_path)
    rec = _checks_by_name(out)["kill_switch_off"]
    assert rec["status"] == "blocked"
    assert out["status"] == "blocked"


@pytest.mark.asyncio
async def test_doctor_live_blocked_when_canary_max_orders_zero(tmp_path):
    out = await _run_doctor("live", {"LIVE_CANARY_MAX_ORDERS": "0"}, tmp_log=tmp_path)
    rec = _checks_by_name(out)["canary_caps_sane"]
    assert rec["status"] == "blocked"


@pytest.mark.asyncio
async def test_doctor_live_logs_path_writable(tmp_path):
    out = await _run_doctor("live", tmp_log=tmp_path)
    rec = _checks_by_name(out)["log_directory_writable"]
    assert rec["status"] == "ok"
    rec_j = _checks_by_name(out)["jsonl_log_writable"]
    assert rec_j["status"] == "ok"


@pytest.mark.asyncio
async def test_doctor_live_logs_path_unwritable_blocks(tmp_path, monkeypatch):
    bad = tmp_path / "no_perm"
    bad.mkdir()
    bad.chmod(0o400)
    try:
        out = await _run_doctor("live", tmp_log=bad)
    finally:
        bad.chmod(0o700)
    rec = _checks_by_name(out)["log_directory_writable"]
    assert rec["status"] == "blocked"
    assert "writable" in rec["fix"].lower()


@pytest.mark.asyncio
async def test_doctor_live_returns_structured_checks_list(tmp_path):
    out = await _run_doctor("live", tmp_log=tmp_path)
    assert isinstance(out["checks"], list)
    for c in out["checks"]:
        assert isinstance(c, dict)
        assert {"name", "status", "detail", "fix"}.issubset(c.keys())
        assert c["status"] in {"ok", "warning", "blocked", "skipped"}


@pytest.mark.asyncio
async def test_doctor_live_passes_when_all_subsystems_wired(tmp_path):
    out = await _run_doctor("live", tmp_log=tmp_path)
    # status may be ok or warning (e.g. metrics disabled); should NOT be blocked
    assert out["status"] in {"ok", "warning"}, [c for c in out["checks"] if c["status"] == "blocked"]


def test_console_summary_renders_fix_line(tmp_path):
    import asyncio
    from bot_cli.commands.doctor import _render_console_summary

    fake = {
        "status": "blocked",
        "mode": "live",
        "checks": [
            {"name": "x", "status": "ok", "detail": "ok", "fix": ""},
            {"name": "drift_ok", "status": "blocked", "detail": "2 unresolved", "fix": "resolve"},
        ],
    }
    out = _render_console_summary(fake)
    assert "❌ drift_ok" in out
    assert "fix: resolve" in out
