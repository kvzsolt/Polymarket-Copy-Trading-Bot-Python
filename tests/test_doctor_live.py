"""PR 8: structured doctor live readiness gate.

Backed by a per-test ``postgres:16-alpine`` testcontainer (PG-A5 sweep) —
each ``_run_doctor`` call spins a fresh container, applies the schema,
and injects the container DSN through the typed database setting so the doctor's
async DB probes hit a real Postgres instance instead of a stub file.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import pmcopy.config.app_config as app_config_mod
from pmcopy.config.env import SignatureType
from tests.config_helpers import load_test_config


_BASE_LIVE_ENV = {
    "PRIVATE_KEY": "0x" + "a" * 64,
    "FUNDER_ADDRESS": "0x" + "b" * 40,
    "USER_ADDRESS": "0x" + "c" * 40,
    "LEADER_WALLETS": "0x" + "d" * 40,
    "RPC_URL": "https://polygon-rpc.example.com",
    "CHAIN_ID": "137",
    "TRADING_MODE": "live",
    "APP_KILL_SWITCH": "false",
    "POSITION_DRIFT_CHECK_ON_STARTUP": "true",
    "ENFORCE_GEOBLOCK": "false",
}


def test_doctor_formats_deposit_wallet_diagnostics() -> None:
    from pmcopy.bot_cli.commands.doctor import _wallet_signature_detail

    detail = _wallet_signature_detail(
        detected_signature_type=SignatureType.DEPOSIT_WALLET,
        detected_wallet_type="DEPOSIT_WALLET",
        detected_signer="0x" + "a" * 40,
        funder_address="0x" + "b" * 40,
        configured_signature_type=SignatureType.DEPOSIT_WALLET,
    )

    assert "walletType=DEPOSIT_WALLET" in detail
    assert "signature_type=3" in detail
    assert "ccxt_explicit_funder_authoritative=true" in detail


def _config_loader(env_overrides: dict[str, str] | None = None):
    env = {**_BASE_LIVE_ENV, **(env_overrides or {})}

    def _load():
        with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
            return load_test_config(env)

    return _load


class _PgFixture:
    """Spin a Postgres testcontainer + apply the schema. Yields the DSN.

    Used by ``_run_doctor`` so each test gets a real DB without per-test
    bookkeeping. Auto-skips when Docker is unavailable.
    """

    def __init__(self) -> None:
        self._ctr = None
        self._dsn = ""

    async def __aenter__(self) -> str:
        try:
            from testcontainers.postgres import PostgresContainer
            import asyncpg
            from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
        except Exception as exc:
            pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
        try:
            self._ctr = PostgresContainer("postgres:16-alpine")
            self._ctr.start()
        except Exception as exc:
            pytest.skip(f"Docker / postgres image unavailable: {exc!r}")

        self._dsn = self._ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=2)
        try:
            async with pool.acquire() as conn:
                await pg_apply(conn)
        finally:
            await pool.close()

        # The doctor's init_pool is module-singleton — make sure prior
        # state is cleared so it picks up this DSN cleanly.
        from pmcopy.storage.postgres.connection import _reset_for_tests, shutdown_pool

        try:
            await shutdown_pool()
        except Exception:
            pass
        _reset_for_tests()
        return self._dsn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        from pmcopy.storage.postgres.connection import _reset_for_tests, shutdown_pool

        try:
            await shutdown_pool()
        except Exception:
            pass
        _reset_for_tests()
        if self._ctr is not None:
            self._ctr.stop()


async def _run_doctor(
    mode: str,
    env_overrides: dict[str, str] | None = None,
    *,
    tmp_log=None,
    tmp_path=None,
    auto_trader=False,
):
    """Run the doctor against a fresh PG testcontainer.

    ``tmp_path`` kept for backwards-compatible call sites; ignored
    (sqlite ``BOT_STATE_DB`` no longer drives the DB probe).
    """
    async with _PgFixture() as dsn:
        overrides = dict(env_overrides or {})
        overrides["DATABASE_URL"] = dsn
        loader = _config_loader(overrides)
        from pmcopy.bot_cli.commands import doctor as doctor_mod
        import pmcopy.orchestrators.startup_health as startup_health_mod
        import pmcopy.services.compliance as compliance_mod

        async def _fake_rpc(_url):
            from types import SimpleNamespace

            return SimpleNamespace(status="ok", details="stub")

        async def _fake_geoblock():
            from types import SimpleNamespace

            return SimpleNamespace(allowed=True, reason="stub")

        patches = [
            patch.object(doctor_mod, "load_config", new=lambda *a, **kw: loader()),
            patch.object(doctor_mod, "_check_wss_endpoint", return_value=(True, "ok")),
            patch.object(startup_health_mod, "_check_rpc", new=_fake_rpc),
            patch.object(compliance_mod, "check_geoblock", new=_fake_geoblock),
        ]
        if tmp_log is not None:
            patches.append(patch.object(doctor_mod, "_resolve_log_dir", return_value=tmp_log))
            patches.append(
                patch.object(doctor_mod, "_resolve_jsonl_path", return_value=tmp_log / "pmcopy.app.jsonl")
            )

        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            return await doctor_mod.run_doctor_checks(mode, auto_trader=auto_trader)


def _checks_by_name(result: dict) -> dict[str, dict]:
    return {c["name"]: c for c in result["checks"]}


@pytest.mark.asyncio
async def test_doctor_paper_returns_structured_list(tmp_path):
    out = await _run_doctor("paper", tmp_log=tmp_path, tmp_path=tmp_path)
    assert isinstance(out["checks"], list)
    assert out["mode"] == "paper"
    for c in out["checks"]:
        assert {"name", "status", "detail", "fix"}.issubset(c.keys())


@pytest.mark.asyncio
async def test_doctor_live_blocked_when_trading_mode_is_paper(tmp_path):
    out = await _run_doctor("live", {"TRADING_MODE": "paper"}, tmp_log=tmp_path, tmp_path=tmp_path)
    rec = _checks_by_name(out)["paper_trade_only_false"]
    assert rec["status"] == "blocked"
    assert out["status"] == "blocked"


@pytest.mark.asyncio
async def test_doctor_live_blocked_when_kill_switch_on(tmp_path):
    out = await _run_doctor(
        "live", {"APP_KILL_SWITCH": "true"}, tmp_log=tmp_path, tmp_path=tmp_path
    )
    rec = _checks_by_name(out)["kill_switch_off"]
    assert rec["status"] == "blocked"
    assert out["status"] == "blocked"


@pytest.mark.asyncio
async def test_doctor_live_logs_path_writable(tmp_path):
    out = await _run_doctor("live", tmp_log=tmp_path, tmp_path=tmp_path)
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
        out = await _run_doctor("live", tmp_log=bad, tmp_path=tmp_path)
    finally:
        bad.chmod(0o700)
    rec = _checks_by_name(out)["log_directory_writable"]
    assert rec["status"] == "blocked"
    assert "not writable" in rec["detail"].lower()
    assert "log_dir" in rec["fix"]
    assert "config/logging.yml" in rec["fix"]


@pytest.mark.asyncio
async def test_doctor_live_returns_structured_checks_list(tmp_path):
    out = await _run_doctor("live", tmp_log=tmp_path, tmp_path=tmp_path)
    assert isinstance(out["checks"], list)
    for c in out["checks"]:
        assert isinstance(c, dict)
        assert {"name", "status", "detail", "fix"}.issubset(c.keys())
        assert c["status"] in {"ok", "warning", "blocked", "skipped"}


@pytest.mark.asyncio
async def test_doctor_live_passes_when_all_subsystems_wired(tmp_path):
    out = await _run_doctor("live", tmp_log=tmp_path, tmp_path=tmp_path)
    # status may be ok or warning (e.g. metrics disabled); should NOT be blocked
    assert out["status"] in {"ok", "warning"}, [
        c for c in out["checks"] if c["status"] == "blocked"
    ]


@pytest.mark.asyncio
async def test_doctor_live_blocks_when_no_manual_leaders_and_roster_start_disabled(
    tmp_path,
):
    out = await _run_doctor(
        "live",
        {
            "LEADER_WALLETS": "",
            "USER_ADDRESS": "",
            "DISCOVERY_SCHEDULER_START_FROM_ROSTER": "false",
        },
        tmp_log=tmp_path,
        tmp_path=tmp_path,
    )
    rec = _checks_by_name(out)["leaders"]
    assert rec["status"] == "blocked"
    assert "roster startup is disabled" in rec["detail"]


@pytest.mark.asyncio
async def test_doctor_live_passes_when_discovery_enabled_without_manual_leaders(
    tmp_path,
):
    out = await _run_doctor(
        "live",
        {
            "LEADER_WALLETS": "",
            "USER_ADDRESS": "",
            "DISCOVERY_API_TOKEN": "test-token",
            "DISCOVERY_SCHEDULER_ENABLED": "true",
        },
        tmp_log=tmp_path,
        tmp_path=tmp_path,
    )
    rec = _checks_by_name(out)["leaders"]
    assert rec["status"] == "ok"
    assert "startup discovery is configured" in rec["detail"]
    assert out["status"] in {"ok", "warning"}


@pytest.mark.asyncio
async def test_doctor_live_blocks_when_discovery_credentials_are_missing(tmp_path):
    out = await _run_doctor(
        "live",
        {
            "LEADER_WALLETS": "",
            "USER_ADDRESS": "",
            "DISCOVERY_API_TOKEN": "",
            "DISCOVERY_SCHEDULER_ENABLED": "true",
        },
        tmp_log=tmp_path,
        tmp_path=tmp_path,
    )
    rec = _checks_by_name(out)["leaders"]
    assert rec["status"] == "blocked"
    assert "discovery.api.token" in rec["fix"]


def test_console_summary_renders_fix_line(tmp_path):
    from pmcopy.bot_cli.commands.doctor import _render_console_summary

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


async def test_automatic_initialization_does_not_require_periodic_discovery(tmp_path):
    out = await _run_doctor(
        "live",
        {
            "LEADER_WALLETS": "",
            "USER_ADDRESS": "",
            "DISCOVERY_API_TOKEN": "test-token",
            "DISCOVERY_SCHEDULER_ENABLED": "false",
        },
        tmp_log=tmp_path,
        auto_trader=True,
    )
    assert _checks_by_name(out)["leaders"]["status"] == "ok"


async def test_saved_automatic_roster_can_restore_without_provider_credentials(
    tmp_path, monkeypatch
):
    from unittest.mock import AsyncMock
    from pmcopy.bot_cli.commands import doctor

    monkeypatch.setattr(
        doctor, "_automatic_rosters_available", AsyncMock(return_value=True), raising=False
    )
    out = await _run_doctor(
        "live",
        {
            "LEADER_WALLETS": "",
            "USER_ADDRESS": "",
            "DISCOVERY_API_TOKEN": "",
            "DISCOVERY_SCHEDULER_ENABLED": "false",
        },
        tmp_log=tmp_path,
        auto_trader=True,
    )
    assert _checks_by_name(out)["leaders"]["status"] == "ok"
