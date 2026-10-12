"""§9 startup-composition scenarios: invalid copier configuration is
rejected before any execution exists (bullet 9), and two processes configured
for the same wallet/environment never become competing active writers because
the DB-backed account lease is acquired before any venue runtime or user
state is built (bullet 10 — composition face; the real two-process SQL lease
contracts live in ``tests/test_account_lease.py`` and
``tests/test_account_lease_postgres.py``).

These tests drive the REAL ``pmcopy.app._run_application`` startup sequence
(the same seam as ``tests/test_doctor_app_startup.py``) with hermetic
externals: a real validated AppConfig, a fake application object, and
sentinel recorders on every execution-side stage so a fail-closed startup can
be proven to have stopped before leases, venue runtimes, user states, or any
trading read.
"""

from __future__ import annotations

import textwrap
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import pmcopy.app as app_module
from pmcopy.orchestrators.leader_registry import LeaderRegistryState
from pmcopy.services.healthServer import ReadinessTracker

from tests.multicopier.conftest import (
    FUNDER_ALPHA,
    FUNDER_BETA,
    LEADER_1,
    LEADER_2,
    USER_ALPHA,
    USER_BETA,
    make_config,
    synth_private_key,
)


class SentinelReached(RuntimeError):
    """Raised by a sentinel to stop startup at a proven stage boundary."""


class _StartupHarness:
    """Patch every external of ``_run_application`` up to the venue stage."""

    def __init__(self, monkeypatch, tmp_path, config):
        self.config = config
        self.stages: list[str] = []
        self.leases_acquired: list[tuple[str, str]] = []
        self.lease_release_all_calls = 0
        self.venue_users: list[str] = []
        self.user_state_users: list[str] = []
        self.balance_wallets: list[str] = []

        monkeypatch.setattr(app_module, "load_config", lambda: config)
        monkeypatch.setattr(app_module, "configure_logging", lambda **_kwargs: None)

        provider = SimpleNamespace(close=MagicMock())
        monkeypatch.setattr(
            "pmcopy.orchestrators.provider_runtime.ProviderRuntime.from_config",
            lambda _config: provider,
        )
        self.evidence_runtime = SimpleNamespace(start=AsyncMock(), aclose=AsyncMock())

        fake_application = SimpleNamespace(
            config=config,
            readiness=ReadinessTracker(),
            registry=SimpleNamespace(
                refresh=AsyncMock(
                    return_value=LeaderRegistryState(
                        configured=[LEADER_1], active_monitored=[LEADER_1], mode="configured"
                    )
                )
            ),
            leaders_repo=SimpleNamespace(list_active=AsyncMock(return_value=[LEADER_1])),
            falcon_client=None,
            auto_traders=None,
        )

        async def _fake_build_app(*, config=None, provider_runtime=None):
            self.stages.append("build_app")
            return fake_application

        monkeypatch.setattr(app_module, "build_app", _fake_build_app)
        monkeypatch.setattr(app_module, "_start_telemetry_runtime", AsyncMock(return_value=None))

        async def _no_db_connect():
            self.stages.append("connect_db")

        monkeypatch.setattr("pmcopy.config.db.connect_db", _no_db_connect)

        fake_pool = MagicMock()

        async def _fake_get_pool():
            self.stages.append("get_pool")
            return fake_pool

        monkeypatch.setattr("pmcopy.storage.postgres.connection.get_pool", _fake_get_pool)
        monkeypatch.setattr(
            "pmcopy.storage.postgres.orders_repo.OrdersRepo.purge_old_processed_trades",
            AsyncMock(return_value=0),
        )

        harness = self

        class _FakeLeaseManager:
            def __init__(self, *, pool=None, **_kwargs):
                harness.stages.append("lease_manager_constructed")

            async def acquire(self, account_id, *, mode):
                harness.leases_acquired.append((str(account_id), mode))
                return SimpleNamespace(acquired=True, held_by=None)

            async def release_all(self):
                harness.lease_release_all_calls += 1

            async def close(self):
                harness.stages.append("lease_manager_closed")

        self.lease_manager_cls = _FakeLeaseManager
        monkeypatch.setattr(app_module, "AccountLeaseManager", _FakeLeaseManager)

        async def _venue_sentinel(*args, **kwargs):
            runtime_user = kwargs.get("runtime_user") or args[0]
            harness.venue_users.append(runtime_user.user_id)
            harness.stages.append(f"venue_runtime:{runtime_user.user_id}")
            raise SentinelReached(f"venue runtime built for {runtime_user.user_id}")

        monkeypatch.setattr(app_module, "build_polymarket_runtime", _venue_sentinel)

        def _user_state_sentinel(*args, **kwargs):
            harness.user_state_users.append(args[0].user_id)
            raise SentinelReached("user state built")

        monkeypatch.setattr(app_module, "build_user_state", _user_state_sentinel)

        async def _balance(wallet, provider_runtime=None):
            harness.balance_wallets.append(wallet)
            from decimal import Decimal

            return Decimal("100")

        monkeypatch.setattr(app_module, "get_my_balance", _balance)
        monkeypatch.setattr(app_module, "fetch_live_positions", AsyncMock(return_value=[]))
        monkeypatch.setattr(
            app_module,
            "perform_startup_health_check",
            AsyncMock(return_value=SimpleNamespace(status="healthy", healthy=True, checks=[])),
        )


def _write(path, content: str) -> str:
    path.write_text(content.lstrip("\n"), encoding="utf-8")
    return str(path)


def _copiers_yaml(tmp_path, *parts: str) -> str:
    """Write a schema v2 copiers roster; each part is dedented separately."""
    body = "".join(textwrap.dedent(part) for part in parts)
    return _write(tmp_path / "copiers.yml", "schema_version: 2\ncopiers:\n" + body)


_GOOD_PRIMARY = f"""\
    - id: {USER_ALPHA}
      enabled: true
      mode: paper
      wallet:
        funder_address_env: ALPHA_FUNDER
      limits:
        max_order_quote: "5.00"
        max_open_exposure_quote: "50.00"
      follows:
        - leader_address: "{LEADER_1}"
          enabled: true
          sizing: {{mode: fixed, quote_amount: "2.00"}}
    """


async def _run_startup(monkeypatch, tmp_path, *, copiers_file=None):
    # The roster path guard confines copiers files to $HOME or the working
    # directory; chdir so the tmp file resolves and the rejection under test
    # is the CONFIG one, not the path one.
    monkeypatch.chdir(tmp_path)
    overrides = {}
    if copiers_file:
        overrides["app"] = {"copiers_config_file": copiers_file}
    config = make_config(**overrides)
    harness = _StartupHarness(monkeypatch, tmp_path, config)
    result = await _run_with_harness(harness)
    return harness, result


async def _run_with_harness(harness):
    """Drive the production ``_run_application`` composition once."""
    registry: list = []
    return await app_module._run_application(
        registry,
        startup_policy=None,
        evidence_runtime=harness.evidence_runtime,
    )


def _assert_no_execution(harness):
    """Nothing past configuration validation may have started trading-side work."""
    assert harness.venue_users == [], "venue runtime built despite invalid config"
    assert harness.user_state_users == [], "user state built despite invalid config"
    assert harness.leases_acquired == [], "lease acquired despite invalid config"
    assert not any(stage.startswith("lease_manager") for stage in harness.stages), (
        "lease manager constructed despite invalid config"
    )
    assert harness.balance_wallets == [], "private balance read despite invalid config"


# ---------------------------------------------------------------------------
# §9 bullet 9 — invalid configurations rejected before execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_copier_id_is_rejected_before_execution(monkeypatch, tmp_path, capsys):
    body = (
        _GOOD_PRIMARY,
        f"""\
        - id: {USER_ALPHA}
          enabled: true
          mode: paper
          wallet:
            funder_address_env: BETA_FUNDER
          limits:
            max_order_quote: "3.00"
            max_open_exposure_quote: "20.00"
          follows:
            - leader_address: "{LEADER_2}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "1.00"}}
        """,
    )
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    monkeypatch.setenv("BETA_FUNDER", FUNDER_BETA)
    harness, result = await _run_startup(
        monkeypatch, tmp_path, copiers_file=_copiers_yaml(tmp_path, *body)
    )
    assert result == 1
    assert "duplicate copier id" in capsys.readouterr().out
    _assert_no_execution(harness)


@pytest.mark.asyncio
async def test_duplicate_canonical_wallet_identity_is_rejected_before_execution(
    monkeypatch, tmp_path, capsys
):
    # Both copiers resolve the SAME funder wallet (one via a second env var).
    body = (
        _GOOD_PRIMARY,
        f"""\
        - id: {USER_BETA}
          enabled: true
          mode: paper
          wallet:
            funder_address_env: ALPHA_FUNDER_AGAIN
          limits:
            max_order_quote: "3.00"
            max_open_exposure_quote: "20.00"
          follows:
            - leader_address: "{LEADER_2}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "1.00"}}
        """,
    )
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    monkeypatch.setenv("ALPHA_FUNDER_AGAIN", FUNDER_ALPHA)
    harness, result = await _run_startup(
        monkeypatch, tmp_path, copiers_file=_copiers_yaml(tmp_path, *body)
    )
    assert result == 1
    assert "the same funder address" in capsys.readouterr().out
    _assert_no_execution(harness)


@pytest.mark.asyncio
async def test_invalid_boolean_is_rejected_before_execution(monkeypatch, tmp_path, capsys):
    body = f"""\
        - id: {USER_ALPHA}
          enabled: "yes"
          mode: paper
          wallet:
            funder_address_env: ALPHA_FUNDER
          limits:
            max_order_quote: "5.00"
            max_open_exposure_quote: "50.00"
          follows:
            - leader_address: "{LEADER_1}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "2.00"}}
        """
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    harness, result = await _run_startup(
        monkeypatch, tmp_path, copiers_file=_copiers_yaml(tmp_path, body)
    )
    assert result == 1
    assert "boolean" in capsys.readouterr().out
    _assert_no_execution(harness)


@pytest.mark.asyncio
async def test_nan_amount_is_rejected_before_execution(monkeypatch, tmp_path, capsys):
    body = f"""\
        - id: {USER_ALPHA}
          enabled: true
          mode: paper
          wallet:
            funder_address_env: ALPHA_FUNDER
          limits:
            max_order_quote: "NaN"
            max_open_exposure_quote: "50.00"
          follows:
            - leader_address: "{LEADER_1}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "2.00"}}
        """
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    harness, result = await _run_startup(
        monkeypatch, tmp_path, copiers_file=_copiers_yaml(tmp_path, body)
    )
    assert result == 1
    assert "finite" in capsys.readouterr().out
    _assert_no_execution(harness)


@pytest.mark.asyncio
async def test_invalid_secret_reference_is_rejected_before_execution(monkeypatch, tmp_path):
    """Live mode naming an unset private-key env var fails closed at startup."""
    body = f"""\
        - id: {USER_ALPHA}
          enabled: true
          mode: live
          wallet:
            funder_address_env: ALPHA_FUNDER
            private_key_env: PRIVATE_KEY_THAT_IS_NOT_SET
          limits:
            max_order_quote: "5.00"
            max_open_exposure_quote: "50.00"
          follows:
            - leader_address: "{LEADER_1}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "2.00"}}
        """
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    monkeypatch.delenv("PRIVATE_KEY_THAT_IS_NOT_SET", raising=False)
    monkeypatch.chdir(tmp_path)
    live_config = make_config(
        **{
            "execution": {"trading_mode": "live"},
            "app": {"copiers_config_file": _copiers_yaml(tmp_path, body)},
        }
    )

    harness = _StartupHarness(monkeypatch, tmp_path, live_config)
    result = await _run_with_harness(harness)
    assert result == 1
    # Rejection is fail-closed and silent about key material.
    _assert_no_execution_for(harness)


def _assert_no_execution_for(harness):
    assert harness.venue_users == []
    assert harness.user_state_users == []
    assert harness.leases_acquired == []


@pytest.mark.asyncio
async def test_mixed_paper_live_roster_is_rejected_before_execution(monkeypatch, tmp_path, capsys):
    """§9 bullet 11, row 2: mixed paper/live copier modes fail at startup."""
    monkeypatch.chdir(tmp_path)
    live_body = f"""\
        - id: {USER_BETA}
          enabled: true
          mode: live
          wallet:
            funder_address_env: BETA_FUNDER
            private_key_env: BETA_PRIVATE_KEY
          limits:
            max_order_quote: "3.00"
            max_open_exposure_quote: "20.00"
          follows:
            - leader_address: "{LEADER_2}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "1.00"}}
        """
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    monkeypatch.setenv("BETA_FUNDER", FUNDER_BETA)
    monkeypatch.setenv("BETA_PRIVATE_KEY", synth_private_key("9"))
    config = make_config(
        **{"app": {"copiers_config_file": _copiers_yaml(tmp_path, _GOOD_PRIMARY, live_body)}}
    )
    harness = _StartupHarness(monkeypatch, tmp_path, config)
    result = await _run_with_harness(harness)

    assert result == 1
    assert "mixed paper/live copiers are rejected" in capsys.readouterr().out
    _assert_no_execution(harness)


# ---------------------------------------------------------------------------
# §9 bullet 10 — lease acquired for EVERY account before any venue runtime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_second_writer_for_the_same_wallet_fails_closed_before_venue_runtimes(
    monkeypatch, tmp_path
):
    """A held lease for one account stops startup before ANY account trades."""
    body = (
        _GOOD_PRIMARY,
        f"""\
        - id: {USER_BETA}
          enabled: true
          mode: paper
          wallet:
            funder_address_env: BETA_FUNDER
          limits:
            max_order_quote: "3.00"
            max_open_exposure_quote: "20.00"
          follows:
            - leader_address: "{LEADER_2}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "1.00"}}
        """,
    )
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    monkeypatch.setenv("BETA_FUNDER", FUNDER_BETA)
    monkeypatch.chdir(tmp_path)
    config = make_config(**{"app": {"copiers_config_file": _copiers_yaml(tmp_path, *body)}})
    harness = _StartupHarness(monkeypatch, tmp_path, config)
    harness.lease_manager_cls = None  # not used; the failing manager is below

    held = {
        USER_BETA: SimpleNamespace(
            acquired=False,
            held_by=SimpleNamespace(holder_host="other-host", expires_at="soon"),
        )
    }

    class _ContendedLeaseManager:
        def __init__(self, *, pool=None, **_kwargs):
            pass

        async def acquire(self, account_id, *, mode):
            harness.leases_acquired.append((str(account_id), mode))
            outcome = held.get(str(account_id))
            if outcome is not None:
                return outcome
            return SimpleNamespace(acquired=True, held_by=None)

        async def release_all(self):
            harness.lease_release_all_calls += 1

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "AccountLeaseManager", _ContendedLeaseManager)

    result = await _run_with_harness(harness)

    assert result == 1
    # Every account's lease was attempted BEFORE any venue runtime existed —
    # the second process never becomes an active writer.
    assert harness.leases_acquired == [(USER_ALPHA, "paper"), (USER_BETA, "paper")]
    assert harness.venue_users == []
    assert harness.user_state_users == []
    # The first account's lease was released for a clean exit.
    assert harness.lease_release_all_calls == 1


@pytest.mark.asyncio
async def test_leases_precede_venue_runtimes_in_startup_order(monkeypatch, tmp_path):
    """All leases acquired first, then venue runtimes per account."""
    body = (
        _GOOD_PRIMARY,
        f"""\
        - id: {USER_BETA}
          enabled: true
          mode: paper
          wallet:
            funder_address_env: BETA_FUNDER
          limits:
            max_order_quote: "3.00"
            max_open_exposure_quote: "20.00"
          follows:
            - leader_address: "{LEADER_2}"
              enabled: true
              sizing: {{mode: fixed, quote_amount: "1.00"}}
        """,
    )
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    monkeypatch.setenv("BETA_FUNDER", FUNDER_BETA)
    monkeypatch.chdir(tmp_path)
    config = make_config(**{"app": {"copiers_config_file": _copiers_yaml(tmp_path, *body)}})
    harness = _StartupHarness(monkeypatch, tmp_path, config)

    with pytest.raises(SentinelReached, match=f"venue runtime built for {USER_ALPHA}"):
        await _run_with_harness(harness)

    # Both leases were acquired before the FIRST venue runtime was built.
    assert harness.leases_acquired == [(USER_ALPHA, "paper"), (USER_BETA, "paper")]
    assert harness.venue_users == [USER_ALPHA]
