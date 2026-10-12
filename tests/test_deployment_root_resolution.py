"""Deployment-root resolution: env var or CWD only — never a parent walk.

Credential discovery must be explicit. A wheel-installed CLI started in an
arbitrary directory must not walk ancestor directories looking for
``secrets.env`` / ``config/runtime.local.yml`` markers: an unrelated file in
a parent (a shared CI home, a leftover deployment) would silently become
part of this process's configuration. The deployment root is exactly:

1. ``PMCOPY_DEPLOYMENT_ROOT`` (when set to an existing directory), else
2. the process CWD.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pmcopy.config.app_config import load_config

ADDRESS = "0x" + "3" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://probe:probe@localhost/probe"},
        "wallet": {"funder_address": ADDRESS},
        "polymarket": {"rpc_url": "https://rpc.invalid"},
    }


def _write_secrets(path: Path, *, mode: int = 0o600) -> None:
    path.write_text(
        "PMCOPY__DATABASE__DSN=postgresql://p:p@l/p\n"
        "PMCOPY__POLYMARKET__RPC_URL=https://from-secrets.invalid\n",
        encoding="utf-8",
    )
    path.chmod(mode)


def test_resolve_deployment_root_never_leaves_the_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pmcopy.config import app_config

    ancestor = tmp_path / "ancestor-with-secrets"
    nested = ancestor / "deep" / "cwd"
    nested.mkdir(parents=True)
    _write_secrets(ancestor / "secrets.env")

    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)
    monkeypatch.chdir(nested)

    assert app_config._resolve_deployment_root() == nested


def test_parent_secrets_env_is_not_loaded_from_a_nested_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A secrets.env in an ancestor directory must never be discovered."""
    deployment = tmp_path / "deployment"
    nested = deployment / "some" / "nested" / "dir"
    nested.mkdir(parents=True)
    _write_secrets(deployment / "secrets.env")

    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)
    monkeypatch.chdir(nested)

    config = load_config(_overrides())

    # The ancestor secrets were not merged: the override rpc_url survives.
    assert config.polymarket.rpc_url == "https://rpc.invalid"


def test_parent_runtime_local_shim_is_not_loaded_from_a_nested_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    (deployment / "config" / "runtime.local.yml").write_text(
        "sizing:\n  percentage: 9.5\n", encoding="utf-8"
    )
    nested = deployment / "sub" / "dir"
    nested.mkdir(parents=True)

    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)
    monkeypatch.chdir(nested)

    config = load_config(_overrides())

    # The packaged default survives; the ancestor shim was not consulted.
    assert config.sizing.percentage != 9.5


def test_cwd_secrets_env_is_loaded_without_the_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Running with CWD == deployment root keeps today's explicit semantics."""
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    _write_secrets(deployment / "secrets.env")

    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)
    monkeypatch.chdir(deployment)

    config = load_config(_overrides())

    assert config.polymarket.rpc_url == "https://from-secrets.invalid"


def test_env_var_root_wins_over_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    _write_secrets(deployment / "secrets.env")
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    _write_secrets(foreign / "secrets.env")

    monkeypatch.chdir(foreign)
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides())

    assert config.polymarket.rpc_url == "https://from-secrets.invalid"


def test_missing_env_var_root_falls_back_to_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pmcopy.config import app_config

    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(tmp_path / "does-not-exist"))

    assert app_config._resolve_deployment_root() == cwd
