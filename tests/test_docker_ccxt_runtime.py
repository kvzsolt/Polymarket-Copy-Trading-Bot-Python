"""Container contracts for the CCXT-only main branch.

These checks use repository artifacts and synthetic configuration only; they
never read operator credentials or start the trading loop.
"""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path
import shlex
import tomllib

import pytest
import yaml

from pmcopy.config.app_config import load_config

ROOT = Path(__file__).resolve().parents[1]


class ContainerArtifacts:
    """Read container declarations without requiring a Docker daemon."""

    def __init__(self) -> None:
        self.dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    def instruction(self, name: str) -> str:
        """Return the last matching single-line Dockerfile instruction."""
        matches = [
            line.split(maxsplit=1)[1]
            for line in self.dockerfile.splitlines()
            if line.startswith(name + " ")
        ]
        return matches[-1] if matches else ""

    def environment(self) -> dict[str, str]:
        """Return image environment defaults from exec-free ENV declarations."""
        result: dict[str, str] = {}
        for line in self.dockerfile.splitlines():
            if line.startswith("ENV "):
                result.update(part.split("=", 1) for part in shlex.split(line[4:]))
        return result


def test_image_matches_the_repository_python_minor() -> None:
    """The image base tracks the repository's supported Python minor.

    The repository targets Python 3.14 only (``requires-python >=3.14,<3.15``),
    but the production image deliberately stays on ``python:3.12`` until the
    modernization plan's Docker task moves it to ``python:3.14.x-slim`` (see
    docs/docker.md). Until then this contract pins the exact interim state:
    image minor 3.12, repository target 3.14, and nothing else. When the
    Docker bump lands, restore the strict equality assertion.
    """
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    minor = project["tool"]["pyright"]["pythonVersion"]
    assert minor == "3.14", "Repository must target Python 3.14 only"
    assert ContainerArtifacts().instruction("FROM").startswith("python:3.12-"), (
        "Interim image base must stay python:3.12 until the Docker task lands"
    )


def test_image_cli_accepts_operator_subcommands_and_defaults_to_run() -> None:
    """A one-shot doctor command must not become ``pmcopy run doctor``."""
    artifacts = ContainerArtifacts()
    entrypoint = json.loads(artifacts.instruction("ENTRYPOINT"))
    assert entrypoint + ["doctor"] == ["pmcopy", "doctor"]
    assert entrypoint + json.loads(artifacts.instruction("CMD")) == ["pmcopy", "run"]


def test_build_verifies_ccxt_prediction_outside_the_source_tree() -> None:
    """Missing mandatory CCXT must fail the build despite the lazy import."""
    checks = [
        line
        for line in ContainerArtifacts().dockerfile.splitlines()
        if line.startswith("RUN cd /tmp && python -c ")
    ]
    assert any("ccxt.prediction" in line and "polymarket" in line for line in checks)
    assert any("ensure_supported_ccxt_version" in line for line in checks)


def test_compose_mounts_current_config_without_a_legacy_setup_file() -> None:
    """Service YAML, environment overlays, and trader files reach the process."""
    artifacts = ContainerArtifacts()
    mounts = artifacts.compose["services"]["bot"]["volumes"]
    mount = next(
        (item for item in mounts if isinstance(item, dict) and item["target"] == "/app/src/pmcopy/config"),
        None,
    )
    assert mount is not None, "Compose must mount the canonical config tree"
    assert mount["type"] == "bind"
    assert mount["source"] == "./src/pmcopy/config"
    assert mount["read_only"] is True
    assert mount["bind"]["create_host_path"] is False
    assert artifacts.environment().get("PYTHONPATH") == "/app/src", (
        "The installed pmcopy command must import config from the mounted tree"
    )
    assert "runtime.local.yml" not in str(mounts)


@pytest.mark.parametrize("target", ["/app/data", "/app/log"])
def test_compose_persists_writable_directories_in_named_volumes(target: str) -> None:
    """Fresh volumes inherit image ownership instead of root-owned host paths."""
    artifacts = ContainerArtifacts()
    mounts = artifacts.compose["services"]["bot"]["volumes"]
    sources = [
        item.split(":")[0]
        for item in mounts
        if isinstance(item, str) and item.split(":")[1] == target
    ]
    assert len(sources) == 1
    assert sources[0] in artifacts.compose["volumes"], "Use a managed volume, not a host bind"
    assert artifacts.compose["volumes"]["postgres_data"]["name"] == (
        "polymarket_copytrading_python_postgres_data"
    )


def test_metrics_are_reachable_with_canonical_config_and_paper_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Container networking overrides must survive the real config loader."""
    artifacts = ContainerArtifacts()
    image_environment = artifacts.environment()
    compose_environment = artifacts.compose["services"]["bot"]["environment"]
    for environment in (image_environment, compose_environment):
        assert environment.get("PMCOPY__APP__METRICS_BIND_HOST") == "0.0.0.0"
        assert not any(key.startswith("PMCOPY__NETWORK__") for key in environment)
        for key, value in environment.items():
            monkeypatch.setenv(key, value)
    config = load_config(
        {
            "database": {"dsn": "postgresql://test:test@localhost/test"},
            "wallet": {"funder_address": "0x" + "1" * 40},
            "polymarket": {"rpc_url": "https://rpc.invalid"},
        },
        environment="paper",
    )
    assert config.execution.trading_mode == "paper"
    assert config.app.enable_metrics_server is True
    assert config.app.metrics_bind_host == "0.0.0.0"
    assert config.app.metrics_port == 9102
    assert artifacts.compose["services"]["bot"]["ports"] == ["127.0.0.1:9102:9102"]


@pytest.mark.parametrize(
    "filename",
    ["secrets.env.bak-2026-08-17", "secrets.env.backup", ".env.local", "config/runtime.local.env"],
)
def test_build_context_excludes_local_credential_backups(filename: str) -> None:
    """Local credential copies must never reach COPY or image history."""
    patterns = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    excluded = False
    for pattern in patterns:
        pattern = pattern.strip()
        if not pattern or pattern.startswith("#"):
            continue
        if fnmatch.fnmatchcase(filename, pattern.removeprefix("!")):
            excluded = not pattern.startswith("!")
    assert excluded, f"Credential backup is not excluded: {filename}"
