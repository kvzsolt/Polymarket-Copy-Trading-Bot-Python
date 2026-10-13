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

#: The production image base, pinned to the exact interpreter patch the test
#: suite runs on (Python 3.13.15). Verified against the registry on
#: 2026-10-13; see docs/docker.md for the recorded digests.
PINNED_BASE = "python:3.13.15-slim-bookworm"


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

    def stages(self) -> list[tuple[str, list[str]]]:
        """Return ``(from_clause, instruction_lines)`` per build stage."""
        stages: list[tuple[str, list[str]]] = []
        for line in self.dockerfile.splitlines():
            if line.startswith("FROM "):
                stages.append((line.split(maxsplit=1)[1], []))
            elif stages:
                stages[-1][1].append(line)
        return stages

    def instructions_mentioning(self, token: str) -> list[str]:
        """Non-comment lines mentioning ``token`` (comments are inert)."""
        return [
            line
            for line in self.dockerfile.splitlines()
            if token in line and not line.lstrip().startswith("#")
        ]

    def environment(self) -> dict[str, str]:
        """Return image environment defaults from exec-free ENV declarations."""
        result: dict[str, str] = {}
        for line in self.dockerfile.splitlines():
            if line.startswith("ENV "):
                result.update(part.split("=", 1) for part in shlex.split(line[4:]))
        return result


def test_image_matches_the_repository_python_minor() -> None:
    """The runtime image base equals the repository's supported Python.

    The repository targets Python 3.13 only (``requires-python >=3.13,<3.14``)
    and the suite is green on 3.13.15, so the image is pinned to the same
    interpreter patch (``python:3.13.15-slim-bookworm``, registry-verified —
    see docs/docker.md). Task 20 restored the strict equality that Task 17
    had temporarily relaxed while the image stayed on 3.12.
    """
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    minor = project["tool"]["pyright"]["pythonVersion"]
    assert minor == "3.13", "Repository must target Python 3.13 only"
    assert ContainerArtifacts().instruction("FROM") == PINNED_BASE, (
        "Runtime image base must be the pinned Python 3.13.15 slim image"
    )


def test_image_is_multi_stage_with_a_wheel_only_runtime() -> None:
    """Builder compiles the wheel; the runtime stage installs only that wheel.

    The interim single-stage image copied the whole checkout into ``/app``
    and set ``PYTHONPATH=/app/src`` so a bind-mounted config tree could
    shadow site-packages. The modernized image (Task 20) removes both: the
    builder stage compiles the pure-Python wheel, the runtime stage installs
    that wheel plus the locked runtime dependencies, and nothing else crosses
    the stage boundary — no source tree, no ``PYTHONPATH``, packaged config
    YAML resolved through importlib.resources from site-packages.
    """
    artifacts = ContainerArtifacts()
    stages = artifacts.stages()
    assert [name for name, _ in stages] == [f"{PINNED_BASE} AS builder", PINNED_BASE], (
        "Exactly two stages on the pinned base: builder, then runtime"
    )
    builder_lines = stages[0][1]
    runtime_lines = stages[1][1]

    assert any("pip wheel" in line for line in builder_lines), (
        "builder stage must compile the application wheel"
    )

    copies = [line for line in runtime_lines if line.startswith(("COPY ", "ADD "))]
    assert copies, "runtime stage must COPY the requirements export"
    assert any(line.startswith("COPY --from=builder /wheels/") for line in copies), (
        "the only application artifact crossing the stage boundary must be the built wheel"
    )
    for line in copies:
        args = [token for token in line.split()[1:] if not token.startswith("--")]
        assert args[0] not in (".", "src") and "src/pmcopy" not in line, (
            f"runtime stage must not contain the source tree: {line!r}"
        )
    assert not artifacts.instructions_mentioning("PYTHONPATH"), (
        "No stage may shadow the installed pmcopy package"
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


def test_compose_config_reaches_the_process_through_the_installed_wheel() -> None:
    """Service YAML, environment overlays, and trader files reach the process.

    The runtime image is wheel-only (no source tree, no ``PYTHONPATH``), so
    the config tree ships inside the installed ``pmcopy`` package and compose
    no longer bind-mounts ``./src/pmcopy/config`` — a mount under ``/app/src``
    would be dead weight the process never reads. Runtime overrides use
    ``PMCOPY__*`` environment variables; changing packaged service YAML
    requires rebuilding the image (Task 20).
    """
    artifacts = ContainerArtifacts()
    mounts = artifacts.compose["services"]["bot"]["volumes"]
    for item in mounts:
        if isinstance(item, dict):
            assert not item["target"].startswith("/app/src"), (
                "the wheel-only image has no /app/src; source-tree mounts are dead"
            )
    assert not artifacts.instructions_mentioning("PYTHONPATH"), (
        "no import shadowing may coexist with the wheel-only runtime"
    )
    assert "runtime.local.yml" not in str(mounts)
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    package_data = project["tool"]["setuptools"]["package-data"]["pmcopy.config"]
    assert {"*.yml", "traders/*.yml", "environments/*/*.yml"} <= set(package_data), (
        "the installed wheel must carry the packaged config tree"
    )


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
