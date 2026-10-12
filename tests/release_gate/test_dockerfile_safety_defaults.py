"""Release gate: Dockerfile must default to paper mode + non-root.

Pre-fix the Dockerfile had:
  * No USER directive — container ran as root.
  * ``CMD ["--paper"]`` was silently ignored (app.py has no argv
    parser), so a deployment missing an env-file ran in live mode
    while the operator believed paper was the default.
  * EXPOSE 9090 + HEALTHCHECK on port 9090, but the metrics server
    actually binds to METRICS_PORT (default 9102). Docker reported
    the container permanently unhealthy.
  * No .dockerignore — ``COPY . .`` baked secrets.env, .git, and
    data/ into image layers.

This test pins the contract by parsing the Dockerfile + .dockerignore
at the source level so any regression is caught before merge.
"""

from __future__ import annotations

from pathlib import Path
import tomllib

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def dockerfile_text() -> str:
    return (REPO_ROOT / "Dockerfile").read_text()


@pytest.fixture(scope="module")
def dockerignore_text() -> str:
    return (REPO_ROOT / ".dockerignore").read_text()


@pytest.fixture(scope="module")
def ci_workflow_text() -> str:
    return (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()


def test_dockerfile_does_not_enable_live_trading(dockerfile_text: str) -> None:
    # P0-2: the live-trading gate moved to the config layer — the LIVE_TRADING
    # master switch defaults to PAPER when absent/unknown (see
    # test_config.py::test_default_is_paper). The image must not ENABLE live via
    # an ENV directive (comments documenting LIVE_TRADING are fine).
    for line in dockerfile_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("ENV "):
            continue
        assert not ("PMCOPY__EXECUTION__TRADING_MODE" in stripped and "live" in stripped.lower()), (
            f"Dockerfile must not bake execution.trading_mode=live: {stripped!r}"
        )


def test_non_root_user_directive(dockerfile_text: str) -> None:
    assert "USER botuser" in dockerfile_text, (
        "Dockerfile must drop privileges to non-root before ENTRYPOINT"
    )
    assert "groupadd -r botuser" in dockerfile_text, "non-root botuser must be created in the image"


def test_healthcheck_port_matches_metrics_port(dockerfile_text: str) -> None:
    # The healthcheck must read the nested metrics port (default 9102) instead of
    # hardcoding the historical 9090.
    assert "METRICS_PORT" in dockerfile_text
    assert "EXPOSE 9102" in dockerfile_text or "EXPOSE 9090" not in dockerfile_text


def test_metrics_server_enabled_by_default(dockerfile_text: str) -> None:
    assert "ENV PMCOPY__APP__ENABLE_METRICS_SERVER=true" in dockerfile_text, (
        "Metrics server must start by default so the HEALTHCHECK has a port"
    )


def test_docker_smoke_uses_hermetic_health_contract(
    ci_workflow_text: str,
) -> None:
    """CI cannot satisfy the production probe with fake paper credentials.

    The image keeps strict ``/health/ready`` for real deployments.  The smoke
    run must override that probe with the local DB/API health payload while
    still waiting for Docker's health state (not merely ``State.Running``).
    """
    smoke_run = ci_workflow_text.split("- name: docker run (paper mode)", maxsplit=1)[1].split(
        "- name: container readiness probe", maxsplit=1
    )[0]
    readiness_probe = ci_workflow_text.split("- name: container readiness probe", maxsplit=1)[
        1
    ].split("- name: cleanup", maxsplit=1)[0]

    assert "--health-cmd=" in smoke_run
    assert "http://localhost:9102/health" in smoke_run
    assert "/health/ready" not in smoke_run
    assert 'payload.get(\\"ok\\")' in smoke_run
    assert ".State.Health.Status" in readiness_probe
    assert ".State.Running" not in readiness_probe


def test_console_entrypoint_modules_are_in_the_installed_wheel() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    setuptools_cfg = project["tool"]["setuptools"]
    find_cfg = setuptools_cfg["packages"]["find"]
    assert find_cfg["where"] == ["src"], "package discovery must target src/ (src-layout)"
    assert set(find_cfg["include"]) == {"pmcopy*"}, (
        "the single installable namespace must be pmcopy under src/ "
        "(pmcopy.app/pmcopy.bootstrap ship as modules of that package for "
        "non-editable Docker installs)"
    )
    assert "py-modules" not in setuptools_cfg, (
        "flat py-modules must not reappear beside the src/pmcopy namespace"
    )


def test_dockerignore_excludes_secrets(dockerignore_text: str) -> None:
    for sensitive in ("secrets.env", ".env", "config/runtime.local.yml", ".git/"):
        assert sensitive in dockerignore_text, (
            f".dockerignore must exclude {sensitive!r} so it never bakes into image layer history"
        )
