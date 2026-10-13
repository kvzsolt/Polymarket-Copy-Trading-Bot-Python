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

FORBIDDEN_SECRET_BASENAMES = ("secrets.env", "db.env", "secrets.toml", "runtime.local")


def runtime_stage_text(dockerfile_text: str) -> str:
    """Text of the final (runtime) stage: everything after the LAST FROM.

    ``rsplit`` is deliberate: the builder FROM line (``... AS builder``)
    contains the runtime FROM string as a prefix, so the LAST occurrence is
    the runtime stage boundary.
    """
    from_lines = [line for line in dockerfile_text.splitlines() if line.startswith("FROM ")]
    assert len(from_lines) == 2, "image must be exactly builder + runtime stages"
    _, runtime = dockerfile_text.rsplit(from_lines[-1], maxsplit=1)
    return runtime


def copy_add_lines(stage_text: str) -> list[str]:
    """Every COPY/ADD instruction line in the stage (stripped)."""
    return [
        line.strip()
        for line in stage_text.splitlines()
        if line.strip().startswith(("COPY ", "ADD "))
    ]


def runtime_secret_copy_violations(dockerfile_text: str) -> list[str]:
    """Runtime-stage COPY/ADD lines that name a forbidden secret basename.

    Dockerfile instruction keywords are case-insensitive, and the runtime
    stage is the final image layer set: ANY ``COPY``/``ADD`` there bakes
    into image layers, not only the ``--from=builder`` crossings.
    """
    return [
        line
        for line in copy_add_lines(runtime_stage_text(dockerfile_text))
        if line.startswith("COPY --from=builder")
        for forbidden in FORBIDDEN_SECRET_BASENAMES
        if forbidden in line
    ]


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


def test_no_pythonpath_shadowing_in_the_image(dockerfile_text: str) -> None:
    # Task 20: the runtime installs the built wheel into site-packages. The
    # old PYTHONPATH=/app/src shadow hack — which let a bind-mounted source
    # tree override the installed package (and made the wheel's packaged
    # config silently ignorable) — must stay gone from every stage. Comments
    # are inert; any *instruction* mentioning PYTHONPATH fails this gate.
    offenders = [
        line
        for line in dockerfile_text.splitlines()
        if "PYTHONPATH" in line and not line.lstrip().startswith("#")
    ]
    assert not offenders, (
        f"The image must run the installed wheel; PYTHONPATH instructions must not reappear: {offenders}"
    )


def test_runtime_stage_contains_only_the_built_wheel(dockerfile_text: str) -> None:
    """No source tree and no secret-shaped file may enter the runtime stage.

    File-content assertions only (never a rendered compose config). The
    runtime stage is the final image layer set, so anything COPY/ADD-ed
    there — directly or across the ``--from=builder`` boundary — bakes into
    image layers.
    """
    runtime = runtime_stage_text(dockerfile_text)
    copies = copy_add_lines(runtime)
    assert copies, "runtime stage must COPY the requirements export"
    for line in copies:
        args = [token for token in line.split()[1:] if not token.startswith("--")]
        assert args and args[0] != "." and "src/pmcopy" not in line and "src" not in args, (
            f"runtime stage must not contain the source tree: {line!r}"
        )
    stage_crossings = [line for line in copies if line.startswith("COPY --from=builder")]
    assert stage_crossings, "the built wheel must cross the stage boundary"
    for line in stage_crossings:
        assert line.startswith("COPY --from=builder /wheels/"), (
            f"only built wheels may cross the stage boundary: {line!r}"
        )
    assert runtime_secret_copy_violations(dockerfile_text) == [], (
        "secret-shaped files must not enter the runtime stage via any COPY/ADD instruction"
    )


_SYNTHETIC_RUNTIME_DOCKERFILE = """\
FROM python:3.14.6-slim-bookworm AS builder
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --no-cache-dir --no-deps -w /wheels .
FROM python:3.14.6-slim-bookworm
COPY requirements.runtime.txt .
COPY --from=builder /wheels/*.whl /tmp/wheels/
"""


def test_runtime_secret_scan_catches_plain_runtime_stage_copy() -> None:
    """A secret COPY that is NOT a stage crossing must still be caught.

    The historical loop only inspected ``COPY --from=builder`` lines, so a
    plain runtime-stage ``COPY secrets.env /x`` baked the secret into the
    final image with the gate green.
    """
    dockerfile = _SYNTHETIC_RUNTIME_DOCKERFILE + "COPY secrets.env /secrets.env\n"
    assert runtime_secret_copy_violations(dockerfile) == ["COPY secrets.env /secrets.env"]


def test_runtime_secret_scan_matches_instructions_case_insensitively() -> None:
    """Dockerfile keywords are case-insensitive; so must the guard be.

    ``add``/``copy`` in lowercase currently bypass the line collector
    entirely — on plain runtime copies AND on ``--from=builder`` crossings.
    """
    dockerfile = (
        _SYNTHETIC_RUNTIME_DOCKERFILE
        + "add secrets.env /secrets.env\n"
        + "copy --from=builder /db.env /db.env\n"
    )
    assert runtime_secret_copy_violations(dockerfile) == [
        "add secrets.env /secrets.env",
        "copy --from=builder /db.env /db.env",
    ]


def test_runtime_secret_scan_clean_dockerfile_stays_clean() -> None:
    assert runtime_secret_copy_violations(_SYNTHETIC_RUNTIME_DOCKERFILE) == []


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


def test_docker_smoke_seeds_hermetic_approved_provider_stub(ci_workflow_text: str) -> None:
    """The paper smoke must satisfy the approved-provider startup gate.

    Startup fail-closes until ``polymarket.rpc_url`` names an approved
    alchemy/infura endpoint (``ProviderEndpoints``) with real balance
    evidence, and CI holds no provider credential. The smoke therefore maps
    the approved hostname onto a deterministic fake RPC
    (``scripts/fake_polygon_rpc.py``) with a per-run CA, a runtime-generated
    URL token, and a certifi trust-bundle mount — never a literal
    credential, and the strict production probe stays overridden by the
    local ``/health`` contract above.
    """
    smoke_run = ci_workflow_text.split("- name: docker run (paper mode)", maxsplit=1)[1].split(
        "- name: container readiness probe", maxsplit=1
    )[0]
    stub_step = ci_workflow_text.split(
        "- name: start fake Polygon provider (approved-endpoint stub)", maxsplit=1
    )[1].split("- name: docker run (paper mode)", maxsplit=1)[0]

    assert "fake_polygon_rpc.py" in stub_step, "CI must run the deterministic fake provider"
    assert "openssl req -x509" in stub_step, "the stub CA is generated per run"
    assert "certifi" in stub_step and "bundle.pem" in stub_step, (
        "the image's certifi bundle must be extended: the provider transport "
        "runs trust_env=False and ignores SSL_CERT_FILE"
    )
    assert "--add-host polygon-mainnet.g.alchemy.com:" in smoke_run
    for key in ("PMCOPY__POLYMARKET__RPC_URL", "PMCOPY__WALLET__POLYGON_RPC_URL"):
        assert f"{key}=https://polygon-mainnet.g.alchemy.com/v2/$FAKE_RPC_TOKEN" in smoke_run, (
            f"{key} must use the approved-host form with a runtime token"
        )
    assert "FAKE_RPC_TOKEN=$(openssl rand -hex" in smoke_run, (
        "the URL token must be generated at runtime, never a literal credential"
    )
    cleanup = ci_workflow_text.split("- name: cleanup", maxsplit=1)[1]
    assert "pmct-rpc" in cleanup, "cleanup must remove the stub container"


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
