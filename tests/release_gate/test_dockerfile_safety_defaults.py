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

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def dockerfile_text() -> str:
    return (REPO_ROOT / "Dockerfile").read_text()


@pytest.fixture(scope="module")
def dockerignore_text() -> str:
    return (REPO_ROOT / ".dockerignore").read_text()


def test_dockerfile_does_not_enable_live_trading(dockerfile_text: str) -> None:
    # P0-2: the live-trading gate moved to the config layer — the LIVE_TRADING
    # master switch defaults to PAPER when absent/unknown (see
    # test_config.py::test_default_is_paper). The image must not ENABLE live via
    # an ENV directive (comments documenting LIVE_TRADING are fine).
    for line in dockerfile_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("ENV "):
            continue
        assert "LIVE_TRADING" not in stripped, (
            f"Dockerfile must not set LIVE_TRADING via ENV (live must be an "
            f"explicit per-deployment opt-in): {stripped!r}"
        )
        assert not ("PAPER_TRADE_ONLY" in stripped and "false" in stripped.lower()), (
            f"Dockerfile must not bake PAPER_TRADE_ONLY=false: {stripped!r}"
        )


def test_non_root_user_directive(dockerfile_text: str) -> None:
    assert "USER botuser" in dockerfile_text, (
        "Dockerfile must drop privileges to non-root before ENTRYPOINT"
    )
    assert "groupadd -r botuser" in dockerfile_text, (
        "non-root botuser must be created in the image"
    )


def test_healthcheck_port_matches_metrics_port(dockerfile_text: str) -> None:
    # The healthcheck must read METRICS_PORT (default 9102) instead of
    # hardcoding the historical 9090.
    assert "METRICS_PORT" in dockerfile_text
    assert "EXPOSE 9102" in dockerfile_text or "EXPOSE 9090" not in dockerfile_text


def test_metrics_server_enabled_by_default(dockerfile_text: str) -> None:
    assert "ENV ENABLE_METRICS_SERVER=true" in dockerfile_text, (
        "Metrics server must start by default so the HEALTHCHECK has a port"
    )


def test_dockerignore_excludes_secrets(dockerignore_text: str) -> None:
    for sensitive in ("secrets.env", ".env", "config/runtime.local.env", ".git/"):
        assert sensitive in dockerignore_text, (
            f".dockerignore must exclude {sensitive!r} so it never bakes into "
            f"image layer history"
        )
