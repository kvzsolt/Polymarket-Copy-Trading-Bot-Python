"""Shared-fixture container identity contracts (F5-1).

The integration tier reuses ONE long-lived container by NAME
(``pmcopy-integration-pg``). Name-only reuse is silent substitution: a
leftover container from a different image (or bound to a different host
port) would be happily reused and every test would run against the wrong
server. Before reusing an EXISTING container the fixture must verify:

* the container's image id equals the pinned local ``postgres:16-alpine``
  image id (``docker image inspect`` — no pulls);
* the container binds the expected ``HOST:PORT -> 5432`` mapping;
* any failed inspect is itself a fail-closed fixture error.

Every mismatch message must tell the operator how to recover
(``docker rm -f pmcopy-integration-pg``) — never reuse silently.

All docker interaction here is faked at the ``tests.integration_postgres._docker``
seam, so these contracts never touch the real daemon and never create
rogue containers.
"""

from __future__ import annotations

import subprocess

import pytest

import tests.integration_postgres as ipg

PINNED_IMAGE_ID = "sha256:" + "a" * 64
OTHER_IMAGE_ID = "sha256:" + "b" * 64
CONTAINER_ID = "sha256:" + "e" * 64


def _completed(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["docker"], returncode=rc, stdout=stdout, stderr=stderr)


class FakeDocker:
    """Scripted ``_docker`` replacement dispatching on the docker subcommand.

    Unrecognised commands (``run`` / ``start`` / ``exec`` ...) are recorded
    in ``unexpected`` and answered with rc 0 — the identity tests assert
    ``unexpected`` stays EMPTY, proving the fixture failed closed BEFORE
    starting, probing, or creating anything.
    """

    def __init__(
        self,
        *,
        image_id: str = PINNED_IMAGE_ID,
        image_rc: int = 0,
        container_image_id: str | None = None,
        container_image_rc: int = 0,
        running: str = "true",
        port_stdout: str | None = None,
        port_rc: int = 0,
        runtime_ok: bool = False,
        ci_pull_heals_image: bool = False,
        tolerate_start: bool = False,
    ) -> None:
        self.image_id = image_id
        self.image_rc = image_rc
        self.container_image_id = container_image_id if container_image_id is not None else image_id
        self.container_image_rc = container_image_rc
        self.running = running
        self.port_stdout = f"{ipg.HOST}:{ipg.PORT}\n" if port_stdout is None else port_stdout
        self.port_rc = port_rc
        self.runtime_ok = runtime_ok
        self.ci_pull_heals_image = ci_pull_heals_image
        self.tolerate_start = tolerate_start
        self.calls: list[str] = []
        self.unexpected: list[str] = []

    def __call__(self, args: list[str], *, timeout: float = 120.0):
        self.calls.append(" ".join(args))
        if args[:2] == ["image", "inspect"]:
            if self.image_rc == 0:
                return _completed(0, stdout=f"{self.image_id}\n")
            if self.ci_pull_heals_image and any(c.startswith("pull ") for c in self.calls[:-1]):
                return _completed(0, stdout=f"{self.image_id}\n")
            return _completed(self.image_rc, stderr="Error: No such image")
        if args[:2] == ["container", "inspect"]:
            if "{{.Image}}" in args:
                if self.container_image_rc != 0:
                    return _completed(self.container_image_rc, stderr="inspect failed")
                return _completed(0, stdout=f"{self.container_image_id}\n")
            return _completed(0, stdout=f"{CONTAINER_ID} {self.running}\n")
        if args[0] == "port":
            return _completed(self.port_rc, stdout=self.port_stdout)
        if args[0] == "start" and self.tolerate_start:
            return _completed(0, stdout=CONTAINER_ID)
        if self.runtime_ok and args[0] == "exec":
            # pg_isready probes and the GC psql listing both succeed quietly.
            return _completed(0, stdout="")
        self.unexpected.append(" ".join(args))
        return _completed(0, stdout="")


@pytest.fixture
def _no_ci_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CI", raising=False)


def _ensure_with(fake: FakeDocker, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setattr(ipg, "_docker", fake)
    return ipg._SharedServer().ensure()


def test_existing_container_with_different_image_fails_closed(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    fake = FakeDocker(container_image_id=OTHER_IMAGE_ID)
    with pytest.raises(
        ipg.IntegrationPostgresUnavailable, match="docker rm -f pmcopy-integration-pg"
    ):
        _ensure_with(fake, monkeypatch)
    # Fail-closed means fail EARLY: no start, no readiness probe, no GC.
    assert fake.unexpected == []


def test_existing_container_with_wrong_port_binding_fails_closed(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    fake = FakeDocker(port_stdout="0.0.0.0:9999\n")
    with pytest.raises(
        ipg.IntegrationPostgresUnavailable, match="docker rm -f pmcopy-integration-pg"
    ):
        _ensure_with(fake, monkeypatch)
    assert fake.unexpected == []


def test_existing_container_without_the_expected_binding_fails_closed(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    # Stopped-looking port output (rc 0, empty) must not pass as a binding.
    fake = FakeDocker(port_stdout="")
    with pytest.raises(
        ipg.IntegrationPostgresUnavailable, match="docker rm -f pmcopy-integration-pg"
    ):
        _ensure_with(fake, monkeypatch)
    assert fake.unexpected == []


def test_container_image_inspect_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    fake = FakeDocker(container_image_rc=1)
    with pytest.raises(
        ipg.IntegrationPostgresUnavailable, match="docker rm -f pmcopy-integration-pg"
    ):
        _ensure_with(fake, monkeypatch)
    assert fake.unexpected == []


def test_port_probe_failure_fails_closed(monkeypatch: pytest.MonkeyPatch, _no_ci_env: None) -> None:
    fake = FakeDocker(port_rc=1, port_stdout="")
    with pytest.raises(
        ipg.IntegrationPostgresUnavailable, match="docker rm -f pmcopy-integration-pg"
    ):
        _ensure_with(fake, monkeypatch)
    assert fake.unexpected == []


def test_matching_existing_container_is_reused_without_creation(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    fake = FakeDocker(runtime_ok=True)
    assert _ensure_with(fake, monkeypatch) == CONTAINER_ID
    # Reuse, not creation: the identity/port checks ran and no `docker run`
    # (or any other unexpected lifecycle command) was issued.
    assert fake.unexpected == []
    assert any(call.startswith("port ") for call in fake.calls)
    assert not any(call.startswith("run ") for call in fake.calls)


def test_stopped_but_valid_container_is_started_then_verified(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    # A valid stopped container is `docker start`-ed, then identity-checked.
    fake = FakeDocker(running="false", runtime_ok=True, tolerate_start=True)
    assert _ensure_with(fake, monkeypatch) == CONTAINER_ID
    assert any(call == f"start {ipg.CONTAINER_NAME}" for call in fake.calls)
    assert fake.unexpected == []


def test_absent_local_image_still_never_pulls_outside_ci(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    fake = FakeDocker(image_rc=1)
    with pytest.raises(ipg.IntegrationPostgresUnavailable, match="never pulls"):
        _ensure_with(fake, monkeypatch)
    assert not any(call.startswith("pull ") for call in fake.calls)


def test_ci_runner_pulls_once_when_image_absent(
    monkeypatch: pytest.MonkeyPatch, _no_ci_env: None
) -> None:
    monkeypatch.setenv("CI", "true")
    fake = FakeDocker(image_rc=1, ci_pull_heals_image=True, runtime_ok=True)
    assert _ensure_with(fake, monkeypatch) == CONTAINER_ID
    pulls = [call for call in fake.calls if call.startswith("pull ")]
    assert pulls == [f"pull {ipg.IMAGE}"]
