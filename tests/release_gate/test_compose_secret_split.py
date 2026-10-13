"""Release gate: container secrets are split between postgres and the bot.

Pre-fix both compose services shared one ``env_file: secrets.env``, so the
Postgres container — which needs nothing beyond ``POSTGRES_PASSWORD`` — also
received the wallet private key, the discovery API token, and the redemption
relayer credentials. Anything executing inside the postgres container could
read every trading secret.

Contract pinned here, asserted on FILE CONTENT ONLY. Never render a populated
compose config in these tests: a rendered ``docker compose config`` output
would embed the operator's real secret values into logs and tracebacks.

* the postgres service reads only ``db.env`` (POSTGRES_PASSWORD);
* the bot service keeps ``secrets.env`` and never references ``db.env``;
* ``db.env.example`` ships, declares POSTGRES_PASSWORD and nothing else, and
  carries no private-key/token-shaped keys or real-looking credential value;
* ``secrets.env.example`` no longer declares the database password;
* ``.gitignore`` and ``.dockerignore`` cover both secret files;
* the Dockerfile never COPYs a secret env file explicitly (``COPY . .`` relies
  on ``.dockerignore`` instead);
* the CI docker-smoke job seeds both files before ``docker compose config``.
"""

from __future__ import annotations

import fnmatch
from pathlib import Path
import re

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

SECRET_FILE_BASENAMES = ("secrets.env", "db.env")

FORBIDDEN_SECRET_BASENAMES = ("secrets.env", "db.env", "secrets.toml", "runtime.local")


def dockerfile_copy_add_lines(dockerfile_text: str) -> list[str]:
    """Every COPY/ADD instruction line in the whole Dockerfile (stripped).

    Dockerfile instruction keywords are case-insensitive (``copy`` and
    ``add`` are valid), so the match must be too.
    """
    return [
        line.strip()
        for line in dockerfile_text.splitlines()
        if line.strip().upper().startswith(("COPY ", "ADD "))
    ]


def dockerfile_secret_copy_violations(dockerfile_text: str) -> list[str]:
    """COPY/ADD lines (any stage) that name a forbidden secret basename.

    No explicit COPY/ADD of a secret file may bypass ``.dockerignore``;
    Dockerfile instruction keywords are case-insensitive, so the match is
    too.
    """
    return [
        line
        for line in dockerfile_copy_add_lines(dockerfile_text)
        for forbidden in FORBIDDEN_SECRET_BASENAMES
        if forbidden in line
    ]


@pytest.fixture(scope="module")
def compose_services() -> dict:
    """Parse the checked-in compose file — no interpolation, no rendering."""
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    return compose["services"]


def _env_files(service: dict) -> list[str]:
    env_file = service.get("env_file", [])
    if isinstance(env_file, str):
        return [env_file]
    return list(env_file)


def _ignore_excludes(filename: str, ignore_text: str) -> bool:
    """Mirror gitignore/dockerignore last-match-wins semantics."""
    excluded = False
    for raw in ignore_text.splitlines():
        pattern = raw.strip()
        if not pattern or pattern.startswith("#"):
            continue
        if fnmatch.fnmatchcase(filename, pattern.removeprefix("!")):
            excluded = not pattern.startswith("!")
    return excluded


def test_postgres_service_reads_only_db_env(compose_services: dict) -> None:
    """The database container must never receive the bot's secret file."""
    env_files = _env_files(compose_services["postgres"])
    assert env_files == ["db.env"], (
        "postgres env_file must be exactly ['db.env'] so the database container "
        "never sees the wallet key, discovery token, or relayer credentials"
    )
    # Belt-and-braces: no secrets.env reference anywhere in the service block.
    assert "secrets.env" not in yaml.safe_dump(compose_services["postgres"])


def test_bot_service_keeps_secrets_env_without_db_env(compose_services: dict) -> None:
    """The bot keeps the full secret file and never needs the DB-only one."""
    env_files = _env_files(compose_services["bot"])
    assert "secrets.env" in env_files, "bot service must keep secrets.env"
    assert "db.env" not in yaml.safe_dump(compose_services["bot"]), (
        "db.env is the postgres-only credential file; the bot reads the DSN "
        "from secrets.env and must not also mount the raw DB password file"
    )


def test_db_env_example_holds_only_the_database_password() -> None:
    path = REPO_ROOT / "db.env.example"
    assert path.is_file(), "db.env.example must ship beside secrets.env.example"
    content = path.read_text(encoding="utf-8")
    keys = [
        line.split("=", 1)[0].strip()
        for line in content.splitlines()
        if line.strip() and not line.strip().startswith("#") and "=" in line
    ]
    assert keys == ["POSTGRES_PASSWORD"], (
        "db.env.example must declare POSTGRES_PASSWORD and nothing else; "
        f"found {keys}"
    )
    # No credential-shaped value may ship in the template — no hex private
    # keys, no random-looking blobs. Empty or obviously-synthetic text only.
    assert not re.search(r"0x[0-9a-fA-F]{40,}", content), (
        "db.env.example must not contain a private-key-shaped value"
    )
    assert not re.search(r"\b[0-9a-fA-F]{24,}\b", content), (
        "db.env.example must not contain a real-looking credential value"
    )


def test_secrets_env_example_no_longer_carries_the_database_password() -> None:
    content = (REPO_ROOT / "secrets.env.example").read_text(encoding="utf-8")
    assert not re.search(r"(?m)^POSTGRES_PASSWORD=", content), (
        "POSTGRES_PASSWORD moved to db.env.example; keeping the declaration in "
        "secrets.env.example invites operators to re-share it with postgres"
    )
    # Pin that the split did not gut the bot-side template: the wallet secret
    # and the DSN (which embeds the password for the bot's own connection)
    # stay bot-side.
    assert re.search(r"(?m)^PMCOPY__WALLET__PRIVATE_KEY=", content)
    assert re.search(r"(?m)^PMCOPY__DATABASE__DSN=", content)


@pytest.mark.parametrize("filename", SECRET_FILE_BASENAMES)
def test_dockerignore_excludes_both_secret_files(filename: str) -> None:
    text = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8")
    assert _ignore_excludes(filename, text), (
        f".dockerignore must exclude {filename!r} so it never bakes into "
        "image layer history"
    )


@pytest.mark.parametrize("filename", SECRET_FILE_BASENAMES)
def test_gitignore_covers_both_secret_files(filename: str) -> None:
    text = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert _ignore_excludes(filename, text), (
        f".gitignore must cover {filename!r} so the populated file is never "
        "committed"
    )


def test_dockerfile_never_copies_secret_env_files() -> None:
    """No explicit COPY of a secret file may bypass .dockerignore."""
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert dockerfile_copy_add_lines(dockerfile), "Dockerfile must exist and COPY its sources"
    assert dockerfile_secret_copy_violations(dockerfile) == [], (
        "Dockerfile must not COPY/ADD secret-shaped files"
    )


_SYNTHETIC_DOCKERFILE = """\
FROM python:3.14.6-slim-bookworm AS builder
COPY src ./src
FROM python:3.14.6-slim-bookworm
COPY --from=builder /wheels/*.whl /tmp/wheels/
COPY requirements.runtime.txt .
"""


def test_dockerfile_secret_copy_guard_matches_copy_add_case_insensitively() -> None:
    """Lowercase ``copy``/``add`` instructions must not bypass the guard.

    Dockerfile instruction keywords are case-insensitive; the historical
    collector only matched uppercase ``COPY `` prefixes.
    """
    dockerfile = (
        _SYNTHETIC_DOCKERFILE
        + "COPY secrets.env /secrets.env\n"
        + "add db.env /db.env\n"
        + "copy secrets.toml /secrets.toml\n"
    )
    assert dockerfile_secret_copy_violations(dockerfile) == [
        "COPY secrets.env /secrets.env",
        "add db.env /db.env",
        "copy secrets.toml /secrets.toml",
    ]


def test_dockerfile_secret_copy_guard_clean_dockerfile_stays_clean() -> None:
    assert dockerfile_secret_copy_violations(_SYNTHETIC_DOCKERFILE) == []


def test_ci_smoke_seeds_both_secret_files_before_compose_config() -> None:
    """``docker compose config`` fails on a missing env_file, so CI must
    create both files — and still never print the rendered (populated)
    configuration."""
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    step = workflow.split("- name: docker compose config", maxsplit=1)[1].split(
        "- name: docker build", maxsplit=1
    )[0]
    assert "install -m 600 secrets.env.example secrets.env" in step
    assert "install -m 600 db.env.example db.env" in step
    # The generated database password seeds db.env, not the bot secret file.
    assert 'openssl rand -hex 24)/" db.env' in step
    assert 'openssl rand -hex 24)/" secrets.env' not in step
    # Both files keep the mode-0600 contract.
    assert 'stat -c \'%a\' db.env' in step
    # The rendered configuration is discarded, never logged.
    assert "config >/dev/null" in step
