"""Doctor output redaction: the DB DSN password must never be printed."""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        (
            "postgresql://bot:s3cr3t@127.0.0.1:5432/pmcopy",
            "postgresql://bot:***@127.0.0.1:5432/pmcopy",
        ),
        (
            "postgresql://bot@127.0.0.1:5432/pmcopy",
            "postgresql://bot@127.0.0.1:5432/pmcopy",
        ),
        ("", "<database.dsn is empty>"),
    ],
)
def test_doctor_redacts_dsn_password(dsn: str, expected: str) -> None:
    """Doctor must never print the Postgres password (§0.8)."""
    from pmcopy.bot_cli.commands.doctor import _redact_dsn

    assert _redact_dsn(dsn) == expected
