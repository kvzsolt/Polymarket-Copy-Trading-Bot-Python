"""The backfill completion marker is keyed on the longitudinal config hash.

``mark_backfill_complete`` persists the hash of the config that produced the
backfill, and ``wallet_backfill_state`` keeps one row per wallet. Probing
completion without that hash makes a wallet backfilled under an older config
look complete forever, so a config change silently never re-backfills.
"""

from __future__ import annotations

import contextlib

import pytest

from storage.postgres.discovery_repo import DiscoveryRepo

pytestmark = pytest.mark.asyncio


class _RecordingConnection:
    def __init__(self, result: object) -> None:
        self.result = result
        self.queries: list[tuple[str, tuple]] = []

    async def fetchval(self, query: str, *args):
        self.queries.append((query, args))
        return self.result


class _RecordingPool:
    def __init__(self, result: object = 1) -> None:
        self.connection = _RecordingConnection(result)

    @contextlib.asynccontextmanager
    async def _acquire(self):
        yield self.connection

    def acquire(self):
        return self._acquire()


async def test_has_completed_backfill_filters_on_config_hash() -> None:
    pool = _RecordingPool(result=1)
    repo = DiscoveryRepo(pool)  # type: ignore[arg-type]

    assert await repo.has_completed_backfill("0xABC", "config-hash") is True

    query, args = pool.connection.queries[0]
    normalized = " ".join(query.split()).lower()
    assert "config_hash = $2" in normalized
    assert args == ("0xabc", "config-hash")


async def test_has_completed_backfill_is_false_for_a_different_config_hash() -> None:
    # The SQL returns no row when the stored hash differs; the repo must
    # report "not complete" so the funnel runs the required new backfill.
    pool = _RecordingPool(result=None)
    repo = DiscoveryRepo(pool)  # type: ignore[arg-type]

    assert await repo.has_completed_backfill("0xABC", "new-hash") is False
