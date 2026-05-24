"""Regression: utils.idempotency resolves BOT_STATE_DB on every call.

Pre-fix the module-level ``_DB_PATH`` was resolved at import. Tests or
multi-user runtimes that switched ``BOT_STATE_DB`` afterwards kept
landing in the first-imported file.
"""
from __future__ import annotations

import utils.idempotency as idem


def test_repo_re_resolves_db_path_from_env(tmp_path, monkeypatch):
    db1 = str(tmp_path / "first.db")
    db2 = str(tmp_path / "second.db")

    monkeypatch.setitem(idem.ENV, "BOT_STATE_DB", db1)
    idem.reset_cache_for_tests()
    r1 = idem._repo()
    assert r1.db_path == db1

    monkeypatch.setitem(idem.ENV, "BOT_STATE_DB", db2)
    idem.reset_cache_for_tests()
    r2 = idem._repo()
    assert r2.db_path == db2, (
        f"second call must pick up the new BOT_STATE_DB; got {r2.db_path}"
    )


def test_schema_init_double_checked_locking_is_threadsafe(tmp_path, monkeypatch):
    """Two threads racing on first _repo() must not double-apply migrations."""
    import threading as _t

    db = str(tmp_path / "race.db")
    monkeypatch.setitem(idem.ENV, "BOT_STATE_DB", db)
    idem.reset_cache_for_tests()

    starts = _t.Event()
    errors: list[Exception] = []

    def worker():
        starts.wait()
        try:
            idem._repo()
        except Exception as e:  # pragma: no cover — defensive
            errors.append(e)

    ts = [_t.Thread(target=worker) for _ in range(8)]
    for t in ts:
        t.start()
    starts.set()
    for t in ts:
        t.join(timeout=5)

    assert not errors, f"concurrent _repo() must not raise; got {errors!r}"
    assert db in idem._SCHEMA_READY
