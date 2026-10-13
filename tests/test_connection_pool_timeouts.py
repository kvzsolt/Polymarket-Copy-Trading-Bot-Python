"""P2-b + P2-r persistence hardening.

P2-b — ``storage/postgres/connection.py`` ``init_pool`` previously set only
``command_timeout`` (a per-query ceiling). It did NOT bound the pool
``acquire()`` wait, and it set no server-side ``statement_timeout`` /
``idle_in_transaction_session_timeout``. Under pool exhaustion every caller
queues on ``acquire()`` forever (livelock, no error), and a hung transaction
held a backend (and its locks) indefinitely. These tests pin:

  * ``init_pool`` forwards a bounded acquire ``timeout`` to ``create_pool``,
  * ``init_pool`` forwards ``server_settings`` carrying ``statement_timeout``
    and ``idle_in_transaction_session_timeout`` (so a hung query/txn is
    bounded server-side),
  * a real ``acquire()`` on an exhausted pool RAISES (``TimeoutError``) rather
    than hanging forever,
  * the server-side GUCs are actually applied on a live connection.

P2-r — ``orders_repo.persist_execution_atomically`` constructed
``Decimal(fill_size)`` without ``str()``. ``fill_size`` is typed ``str`` so it
is latent today, but ``Decimal(<float>)`` would inject binary-float noise if
the method were ever wired to a float caller. The test feeds a float-ish value
through the atomic-ledger position math against a real Postgres and asserts the
stored size is exact.
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal

import pytest

pytestmark = pytest.mark.asyncio

VALID_TOKEN = "52114319501245915516055106046884209969926127482827954674443846427813813222426"


# ---------------------------------------------------------------------------
# P2-b — unit: init_pool wires bounded acquire timeout + server-side GUCs.
# ---------------------------------------------------------------------------


class _FakePool:
    """Minimal stand-in for an asyncpg.Pool returned by create_pool."""

    async def close(self) -> None:  # pragma: no cover - tidy path
        return None


async def test_init_pool_forwards_acquire_timeout_and_server_settings(monkeypatch):
    """``init_pool`` must hand ``create_pool`` a bounded acquire ``timeout``
    and server-side ``statement_timeout`` / ``idle_in_transaction_session_timeout``."""
    import pmcopy.storage.postgres.connection as conn_mod

    conn_mod._reset_for_tests()
    captured: dict = {}

    async def _fake_create_pool(**kwargs):
        captured.update(kwargs)
        return _FakePool()

    monkeypatch.setattr(conn_mod.asyncpg, "create_pool", _fake_create_pool)

    try:
        pool = await conn_mod.init_pool(
            "postgresql://u:p@localhost:5432/db",
            acquire_timeout_seconds=10.0,
            statement_timeout_seconds=30.0,
            idle_in_transaction_timeout_seconds=60.0,
        )
        assert isinstance(pool, _FakePool)

        # (a) bounded acquire/connect timeout forwarded.
        assert captured.get("timeout") == pytest.approx(10.0)

        # (b) server-side guards forwarded as Postgres GUC strings (milliseconds).
        srv = captured.get("server_settings")
        assert isinstance(srv, dict)
        assert srv.get("statement_timeout") == "30000"
        assert srv.get("idle_in_transaction_session_timeout") == "60000"

        # command_timeout (per-query ceiling) is still set.
        assert captured.get("command_timeout") == pytest.approx(30.0)
    finally:
        conn_mod._reset_for_tests()


async def test_init_pool_defaults_are_sane_and_bounded(monkeypatch):
    """With no overrides the defaults must still bound acquire + server side
    (never ``None``/unbounded)."""
    import pmcopy.storage.postgres.connection as conn_mod

    conn_mod._reset_for_tests()
    captured: dict = {}

    async def _fake_create_pool(**kwargs):
        captured.update(kwargs)
        return _FakePool()

    monkeypatch.setattr(conn_mod.asyncpg, "create_pool", _fake_create_pool)

    try:
        await conn_mod.init_pool("postgresql://u:p@localhost:5432/db")
        assert captured.get("timeout") is not None
        assert float(captured["timeout"]) > 0
        srv = captured.get("server_settings")
        assert isinstance(srv, dict)
        assert int(srv["statement_timeout"]) > 0
        assert int(srv["idle_in_transaction_session_timeout"]) > 0
    finally:
        conn_mod._reset_for_tests()


# ---------------------------------------------------------------------------
# P2-b — real-PG: an exhausted pool times out on acquire instead of hanging.
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _pg_dsn():
    """Spin a Postgres testcontainer and yield its asyncpg DSN. Auto-skips
    when Docker / the image is unavailable."""
    try:
        from tests.integration_postgres import PostgresContainer
    except Exception as exc:  # pragma: no cover - env without docker
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        yield dsn
    finally:
        ctr.stop()


async def test_exhausted_pool_acquire_times_out_not_hangs():
    """The whole point of P2-b: a default acquire timeout means a caller on an
    EXHAUSTED pool fails fast rather than livelocking forever."""
    import pmcopy.storage.postgres.connection as conn_mod

    async with _pg_dsn() as dsn:
        conn_mod._reset_for_tests()
        # max_size=1 so the second acquire has nothing to grab.
        await conn_mod.init_pool(
            dsn,
            min_size=1,
            max_size=1,
            acquire_timeout_seconds=0.5,
            statement_timeout_seconds=30.0,
            idle_in_transaction_timeout_seconds=60.0,
        )
        try:
            pool = await conn_mod.get_pool()
            held = await pool.acquire()  # hold the only connection
            try:
                with pytest.raises((asyncio.TimeoutError, TimeoutError)):
                    # Default (no explicit timeout) acquire must still be
                    # bounded by the pool default we wired in.
                    await pool.acquire()
            finally:
                await pool.release(held)
        finally:
            await conn_mod.shutdown_pool()


async def test_server_side_timeouts_applied_on_live_connection():
    """The GUCs we set must actually land on the backend session."""
    import pmcopy.storage.postgres.connection as conn_mod

    async with _pg_dsn() as dsn:
        conn_mod._reset_for_tests()
        await conn_mod.init_pool(
            dsn,
            min_size=1,
            max_size=2,
            acquire_timeout_seconds=5.0,
            statement_timeout_seconds=30.0,
            idle_in_transaction_timeout_seconds=60.0,
        )
        try:
            pool = await conn_mod.get_pool()
            async with pool.acquire() as conn:
                stmt = await conn.fetchval("SHOW statement_timeout")
                idle = await conn.fetchval("SHOW idle_in_transaction_session_timeout")
            # Postgres renders 30000ms as "30s" and 60000ms as "1min".
            assert stmt == "30s"
            assert idle == "1min"
        finally:
            await conn_mod.shutdown_pool()


# ---------------------------------------------------------------------------
# P2-r — atomic-ledger Decimal(str(...)) defends against float precision loss.
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


async def _position_size(pool, user_id, condition_id, token_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT size_tokens FROM positions "
            "WHERE user_id=$1 AND condition_id=$2 AND token_id=$3",
            user_id,
            condition_id,
            token_id,
        )
    return Decimal(str(row["size_tokens"])) if row else None


async def test_atomic_ledger_decimal_sites_are_str_wrapped():
    """Source-level guard: every ``Decimal(fill_size|fill_price)`` in the atomic
    ledger must be ``Decimal(str(...))``.

    ``persist_execution_atomically`` binds ``fill_size`` to a str-typed column
    parameter upstream of the position math, so a stray ``float`` is rejected at
    the fills INSERT before it can reach ``Decimal(...)`` — which makes a pure
    runtime test unable to exercise the float path. The defensive ``str()``
    wrapping is still required (matches the rest of the file and protects the
    avg-price/clamp math if a float caller is ever wired in), so we pin it here.

    (Declared ``async`` only so pytest-asyncio's ``auto`` mode collects it
    uniformly with the rest of the module; it awaits nothing.)
    """
    import inspect

    from pmcopy.storage.postgres import orders_repo

    src = inspect.getsource(orders_repo.OrdersRepo.persist_execution_atomically)
    import re

    bad = re.findall(r"Decimal\(\s*(fill_size|fill_price)\s*\)", src)
    assert not bad, f"unwrapped Decimal(...) sites remain: {bad}"
    # And the wrapped forms are present.
    assert "Decimal(str(fill_size))" in src
    assert "Decimal(str(fill_price))" in src


async def test_persist_execution_decimal_math_is_exact():
    """End-to-end: two BUY fills of ``"0.1"`` settle to an EXACT ``0.2``.

    Exercises the ``Decimal(str(fill_size))`` / ``Decimal(str(fill_price))``
    avg-price path against a real Postgres. ``fill_size`` is passed as the
    str its signature declares; the assertion is that the decimal math carries
    no binary-float dust (which a ``Decimal(<float>)`` regression would add).
    """
    from pmcopy.storage.postgres.orders_repo import OrdersRepo

    # Latent-bug witness: raw Decimal(float) corrupts; Decimal(str(float)) is clean.
    assert Decimal(0.1) != Decimal("0.1")
    assert Decimal(str(0.1)) == Decimal("0.1")

    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        user, cond, tok = "0xUserChecksum", "0xcond-p2r", VALID_TOKEN

        async def _buy(order_id: str, trade_key: str, ts: int) -> bool:
            return await repo.persist_execution_atomically(
                fill={"order_id": order_id},
                user_id=user,
                leader_id=None,
                condition_id=cond,
                token_id=tok,
                side="BUY",
                fill_size="0.1",  # str per the typed contract
                fill_price="0.5",
                fill_fee="0",
                fill_ts=ts,
                fill_json="{}",
                trade_key=trade_key,
                tx_hash=None,
                asset=None,
                trade_status="processed",
            )

        assert await _buy("ord-p2r-1", "tk-p2r-1", 1000) is True
        assert await _buy("ord-p2r-2", "tk-p2r-2", 1001) is True

        size = await _position_size(pool, user, cond, tok)
        # Exact: 0.1 + 0.1 == 0.2 (no 0.20000000000000001... dust).
        assert size == Decimal("0.2")
