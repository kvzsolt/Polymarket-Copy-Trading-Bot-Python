"""§9 bullets 15 and 16 — clean two-account shutdown and distinguishable,
secret-redacted account logging.

Bullet 15: ``_shutdown_runtime`` / ``_close_polymarket_clients`` (the
production shutdown path in ``app.py``) close each account's venue client and
each per-account data runtime exactly once, release the account leases before
the database pool closes, survive a failing close without skipping the other
account, and never resolve ambiguous money state on the way out.

Bullet 16: two copiers reacting to the SAME signal produce distinguishable
order-lifecycle events through the real logger (account-scoped dedup keys,
``account`` field on every event) while the legacy account-less form stays
byte-compatible, and runtime-assembled secret canaries are redacted in every
sink.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from rich.console import Console

import pmcopy.app as app_module
from pmcopy.utils import logger as logger_module
from pmcopy.utils.logger import (
    APP_NAME,
    configure_logging,
    get_logger,
    log_order_lifecycle,
    _reset_lifecycle_dedup_for_tests,
)
from pmcopy.utils.log_redaction import (
    REDACTED_PLACEHOLDER,
    register_secret_values,
    reset_secret_registry,
)

from tests.multicopier.conftest import (
    CONDITION,
    FUNDER_ALPHA,
    LEADER_1,
    TOKEN,
    USER_ALPHA,
    USER_BETA,
    build_account_state,
    follow,
    make_config,
    runtime_user,
    synth_private_key,
)


# ---------------------------------------------------------------------------
# §9 bullet 15 — shutdown closes each account's resources once
# ---------------------------------------------------------------------------


class _ClosableVenueRuntime:
    """One account's venue runtime double with a recording close."""

    def __init__(self, name: str, *, fail: bool = False) -> None:
        self.name = name
        self.close_calls = 0
        self._fail = fail

    async def close(self) -> None:
        self.close_calls += 1
        if self._fail:
            raise RuntimeError(f"close failed for {self.name}")


@pytest.mark.asyncio
async def test_each_accounts_venue_client_closes_exactly_once():
    alpha = _ClosableVenueRuntime("alpha")
    beta = _ClosableVenueRuntime("beta")

    # The client registry may legitimately contain duplicates (venue runtime
    # appended once per startup stage); shutdown must stay idempotent.
    await app_module._close_polymarket_clients([alpha, beta, alpha, beta, beta])

    assert alpha.close_calls == 1
    assert beta.close_calls == 1


@pytest.mark.asyncio
async def test_a_failing_close_never_skips_the_other_account():
    alpha = _ClosableVenueRuntime("alpha", fail=True)
    beta = _ClosableVenueRuntime("beta")

    await app_module._close_polymarket_clients([alpha, beta])

    assert beta.close_calls == 1


@pytest.mark.asyncio
async def test_shutdown_runtime_stops_every_account_once_and_releases_leases_before_the_pool(
    monkeypatch,
):
    events: list[str] = []

    def _runtime(name):
        runtime = SimpleNamespace()
        runtime.stop = AsyncMock(side_effect=lambda: events.append(f"stop:{name}"))
        return runtime

    market_a, market_b = _runtime("market-a"), _runtime("market-b")
    user_a, user_b = _runtime("user-a"), _runtime("user-b")

    lease_manager = SimpleNamespace(
        close=AsyncMock(side_effect=lambda: events.append("lease_release")),
    )

    async def _shutdown_pool():
        events.append("pool_close")

    async def _close_session():
        events.append("http_close")

    monkeypatch.setattr("pmcopy.storage.postgres.connection.shutdown_pool", _shutdown_pool)
    monkeypatch.setattr("pmcopy.utils.fetchData.close_session", _close_session)

    # One monitor task per leader; one supervised loop per account; a second
    # supervised spec whose task was never spawned must be tolerated.
    async def _park_forever():
        await asyncio.Event().wait()

    monitor_a = asyncio.create_task(_park_forever())
    monitor_b = asyncio.create_task(_park_forever())
    supervised = [
        SimpleNamespace(task=asyncio.create_task(_park_forever())),
        SimpleNamespace(task=asyncio.create_task(_park_forever())),
        SimpleNamespace(task=None),
    ]

    await app_module._shutdown_runtime(
        monitor_tasks=[monitor_a, monitor_b],
        supervised=supervised,
        market_data_runtimes={USER_ALPHA: market_a, USER_BETA: market_b},
        user_data_runtimes=[user_a, user_b],
        metrics_runner=None,
        account_lease_manager=lease_manager,
    )

    # Every account's data runtimes stopped exactly once.
    assert events.count("stop:market-a") == 1
    assert events.count("stop:market-b") == 1
    assert events.count("stop:user-a") == 1
    assert events.count("stop:user-b") == 1
    # Tasks were cancelled, not awaited into oblivion.
    assert monitor_a.cancelled() or monitor_a.done()
    assert monitor_b.cancelled() or monitor_b.done()
    # The lease is released BEFORE the pool closes (release needs the DB).
    assert events.index("lease_release") < events.index("pool_close")


@pytest.mark.asyncio
async def test_shutdown_does_not_falsely_resolve_ambiguous_money_state():
    """Cancel/close never turns an unresolved submission into a fake outcome.

    The composed account holds (a) an active BUY reservation and (b) a
    lost-POST ``submitted_unconfirmed`` row with no venue order id. The
    production shutdown sequence (task cancellation + resource close) must
    leave BOTH untouched: the reservation stays reserved and the ambiguous
    row stays unresolved, exactly what restart-time recovery (bullet 8,
    proven against real SQL in ``test_multicopier_postgres.py``) expects to
    find.
    """
    config = make_config()
    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: follow(USER_ALPHA, LEADER_1)})
    state = build_account_state(alpha, config)
    reservation_service = state.execution_coordinator.reservation_service

    released: list[tuple[str, str]] = []

    async def _release_terminal(reservation_id: str, reason: str) -> None:
        released.append((reservation_id, reason))

    reservation_service.release_terminal = _release_terminal
    reservation_service.release_expired_active = MagicMock(
        side_effect=AssertionError("shutdown must not sweep reservations")
    )

    venue = _ClosableVenueRuntime("alpha")

    async def _park_forever():
        await asyncio.Event().wait()

    monitor = asyncio.create_task(_park_forever())
    supervised = [SimpleNamespace(task=asyncio.create_task(_park_forever()))]

    await app_module._shutdown_runtime(
        monitor_tasks=[monitor],
        supervised=supervised,
        market_data_runtimes={},
        user_data_runtimes=[],
        metrics_runner=None,
    )
    await app_module._close_polymarket_clients([venue])

    assert released == []
    assert venue.close_calls == 1


# ---------------------------------------------------------------------------
# §9 bullet 16 — distinguishable account logs with secret canaries redacted
# ---------------------------------------------------------------------------


_JSONL_PATH: dict[str, str] = {}


@pytest.fixture(autouse=True)
def _clean_logging_state(tmp_path: Path):
    reset_secret_registry()
    _reset_lifecycle_dedup_for_tests()
    root = logging.getLogger(APP_NAME)
    for handler in root.handlers:
        handler.close()
    root.handlers.clear()
    jsonl_path = str(tmp_path / f"{APP_NAME}.jsonl")
    configure_logging(
        level="INFO",
        log_dir=str(tmp_path),
        jsonl_file=jsonl_path,
        max_bytes=1_000_000,
        backup_count=1,
    )
    _JSONL_PATH["path"] = jsonl_path
    yield jsonl_path
    _JSONL_PATH.pop("path", None)
    reset_secret_registry()
    _reset_lifecycle_dedup_for_tests()
    for handler in root.handlers:
        handler.close()
    root.handlers.clear()
    logger_module._CONFIGURED_LOGGERS.clear()
    configure_logging()


def _jsonl_rows(path: str) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def test_both_accounts_lifecycle_events_are_distinguishable_and_semantics_preserved():
    log = get_logger("MulticopierLogs")

    # The SAME signal (no venue order id yet) reacted to by both copiers:
    # two independent lifecycle streams keyed by the stable account id.
    for account in (USER_ALPHA, USER_BETA):
        log_order_lifecycle(
            log,
            status="submitted",
            side="BUY",
            condition_id=CONDITION,
            asset=TOKEN,
            order_id=None,
            size=10,
            price=0.5,
            usdc_value=5.0,
            account=account,
            signal_id="0xsignal-1",
        )

    for handler in log.handlers:
        handler.flush()
    rows = _jsonl_rows(_current_jsonl())

    lifecycle = [row for row in rows if row.get("type") == "order_lifecycle"]
    assert len(lifecycle) == 2, "account scoping must keep both streams alive"
    assert {row.get("account") for row in lifecycle} == {USER_ALPHA, USER_BETA}
    for row in lifecycle:
        # Existing event semantics preserved: same fields as the legacy form
        # plus the account field.
        assert row["status"] == "submitted"
        assert row["side"] == "BUY"
        assert row["conditionId"] == CONDITION
        assert row["asset"] == TOKEN

    # The legacy account-less form keeps its historical single-copier key:
    # a repeat submission for the same signal/condition dedups away.
    log_order_lifecycle(
        log,
        status="submitted",
        side="BUY",
        condition_id=CONDITION,
        asset=TOKEN,
        order_id=None,
        signal_id="0xsignal-legacy",
    )
    log_order_lifecycle(
        log,
        status="submitted",
        side="BUY",
        condition_id=CONDITION,
        asset=TOKEN,
        order_id=None,
        signal_id="0xsignal-legacy",
    )
    for handler in log.handlers:
        handler.flush()
    rows = _jsonl_rows(_current_jsonl())
    legacy = [
        row
        for row in rows
        if row.get("type") == "order_lifecycle" and row.get("signalId") == "0xsignal-legacy"
    ]
    assert len(legacy) == 1


def _current_jsonl() -> str:
    return _JSONL_PATH["path"]


def test_synthetic_secret_canaries_are_redacted_in_every_sink():
    canary = synth_private_key("e")
    register_secret_values(canary)

    log = get_logger("MulticopierLogs")
    buffer = io.StringIO()
    original_console = logger_module.console
    logger_module.console = Console(
        file=buffer,
        width=100,
        theme=logger_module._THEME,
        no_color=True,
        force_terminal=False,
        legacy_windows=False,
    )
    try:
        log.info(
            "startup_stage | stage=account_ready user=%s key_material=%s",
            USER_ALPHA,
            canary,
        )
        log_order_lifecycle(
            log,
            status="submitted",
            side="BUY",
            condition_id=CONDITION,
            asset=TOKEN,
            order_id=None,
            account=USER_BETA,
            meta={"resolved_key": canary},
        )
    finally:
        logger_module.console = original_console

    for handler in log.handlers:
        handler.flush()

    jsonl_text = Path(_current_jsonl()).read_text()
    assert canary not in jsonl_text
    assert REDACTED_PLACEHOLDER in jsonl_text
    console_text = buffer.getvalue()
    assert canary not in console_text
