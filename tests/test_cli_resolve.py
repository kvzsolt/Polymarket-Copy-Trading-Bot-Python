"""bot resolve — dry-run evidence and explicit manual terminal resolution.

The command replaces direct DB edits for stuck orders: a read-only evidence
report by default, and an explicit ``--apply`` path that validates operator
evidence, writes the terminal status, releases the linked reservation, records a
``manual_resolution`` audit metadata entry and persists a backup of the affected
rows. It must never mutate the venue.
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal

import pytest

from bot_cli.commands.resolve import ResolveCommand, register_resolve_command

ORDER_ID = "0xROWHASH"


def _order(**overrides) -> dict:
    order = {
        "idempotency_key": "idem-1",
        "user_id": "0xUSER",
        "leader_id": "0xLEADER",
        "order_id": ORDER_ID,
        "status": "ambiguous_timeout",
        "attempt_count": 2,
        "last_error": None,
        "request": {
            "side": "BUY",
            "amount": "4.056",
            "price_limit": "0.3570",
            "condition_id": "cond-1",
            "token_id": "tok-1",
            "signal_id": "sig-1",
            "reservation_id": "res-1",
        },
        "response": {
            "status": "ambiguous_timeout",
            "orderID": ORDER_ID,
            "attempted_order_ids": [ORDER_ID],
            "size_matched": "0",
        },
        "created_at": "2026-09-19T07:26:50+00:00",
        "updated_at": "2026-09-19T07:29:05+00:00",
        "matched_total": "0",
    }
    order.update(overrides)
    return order


def _reservation():
    from types import SimpleNamespace

    return SimpleNamespace(
        id="res-1",
        user_id="0xUSER",
        condition_id="cond-1",
        token_id="tok-1",
        status="active",
        reserved_usdc=Decimal("4.056"),
        reserved_size=Decimal("0"),
        order_id=ORDER_ID,
        venue_order_id=None,
    )


class _OrdersRepo:
    def __init__(self, order=None):
        self.order = order
        self.updates: list[dict] = []
        self.matched: list[dict] = []
        self.processed_marks: list[dict] = []
        self.processed_deletes: list[str] = []

    async def get_by_order_id(self, order_id: str):
        if self.order and self.order.get("order_id") == order_id:
            return self.order
        return None

    async def get_by_idempotency_key(self, key: str):
        if self.order and self.order.get("idempotency_key") == key:
            return self.order
        return None

    async def update_submission_status(self, **kwargs) -> None:
        self.updates.append(kwargs)

    async def upsert_matched_total(self, **kwargs) -> None:
        self.matched.append(kwargs)

    async def mark_processed_trade(self, **kwargs) -> None:
        self.processed_marks.append(kwargs)

    async def delete_processed_trade(self, trade_key: str) -> None:
        self.processed_deletes.append(trade_key)


class _ReservationsRepo:
    def __init__(self, reservation=None):
        self.reservation = reservation
        self.releases: list[tuple] = []
        self.metadata: list[tuple] = []

    async def get_active_by_order_id(self, order_id: str):
        return self.reservation

    async def mark_released(self, reservation_id: str, reason: str, released_at: int) -> None:
        self.releases.append((reservation_id, reason, released_at))

    async def record_manual_resolution(self, reservation_id: str, payload: dict) -> None:
        self.metadata.append((reservation_id, payload))


class _Redriver:
    """Record guarded leader-SELL re-drive delegations without venue I/O."""

    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[dict, str]] = []
        self.releases_seen: list[int] = []
        self.reservations_repo = None
        self._fail = fail

    async def redrive_after_terminal_no_fill(self, record, *, reason: str) -> bool:
        """Capture one delegated re-drive attempt."""
        self.calls.append((record, reason))
        if self.reservations_repo is not None:
            self.releases_seen.append(len(self.reservations_repo.releases))
        if self._fail:
            raise RuntimeError("redrive exploded")
        return True


def _args(tmp_path, **overrides) -> argparse.Namespace:
    values = {
        "order_id": ORDER_ID,
        "idempotency_key": None,
        "user_id": None,
        "apply": False,
        "status": None,
        "size_matched": None,
        "avg_price": None,
        "note": None,
        "backup_dir": str(tmp_path),
        "json": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _command(tmp_path, order=None, reservation=None, redriver=None):
    lines: list[str] = []
    kwargs = {} if redriver is None else {"redriver": redriver}
    cmd = ResolveCommand(
        _OrdersRepo(order),
        _ReservationsRepo(reservation),
        backup_dir=str(tmp_path),
        emit=lines.append,
        **kwargs,
    )
    return cmd, lines


def _payload(lines: list[str]) -> dict:
    assert lines, "command emitted no output"
    return json.loads(lines[-1])


@pytest.mark.asyncio
async def test_dry_run_reports_evidence_without_mutation(tmp_path):
    cmd, lines = _command(tmp_path, _order(), _reservation())

    code = await cmd.run(_args(tmp_path))

    assert code == 0
    payload = _payload(lines)
    assert payload["dry_run"] is True
    assert payload["order"]["order_id"] == ORDER_ID
    assert payload["order"]["status"] == "ambiguous_timeout"
    assert payload["order"]["attempted_order_ids"] == [ORDER_ID]
    assert payload["reservation"]["id"] == "res-1"
    assert payload["resolvable"] is True
    assert "--apply --status filled" in payload["suggested_command"]
    assert cmd._orders_repo.updates == []
    assert cmd._reservations_repo.releases == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_apply_filled_writes_terminal_state_backup_and_release(tmp_path):
    cmd, lines = _command(tmp_path, _order(), _reservation())

    code = await cmd.run(
        _args(
            tmp_path,
            apply=True,
            status="filled",
            size_matched="11.58",
            avg_price="0.35",
            note="data-api activity confirmed the fill",
        )
    )

    assert code == 0
    payload = _payload(lines)
    assert payload["applied"] is True
    assert payload["status"] == "filled_reconciled"

    update = cmd._orders_repo.updates[0]
    assert update["order_id"] == ORDER_ID
    assert update["status"] == "filled_reconciled"
    patch = update["response_patch"]
    assert patch["size_matched"] == "11.58"
    assert patch["manual_resolution"]["note"] == "data-api activity confirmed the fill"
    assert patch["manual_resolution"]["prior_status"] == "ambiguous_timeout"
    assert patch["manual_resolution"]["operator"] == "bot_cli"

    assert cmd._orders_repo.matched == [{"order_id": ORDER_ID, "matched": "11.58"}]

    reservation_id, reason, released_at = cmd._reservations_repo.releases[0]
    assert reservation_id == "res-1"
    assert reason == "manual_resolution:filled"
    assert isinstance(released_at, int)

    metadata_id, metadata_payload = cmd._reservations_repo.metadata[0]
    assert metadata_id == "res-1"
    assert metadata_payload["status"] == "filled_reconciled"
    assert metadata_payload["note"] == "data-api activity confirmed the fill"

    assert cmd._orders_repo.processed_marks[0]["trade_key"] == "sig-1"
    assert cmd._orders_repo.processed_marks[0]["status"] == "filled_reconciled"

    backups = list(tmp_path.glob("manual-resolution-backup-*.json"))
    assert len(backups) == 1
    backup = json.loads(backups[0].read_text())
    assert backup["orders"][0]["order_id"] == ORDER_ID
    assert backup["reservations"][0]["id"] == "res-1"


@pytest.mark.asyncio
async def test_apply_canceled_releases_without_fill_and_unlocks_retry(tmp_path):
    cmd, lines = _command(tmp_path, _order(), _reservation())

    code = await cmd.run(
        _args(tmp_path, apply=True, status="canceled", note="venue confirmed no fill")
    )

    assert code == 0
    payload = _payload(lines)
    assert payload["status"] == "manually_canceled"
    assert cmd._orders_repo.updates[0]["status"] == "manually_canceled"
    assert cmd._orders_repo.matched == []
    assert cmd._orders_repo.processed_deletes == ["sig-1"]
    assert cmd._reservations_repo.releases[0][1] == "manual_resolution:canceled"


@pytest.mark.asyncio
async def test_no_fill_resolution_delegates_guarded_redrive(tmp_path):
    """An applied no-fill resolution re-drives the newer leader SELL target once."""
    redriver = _Redriver()
    cmd, lines = _command(tmp_path, _order(), _reservation(), redriver=redriver)
    redriver.reservations_repo = cmd._reservations_repo

    code = await cmd.run(
        _args(tmp_path, apply=True, status="canceled", note="venue confirmed no fill")
    )

    assert code == 0
    payload = _payload(lines)
    assert payload["applied"] is True
    assert payload["reservation_released"] is True
    assert payload["redrive"] == "delegated"

    assert len(redriver.calls) == 1
    record, reason = redriver.calls[0]
    assert reason == "manual_resolution:canceled"
    assert record["order_id"] == ORDER_ID
    assert record["request"]["reservation_id"] == "res-1"
    # The re-drive may only observe an already-recorded reservation release.
    assert redriver.releases_seen == [1]


@pytest.mark.asyncio
async def test_filled_resolution_never_triggers_redrive(tmp_path):
    """A confirmed fill frees no shares, so it must never re-drive a target."""
    redriver = _Redriver()
    cmd, lines = _command(tmp_path, _order(), _reservation(), redriver=redriver)

    code = await cmd.run(
        _args(
            tmp_path,
            apply=True,
            status="filled",
            size_matched="11.58",
            avg_price="0.35",
            note="data-api activity confirmed the fill",
        )
    )

    assert code == 0
    assert redriver.calls == []
    assert _payload(lines)["redrive"] == "not_applicable"


@pytest.mark.asyncio
async def test_redrive_failure_never_fails_the_resolution(tmp_path):
    """A failing re-drive is reported, never rolled back onto the resolution."""
    redriver = _Redriver(fail=True)
    cmd, lines = _command(tmp_path, _order(), _reservation(), redriver=redriver)

    code = await cmd.run(
        _args(tmp_path, apply=True, status="expired", note="venue expired the order")
    )

    assert code == 0
    payload = _payload(lines)
    assert payload["applied"] is True
    assert payload["reservation_released"] is True
    assert payload["status"] == "manually_expired"
    assert payload["redrive"] == "failed"
    assert len(redriver.calls) == 1
    assert cmd._orders_repo.updates[0]["status"] == "manually_expired"
    assert cmd._reservations_repo.releases[0][1] == "manual_resolution:expired"


@pytest.mark.asyncio
async def test_default_construction_stays_venue_free(tmp_path):
    """Without an injected re-driver the command behaves exactly as before."""
    cmd, lines = _command(tmp_path, _order(), _reservation())

    code = await cmd.run(
        _args(tmp_path, apply=True, status="confirmed_absent", note="venue has no such order")
    )

    assert code == 0
    payload = _payload(lines)
    assert payload["applied"] is True
    assert payload["status"] == "manually_confirmed_absent"
    assert payload["reservation_released"] is True
    assert payload["redrive"] == "skipped"
    assert cmd._redriver is None


@pytest.mark.asyncio
async def test_apply_refuses_terminal_order(tmp_path):
    cmd, lines = _command(tmp_path, _order(status="filled_reconciled"), _reservation())

    code = await cmd.run(
        _args(tmp_path, apply=True, status="filled", size_matched="1", avg_price="0.5", note="x")
    )

    assert code == 1
    payload = _payload(lines)
    assert payload["error"] == "order_status_terminal"
    assert cmd._orders_repo.updates == []
    assert cmd._reservations_repo.releases == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"size_matched": None, "avg_price": "0.35"},
        {"size_matched": "11.58", "avg_price": None},
        {"size_matched": "11.58", "avg_price": "1"},
        {"size_matched": "0", "avg_price": "0.35"},
    ],
)
async def test_apply_filled_requires_valid_size_and_price(tmp_path, overrides):
    cmd, lines = _command(tmp_path, _order(), _reservation())
    values = {"apply": True, "status": "filled", "note": "x", **overrides}

    code = await cmd.run(_args(tmp_path, **values))

    assert code == 1
    assert "error" in _payload(lines)
    assert cmd._orders_repo.updates == []


@pytest.mark.asyncio
async def test_apply_requires_note(tmp_path):
    cmd, lines = _command(tmp_path, _order(), _reservation())

    code = await cmd.run(
        _args(tmp_path, apply=True, status="canceled", note=None)
    )

    assert code == 1
    assert _payload(lines)["error"] == "note_required"
    assert cmd._orders_repo.updates == []


@pytest.mark.asyncio
async def test_apply_refuses_order_without_order_id(tmp_path):
    cmd, lines = _command(tmp_path, _order(order_id=None), _reservation())

    code = await cmd.run(
        _args(tmp_path, order_id=None, apply=True, status="canceled", note="x")
    )

    assert code == 1
    assert _payload(lines)["error"] == "order_not_found"
    assert cmd._orders_repo.updates == []


def test_register_resolve_command_parses_apply_flags():
    parser = argparse.ArgumentParser()
    register_resolve_command(parser.add_subparsers(dest="command", required=True))

    args = parser.parse_args(
        [
            "resolve",
            "--order-id",
            ORDER_ID,
            "--apply",
            "--status",
            "filled",
            "--size-matched",
            "11.58",
            "--avg-price",
            "0.35",
            "--note",
            "confirmed",
        ]
    )

    assert args.order_id == ORDER_ID
    assert args.apply is True
    assert args.status == "filled"


# ----------------------------------------------------------------------
# Composition wiring — run_resolve / _compose_redriver
# ----------------------------------------------------------------------


class _RuntimeUser:
    """Minimal follower runtime identity for the composition seams."""

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.follows: dict = {}


class _VenueRuntime:
    """Record whether a composed venue runtime was closed."""

    def __init__(self) -> None:
        self.gateway = object()
        self.client = object()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _patch_composition(
    monkeypatch,
    *,
    users,
    venue_runtime=None,
    user_state_builder=None,
    venue_builder_error: Exception | None = None,
) -> None:
    """Patch the production composition helpers used by ``_compose_redriver``."""
    from types import SimpleNamespace

    import orchestrators.runtime_bootstrap as bootstrap
    import services.multiUserConfig as multi_user

    monkeypatch.setattr(bootstrap, "configured_leaders", lambda config: ["0xleader"])
    monkeypatch.setattr(multi_user, "load_runtime_users", lambda leaders, config: list(users))
    monkeypatch.setattr(bootstrap, "ensure_runtime_follows", lambda *a, **k: None)

    async def _build_venue(user, config, **kwargs):
        if venue_builder_error is not None:
            raise venue_builder_error
        return venue_runtime

    monkeypatch.setattr(bootstrap, "build_user_venue_runtime", _build_venue)
    if user_state_builder is None:

        def user_state_builder(*a, **k):
            return SimpleNamespace(execution_coordinator=None)

    monkeypatch.setattr(bootstrap, "build_user_state", user_state_builder)


@pytest.mark.asyncio
async def test_compose_redriver_binds_only_the_owning_runtime_user(monkeypatch, tmp_path):
    """The re-driver is bound to the follower wallet that owns the order."""
    from bot_cli.commands.resolve import _compose_redriver

    sentinel = object()
    venue = _VenueRuntime()
    captured: dict = {}

    def _build_state(user, client, pool, config, **kwargs):
        captured["user"] = user
        captured["clob_gateway"] = kwargs.get("clob_gateway")
        from types import SimpleNamespace

        return SimpleNamespace(
            execution_coordinator=SimpleNamespace(leader_sell_target_service=sentinel)
        )

    _patch_composition(
        monkeypatch,
        users=[_RuntimeUser("0xOTHER"), _RuntimeUser("0xUSER")],
        venue_runtime=venue,
        user_state_builder=_build_state,
    )

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(_order()),
    )

    assert redriver is sentinel
    assert reason is None
    assert returned is venue
    assert captured["user"].user_id == "0xUSER"
    assert captured["clob_gateway"] is venue.gateway


@pytest.mark.asyncio
async def test_compose_redriver_skips_when_no_runtime_user_owns_the_order(
    monkeypatch, tmp_path
):
    """A foreign order must never borrow another wallet's venue runtime."""
    from bot_cli.commands.resolve import _compose_redriver

    _patch_composition(monkeypatch, users=[_RuntimeUser("0xOTHER")], venue_runtime=_VenueRuntime())

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(_order()),
    )

    assert redriver is None
    assert reason == "no_runtime_user_for_order"
    assert returned is None


@pytest.mark.asyncio
async def test_compose_redriver_skips_ambiguous_runtime_user(monkeypatch, tmp_path):
    """Duplicate runtime users for one wallet must not pick a venue arbitrarily."""
    from bot_cli.commands.resolve import _compose_redriver

    _patch_composition(
        monkeypatch,
        users=[_RuntimeUser("0xUSER"), _RuntimeUser("0xUSER")],
        venue_runtime=_VenueRuntime(),
    )

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(_order()),
    )

    assert redriver is None
    assert reason == "ambiguous_runtime_user"
    assert returned is None


@pytest.mark.asyncio
async def test_compose_redriver_skips_unknown_order_user(monkeypatch, tmp_path):
    """An order with no follower identity cannot be bound to a venue runtime."""
    from bot_cli.commands.resolve import _compose_redriver

    _patch_composition(monkeypatch, users=[_RuntimeUser("0xUSER")], venue_runtime=_VenueRuntime())

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(_order(user_id=None)),
    )

    assert redriver is None
    assert reason == "order_user_unknown"
    assert returned is None


@pytest.mark.asyncio
async def test_compose_redriver_skips_missing_order_row(monkeypatch, tmp_path):
    """A vanished order row reports a missing row instead of composing a venue."""
    from bot_cli.commands.resolve import _compose_redriver

    _patch_composition(monkeypatch, users=[_RuntimeUser("0xUSER")], venue_runtime=_VenueRuntime())

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(None),
    )

    assert redriver is None
    assert reason == "order_not_found"
    assert returned is None


@pytest.mark.asyncio
async def test_compose_redriver_fails_closed_on_composition_error(monkeypatch, tmp_path):
    """A broken venue composition downgrades the delegation, never the resolution."""
    from bot_cli.commands.resolve import _compose_redriver

    _patch_composition(
        monkeypatch,
        users=[_RuntimeUser("0xUSER")],
        venue_builder_error=RuntimeError("venue unavailable"),
    )

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(_order()),
    )

    assert redriver is None
    assert reason == "redriver_unavailable:RuntimeError"
    assert returned is None


@pytest.mark.asyncio
async def test_compose_redriver_reports_missing_execution_coordinator(monkeypatch, tmp_path):
    """A state without a target service skips the re-drive but still frees the venue."""
    from bot_cli.commands.resolve import _compose_redriver

    from types import SimpleNamespace

    venue = _VenueRuntime()
    _patch_composition(
        monkeypatch,
        users=[_RuntimeUser("0xUSER")],
        venue_runtime=venue,
        user_state_builder=lambda *a, **k: SimpleNamespace(execution_coordinator=None),
    )

    redriver, reason, returned = await _compose_redriver(
        pool=object(),
        config=object(),
        args=_args(tmp_path),
        orders_repo=_OrdersRepo(_order()),
    )

    assert redriver is None
    assert reason == "execution_coordinator_unavailable"
    assert returned is venue


def _patch_run_resolve(monkeypatch, compose):
    """Patch config, pool and repositories so ``run_resolve`` can run offline."""
    from types import SimpleNamespace

    import bot_cli.commands.resolve as resolve_module
    import storage.postgres.connection as connection
    import storage.postgres.orders_repo as orders_module
    import storage.postgres.reservations_repo as reservations_module

    monkeypatch.setattr(
        resolve_module,
        "load_config",
        lambda: SimpleNamespace(database=SimpleNamespace(dsn="postgres://offline")),
    )

    async def _init_pool(*args, **kwargs):
        return object()

    async def _shutdown_pool() -> None:
        return None

    monkeypatch.setattr(connection, "init_pool", _init_pool)
    monkeypatch.setattr(connection, "shutdown_pool", _shutdown_pool)
    monkeypatch.setattr(orders_module, "OrdersRepo", lambda pool: _OrdersRepo(_order()))
    monkeypatch.setattr(
        reservations_module,
        "ReservationsRepo",
        lambda pool: _ReservationsRepo(_reservation()),
    )
    monkeypatch.setattr(resolve_module, "_compose_redriver", compose)


def test_run_resolve_dry_run_never_composes_a_venue_runtime(monkeypatch, tmp_path, capsys):
    """A read-only report must never build an authenticated venue runtime."""
    import bot_cli.commands.resolve as resolve_module

    composed: list = []

    async def _compose(**kwargs):
        composed.append(kwargs)
        raise AssertionError("dry run must not compose a venue runtime")

    _patch_run_resolve(monkeypatch, _compose)

    code = resolve_module.run_resolve(_args(tmp_path))

    assert code == 0
    assert composed == []
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True


def test_run_resolve_apply_closes_the_venue_runtime(monkeypatch, tmp_path, capsys):
    """An applied resolution reuses and then closes the composed venue runtime."""
    import bot_cli.commands.resolve as resolve_module

    venue = _VenueRuntime()
    redriver = _Redriver()
    calls: list = []

    async def _compose(**kwargs):
        calls.append(kwargs)
        return redriver, None, venue

    _patch_run_resolve(monkeypatch, _compose)

    code = resolve_module.run_resolve(
        _args(tmp_path, apply=True, status="canceled", note="venue confirmed no fill")
    )

    assert code == 0
    assert len(calls) == 1
    assert venue.closed is True
    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True
    assert payload["redrive"] == "delegated"
    assert [reason for _, reason in redriver.calls] == ["manual_resolution:canceled"]
