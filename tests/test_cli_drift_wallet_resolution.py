"""Regression: ``pmcopy drift check`` must resolve the live Deposit Wallet.

A live one-secret config legitimately omits ``wallet.funder_address`` and lets
the authenticated SDK derive it from ``private_key``. Pre-fix the CLI never
created a client, so every user still carried an empty funder, was skipped by
the holdings loop, and the command reported ``checked_users: 0`` — which reads
exactly like "no drift" on a wallet that may well be drifting.

Paper is the mirror image: it never authenticates, so nothing resolves an
address and the configured funder (which AppConfig requires in paper) must be
used as-is, with no client built at all.
"""

from __future__ import annotations

import argparse
import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot_cli.commands import drift as drift_mod
from config.env import SignatureType, WalletConfig
from services.multiUserConfig import RuntimeUser, UserFollowConfig

_PRIVATE_KEY = "0x" + "a" * 64
_RESOLVED_WALLET = "0x" + "d" * 40
_PAPER_FUNDER = "0x" + "b" * 40
_LEADER = "0x" + "e" * 40


def _config(*, paper: bool) -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(paper_trade_only=paper),
        leader=SimpleNamespace(wallets=[_LEADER]),
        database=SimpleNamespace(dsn="postgresql://test:test@localhost/test"),
    )


def _runtime_user(*, user_id: str, funder: str) -> RuntimeUser:
    return RuntimeUser(
        user_id=user_id,
        wallet=WalletConfig(
            private_key=_PRIVATE_KEY,
            chain_id=137,
            signature_type=SignatureType.DEPOSIT_WALLET,
            funder_address=funder,
            polygon_rpc_url="https://polygon-rpc.example.com",
        ),
        follows={
            _LEADER: UserFollowConfig(
                user_id=user_id,
                leader_id=_LEADER,
                enabled=True,
                allocation_pct=0.1,
                max_trade_usdc=10.0,
                max_market_exposure_usdc=100.0,
            )
        },
    )


def _client(wallet_address: str | None) -> MagicMock:
    client = MagicMock()
    client.wallet_address = wallet_address
    client.close = AsyncMock()
    return client


class _Harness:
    """Patch every boundary ``_run_check_async`` reaches past the wallet logic.

    ``fetch_live_positions``/``init_pool`` are deferred imports inside the
    command, so they must be patched on their *source* modules — patching an
    attribute on ``drift`` would never be consulted.
    """

    def __init__(self, *, paper: bool, users: list[RuntimeUser], client: MagicMock):
        self.create_client = AsyncMock(return_value=client)
        self.fetch_live = AsyncMock(return_value=[])
        self._stack = ExitStack()
        self._patches = [
            patch.object(drift_mod, "load_config", return_value=_config(paper=paper)),
            patch.object(drift_mod, "load_runtime_users", return_value=users),
            patch.object(
                drift_mod, "_list_persisted_positions", AsyncMock(return_value=[])
            ),
            patch(
                "integrations.polymarket.sdk_client.create_polymarket_client",
                self.create_client,
            ),
            patch(
                "storage.postgres.connection.init_pool",
                AsyncMock(return_value=MagicMock()),
            ),
            patch("storage.postgres.connection.shutdown_pool", AsyncMock()),
            patch(
                "orchestrators.runtime_bootstrap.fetch_live_positions", self.fetch_live
            ),
        ]

    def __enter__(self) -> "_Harness":
        # ExitStack, not start()/stop() pairs: a patch that fails to start
        # would otherwise leave the earlier ones live for the whole session,
        # and these target shared modules like storage.postgres.connection.
        for item in self._patches:
            self._stack.enter_context(item)
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stack.close()


async def _run(capsys, *, user_id: str | None = None) -> tuple[int, dict]:
    rc = await drift_mod._run_check_async(
        SimpleNamespace(user_id=user_id, epsilon="0.000001")
    )
    return rc, json.loads(capsys.readouterr().out)


@pytest.mark.asyncio
async def test_live_omitted_funder_is_resolved_and_checked(capsys) -> None:
    """The whole point: a live entry with no funder must be checked, not skipped."""
    user = _runtime_user(user_id="default", funder="")
    client = _client(_RESOLVED_WALLET)

    with _Harness(paper=False, users=[user], client=client) as h:
        rc, output = await _run(capsys)

    assert rc == 0
    # Pre-fix this was 0: the empty funder skipped the only user.
    assert output["checked_users"] == 1
    assert output["user_ids"] == [_RESOLVED_WALLET]
    assert "skipped_users" not in output

    # The SDK was asked to authenticate, and its resolved wallet was bound back
    # onto the runtime user *and* its follows.
    assert h.create_client.await_args.kwargs["authenticated"] is True
    assert user.wallet.funder_address == _RESOLVED_WALLET
    assert user.user_id == _RESOLVED_WALLET
    assert user.follows[_LEADER].user_id == _RESOLVED_WALLET

    # Holdings were probed against the resolved wallet, never against "".
    h.fetch_live.assert_awaited_once_with(_RESOLVED_WALLET)
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_user_filter_matches_the_bound_identity(capsys) -> None:
    """Binding rewrites a "default" user_id, so --user-id must match after it."""
    user = _runtime_user(user_id="default", funder="")

    with _Harness(paper=False, users=[user], client=_client(_RESOLVED_WALLET)):
        rc, output = await _run(capsys, user_id=_RESOLVED_WALLET)

    assert rc == 0
    assert output["checked_users"] == 1
    assert output["user_ids"] == [_RESOLVED_WALLET]


@pytest.mark.asyncio
async def test_user_filter_rejecting_every_user_reports_the_warning(capsys) -> None:
    """A filter matching nothing is a distinct, named outcome — not a silent 0."""
    user = _runtime_user(user_id="default", funder="")

    with _Harness(paper=False, users=[user], client=_client(_RESOLVED_WALLET)):
        rc, output = await _run(capsys, user_id="0x" + "f" * 40)

    assert rc == 0
    assert output["checked_users"] == 0
    assert output["warning"] == "no_runtime_users_resolved"


@pytest.mark.asyncio
async def test_paper_uses_configured_funder_and_builds_no_client(capsys) -> None:
    """Paper never authenticates: no client, and the configured funder stands."""
    user = _runtime_user(user_id=_PAPER_FUNDER, funder=_PAPER_FUNDER)

    with _Harness(paper=True, users=[user], client=_client(_RESOLVED_WALLET)) as h:
        rc, output = await _run(capsys)

    assert rc == 0
    assert output["checked_users"] == 1
    assert output["user_ids"] == [_PAPER_FUNDER]
    h.create_client.assert_not_awaited()
    h.fetch_live.assert_awaited_once_with(_PAPER_FUNDER)
    assert user.wallet.funder_address == _PAPER_FUNDER


@pytest.mark.asyncio
async def test_client_is_closed_even_when_binding_raises(capsys) -> None:
    """A wallet the SDK could not resolve must fail loudly, not report 0 drift."""
    user = _runtime_user(user_id="default", funder="")
    # Empty string (not None) => a secure client exists but resolved no wallet,
    # which bind_resolved_deposit_wallet rejects.
    client = _client("")

    with _Harness(paper=False, users=[user], client=client):
        rc, output = await _run(capsys)

    assert rc == 1
    assert output["error"] == "wallet_resolution_failed"
    # The transport must not leak just because binding failed.
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_is_closed_when_the_sdk_raises(capsys) -> None:
    """An SDK auth failure is reported redacted, and never as a clean check."""
    user = _runtime_user(user_id="default", funder="")

    with _Harness(paper=False, users=[user], client=_client(_RESOLVED_WALLET)) as h:
        h.create_client.side_effect = RuntimeError("connection refused")
        rc, output = await _run(capsys)

    assert rc == 1
    assert output["error"] == "wallet_resolution_failed"
    # redact_error_detail buckets the raw message rather than echoing it.
    assert output["details"] == "network_error"


@pytest.mark.asyncio
async def test_live_user_with_no_secure_client_is_not_a_clean_check(capsys) -> None:
    """An unauthenticated live user must fail loudly, not report zero drift.

    ``wallet_address`` is None exactly when the SDK built no secure client,
    which past the paper guard means a live entry it could not authenticate.
    Binding unconditionally is what turns that into a named failure instead of
    an empty funder the holdings loop skips into a clean ``checked_users: 0``.
    """
    user = _runtime_user(user_id="default", funder="")
    client = _client(None)

    with _Harness(paper=False, users=[user], client=client) as h:
        rc, output = await _run(capsys)

    assert rc == 1
    assert output["error"] == "wallet_resolution_failed"
    assert "checked_users" not in output
    client.close.assert_awaited_once()
    # An unresolved wallet must never reach the holdings probe.
    h.fetch_live.assert_not_awaited()


@pytest.mark.asyncio
async def test_wallet_less_user_is_never_reported_as_a_clean_check(capsys) -> None:
    """Defence in depth: a skipped user means the check did not run.

    Every valid config binds or rejects a wallet-less user before the holdings
    loop, so this is unreachable in practice. It is pinned anyway because the
    failure it guards is silent: a skip that still exits 0 reads to any script
    as "checked, no drift".
    """
    user = _runtime_user(user_id="paper-user", funder="")

    with _Harness(paper=True, users=[user], client=_client(_RESOLVED_WALLET)) as h:
        rc, output = await _run(capsys)

    assert rc == 1
    assert output["checked_users"] == 0
    assert output["skipped_users"] == ["paper-user"]
    assert output["error"] == "users_without_wallet_skipped"
    h.fetch_live.assert_not_awaited()


def test_real_cli_entrypoint_resolves_the_wallet(capsys) -> None:
    """Drive the command the way ``pmcopy drift check`` actually does.

    Sync on purpose: ``run_drift`` owns its own ``asyncio.run``. Going through
    the real parser proves the subcommand is registered and that its argument
    dests still line up with what ``_run_check_async`` reads — wiring the other
    tests skip by calling ``_run_check_async`` directly.
    """
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    drift_mod.register_drift_command(sub)
    args = parser.parse_args(["drift", "check"])

    user = _runtime_user(user_id="default", funder="")
    with _Harness(paper=False, users=[user], client=_client(_RESOLVED_WALLET)):
        rc = drift_mod.run_drift(args)
    output = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert output["checked_users"] == 1
    assert output["user_ids"] == [_RESOLVED_WALLET]
