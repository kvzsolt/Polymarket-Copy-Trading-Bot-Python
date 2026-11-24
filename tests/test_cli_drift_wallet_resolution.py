"""CCXT drift-command Deposit Wallet identity contracts."""

from __future__ import annotations

import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pmcopy.bot_cli.commands import drift as drift_mod
from pmcopy.config.env import SignatureType, WalletConfig
from pmcopy.services.accounts.multi_user_config import RuntimeUser, UserFollowConfig

_PRIVATE_KEY = "0x" + "a" * 64
_FUNDER = "0x" + "d" * 40
_LEADER = "0x" + "e" * 40


def _config(*, paper: bool) -> SimpleNamespace:
    """Return the narrow settings surface used by the command."""

    return SimpleNamespace(
        execution=SimpleNamespace(paper_trade_only=paper),
        copy_trading=SimpleNamespace(enabled_wallets=lambda: (_LEADER,)),
        database=SimpleNamespace(dsn="postgresql://test:test@localhost/test"),
    )


def _runtime_user(*, user_id: str, funder: str) -> RuntimeUser:
    """Return one deterministic runtime-user fixture."""

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


class _Harness:
    """Patch deterministic I/O boundaries reached after wallet validation."""

    def __init__(self, *, paper: bool, users: list[RuntimeUser]) -> None:
        self.fetch_live = AsyncMock(return_value=[])
        self._stack = ExitStack()
        self._patches = (
            patch.object(drift_mod, "load_config", return_value=_config(paper=paper)),
            patch.object(drift_mod, "load_runtime_users", return_value=users),
            patch.object(drift_mod, "_list_persisted_positions", AsyncMock(return_value=[])),
            patch("pmcopy.storage.postgres.connection.init_pool", AsyncMock(return_value=MagicMock())),
            patch("pmcopy.storage.postgres.connection.shutdown_pool", AsyncMock()),
            patch("pmcopy.orchestrators.bootstrap.runtime_bootstrap.fetch_live_positions", self.fetch_live),
        )

    def __enter__(self) -> _Harness:
        """Activate every patch and return the harness."""

        for item in self._patches:
            self._stack.enter_context(item)
        return self

    def __exit__(self, *_exc: object) -> None:
        """Restore all patched boundaries."""

        self._stack.close()


async def _run(capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, object]]:
    """Run the command and decode its structured output."""

    code = await drift_mod._run_check_async(SimpleNamespace(user_id=None, epsilon="0.000001"))
    return code, json.loads(capsys.readouterr().out)


@pytest.mark.asyncio
async def test_live_missing_explicit_funder_fails_closed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Never infer a signature-type-3 funder from the EOA private key."""

    user = _runtime_user(user_id="default", funder="")
    with _Harness(paper=False, users=[user]) as harness:
        code, output = await _run(capsys)

    assert code == 1
    assert output["error"] == "wallet_resolution_failed"
    harness.fetch_live.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_configured_funder_is_bound_and_checked(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Use the configured Deposit Wallet as the sole holdings identity."""

    user = _runtime_user(user_id="default", funder=_FUNDER)
    with _Harness(paper=False, users=[user]) as harness:
        code, output = await _run(capsys)

    assert code == 0
    assert output["checked_users"] == 1
    assert output["user_ids"] == [_FUNDER]
    harness.fetch_live.assert_awaited_once_with(_FUNDER)
    assert user.user_id == _FUNDER
    assert user.follows[_LEADER].user_id == _FUNDER


@pytest.mark.asyncio
async def test_paper_uses_configured_funder_without_authentication(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Keep the paper holdings probe public and explicitly addressed."""

    user = _runtime_user(user_id=_FUNDER, funder=_FUNDER)
    with _Harness(paper=True, users=[user]) as harness:
        code, output = await _run(capsys)

    assert code == 0
    assert output["user_ids"] == [_FUNDER]
    harness.fetch_live.assert_awaited_once_with(_FUNDER)
