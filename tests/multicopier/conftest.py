"""Shared deterministic harness for the multi-copier acceptance matrix.

Everything here is built from production composition seams:

* ``build_account_state`` composes one copier account runtime through the real
  ``orchestrators.runtime_bootstrap.build_user_state`` (execution service,
  execution coordinator, reservation service, account-owned position
  coordinator, ambiguous recovery, market data service) with a fake venue
  gateway and a non-SQL pool double — the same wiring posture as
  ``tests/test_account_owned_position_coordinator.py``.
* ``build_two_copier_world`` indexes per-leader trading contexts through the
  real ``build_context_maps``.

All wallet addresses / private keys are synthetic values assembled at runtime;
no credential-shaped literal appears in this package.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import pmcopy.config.app_config as app_config_mod
from pmcopy.config.env import WalletConfig
from pmcopy.orchestrators.runtime_bootstrap import build_user_state
from pmcopy.services.multiUserConfig import RuntimeUser, UserFollowConfig
from tests.config_helpers import load_test_config

# ---------------------------------------------------------------------------
# Synthetic identities (runtime-assembled, never credential literals)
# ---------------------------------------------------------------------------


def synth_address(char: str) -> str:
    """Return a synthetic 0x + 40-hex address built from one repeated char."""
    return "0x" + (char * 40)


def synth_private_key(char: str) -> str:
    """Return a synthetic 0x + 64-hex key built from one repeated char."""
    return "0x" + (char * 64)


LEADER_1 = synth_address("1")
LEADER_2 = synth_address("2")
FUNDER_ALPHA = synth_address("a")
FUNDER_BETA = synth_address("b")
USER_ALPHA = "copier-alpha"
USER_BETA = "copier-beta"

CONDITION = "0x" + "ab" * 32
TOKEN = str(71321045679252212594626385532706912750332728571942532223535764789812338637396)


def follow(
    user_id: str,
    leader_id: str,
    *,
    enabled: bool = True,
    allocation_pct: str = "0.05",
    max_trade_usdc: str = "50",
    max_market_exposure_usdc: str = "300",
) -> UserFollowConfig:
    """One explicit follow binding with exact-decimal money."""
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=enabled,
        allocation_pct=Decimal(allocation_pct),
        max_trade_usdc=Decimal(max_trade_usdc),
        max_market_exposure_usdc=Decimal(max_market_exposure_usdc),
    )


def runtime_user(
    user_id: str,
    funder: str,
    follows: dict[str, UserFollowConfig],
    *,
    explicit: bool = True,
) -> RuntimeUser:
    return RuntimeUser(
        user_id=user_id,
        wallet=WalletConfig(funder_address=funder),
        follows=dict(follows),
        follows_explicit=explicit,
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def make_config(**nested_overrides):
    """A real validated AppConfig: paper mode with permissive copy gates."""
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(
            {
                "PRIVATE_KEY": synth_private_key("c"),
                "FUNDER_ADDRESS": FUNDER_ALPHA,
                "RPC_URL": "https://polygon-rpc.example.com",
                "TRADING_MODE": "paper",
                "POLYMARKET_ENFORCE_GEOBLOCK": "false",
                "COPY_DECISION_MIN_SCORE": "0.01",
                "COPY_MAX_SIGNAL_AGE_SECONDS": "999999",
                "COPY_MAX_PRICE_DRIFT_PCT": "999",
                "COPY_MAX_EDGE_DECAY_PCT": "999",
                "COPY_MIN_SHARES": "0.001",
                "COPY_MIN_NOTIONAL_USDC": "0.01",
                "BUY_MIN_AMOUNT": "0.01",
                "BUY_MAX_AMOUNT": "1000",
                "MAX_MARKET_EXPOSURE_USDC": "300",
                "MAX_POSITION_PCT": "1",
                "ENABLE_BALANCE_TIERED_POLICY": "false",
            },
            **nested_overrides,
        )


@pytest.fixture
def app_config():
    return make_config()


# ---------------------------------------------------------------------------
# Fake venue gateway (money behavior; records every private write attempt)
# ---------------------------------------------------------------------------


class RecordingGateway:
    """Fake venue gateway that records every private write attempt.

    Delegates the awaitable surface to the project ``make_clob_gateway``
    mock (``tests/conftest.py``) so the production services see the same
    contract, while capturing the exact ``MarketableOrderRequest`` objects
    each account submits and every cancel each account issues.
    """

    def __init__(self, submit_results=None):
        from tests.conftest import make_clob_gateway

        self._inner = make_clob_gateway()
        self.submissions = []
        self.cancels = []
        self._submit_results = list(submit_results or [])

    def _record_and_result(self, request):
        self.submissions.append(request)
        results = self._submit_results
        if results:
            outcome = results.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return self._inner.submit_marketable_order.return_value

    async def submit_marketable_order(self, request):
        return self._record_and_result(request)

    async def submit_limit_order(self, request):
        self.submissions.append(request)
        return await self._inner.submit_limit_order(request)

    async def cancel_order(self, order_id):
        self.cancels.append(order_id)
        return await self._inner.cancel_order(order_id)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def recording_gateway(submit_results=None) -> RecordingGateway:
    return RecordingGateway(submit_results)


# ---------------------------------------------------------------------------
# Account runtime composition
# ---------------------------------------------------------------------------


def build_account_state(
    user: RuntimeUser,
    config,
    gateway=None,
) -> object:
    """Compose one copier account runtime through the production bootstrap."""
    if gateway is None:
        gateway = recording_gateway()
    return build_user_state(
        user,
        client=object(),
        pool=MagicMock(),
        config=config,
        clob_gateway=gateway,
    )


@pytest.fixture(autouse=True)
def _quiet_bootstrap(monkeypatch):
    """Keep context composition off live DB paths (same posture as T10 tests)."""
    monkeypatch.setattr(
        "pmcopy.orchestrators.runtime_bootstrap.rehydrate_position_manager",
        AsyncMock(return_value=None),
    )

    async def _no_recovery(*args, **kwargs):
        return 0

    monkeypatch.setattr(
        "pmcopy.services.execution.intent_recovery.recover_intents_from_db", _no_recovery
    )


@pytest.fixture
def two_copier_world(app_config):
    """Two copier accounts + their composed runtimes, keyed by user id."""
    users = {
        USER_ALPHA: runtime_user(USER_ALPHA, FUNDER_ALPHA, {}),
        USER_BETA: runtime_user(USER_BETA, FUNDER_BETA, {}),
    }
    gateways = {}
    states = {}
    for user_id, user in users.items():
        gateway = recording_gateway()
        gateways[user_id] = gateway
        states[user_id] = build_account_state(user, app_config, gateway=gateway)
    return SimpleNamespace(users=users, gateways=gateways, states=states, config=app_config)
