"""§9 bullet 11 — an all-paper configuration makes ZERO private trading
writes, including the error and cleanup paths.

Composition under test: the REAL venue runtime builder
(``build_polymarket_runtime`` in paper mode wraps public CCXT market reads
behind ``PaperExecutionGateway``) feeding the REAL account composition
(``build_user_state`` → ``ExecutionService``). The CCXT exchange is a fake
double whose private write surface (``create_order`` / ``cancel_order``) is an
AsyncMock recorder — any attempt by the paper path to place or cancel a real
venue order fails the test.

Boundary rules proven:

* happy-path submit routes to the local simulator, never the exchange;
* a metadata-unavailable error path returns before any submission attempt;
* the cleanup path (cancel) routes to the simulator, never the exchange;
* paper startup never constructs an authenticated exchange (no apiKey /
  private key handed to the transport).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.execution import ExecutionMetadata, ExecutionOrderType
from pmcopy.integrations.polymarket.paper_execution import PaperExecutionGateway
from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime
from pmcopy.services.execution.types import ExecutionRequest

from tests.multicopier.conftest import (
    CONDITION,
    FUNDER_ALPHA,
    TOKEN,
    USER_ALPHA,
    build_account_state,
    make_config,
    runtime_user,
    synth_private_key,
    follow,
    LEADER_1,
)


class PrivateWriteRecordingExchange:
    """CCXT exchange double whose every private write is recorded."""

    def __init__(self, settings: dict) -> None:
        self.settings = settings
        self.options = dict(settings.get("options") or {})
        self.has = {
            "prediction": True,
            "createOrder": True,
            "fetchOrderBook": True,
            "fetchOrder": True,
            "fetchOpenOrders": True,
            "fetchMyTrades": True,
            "cancelOrder": True,
        }
        self.private_writes: list[str] = []
        self.create_order = AsyncMock(side_effect=self._record("create_order"))
        self.cancel_order = AsyncMock(side_effect=self._record("cancel_order"))
        self.close = AsyncMock()
        self.load_api_credentials = AsyncMock(return_value={"apiKey": "dk"})
        self.clob_private_get_data_orders = self._noop_raw
        self.clob_private_get_data_trades = self._noop_raw

    def _record(self, name: str):
        async def _recorder(*_args, **_kwargs):
            self.private_writes.append(name)
            raise AssertionError(f"paper path attempted a private venue write: {name}")

        return _recorder

    @staticmethod
    def _noop_raw(_params: object) -> dict:
        return {"data": [], "next_cursor": ""}

    def polymarket_order_raw_amounts(self, *_args: object) -> dict[str, str]:
        return {"makerAmount": "0", "takerAmount": "0"}


@pytest.fixture
def paper_exchange_factory():
    created: list[PrivateWriteRecordingExchange] = []

    def factory(settings: dict) -> PrivateWriteRecordingExchange:
        exchange = PrivateWriteRecordingExchange(settings)
        created.append(exchange)
        return exchange

    return factory, created


async def _build_paper_runtime(config, factory):
    user = SimpleNamespace(
        user_id=USER_ALPHA,
        wallet=SimpleNamespace(funder_address=FUNDER_ALPHA, private_key=synth_private_key("d")),
    )
    return await build_polymarket_runtime(
        runtime_user=user,
        config=config,
        exchange_factory=factory,
        ccxt_version="4.5.73",
        tick_size_resolver=SimpleNamespace(
            resolve=AsyncMock(
                return_value=ExecutionMetadata(
                    token_id=TOKEN,
                    tick_size=Decimal("0.01"),
                    min_order_size=Decimal("1"),
                    neg_risk=False,
                )
            )
        ),
    )


def _patch_execution_persistence(monkeypatch, state) -> list[dict]:
    """Instance-patch the execution service's order-repo seam (no SQL here)."""
    saved: list[dict] = []
    repo = state.orders_repo

    async def _get_by_idempotency_key(_key):
        return None

    async def _try_acquire_submission_lock(*_args, **_kwargs):
        return True

    async def _save_submission(*_args, **_kwargs):
        saved.append({"stage": "save"})
        return None

    async def _update_submission_response(*_args, **_kwargs):
        saved.append({"stage": "response"})
        return None

    monkeypatch.setattr(repo, "get_by_idempotency_key", _get_by_idempotency_key)
    monkeypatch.setattr(repo, "try_acquire_submission_lock", _try_acquire_submission_lock)
    monkeypatch.setattr(repo, "save_submission", _save_submission)
    monkeypatch.setattr(
        repo, "update_submission_response", _update_submission_response, raising=False
    )
    return saved


def _copy_fak_buy_request(*, amount: Decimal = Decimal("5")) -> ExecutionRequest:
    return ExecutionRequest(
        user_id=USER_ALPHA,
        leader_id=LEADER_1,
        token_id=TOKEN,
        side="BUY",
        amount=amount,
        max_slippage_bps=200,
        reason="copy_trade",
        signal_id="0xsig-paper-1",
        condition_id=CONDITION,
        recovery_metadata={
            "source": "copy_trade",
            "order_type": "FAK",
            "price_limit": "0.50",
        },
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.50"),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )


@pytest.mark.asyncio
async def test_paper_runtime_happy_path_error_and_cleanup_never_touch_the_venue(
    monkeypatch, paper_exchange_factory
):
    config = make_config()
    factory, created = paper_exchange_factory
    runtime = await _build_paper_runtime(config, factory)

    # Paper composition: a PaperExecutionGateway in front of the exchange.
    assert isinstance(runtime.gateway, PaperExecutionGateway)
    exchange = created[0]

    # Paper startup constructed the PUBLIC transport only: no api key and no
    # private key material was handed to the exchange.
    assert "apiKey" not in exchange.settings
    assert not exchange.settings.get("privateKey")

    user = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: follow(USER_ALPHA, LEADER_1)})
    state = build_account_state(user, config, gateway=runtime.gateway)
    persistence = _patch_execution_persistence(monkeypatch, state)

    # Happy path: the copy FAK is accepted and routed to the simulator
    # (fill accounting lands through the normal reconciliation path).
    result = await state.execution_service.execute(_copy_fak_buy_request())
    assert result.accepted is True, result.error
    assert str(result.order_id).startswith("paper-")
    assert persistence, "submission was persisted through the order-repo seam"

    # The simulated order used BUY quote semantics: 5 USDC spend at a 0.50
    # cap → 10 derived shares, entirely inside the local simulator.
    simulated = await runtime.gateway.simulator.get_order(str(result.order_id))
    assert simulated.filled_size == Decimal("10")
    assert simulated.status == "closed"

    # Error path: missing copy execution metadata fails closed BEFORE any
    # submission attempt (paper or otherwise).
    bad = ExecutionRequest(
        user_id=USER_ALPHA,
        leader_id=LEADER_1,
        token_id=TOKEN,
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=200,
        reason="copy_trade",
        signal_id="0xsig-paper-2",
        condition_id=CONDITION,
        recovery_metadata={"source": "copy_trade", "order_type": "FAK", "price_limit": "0.50"},
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.50"),
        tick_size=None,  # metadata unavailable → fail closed
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    rejected = await state.execution_service.execute(bad)
    assert rejected.accepted is False
    assert rejected.status == "market_metadata_unavailable"

    # Cleanup path: cancelling a resting order stays inside the simulator
    # (cancel-after-fill is idempotent there) and never hits the exchange.
    gateway: PaperExecutionGateway = runtime.gateway
    resting = await gateway.submit_limit_order(
        SimpleNamespace(
            token_id=TOKEN,
            side="BUY",
            price=Decimal("0.40"),
            size=Decimal("5"),
        )
    )
    assert resting.success is True
    cancelled = await gateway.cancel_order(resting.order_id)
    assert cancelled.success is True

    # Zero private trading writes across happy-path, error, and cleanup.
    assert exchange.private_writes == []
    exchange.create_order.assert_not_called()
    exchange.cancel_order.assert_not_called()


@pytest.mark.asyncio
async def test_all_paper_two_copier_configuration_routes_writes_to_simulators(
    monkeypatch, paper_exchange_factory
):
    """Two paper copiers in one process: each account's writes stay local.

    Each copier gets its own venue runtime (one exchange per follower wallet);
    submitting through each account's composed execution service fills that
    account's OWN simulator and never either exchange.
    """
    config = make_config()
    factory, created = paper_exchange_factory
    from tests.multicopier.conftest import FUNDER_BETA, USER_BETA

    runtime_alpha = await _build_paper_runtime(config, factory)
    runtime_beta = await _build_paper_runtime(config, factory)
    assert len(created) == 2

    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: follow(USER_ALPHA, LEADER_1)})
    beta = runtime_user(USER_BETA, FUNDER_BETA, {LEADER_1: follow(USER_BETA, LEADER_1)})
    state_alpha = build_account_state(alpha, config, gateway=runtime_alpha.gateway)
    state_beta = build_account_state(beta, config, gateway=runtime_beta.gateway)
    _patch_execution_persistence(monkeypatch, state_alpha)
    _patch_execution_persistence(monkeypatch, state_beta)

    result_a = await state_alpha.execution_service.execute(_copy_fak_buy_request())
    result_b = await state_beta.execution_service.execute(_copy_fak_buy_request())
    assert result_a.accepted and result_b.accepted
    assert str(result_a.order_id).startswith("paper-")
    assert str(result_b.order_id).startswith("paper-")
    assert result_a.order_id != result_b.order_id

    # Each simulator holds exactly its own account's order.
    sim_a = runtime_alpha.gateway.simulator
    sim_b = runtime_beta.gateway.simulator
    order_a = await sim_a.get_order(str(result_a.order_id))
    order_b = await sim_b.get_order(str(result_b.order_id))
    assert order_a.token_id == TOKEN and order_b.token_id == TOKEN
    from pmcopy.integrations.polymarket.errors import VenueOrderNotFoundError

    with pytest.raises(VenueOrderNotFoundError):
        # B's simulator has no knowledge of A's paper order.
        await sim_b.get_order(str(result_a.order_id))

    for exchange in created:
        assert exchange.private_writes == []
