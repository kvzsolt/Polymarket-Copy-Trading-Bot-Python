from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
import config.app_config as app_config_mod
from application.trading_context import TradingContext
from config.env import SignatureType
from tests.config_helpers import load_test_config
import orchestrators.runtime_bootstrap as runtime_bootstrap
from orchestrators.runtime_bootstrap import (
    build_trading_context,
    configured_leaders,
    default_follow,
    startup_reconcile_unconfirmed,
)
from services.multiUserConfig import bind_resolved_deposit_wallet, load_runtime_users


_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "PERCENTAGE": "7",
        "BUY_MIN_AMOUNT": "3",
        "BUY_MAX_AMOUNT": "42",
        "MAX_MARKET_EXPOSURE_USDC": "123",
        "SLIPPAGE_TOLERANCE": "1.5",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def test_configured_leaders_uses_typed_config_wallets():
    cfg = _config(LEADER_WALLETS="0x" + "d" * 40 + ",0x" + "e" * 40)
    assert configured_leaders(cfg) == ["0x" + "d" * 40, "0x" + "e" * 40]


def test_default_follow_uses_typed_config_thresholds():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    assert follow.allocation_pct == 0.07
    assert follow.max_trade_usdc == 42.0
    assert follow.max_market_exposure_usdc == 123.0


def test_load_runtime_users_single_user_uses_typed_config_defaults():
    cfg = _config(LEADER_WALLETS="0x" + "d" * 40)
    users = load_runtime_users(["0x" + "d" * 40], cfg)
    assert len(users) == 1
    user = users[0]
    # The fixture configures FUNDER_ADDRESS, so the single-user path must
    # carry it onto the RuntimeUser wallet: in paper mode nothing else ever
    # resolves an address, and it is the identity health probes use.
    assert user.wallet.funder_address == _VALID_FUNDER
    assert user.user_id == _VALID_FUNDER
    assert user.follows["0x" + "d" * 40].user_id == _VALID_FUNDER
    assert user.wallet.signature_type == SignatureType.DEPOSIT_WALLET
    assert user.follows["0x" + "d" * 40].allocation_pct == 0.07
    assert user.follows["0x" + "d" * 40].max_trade_usdc == 42.0


def test_bind_resolved_deposit_wallet_sets_runtime_identity():
    cfg = _config(LEADER_WALLETS="0x" + "d" * 40)
    user = load_runtime_users(["0x" + "d" * 40], cfg)[0]

    bind_resolved_deposit_wallet(user, _VALID_FUNDER)

    assert user.wallet.funder_address == _VALID_FUNDER
    assert user.wallet.signature_type == SignatureType.DEPOSIT_WALLET
    assert user.user_id == _VALID_FUNDER
    assert user.follows["0x" + "d" * 40].user_id == _VALID_FUNDER


def test_load_runtime_users_accepts_deposit_wallet_signature_type(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    config_path = tmp_path / "users.json"
    config_path.write_text(
        json.dumps(
            {
                "users": [
                    {
                        "user_id": "deposit-user",
                        "private_key": _VALID_PRIVATE_KEY,
                        "funder_address": _VALID_FUNDER,
                        "signature_type": 3,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    cfg = _config(MULTI_USER_CONFIG_FILE=str(config_path))

    users = load_runtime_users([], cfg)

    assert users[0].wallet.signature_type == SignatureType.DEPOSIT_WALLET


def _multi_user_file(tmp_path, entry: dict) -> str:
    path = tmp_path / "users.json"
    path.write_text(json.dumps({"users": [entry]}), encoding="utf-8")
    return str(path)


@pytest.mark.parametrize("funder", [None, "", "   "])
def test_load_runtime_users_rejects_paper_entry_without_funder(
    funder,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Paper entries must carry a funder, matching the single-user invariant.

    Paper never authenticates, so no secure client resolves a Deposit Wallet
    and ``bind_resolved_deposit_wallet`` never runs. An omitted funder leaves
    "" as the entry's only wallet identity, which the balance/live-position
    probes then query — failing readiness with no actionable error.
    """
    entry = {
        "user_id": "paper-user",
        "private_key": _VALID_PRIVATE_KEY,
        "signature_type": 3,
    }
    if funder is not None:
        entry["funder_address"] = funder
    config_path = _multi_user_file(tmp_path, entry)
    monkeypatch.chdir(tmp_path)
    cfg = _config(MULTI_USER_CONFIG_FILE=config_path, TRADING_MODE="paper")

    with pytest.raises(ValueError, match=r"'paper-user' is missing funder_address"):
        load_runtime_users([], cfg)


def test_load_runtime_users_allows_live_entry_without_funder(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Live entries may omit the funder — the authenticated SDK derives it.

    Requiring it here would break the one-secret auth chain; the resolved
    address is bound back by ``bind_resolved_deposit_wallet`` after startup.
    """
    config_path = _multi_user_file(
        tmp_path,
        {
            "user_id": "live-user",
            "private_key": _VALID_PRIVATE_KEY,
            "signature_type": 3,
        },
    )
    monkeypatch.chdir(tmp_path)
    cfg = _config(MULTI_USER_CONFIG_FILE=config_path, TRADING_MODE="live")

    users = load_runtime_users([], cfg)

    assert users[0].wallet.funder_address == ""


@pytest.mark.asyncio
async def test_startup_reconcile_unconfirmed_dedupes_user_contexts_and_rehydrates_once():
    ctx1 = SimpleNamespace(user_id="user-1", execution_service=object())
    ctx2 = SimpleNamespace(user_id="user-1", execution_service=object())
    ctx3 = SimpleNamespace(user_id="user-2", execution_service=None)

    with (
        patch(
            "orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", side_effect=[2, 99]
        ) as reconcile,
        patch(
            "orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders", return_value=0
        ) as reconcile_risk,
        patch("orchestrators.runtime_bootstrap.rehydrate_position_manager") as rehydrate,
    ):
        result = await startup_reconcile_unconfirmed([ctx1, ctx2, ctx3])

    assert result == {"user-1": 2}
    reconcile.assert_called_once_with(ctx1)
    reconcile_risk.assert_called_once_with(ctx1)
    rehydrate.assert_called_once_with(ctx1)


@pytest.mark.asyncio
async def test_startup_reconcile_unconfirmed_rehydrates_on_risk_only_convergence():
    ctx = SimpleNamespace(user_id="user-1", execution_service=object())

    with (
        patch(
            "orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", return_value=0
        ) as reconcile,
        patch(
            "orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders", return_value=3
        ) as reconcile_risk,
        patch("orchestrators.runtime_bootstrap.rehydrate_position_manager") as rehydrate,
    ):
        result = await startup_reconcile_unconfirmed([ctx])

    assert result == {"user-1": 3}
    reconcile.assert_called_once_with(ctx)
    reconcile_risk.assert_called_once_with(ctx)
    rehydrate.assert_called_once_with(ctx)


@pytest.mark.asyncio
async def test_startup_reconcile_unconfirmed_sweeps_expired_reservations():
    reservation_service = SimpleNamespace(
        release_expired_active=AsyncMock(return_value=["res-1", "res-2"]),
    )
    ctx = SimpleNamespace(
        user_id="user-1",
        execution_service=object(),
        extra={"reservation_service": reservation_service},
    )

    with (
        patch(
            "orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", return_value=0
        ) as reconcile,
        patch(
            "orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders", return_value=0
        ) as reconcile_risk,
        patch("orchestrators.runtime_bootstrap.rehydrate_position_manager") as rehydrate,
    ):
        result = await startup_reconcile_unconfirmed([ctx])

    assert result == {"user-1": 2}
    reconcile.assert_called_once_with(ctx)
    reconcile_risk.assert_called_once_with(ctx)
    reservation_service.release_expired_active.assert_awaited_once_with()
    rehydrate.assert_called_once_with(ctx)


@pytest.mark.asyncio
async def test_expired_reservation_callback_reconciles_order_row_linked_only_by_metadata():
    from integrations.polymarket.models import VenueOrderState

    active = SimpleNamespace(
        id="res-1",
        status="active",
        order_id=None,
        venue_order_id=None,
        user_id="user-1",
        leader_wallet="leader-1",
        condition_id="cond-1",
        token_id="token-1",
        side="BUY",
        intent_key="intent-1",
    )

    class _ReservationsRepo:
        async def get_by_id(self, reservation_id: str):
            assert reservation_id == "res-1"
            return active

    class _ReservationService:
        def __init__(self):
            self.reservations_repo = _ReservationsRepo()
            self.released = []
            self.reconciler_callback = None

        async def release_terminal(self, reservation_id: str, reason: str):
            self.released.append((reservation_id, reason))
            active.status = "released"

    class _OrdersRepo:
        def __init__(self):
            self.order_row = {
                "idempotency_key": "idem-1",
                "user_id": "user-1",
                "leader_id": "leader-1",
                "order_id": "order-1",
                "status": "submitted_unconfirmed",
                "request": {
                    "user_id": "user-1",
                    "leader_id": "leader-1",
                    "condition_id": "cond-1",
                    "token_id": "token-1",
                    "side": "BUY",
                    "signal_id": "tx-1",
                    "recovery_metadata": {"reservation_id": "res-1"},
                },
                "matched_total": "0",
            }
            self.updated = []
            self.processed = []

        async def get_unconfirmed_buys(self, max_age_seconds=300.0, *, user_id=None):
            return []

        async def get_unconfirmed_sells(self, max_age_seconds=300.0, *, user_id=None):
            return []

        async def get_unconfirmed_order_submissions(self, user_id=None, side=None):
            return []

        async def get_by_reservation_id(self, reservation_id: str):
            assert reservation_id == "res-1"
            return self.order_row

        async def get_by_order_id(self, order_id: str):
            assert order_id == "order-1"
            return self.order_row

        async def update_submission_status(
            self, *, order_id: str, status: str, response_patch=None
        ):
            self.updated.append((order_id, status, response_patch))
            self.order_row["status"] = status

        async def mark_processed_trade(self, **kwargs):
            self.processed.append(kwargs)

    class _FillsRepo:
        def __init__(self):
            self.fills = []

        async def append_fill(self, fill, raw_payload=None):
            self.fills.append((fill, raw_payload))

        async def append_fill_and_apply(self, record, positions_repo, *, raw_payload=None):
            # Mirror FillsRepo.append_fill_and_apply (P0-1): dedup on dedup_key,
            # else append + apply the position delta. (0, None) on a duplicate.
            seen = getattr(self, "_applied_keys", None)
            if seen is None:
                seen = self._applied_keys = set()
            key = (record.order_id, record.side, getattr(record, "dedup_key", None))
            if key[2] is not None and key in seen:
                return 0, None
            seen.add(key)
            await self.append_fill(record, raw_payload=raw_payload)
            updated = await positions_repo.apply_fill(
                user_id=record.user_id,
                leader_id=record.leader_id,
                condition_id=record.condition_id,
                token_id=record.token_id,
                side=record.side,
                fill_size=record.size_tokens,
                fill_price=record.price,
            )
            return 1, updated

    class _PositionsRepo:
        def __init__(self):
            self.applied = []

        async def apply_fill(self, **kwargs):
            self.applied.append(kwargs)

        async def get_position(self, *args, **kwargs):
            return None

    async def _get_order(order_id: str):
        return VenueOrderState(
            order_id=order_id,
            status="confirmed",
            filled_size=Decimal("2"),
            avg_fill_price=Decimal("0.50"),
        )

    reservation_service = _ReservationService()
    orders_repo = _OrdersRepo()
    fills_repo = _FillsRepo()
    positions_repo = _PositionsRepo()
    ctx = SimpleNamespace(
        user_id="user-1",
        wallet_address="0xfunder",
        execution_service=object(),
        extra={"clob_gateway": SimpleNamespace(get_order=_get_order)},
        orders_repo=orders_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        risk_blocks_repo=None,
        risk_triggers_repo=None,
        audit_logger=None,
        pnl_tracker=None,
    )
    coord = SimpleNamespace(
        reservation_service=reservation_service,
        reservations_repo=reservation_service.reservations_repo,
    )

    with patch(
        "orchestrators.runtime_bootstrap.rehydrate_position_manager",
        new=AsyncMock(return_value=None),
    ):
        runtime_bootstrap._bind_reservation_runtime(ctx, coord)
        released = await reservation_service.reconciler_callback("res-1")

    assert released is True
    assert reservation_service.released == [("res-1", "confirmed")]
    assert fills_repo.fills
    assert positions_repo.applied[0]["fill_size"] == Decimal("2")


def test_build_trading_context_uses_typed_sizing_execution_values():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    risk_blocks_repo = object()
    user_state = SimpleNamespace(
        user=SimpleNamespace(
            user_id="user-1", wallet=SimpleNamespace(funder_address=_VALID_FUNDER)
        ),
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=risk_blocks_repo,
        pnl_tracker=object(),
        leader_performance=None,
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=object(),
        clob_gateway=None,
        market_store=object(),
        risk_triggers_repo=None,
    )

    with patch(
        "orchestrators.runtime_bootstrap.rehydrate_position_manager",
        new=AsyncMock(return_value=None),
    ):
        ctx = asyncio.run(
            build_trading_context(
                user_state,
                follow,
                cfg,
                readiness=object(),
            )
        )

    assert ctx.buy_min_amount == 3.0
    assert ctx.buy_max_amount == 42.0
    assert ctx.extra["slippage_tolerance_bps"] == 150
    assert ctx.extra["leader_wallet"] == "leader-1"
    assert ctx.entry_permission_service is not None
    assert ctx.entry_permission_service._repo is risk_blocks_repo
    assert ctx.market_quotes is user_state.market_store
    assert ctx.holdings_reader is not None
    assert ctx.config is cfg
    assert ctx.leader_scorer._config is cfg
    assert ctx.copy_decision_service._config is cfg


def test_build_trading_context_constructs_copy_decision_service_only_with_injected_config():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    user_state = SimpleNamespace(
        user=SimpleNamespace(
            user_id="user-1", wallet=SimpleNamespace(funder_address=_VALID_FUNDER)
        ),
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        leader_performance=None,
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=object(),
        clob_gateway=None,
        risk_triggers_repo=None,
    )

    with (
        patch("application.trading_context.CopyDecisionService") as copy_decision_cls,
        patch(
            "orchestrators.runtime_bootstrap.rehydrate_position_manager",
            new=AsyncMock(return_value=None),
        ),
    ):
        asyncio.run(build_trading_context(user_state, follow, cfg, readiness=object()))

    copy_decision_cls.assert_called_once_with(config=cfg, leader_scorer=ANY)


def test_build_user_state_injects_typed_config_into_runtime_services():
    cfg = _config()
    runtime_user = SimpleNamespace(
        user_id="user-1",
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
    )

    with (
        patch("orchestrators.runtime_bootstrap.PositionManager") as position_manager_cls,
        patch("orchestrators.runtime_bootstrap.ExecutionService") as execution_service_cls,
    ):
        position_manager_instance = position_manager_cls.return_value
        execution_service_instance = execution_service_cls.return_value
        clob_gateway = object()
        state = runtime_bootstrap.build_user_state(
            runtime_user,
            client=object(),
            pool=MagicMock(),
            config=cfg,
            clob_gateway=clob_gateway,
        )

    position_manager_cls.assert_called_once_with(config=cfg)
    execution_service_cls.assert_called_once()
    assert execution_service_cls.call_args.kwargs["config"] is cfg
    assert execution_service_cls.call_args.kwargs["clob_gateway"] is clob_gateway
    assert state.position_manager is position_manager_instance
    assert state.execution_service is execution_service_instance
    assert state.execution_coordinator is not None
    reservation_service = state.execution_coordinator.reservation_service
    assert state.position_drift_service._reservations_repo is reservation_service.reservations_repo


def test_copy_notional_prefers_typed_config_over_env():
    cfg = _config(
        SIZING_MODE="fixed",
        FIXED_AMOUNT="88",
        TRADE_MULTIPLIER="9",
        TIERED_TRADE_MULTIPLIERS="0-100:2.0",
        ADAPTIVE_SIZING_BASIS="performance",
        ADAPTIVE_MIN_PERCENT="5",
        ADAPTIVE_MAX_PERCENT="95",
        ADAPTIVE_THRESHOLD_USDC="777",
    )
    ctx = TradingContext(
        user_id="user-1",
        wallet_address=_VALID_FUNDER,
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        config=cfg,
        buy_min_amount=3.0,
        buy_max_amount=42.0,
    )

    with patch.dict(
        "os.environ",
        {
            "SIZING_MODE": "proportional",
            "FIXED_AMOUNT": "1",
            "TRADE_MULTIPLIER": "1",
            "TIERED_TRADE_MULTIPLIERS": "",
            "ADAPTIVE_SIZING_BASIS": "trade_size",
            "ADAPTIVE_MIN_PERCENT": "0",
            "ADAPTIVE_MAX_PERCENT": "0",
            "ADAPTIVE_THRESHOLD_USDC": "1",
        },
        clear=False,
    ):
        result = ctx.copy_notional(Decimal("200"))

    assert result.strategy == "fixed"
    assert float(result.base_notional_usdc) == pytest.approx(88.0)
    assert float(result.final_notional_usdc) == pytest.approx(42.0)
