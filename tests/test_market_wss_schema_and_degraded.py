"""Tests for market WSS schema fixes and REST degraded fallback (Part A + B).

Coverage:
  - WSS subscription payloads include initial_dump + level=2
  - price_change reads price_changes field (Polymarket spec) with fallback
  - market_resolved iterates assets_ids list
  - Heartbeat constants are 10s
  - MarketDataConfig parsing and defaults
  - REST degraded fallback gated on WSS health
  - Safety invariants: tick_size_change, best_bid_ask, source isolation
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import MarketStateStore
from clients.market_ws import MarketWebSocket
from clients.ws_constants import MARKET_HEARTBEAT_INTERVAL, USER_HEARTBEAT_INTERVAL
from services.market_data_service import (
    MarketDataPolicy,
    MarketDataService,
    MarketDataSource,
    StaleDataError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOKEN_A = "0xaaa"
TOKEN_B = "0xbbb"
TOKEN_C = "0xccc"


def _make_ws(cache: MarketStateStore | None = None) -> MarketWebSocket:
    return MarketWebSocket(
        cache or MarketStateStore(),
        ws_url="wss://test.example.com/ws/market",
    )


def _make_mds(
    cache: MarketStateStore,
    gateway=None,
    *,
    allow_rest: bool = False,
    require_wss: bool = True,
    is_wss_down: bool | None = None,
) -> MarketDataService:
    if gateway is None:
        gateway = MagicMock()
    policy = MarketDataPolicy(
        allow_rest_fallback=allow_rest,
        require_wss_fresh=require_wss,
    )
    wss_fn = (lambda: is_wss_down) if is_wss_down is not None else None
    return MarketDataService(cache, gateway, policy=policy, is_wss_down_fn=wss_fn)


def _seed_executable_wss(cache: MarketStateStore, token_id: str) -> None:
    """Put a fully WSS-fresh executable token into the cache."""
    cache.replace_book_snapshot(
        token_id,
        bids={Decimal("0.50"): Decimal("100")},
        asks={Decimal("0.55"): Decimal("80")},
        raw_event={"source": "wss"},
    )
    cache.set_tick(
        token_id,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        source="wss",
    )
    # Clear dirty flag set by set_tick
    cache.mark_fresh(token_id, source="wss")


# ---------------------------------------------------------------------------
# Part A — WSS schema
# ---------------------------------------------------------------------------


class TestSubscribePayload:
    """Initial and dynamic subscription payloads must match Polymarket spec."""

    @pytest.mark.asyncio
    async def test_subscribe_includes_initial_dump_and_level2(self) -> None:
        ws = _make_ws()
        mock_conn = AsyncMock()
        sent_payloads: list[dict] = []

        async def capture_send(msg: str) -> None:
            sent_payloads.append(json.loads(msg))

        mock_conn.send = capture_send

        await ws._subscribe(mock_conn)

        assert len(sent_payloads) == 1
        p = sent_payloads[0]
        assert p.get("initial_dump") is True
        assert p.get("level") == 2
        assert p.get("type") == "market"

    @pytest.mark.asyncio
    async def test_dynamic_subscribe_includes_level2(self) -> None:
        ws = _make_ws()
        ws._asset_ids = [TOKEN_A]
        mock_ws = AsyncMock()
        ws._ws = mock_ws

        await ws.subscribe([TOKEN_B])

        mock_ws.send.assert_called_once()
        payload = json.loads(mock_ws.send.call_args[0][0])
        assert payload.get("level") == 2
        assert payload.get("operation") == "subscribe"
        assert TOKEN_B in payload.get("assets_ids", [])

    @pytest.mark.asyncio
    async def test_dynamic_subscribe_does_not_resend_known_tokens(self) -> None:
        ws = _make_ws()
        ws._asset_ids = [TOKEN_A]
        mock_ws = AsyncMock()
        ws._ws = mock_ws

        await ws.subscribe([TOKEN_A])

        mock_ws.send.assert_not_called()


class TestPriceChangeField:
    """price_change handler must read price_changes (Polymarket spec)."""

    def test_price_change_reads_price_changes_field(self) -> None:
        cache = MarketStateStore()
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "wss"},
        )
        ws = _make_ws(cache)

        event = json.dumps({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": TOKEN_A, "side": "BUY", "price": "0.52", "size": "50"},
            ],
        })
        ws._handle_message(event)

        state = cache.get_state(TOKEN_A)
        assert state is not None
        assert Decimal("0.52") in state.bids

    def test_price_change_falls_back_to_changes_field(self) -> None:
        cache = MarketStateStore()
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "wss"},
        )
        ws = _make_ws(cache)

        event = json.dumps({
            "event_type": "price_change",
            "changes": [
                {"asset_id": TOKEN_A, "side": "SELL", "price": "0.57", "size": "30"},
            ],
        })
        ws._handle_message(event)

        state = cache.get_state(TOKEN_A)
        assert state is not None
        assert Decimal("0.57") in state.asks

    def test_price_change_size_zero_deletes_level(self) -> None:
        cache = MarketStateStore()
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "wss"},
        )
        ws = _make_ws(cache)

        event = json.dumps({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": TOKEN_A, "side": "BUY", "price": "0.50", "size": "0"},
            ],
        })
        ws._handle_message(event)

        state = cache.get_state(TOKEN_A)
        assert state is not None
        assert Decimal("0.50") not in state.bids


class TestMarketResolved:
    """market_resolved must iterate assets_ids list and mark all tokens stale."""

    def test_market_resolved_iterates_assets_ids_list(self) -> None:
        cache = MarketStateStore()
        for t in [TOKEN_A, TOKEN_B, TOKEN_C]:
            cache.replace_book_snapshot(
                t,
                bids={Decimal("0.50"): Decimal("100")},
                asks={Decimal("0.55"): Decimal("80")},
                raw_event={"source": "wss"},
            )
        ws = _make_ws(cache)

        event = json.dumps({
            "event_type": "market_resolved",
            "assets_ids": [TOKEN_A, TOKEN_B, TOKEN_C],
        })
        ws._handle_message(event)

        for t in [TOKEN_A, TOKEN_B, TOKEN_C]:
            assert cache.is_resolved(t)
            assert cache.is_stale(t)

    def test_market_resolved_handles_single_asset_id(self) -> None:
        cache = MarketStateStore()
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "wss"},
        )
        ws = _make_ws(cache)

        event = json.dumps({
            "event_type": "market_resolved",
            "asset_id": TOKEN_A,
        })
        ws._handle_message(event)

        assert cache.is_resolved(TOKEN_A)

    def test_market_resolved_deduplicates_assets_ids_and_asset_id(self) -> None:
        cache = MarketStateStore()
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "wss"},
        )
        ws = _make_ws(cache)

        # Both assets_ids and asset_id point to same token — should not double-resolve
        event = json.dumps({
            "event_type": "market_resolved",
            "assets_ids": [TOKEN_A],
            "asset_id": TOKEN_A,
        })
        ws._handle_message(event)

        assert cache.is_resolved(TOKEN_A)


class TestHeartbeatConstants:
    def test_market_heartbeat_constant_is_5(self) -> None:
        # Phase 2 spec — 5s cadence + 10s pong timeout.
        assert MARKET_HEARTBEAT_INTERVAL == 5

    def test_user_heartbeat_constant_is_5(self) -> None:
        assert USER_HEARTBEAT_INTERVAL == 5


# ---------------------------------------------------------------------------
# Part B — MarketDataConfig
# ---------------------------------------------------------------------------


class TestMarketDataConfig:
    """MarketDataConfig parsing and defaults."""

    def test_market_data_config_defaults(self) -> None:
        from config.app_config import MarketDataConfig
        cfg = MarketDataConfig()
        assert cfg.allow_rest_when_wss_down is False
        assert cfg.wss_down_after_seconds == 15.0
        assert cfg.rest_degraded_max_age_seconds == 2.0
        assert cfg.rest_degraded_max_spread_bps == 500
        assert cfg.rest_degraded_log_every_seconds == 30.0

    def test_market_data_config_allow_rest_default_false(self) -> None:
        from config.app_config import MarketDataConfig
        cfg = MarketDataConfig()
        assert cfg.allow_rest_when_wss_down is False

    def test_market_data_config_is_frozen(self) -> None:
        import dataclasses
        from config.app_config import MarketDataConfig
        cfg = MarketDataConfig()
        with pytest.raises((dataclasses.FrozenInstanceError, TypeError)):
            cfg.allow_rest_when_wss_down = True  # type: ignore[misc]

    def test_app_config_has_market_data_field(self) -> None:
        from config.app_config import AppConfig
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(AppConfig)}
        assert "market_data" in field_names

    def test_market_data_config_env_parse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from types import SimpleNamespace
        monkeypatch.setattr("config.app_config.sys.version_info", SimpleNamespace(major=3, minor=12))
        from config.app_config import load_config as build_app_config
        _VALID_KEY = "0x" + "a" * 64
        _VALID_ADDR = "0x" + "a" * 40
        env = {
            "PRIVATE_KEY": _VALID_KEY,
            "FUNDER_ADDRESS": _VALID_ADDR,
            "RPC_URL": "https://polygon-rpc.example.com",
            "CHAIN_ID": "137",
            "USER_ADDRESS": _VALID_ADDR,
            "MARKET_DATA_ALLOW_REST_WHEN_WSS_DOWN": "true",
            "MARKET_DATA_WSS_DOWN_AFTER_SECONDS": "30",
            "MARKET_DATA_REST_DEGRADED_MAX_SPREAD_BPS": "200",
        }
        cfg = build_app_config(env)
        assert cfg.market_data.allow_rest_when_wss_down is True
        assert cfg.market_data.wss_down_after_seconds == 30.0
        assert cfg.market_data.rest_degraded_max_spread_bps == 200


# ---------------------------------------------------------------------------
# Part B — REST degraded fallback gating
# ---------------------------------------------------------------------------


class TestMarketDataSource:
    def test_rest_degraded_enum_value(self) -> None:
        assert MarketDataSource.REST_DEGRADED == "rest_degraded"
        assert MarketDataSource.REST_DEGRADED.value == "rest_degraded"

    def test_rest_degraded_distinct_from_rest_snapshot(self) -> None:
        assert MarketDataSource.REST_DEGRADED != MarketDataSource.REST_SNAPSHOT
        assert MarketDataSource.REST_DEGRADED != MarketDataSource.WSS


class TestRESTDegradedFallbackGating:
    """REST fallback must ONLY activate when WSS is globally down."""

    @pytest.mark.asyncio
    async def test_no_rest_fallback_when_wss_healthy(self) -> None:
        cache = MarketStateStore()
        mds = _make_mds(cache, allow_rest=True, require_wss=False, is_wss_down=False)

        with pytest.raises(StaleDataError, match="market WSS is healthy"):
            await mds.require_fresh_data(TOKEN_A)

    @pytest.mark.asyncio
    async def test_stale_error_when_wss_down_and_rest_disabled(self) -> None:
        cache = MarketStateStore()
        mds = _make_mds(cache, allow_rest=False, is_wss_down=True)

        with pytest.raises(StaleDataError, match="REST fallback disabled"):
            await mds.require_fresh_data(TOKEN_A)

    @pytest.mark.asyncio
    async def test_stale_error_when_rest_fallback_disabled_wss_healthy(self) -> None:
        cache = MarketStateStore()
        mds = _make_mds(cache, allow_rest=False, require_wss=True)

        with pytest.raises(StaleDataError):
            await mds.require_fresh_data(TOKEN_A)

    @pytest.mark.asyncio
    async def test_wss_healthy_rest_fallback_raises_even_if_configured(self) -> None:
        """allow_rest_fallback=True but WSS is healthy → still raises."""
        cache = MarketStateStore()
        mds = _make_mds(cache, allow_rest=True, require_wss=False, is_wss_down=False)

        with pytest.raises(StaleDataError, match="market WSS is healthy"):
            await mds.require_fresh_data(TOKEN_A)

    @pytest.mark.asyncio
    async def test_rest_fallback_invoked_when_wss_down_and_enabled(self) -> None:
        cache = MarketStateStore()
        gateway = AsyncMock()

        book_mock = MagicMock()
        book_mock.bids = [MagicMock(price="0.50", size="100")]
        book_mock.asks = [MagicMock(price="0.55", size="80")]
        gateway.get_orderbook = AsyncMock(return_value=book_mock)
        gateway.get_market_info = AsyncMock(return_value={
            "tick_size": "0.01",
            "min_order_size": "5",
            "neg_risk": False,
        })

        policy = MarketDataPolicy(
            allow_rest_fallback=True,
            require_wss_fresh=False,
            allow_rest_snapshot_for_execution=True,
        )
        mds = MarketDataService(
            cache, gateway, policy=policy, is_wss_down_fn=lambda: True
        )

        bundle = await mds.require_fresh_data(TOKEN_A)
        assert bundle is not None
        gateway.get_orderbook.assert_awaited_once_with(TOKEN_A)

    @pytest.mark.asyncio
    async def test_no_is_wss_down_fn_means_wss_not_down(self) -> None:
        """Without is_wss_down_fn, default is wss_down=False → REST blocked."""
        cache = MarketStateStore()
        policy = MarketDataPolicy(allow_rest_fallback=True, require_wss_fresh=False)
        mds = MarketDataService(cache, MagicMock(), policy=policy)

        with pytest.raises(StaleDataError, match="market WSS is healthy"):
            await mds.require_fresh_data(TOKEN_A)


class TestIsWssDown:
    """MarketDataRuntime.is_wss_down health tracking."""

    def test_is_wss_down_true_when_not_running(self) -> None:
        from orchestrators.market_data_runtime import MarketDataRuntime
        rt = MarketDataRuntime(
            market_store=MarketStateStore(),
            gateway=MagicMock(),
            readiness=MagicMock(),
            config=MagicMock(),
        )
        assert rt.is_wss_down(threshold_seconds=15.0) is True

    def test_is_wss_down_true_when_no_messages_received(self) -> None:
        from orchestrators.market_data_runtime import MarketDataRuntime
        rt = MarketDataRuntime(
            market_store=MarketStateStore(),
            gateway=MagicMock(),
            readiness=MagicMock(),
            config=MagicMock(),
        )
        rt._status.running = True
        # _last_wss_message_at is 0.0 — never received a message
        assert rt.is_wss_down(threshold_seconds=15.0) is True

    def test_is_wss_down_false_when_recent_message(self) -> None:
        import time
        from orchestrators.market_data_runtime import MarketDataRuntime
        rt = MarketDataRuntime(
            market_store=MarketStateStore(),
            gateway=MagicMock(),
            readiness=MagicMock(),
            config=MagicMock(),
        )
        rt._status.running = True
        rt._wss_connected = True
        rt._last_wss_message_at = time.monotonic()
        assert rt.is_wss_down(threshold_seconds=15.0) is False

    def test_is_wss_down_true_after_threshold(self) -> None:
        import time
        from orchestrators.market_data_runtime import MarketDataRuntime
        rt = MarketDataRuntime(
            market_store=MarketStateStore(),
            gateway=MagicMock(),
            readiness=MagicMock(),
            config=MagicMock(),
        )
        rt._status.running = True
        rt._wss_connected = True
        rt._last_wss_message_at = time.monotonic() - 30.0
        assert rt.is_wss_down(threshold_seconds=15.0) is True


# ---------------------------------------------------------------------------
# Safety invariants
# ---------------------------------------------------------------------------


class TestSafetyInvariants:
    """Invariants from CLAUDE.md hard rules."""

    def test_tick_size_change_does_not_promote_rest_book_to_wss(self) -> None:
        cache = MarketStateStore()
        # Seed with REST book only
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "rest_snapshot"},
        )
        assert cache.book_source_of(TOKEN_A) == "rest_snapshot"

        # WSS tick_size_change fires
        ws = _make_ws(cache)
        event = json.dumps({
            "event_type": "tick_size_change",
            "asset_id": TOKEN_A,
            "tick_size": "0.01",
        })
        ws._handle_message(event)

        # Book source must NOT have been promoted to wss
        assert cache.book_source_of(TOKEN_A) == "rest_snapshot"
        # Tick source IS now wss
        assert cache.tick_source_of(TOKEN_A) == "wss"
        # Token must be dirty (tick changed → sizing invalidated)
        assert cache.is_dirty(TOKEN_A)

    def test_tick_size_change_does_not_clear_dirty(self) -> None:
        cache = MarketStateStore()
        cache.mark_dirty(TOKEN_A, "some_reason")
        ws = _make_ws(cache)
        event = json.dumps({
            "event_type": "tick_size_change",
            "asset_id": TOKEN_A,
            "tick_size": "0.01",
        })
        ws._handle_message(event)
        # tick_size_change sets dirty, never clears it
        assert cache.is_dirty(TOKEN_A)

    def test_best_bid_ask_updates_quote_source_only(self) -> None:
        cache = MarketStateStore()
        # Pre-seed with REST book
        cache.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            raw_event={"source": "rest_snapshot"},
        )
        assert cache.book_source_of(TOKEN_A) == "rest_snapshot"

        ws = _make_ws(cache)
        event = json.dumps({
            "event_type": "best_bid_ask",
            "asset_id": TOKEN_A,
            "best_bid": "0.51",
            "best_ask": "0.54",
        })
        ws._handle_message(event)

        # Quote source updated by price update
        assert cache.quote_source_of(TOKEN_A) == "wss"
        # Book source must NOT have been promoted
        assert cache.book_source_of(TOKEN_A) == "rest_snapshot"

    @pytest.mark.asyncio
    async def test_market_resolved_prevents_execution(self) -> None:
        from services.market_data_service import MarketDataNotReady
        cache = MarketStateStore()
        _seed_executable_wss(cache, TOKEN_A)
        cache.on_market_resolved(TOKEN_A, {"event_type": "market_resolved"})

        mds = _make_mds(cache)

        with pytest.raises(MarketDataNotReady, match="resolved"):
            await mds.require_fresh_data(TOKEN_A)

    @pytest.mark.asyncio
    async def test_wss_fresh_bundle_source_is_wss(self) -> None:
        cache = MarketStateStore()
        _seed_executable_wss(cache, TOKEN_A)

        mds = _make_mds(cache)
        bundle = await mds.require_fresh_data(TOKEN_A)
        assert bundle.source == MarketDataSource.WSS
