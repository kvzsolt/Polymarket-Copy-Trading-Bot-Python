"""Focused TDD tests for Alchemy-only detection, per-user filtering, and WSS cost controls."""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal
from pathlib import Path

import pytest

from pmcopy.config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from pmcopy.config.app_config import EnvValidationError, load_config
from pmcopy.config.trader_config import TraderConfig
from pmcopy.domain.detection import DetectionMode
from pmcopy.domain.provider_budget import ProviderConfigurationError
from pmcopy.orchestrators.bootstrap.provider_runtime import ProviderRuntime


ALCHEMY_WSS = "wss://polygon-mainnet.g.alchemy.com/v2/test-alchemy-key-123"
ALCHEMY_HTTP = "https://polygon-mainnet.g.alchemy.com/v2/test-alchemy-key-123"
FUNDER = "0x" + "1" * 40
TRADER_EOA = "0xAbCdEf0123456789AbCdEf0123456789AbCdEf01"
TRADER_PROXY = "0x9988776655443322110099887766554433221100"


def _base_overrides(**extra) -> dict:
    base = {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": FUNDER},
        "detection": {
            "mode": "on_chain_logs_only",
            "onchain": {"alchemy_wss_url": ALCHEMY_WSS},
        },
    }
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = {**base[key], **value}
        else:
            base[key] = value
    return base


class TestSlice1ConfigModesAndTrackedUsers:
    """Slice 1: Exchange addresses, tracked-user normalization, mode & Alchemy-only config."""

    def test_v1_and_v2_exchange_address_selection(self) -> None:
        from pmcopy.config.addresses import (
            ALL_VERIFIED_EXCHANGE_ADDRESSES,
            POLYMARKET_CTF_EXCHANGE_V1_ADDRESS,
            POLYMARKET_NEG_RISK_EXCHANGE_V1_ADDRESS,
            verified_polymarket_exchange_addresses,
        )

        assert POLYMARKET_CTF_EXCHANGE_V1_ADDRESS == "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        assert (
            POLYMARKET_NEG_RISK_EXCHANGE_V1_ADDRESS == "0xC5d563A36AE78145C45a50134d48A1215220f80a"
        )

        v2_only = verified_polymarket_exchange_addresses(include_legacy_v1=False)
        assert v2_only == (
            POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
            POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
        )

        v2_and_v1 = verified_polymarket_exchange_addresses(include_legacy_v1=True)
        assert v2_and_v1 == (
            POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
            POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
            POLYMARKET_CTF_EXCHANGE_V1_ADDRESS.lower(),
            POLYMARKET_NEG_RISK_EXCHANGE_V1_ADDRESS.lower(),
        )
        assert len(ALL_VERIFIED_EXCHANGE_ADDRESSES) == 4

    def test_tracked_user_directory_normalizes_eoa_and_proxy_addresses(self) -> None:
        from pmcopy.domain.tracked_users import TrackedTraderAddresses, TrackedUserDirectory

        directory = TrackedUserDirectory.from_entries(
            [
                TrackedTraderAddresses(
                    leader_wallet=TRADER_PROXY,
                    eoa_address=TRADER_EOA,
                    proxy_address=TRADER_PROXY,
                )
            ]
        )
        eoa_hex40 = TRADER_EOA[2:].lower()
        proxy_hex40 = TRADER_PROXY[2:].lower()

        assert directory.hex40_without_prefix == frozenset({eoa_hex40, proxy_hex40})
        assert directory.padded_topics == tuple(
            sorted(
                (
                    "0x" + ("0" * 24) + eoa_hex40,
                    "0x" + ("0" * 24) + proxy_hex40,
                )
            )
        )
        assert all(len(topic) == 66 for topic in directory.padded_topics)
        # Both EOA and Proxy resolve back to the trader's canonical leader_wallet
        assert directory.canonical_leader_for(TRADER_EOA) == TRADER_PROXY.lower()
        assert directory.canonical_leader_for(TRADER_PROXY.upper().replace("0X", "0x")) == (
            TRADER_PROXY.lower()
        )
        assert directory.canonical_leader_for("0x" + "f" * 40) is None

    def test_trader_config_accepts_optional_eoa_and_proxy_addresses(self) -> None:
        trader = TraderConfig.model_validate(
            {
                "name": "whale_alpha",
                "wallet": TRADER_PROXY,
                "eoa_address": TRADER_EOA,
                "proxy_address": TRADER_PROXY,
            }
        )
        assert trader.eoa_address == TRADER_EOA
        assert trader.proxy_address == TRADER_PROXY

    @pytest.mark.parametrize(
        ("raw_mode", "expected"),
        [
            ("on_chain_logs_only", DetectionMode.ON_CHAIN_LOGS_ONLY),
            ("on_chain_pending", DetectionMode.ON_CHAIN_PENDING),
            ("confirmed+mempool_reserved", DetectionMode.CONFIRMED_MEMPOOL_RESERVED),
            (
                "on_chain_confirmed+mempool_reserved",
                DetectionMode.CONFIRMED_MEMPOOL_RESERVED,
            ),
        ],
    )
    def test_valid_detection_modes_and_alias_normalization(
        self, raw_mode: str, expected: DetectionMode, tmp_path: Path
    ) -> None:
        cfg = load_config(
            _base_overrides(
                detection={
                    "mode": raw_mode,
                    "onchain": {"alchemy_wss_url": ALCHEMY_WSS},
                }
            ),
            config_dir=tmp_path,
            secrets_path=tmp_path / "absent.env",
        )
        assert cfg.detection.detection_mode is expected
        assert cfg.detection.mode == expected.value
        assert cfg.polymarket.rpc_url == ALCHEMY_HTTP

    @pytest.mark.parametrize(
        "bad_mode",
        [
            "polling",
            "on_chain_confirmed",
            "newHeads",
            "new_heads",
            "infura",
            "quicknode",
            "generic",
            "hybrid",
        ],
    )
    def test_unsupported_detection_modes_fail_fast(self, bad_mode: str, tmp_path: Path) -> None:
        with pytest.raises(EnvValidationError, match="detection.mode"):
            load_config(
                _base_overrides(
                    detection={
                        "mode": bad_mode,
                        "onchain": {"alchemy_wss_url": ALCHEMY_WSS},
                    }
                ),
                config_dir=tmp_path,
                secrets_path=tmp_path / "absent.env",
            )

    @pytest.mark.parametrize(
        "bad_wss",
        [
            "",
            "https://polygon-mainnet.g.alchemy.com/v2/key",
            "ws://polygon-mainnet.g.alchemy.com/v2/key",
            "wss://polygon-mainnet.infura.io/ws/v3/key",
            "wss://holy-node.matic.quiknode.pro/key/",
            "wss://polygon-mainnet.g.alchemy.com/v2/",
            "wss://user:pass@polygon-mainnet.g.alchemy.com/v2/key",
            "wss://polygon-mainnet.g.alchemy.com/v2/key?foo=bar",
        ],
    )
    def test_non_alchemy_or_missing_wss_url_rejected_fail_closed(
        self, bad_wss: str, tmp_path: Path
    ) -> None:
        with pytest.raises(EnvValidationError):
            load_config(
                _base_overrides(
                    detection={
                        "mode": "on_chain_logs_only",
                        "onchain": {"alchemy_wss_url": bad_wss},
                    }
                ),
                config_dir=tmp_path,
                secrets_path=tmp_path / "absent.env",
            )

    def test_flat_pmcopy_env_keys_supported_in_secrets_and_process_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        secrets_file = tmp_path / "secrets.env"
        secrets_file.write_text(
            "\n".join(
                [
                    "PMCOPY__DATABASE__DSN=postgresql://test:test@localhost/test",
                    f"PMCOPY__WALLET__FUNDER_ADDRESS={FUNDER}",
                    f"PMCOPY__ALCHEMY_WSS_URL={ALCHEMY_WSS}",
                    "PMCOPY__DETECTION__MODE=on_chain_confirmed+mempool_reserved",
                    "PMCOPY__INCLUDE_LEGACY_V1_EXCHANGES=true",
                    "PMCOPY__TRACK_DIRECT_USER_TXS=true",
                    "PMCOPY__MAX_HOURLY_WS_MB=125.5",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        os.chmod(secrets_file, 0o600)
        monkeypatch.setenv("PMCOPY__MAX_HOURLY_WS_MB", "180")

        cfg = load_config(config_dir=tmp_path, secrets_path=secrets_file)
        assert cfg.detection.mode == "confirmed+mempool_reserved"
        assert cfg.detection.detection_mode is DetectionMode.CONFIRMED_MEMPOOL_RESERVED
        assert cfg.detection.onchain.alchemy_wss_url == ALCHEMY_WSS
        assert cfg.detection.onchain.include_legacy_v1_exchanges is True
        assert cfg.detection.onchain.track_direct_user_txs is True
        assert cfg.detection.onchain.max_hourly_ws_mb == Decimal("180")
        assert cfg.polymarket.rpc_url == ALCHEMY_HTTP

    def test_provider_runtime_accepts_derived_alchemy_and_rejects_infura(
        self, tmp_path: Path
    ) -> None:
        cfg = load_config(
            _base_overrides(
                provider_budget={"ledger_path": str(tmp_path / "budget.sqlite3")},
            ),
            config_dir=tmp_path,
            secrets_path=tmp_path / "absent.env",
        )
        runtime = ProviderRuntime.from_config(cfg)
        try:
            assert runtime.primary == "alchemy"
            assert runtime.endpoints.http_url("alchemy") == ALCHEMY_HTTP
            assert runtime.endpoints.websocket_url("alchemy") == ALCHEMY_WSS
            with pytest.raises(ProviderConfigurationError):
                runtime.endpoints.http_url("infura")
        finally:
            runtime.close()


class TestSlice2BandwidthMeterAndCircuitBreaker:
    """Slice 2: Exact UTF-8 byte & CU metering, 60s telemetry, and MAX_HOURLY_WS_MB breaker."""

    def test_exact_utf8_bytes_cu_math_and_60s_telemetry_snapshot(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging
        from pmcopy.services.onchain_detection.alchemy_ws_meter import AlchemyWsMeter

        clock_now = 1000.0
        meter = AlchemyWsMeter(
            max_hourly_ws_mb=Decimal("250"),
            mode_getter=lambda: DetectionMode.ON_CHAIN_LOGS_ONLY,
            active_subscriptions_getter=lambda: 2,
            clock=lambda: clock_now,
        )

        # Multi-byte UTF-8 string ("€" is 3 UTF-8 bytes) + raw bytes frame
        b1 = meter.record_frame('{"note":"€"}')  # 11 ASCII + 3 UTF-8 = 14 bytes
        b2 = meter.record_frame(b'{"id":1,"result":"0xabc"}')  # 25 bytes
        assert b1 == 14
        assert b2 == 25

        meter.record_reconnect()
        meter.record_matched_user_event(2)

        with caplog.at_level(logging.INFO):
            snap = meter.emit_telemetry_interval()

        assert snap.ws_frames_in == 2
        assert snap.ws_bytes_in == 39
        assert snap.estimated_cu_min == Decimal("39") * Decimal("0.04")
        assert snap.estimated_cu_day_projection == snap.estimated_cu_min * Decimal("1440")
        assert snap.active_subscriptions == 2
        assert snap.reconnect_count == 1
        assert snap.matched_user_events == 2
        assert any("alchemy_ws_telemetry" in rec.message for rec in caplog.records)
        log_line = next(
            rec.message for rec in caplog.records if "alchemy_ws_telemetry" in rec.message
        )
        for required_field in (
            "ws_frames_in=2",
            "ws_bytes_in=39",
            "estimated_cu_min=1.56",
            "estimated_cu_day_projection=2246.40",
            "active_subscriptions=2",
            "reconnect_count=1",
            "matched_user_events=2",
        ):
            assert required_field in log_line

    @pytest.mark.asyncio
    async def test_hourly_ws_mb_circuit_breaker_logs_critical_and_downgrades_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging
        from pmcopy.services.onchain_detection.alchemy_ws_meter import AlchemyWsMeter

        clock_now = 5000.0
        current_mode = DetectionMode.CONFIRMED_MEMPOOL_RESERVED
        downgrade_calls: list[int] = []

        async def _on_downgrade(rolling_bytes: int) -> None:
            nonlocal current_mode
            downgrade_calls.append(rolling_bytes)
            current_mode = DetectionMode.ON_CHAIN_LOGS_ONLY

        # Set small threshold: 0.001 MB = 1048 bytes
        meter = AlchemyWsMeter(
            max_hourly_ws_mb=Decimal("0.001"),
            mode_getter=lambda: current_mode,
            active_subscriptions_getter=lambda: 3,
            on_hourly_limit_exceeded=_on_downgrade,
            clock=lambda: clock_now,
        )

        # 600 bytes at t=5000 -> under 1048 bytes, does not trip
        meter.record_frame(b"x" * 600)
        await meter.check_and_enforce()
        assert downgrade_calls == []
        assert current_mode is DetectionMode.CONFIRMED_MEMPOOL_RESERVED

        # Advance 3,601s -> first 600-byte frame ages out of rolling hour window!
        clock_now += 3601.0
        meter.record_frame(b"x" * 600)
        await meter.check_and_enforce()
        assert downgrade_calls == []

        # Another 600 bytes within the same hour -> 1,200 > 1,048 bytes -> TRIPS!
        clock_now += 10.0
        with caplog.at_level(logging.CRITICAL):
            meter.record_frame(b"x" * 600)
            tripped = await meter.check_and_enforce()
            # Subsequent frame in degraded on_chain_logs_only mode must not double-trip
            meter.record_frame(b"x" * 600)
            tripped_again = await meter.check_and_enforce()

        assert tripped is True
        assert tripped_again is False
        assert downgrade_calls == [1200]
        assert current_mode is DetectionMode.ON_CHAIN_LOGS_ONLY
        assert any(
            rec.levelno == logging.CRITICAL
            and "alchemy_ws_bandwidth_circuit_breaker_tripped" in rec.message
            for rec in caplog.records
        )


class _FakeDuplexWs:
    """In-memory fake WebSocket supporting concurrent JSON-RPC send/recv on 1 socket."""

    def __init__(self) -> None:
        self.sent_frames: list[dict[str, object]] = []
        self._incoming: asyncio.Queue[str | bytes | None] = asyncio.Queue()
        self.closed = False
        self._next_sub_num = 0

    async def send(self, data: str | bytes) -> None:
        import json

        raw = data.decode("utf-8") if isinstance(data, bytes) else data
        payload = json.loads(raw)
        self.sent_frames.append(payload)
        method = payload.get("method")
        req_id = payload.get("id")
        if method == "eth_subscribe":
            self._next_sub_num += 1
            sub_id = f"0xsub{self._next_sub_num}"
            await self._incoming.put(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": sub_id}))
        elif method == "eth_unsubscribe":
            await self._incoming.put(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": True}))

    async def push_subscription_event(self, sub_id: str, result: dict[str, object]) -> None:
        import json

        await self._incoming.put(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {"subscription": sub_id, "result": result},
                }
            )
        )

    async def push_raw(self, raw: str | bytes) -> None:
        await self._incoming.put(raw)

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self._incoming.put(None)

    def __aiter__(self) -> "_FakeDuplexWs":
        return self

    async def __anext__(self) -> str | bytes:
        item = await self._incoming.get()
        if item is None:
            raise StopAsyncIteration
        return item


class _FakeConnectContext:
    def __init__(self, ws: _FakeDuplexWs, on_enter: object) -> None:
        self._ws = ws
        self._on_enter = on_enter

    async def __aenter__(self) -> _FakeDuplexWs:
        if callable(self._on_enter):
            self._on_enter()
        return self._ws

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        await self._ws.close()
        return False


class TestSlice3SingletonMultiplexWebSocket:
    """Slice 3: Singleton WebSocket invariant, demux, empty-roster idle, and 2s debounce."""

    @pytest.mark.asyncio
    async def test_singleton_connection_multiplexes_slots_and_meters_frames(self) -> None:
        from pmcopy.clients.ws.alchemy_multiplex_ws import (
            AlchemyMultiplexWebSocket,
            SlotSubscription,
        )
        from pmcopy.services.onchain_detection.alchemy_ws_meter import AlchemyWsMeter

        fake_ws = _FakeDuplexWs()
        connect_calls = 0

        def _connect_factory(url: str, **kwargs: object) -> _FakeConnectContext:
            nonlocal connect_calls
            assert url == ALCHEMY_WSS
            connect_calls += 1
            return _FakeConnectContext(fake_ws, None)

        maker_events: list[dict[str, object]] = []
        taker_events: list[dict[str, object]] = []
        pending_events: list[dict[str, object]] = []
        all_received = asyncio.Event()

        async def _on_maker(msg: dict[str, object]) -> None:
            maker_events.append(msg)
            if maker_events and taker_events and pending_events:
                all_received.set()

        async def _on_taker(msg: dict[str, object]) -> None:
            taker_events.append(msg)
            if maker_events and taker_events and pending_events:
                all_received.set()

        async def _on_pending(msg: dict[str, object]) -> None:
            pending_events.append(msg)
            if maker_events and taker_events and pending_events:
                all_received.set()

        mux: AlchemyMultiplexWebSocket | None = None
        meter = AlchemyWsMeter(
            max_hourly_ws_mb=Decimal("250"),
            mode_getter=lambda: DetectionMode.CONFIRMED_MEMPOOL_RESERVED,
            active_subscriptions_getter=lambda: (
                mux.active_subscription_count if mux is not None else 0
            ),
        )
        mux = AlchemyMultiplexWebSocket(
            ws_url=ALCHEMY_WSS,
            meter=meter,
            debounce_seconds=0.05,
            connect_factory=_connect_factory,
        )
        await mux.start(
            {
                "logs_maker": SlotSubscription(
                    params=["logs", {"topics": ["0xmaker"]}],
                    on_message=_on_maker,
                ),
                "logs_taker": SlotSubscription(
                    params=["logs", {"topics": ["0xtaker"]}],
                    on_message=_on_taker,
                ),
                "pending_exchange": SlotSubscription(
                    params=[
                        "alchemy_pendingTransactions",
                        {"toAddress": [POLYMARKET_CTF_EXCHANGE_V2_ADDRESS], "hashesOnly": False},
                    ],
                    on_message=_on_pending,
                ),
            }
        )
        try:
            await mux.wait_until_subscribed(wait_seconds=2.0)
            # STRICT INVARIANT: 3 subscriptions share 1 single WebSocket connection
            assert connect_calls == 1
            assert mux.active_subscription_count == 3
            assert "test-alchemy-key-123" not in repr(mux)
            assert "test-alchemy-key-123" not in mux.redacted_endpoint

            await fake_ws.push_subscription_event("0xsub1", {"kind": "maker_fill"})
            await fake_ws.push_subscription_event("0xsub2", {"kind": "taker_fill"})
            await fake_ws.push_subscription_event("0xsub3", {"kind": "pending_tx"})
            await asyncio.wait_for(all_received.wait(), timeout=2.0)

            assert len(maker_events) == 1 and maker_events[0]["kind"] == "maker_fill"
            assert "_source_receipt" in maker_events[0]
            assert len(taker_events) == 1 and taker_events[0]["kind"] == "taker_fill"
            assert len(pending_events) == 1 and pending_events[0]["kind"] == "pending_tx"

            snap = meter.emit_telemetry_interval()
            # 3 eth_subscribe responses + 3 subscription push frames = 6 frames metered
            assert snap.ws_frames_in == 6
            assert snap.ws_bytes_in > 0
            assert snap.active_subscriptions == 3

            # Immediate unsubscribe of pending_exchange preserves the 2 logs subs on same socket
            await mux.unsubscribe_slot("pending_exchange")
            assert connect_calls == 1
            assert mux.active_subscription_count == 2
            unsub_calls = [f for f in fake_ws.sent_frames if f.get("method") == "eth_unsubscribe"]
            assert len(unsub_calls) == 1
            assert unsub_calls[0]["params"] == ["0xsub3"]
        finally:
            await mux.stop()

    @pytest.mark.asyncio
    async def test_empty_startup_stays_idle_and_debounces_dynamic_resubscriptions(self) -> None:
        from pmcopy.clients.ws.alchemy_multiplex_ws import (
            AlchemyMultiplexWebSocket,
            SlotSubscription,
        )

        fake_ws = _FakeDuplexWs()
        connect_calls = 0

        def _connect_factory(url: str, **kwargs: object) -> _FakeConnectContext:
            nonlocal connect_calls
            connect_calls += 1
            return _FakeConnectContext(fake_ws, None)

        async def _noop(msg: dict[str, object]) -> None:
            return None

        mux = AlchemyMultiplexWebSocket(
            ws_url=ALCHEMY_WSS,
            debounce_seconds=0.06,
            connect_factory=_connect_factory,
        )
        # Empty desired subscriptions at startup -> zero WS connections, zero eth_subscribe
        await mux.start({})
        try:
            await asyncio.sleep(0.03)
            assert connect_calls == 0
            assert mux.active_subscription_count == 0
            assert fake_ws.sent_frames == []

            # First trader added -> reconcile_now opens the single WS and subscribes
            await mux.reconcile_now(
                {
                    "logs_maker": SlotSubscription(
                        params=["logs", {"topics": ["0xt0", None, ["0xuser1"]]}],
                        on_message=_noop,
                    ),
                    "logs_taker": SlotSubscription(
                        params=["logs", {"topics": ["0xt0", None, None, ["0xuser1"]]}],
                        on_message=_noop,
                    ),
                }
            )
            assert connect_calls == 1
            assert mux.active_subscription_count == 2
            assert len(fake_ws.sent_frames) == 2

            # Rapid burst of 4 trader roster updates within debounce window -> coalesced to ONE update!
            for idx in range(2, 6):
                users = [f"0xuser{n}" for n in range(1, idx + 1)]
                mux.schedule_reconcile(
                    {
                        "logs_maker": SlotSubscription(
                            params=["logs", {"topics": ["0xt0", None, users]}],
                            on_message=_noop,
                        ),
                        "logs_taker": SlotSubscription(
                            params=["logs", {"topics": ["0xt0", None, None, users]}],
                            on_message=_noop,
                        ),
                    }
                )
                await asyncio.sleep(0.01)

            # Wait for 60ms debounce + RPC completion
            await asyncio.sleep(0.12)
            assert connect_calls == 1
            assert mux.active_subscription_count == 2

            methods = [f.get("method") for f in fake_ws.sent_frames]
            # Initial 2 eth_subscribe + 1 coalesced zero-gap update (subscribe new -> unsubscribe old per slot) = 6 total
            assert methods == [
                "eth_subscribe",
                "eth_subscribe",
                "eth_subscribe",
                "eth_unsubscribe",
                "eth_subscribe",
                "eth_unsubscribe",
            ]
            # Final eth_subscribe frame (-2, right before old sub's eth_unsubscribe) carries all 5 coalesced users
            last_sub_params = fake_ws.sent_frames[-2]["params"]
            assert isinstance(last_sub_params, list)
            assert last_sub_params[1]["topics"][3] == [
                "0xuser1",
                "0xuser2",
                "0xuser3",
                "0xuser4",
                "0xuser5",
            ]
        finally:
            await mux.stop()


class TestSlice4PerUserLogsFilterAndDeduplication:
    """Slice 4: Strict server-side maker/taker topic filters, V2+V1 log decode, 16,384 LRU."""

    def test_unfiltered_log_subscription_removed_and_dual_role_topics_exact(self) -> None:
        from pmcopy.config.addresses import (
            POLYMARKET_CTF_EXCHANGE_V1_ADDRESS,
            POLYMARKET_NEG_RISK_EXCHANGE_V1_ADDRESS,
            verified_polymarket_exchange_addresses,
        )
        from pmcopy.domain.tracked_users import (
            TrackedTraderAddresses,
            TrackedUserDirectory,
        )
        from pmcopy.services.onchain_detection import order_filled_detector

        # Unfiltered firehose helper must NOT exist
        assert not hasattr(order_filled_detector, "log_subscription_params")

        build_subs = order_filled_detector.build_user_filtered_log_subscriptions
        # Empty user set -> zero subscriptions opened
        assert (
            build_subs(
                verified_polymarket_exchange_addresses(include_legacy_v1=False),
                (),
            )
            == ()
        )

        directory = TrackedUserDirectory.from_entries(
            [
                TrackedTraderAddresses(
                    leader_wallet=TRADER_EOA,
                    eoa_address=TRADER_EOA,
                    proxy_address=TRADER_PROXY,
                )
            ]
        )
        v2_exchanges = verified_polymarket_exchange_addresses(include_legacy_v1=False)
        maker_sub, taker_sub = build_subs(
            v2_exchanges,
            directory.padded_topics,
            include_legacy_v1=False,
        )
        v2_topic0 = "0x" + order_filled_detector.ORDER_FILLED_V2_TOPIC0.hex()
        expected_addrs = [a.lower() for a in v2_exchanges]
        expected_users = list(directory.padded_topics)
        assert maker_sub == [
            "logs",
            {
                "address": expected_addrs,
                "topics": [[v2_topic0], None, expected_users],
            },
        ]
        assert taker_sub == [
            "logs",
            {
                "address": expected_addrs,
                "topics": [[v2_topic0], None, None, expected_users],
            },
        ]

        # Enabling V1 adds V1 exchanges and V1 OrderFilled topic0
        all_exchanges = verified_polymarket_exchange_addresses(include_legacy_v1=True)
        maker_v1, taker_v1 = build_subs(
            all_exchanges,
            directory.padded_topics,
            include_legacy_v1=True,
        )
        v1_topic0 = "0x" + order_filled_detector.ORDER_FILLED_V1_TOPIC0.hex()
        assert POLYMARKET_CTF_EXCHANGE_V1_ADDRESS.lower() in maker_v1[1]["address"]
        assert POLYMARKET_NEG_RISK_EXCHANGE_V1_ADDRESS.lower() in taker_v1[1]["address"]
        assert maker_v1[1]["topics"][0] == [v2_topic0, v1_topic0]
        assert taker_v1[1]["topics"][0] == [v2_topic0, v1_topic0]

        # Reject unverified exchange address fail-closed
        with pytest.raises(ValueError, match="unverified"):
            build_subs(
                ("0x" + "d" * 40,),
                directory.padded_topics,
            )

    def test_v2_and_v1_decode_proxy_alias_mapping_and_16384_lru_dedup(self) -> None:
        from pmcopy.config.addresses import POLYMARKET_CTF_EXCHANGE_V1_ADDRESS
        from pmcopy.domain.tracked_users import (
            TrackedTraderAddresses,
            TrackedUserDirectory,
        )
        from pmcopy.services.onchain_detection.mined_fill_deduplicator import (
            DEFAULT_LOG_LRU_CAPACITY,
            LogEventLruDeduplicator,
        )
        from pmcopy.services.onchain_detection.order_filled_detector import (
            ORDER_FILLED_V1_TOPIC0,
            ORDER_FILLED_V2_TOPIC0,
            build_signals_from_log_for_directory,
            decode_order_filled_log,
        )

        assert DEFAULT_LOG_LRU_CAPACITY == 16_384
        lru = LogEventLruDeduplicator()
        assert lru.max_entries == 16_384

        # Verify true LRU eviction (accessing key0 refreshes it so key1 is evicted instead)
        small_lru = LogEventLruDeduplicator(max_entries=2)
        tx_a = "0x" + "aa" * 32
        assert small_lru.is_duplicate(tx_a, 0) is False
        assert small_lru.is_duplicate(tx_a, 1) is False
        assert small_lru.is_duplicate(tx_a, 0) is True  # refreshes (tx_a, 0) to MRU
        assert small_lru.is_duplicate(tx_a, 2) is False  # evicts (tx_a, 1), NOT (tx_a, 0)
        assert small_lru.is_duplicate(tx_a, 0) is True
        assert small_lru.is_duplicate(tx_a, 1) is False

        trader2_eoa = "0x" + "22" * 20
        directory = TrackedUserDirectory.from_entries(
            [
                TrackedTraderAddresses(
                    leader_wallet=TRADER_EOA,
                    eoa_address=TRADER_EOA,
                    proxy_address=TRADER_PROXY,
                ),
                TrackedTraderAddresses(leader_wallet=trader2_eoa),
            ]
        )

        # V2 fill where maker = Trader1 Proxy (SELL 100 shares for 55 USDC) and taker = Trader2 EOA (BUY)
        words_v2 = [
            1,  # maker_side = SELL
            777888,  # tokenId
            100_000_000,  # makerAmountFilled (100 shares)
            55_000_000,  # takerAmountFilled (55 USDC)
            0,
            0,
            0,
        ]
        raw_v2_log = {
            "address": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            "topics": [
                "0x" + ORDER_FILLED_V2_TOPIC0.hex(),
                "0x" + "cc" * 32,
                "0x" + TRADER_PROXY[2:].lower().rjust(64, "0"),
                "0x" + trader2_eoa[2:].lower().rjust(64, "0"),
            ],
            "data": "0x" + "".join(f"{w:064x}" for w in words_v2),
            "transactionHash": "0x" + "ab" * 32,
            "logIndex": "0x5",
            "blockNumber": "0x100",
        }

        # Arrives on logs_maker first -> processed once; second copy on logs_taker -> LRU drops!
        assert lru.is_duplicate_raw_log(raw_v2_log) is False
        assert lru.is_duplicate_raw_log(raw_v2_log) is True

        decoded_v2 = decode_order_filled_log(raw_v2_log)
        assert decoded_v2 is not None
        signals = build_signals_from_log_for_directory(decoded_v2, directory, event_ts=1700000000)
        # Only canonical maker leg (topics[2]) emits here; Trader2 emits on its own taker-summary log!
        assert len(signals) == 1
        assert signals[0].leader_wallet == TRADER_EOA.lower()
        assert signals[0].watched_role == "maker"
        assert signals[0].side == "SELL"
        assert signals[0].size == Decimal("100")
        assert signals[0].usdc_size == Decimal("55")
        assert signals[0].price == Decimal("0.55")

        # Canonical taker-summary log for Trader2 (topics[2] = trader2_eoa, topics[3] = exchange)
        summary_words_v2 = [0, 777888, 55_000_000, 100_000_000, 0, 0, 0]
        raw_summary_log = {
            **raw_v2_log,
            "topics": [
                "0x" + ORDER_FILLED_V2_TOPIC0.hex(),
                "0x" + "ce" * 32,
                "0x" + trader2_eoa[2:].lower().rjust(64, "0"),
                "0x" + POLYMARKET_CTF_EXCHANGE_V2_ADDRESS[2:].lower().rjust(64, "0"),
            ],
            "data": "0x" + "".join(f"{w:064x}" for w in summary_words_v2),
            "logIndex": "0x6",
        }
        decoded_summary = decode_order_filled_log(raw_summary_log)
        assert decoded_summary is not None
        summary_signals = build_signals_from_log_for_directory(
            decoded_summary, directory, event_ts=1700000000
        )
        assert len(summary_signals) == 1
        assert summary_signals[0].leader_wallet == trader2_eoa.lower()
        assert summary_signals[0].side == "BUY"
        assert summary_signals[0].size == Decimal("100")

        # V1 OrderFilled log (5 words: makerAssetId=0 -> BUY, takerAssetId=999111, makerFilled=40 USDC, takerFilled=80 shares, fee=0)
        words_v1 = [0, 999111, 40_000_000, 80_000_000, 0]
        raw_v1_log = {
            "address": POLYMARKET_CTF_EXCHANGE_V1_ADDRESS,
            "topics": [
                "0x" + ORDER_FILLED_V1_TOPIC0.hex(),
                "0x" + "dd" * 32,
                "0x" + TRADER_PROXY[2:].lower().rjust(64, "0"),
                "0x" + "ff" * 20,  # Wait: pad to 64 hex chars below
            ],
            "data": "0x" + "".join(f"{w:064x}" for w in words_v1),
            "transactionHash": "0x" + "cd" * 32,
            "logIndex": "0x1",
            "blockNumber": "0x101",
        }
        raw_v1_log["topics"][3] = "0x" + ("ff" * 20).rjust(64, "0")

        # Disabled when include_legacy_v1=False, decoded when include_legacy_v1=True
        assert decode_order_filled_log(raw_v1_log, include_legacy_v1=False) is None
        decoded_v1 = decode_order_filled_log(raw_v1_log, include_legacy_v1=True)
        assert decoded_v1 is not None
        assert decoded_v1.token_id == 999111
        signals_v1 = build_signals_from_log_for_directory(
            decoded_v1, directory, event_ts=1700000005
        )
        assert len(signals_v1) == 1
        assert signals_v1[0].leader_wallet == TRADER_EOA.lower()
        assert signals_v1[0].side == "BUY"
        assert signals_v1[0].size == Decimal("80")
        assert signals_v1[0].usdc_size == Decimal("40")
        assert signals_v1[0].price == Decimal("0.5")


class TestSlice5MempoolSafetyAndFastHexPrefilter:
    """Slice 5: Hard toAddress<=4 Exchange lock, direct-tx toggle, tx.hash TTL, zero-alloc hex filter."""

    def test_pending_subscription_hard_toaddress_assertions_and_direct_tx_flag(self) -> None:
        from pmcopy.config.addresses import verified_polymarket_exchange_addresses
        from pmcopy.services.onchain_detection.pending_tx_detector import (
            build_direct_user_pending_subscriptions,
            build_exchange_pending_subscription_params,
        )

        v2 = verified_polymarket_exchange_addresses(include_legacy_v1=False)
        sub = build_exchange_pending_subscription_params(v2)
        assert sub == [
            "alchemy_pendingTransactions",
            {
                "toAddress": [a.lower() for a in v2],
                "hashesOnly": False,
            },
        ]
        assert "fromAddress" not in sub[1]

        v1_and_v2 = verified_polymarket_exchange_addresses(include_legacy_v1=True)
        sub4 = build_exchange_pending_subscription_params(v1_and_v2)
        assert len(sub4[1]["toAddress"]) == 4

        # Refuse empty toAddress (prevents full Polygon mempool billing disaster!)
        with pytest.raises(ValueError, match="non-empty"):
            build_exchange_pending_subscription_params(())

        # Refuse unverified contract address
        with pytest.raises(ValueError, match="unverified"):
            build_exchange_pending_subscription_params(("0x" + "ee" * 20,))

        # Refuse > 4 addresses even if duplicated/padded
        with pytest.raises(ValueError, match="at most 4"):
            build_exchange_pending_subscription_params((*v1_and_v2, "0x" + "ab" * 20))

        # Direct user pending tx tracking is OFF by default, chunked <= 1,000 when explicitly ON
        many_users = [f"0x{i:040x}" for i in range(1, 1206)]
        assert build_direct_user_pending_subscriptions(many_users) == ()
        assert (
            build_direct_user_pending_subscriptions(many_users, track_direct_user_txs=False) == ()
        )
        direct_subs = build_direct_user_pending_subscriptions(
            many_users, track_direct_user_txs=True
        )
        assert len(direct_subs) == 2
        assert direct_subs[0][0] == "alchemy_pendingTransactions"
        assert len(direct_subs[0][1]["fromAddress"]) == 1000
        assert len(direct_subs[1][1]["fromAddress"]) == 205
        assert direct_subs[0][1]["hashesOnly"] is False

    def test_zero_alloc_hex_prefilter_skips_abi_decode_and_deduplicates_tx_hash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from eth_abi import encode
        from pmcopy.config.addresses import POLYMARKET_CTF_EXCHANGE_V1_ADDRESS
        from pmcopy.domain.tracked_users import (
            TrackedTraderAddresses,
            TrackedUserDirectory,
        )
        from pmcopy.services.onchain_detection import calldata_decoder
        from pmcopy.services.onchain_detection.pending_tx_detector import (
            PendingTxHashCache,
            build_signals_from_pending_tx_for_directory,
        )

        real_abi_decode = calldata_decoder.abi_decode
        decode_calls = 0

        def _counting_abi_decode(*args: object, **kwargs: object) -> object:
            nonlocal decode_calls
            decode_calls += 1
            return real_abi_decode(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(calldata_decoder, "abi_decode", _counting_abi_decode)

        directory = TrackedUserDirectory.from_entries(
            [
                TrackedTraderAddresses(
                    leader_wallet=TRADER_EOA,
                    eoa_address=TRADER_EOA,
                    proxy_address=TRADER_PROXY,
                )
            ]
        )

        order_tuple_v2 = (
            "(uint256,address,address,uint256,uint256,uint256,"
            "uint8,uint8,uint256,bytes32,bytes32,bytes)"
        )
        match_orders_v2_type = (
            f"(bytes32,{order_tuple_v2},{order_tuple_v2}[],uint256,uint256[],uint256,uint256[])"
        )

        def _v2_order(maker: str, signer: str, *, side: int = 0) -> tuple[object, ...]:
            return (
                1,
                maker,
                signer,
                555666,
                25_000_000,  # 25 USDC
                50_000_000,  # 50 shares
                side,
                2,
                1700000000,
                b"\x00" * 32,
                b"\x00" * 32,
                b"\x11\x22\x33\x44",
            )

        untracked_a = "0x" + "aa" * 20
        untracked_b = "0x" + "bb" * 20
        untracked_calldata = calldata_decoder.MATCH_ORDERS_SELECTOR + encode(
            [match_orders_v2_type],
            [
                (
                    b"\x99" * 32,
                    _v2_order(untracked_a, untracked_a),
                    [_v2_order(untracked_b, untracked_b, side=1)],
                    25_000_000,
                    [50_000_000],
                    0,
                    [0],
                )
            ],
        )

        now_mono = 100.0
        tx_cache = PendingTxHashCache(max_entries=1024, ttl_seconds=60.0, clock=lambda: now_mono)

        # 1. Non-matchOrders selector -> dropped immediately, 0 ABI decode calls
        assert (
            calldata_decoder.calldata_matches_tracked_users(
                "0xa9059cbb" + TRADER_PROXY[2:].lower().rjust(64, "0"),
                directory.hex40_without_prefix,
            )
            is False
        )
        assert decode_calls == 0

        # 2. Irrelevant matchOrders tx -> dropped by hex scan, ZERO ABI decode calls!
        irrelevant_tx = {
            "hash": "0x" + "10" * 32,
            "to": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            "from": "0x" + "ff" * 20,  # Polymarket operator relayer, NOT user!
            "input": "0x" + untracked_calldata.hex().upper(),
        }
        assert (
            build_signals_from_pending_tx_for_directory(
                irrelevant_tx, directory, tx_hash_cache=tx_cache, recv_ts_ms=1700000100
            )
            == []
        )
        assert decode_calls == 0

        # 3. Relevant matchOrders tx where Proxy is maker (and operator is tx.from) -> passes & decodes!
        tracked_calldata = calldata_decoder.MATCH_ORDERS_SELECTOR + encode(
            [match_orders_v2_type],
            [
                (
                    b"\x77" * 32,
                    _v2_order(untracked_a, untracked_a, side=1),
                    [_v2_order(TRADER_PROXY, TRADER_EOA, side=0)],
                    50_000_000,
                    [25_000_000],
                    0,
                    [0],
                )
            ],
        )
        relevant_tx = {
            "hash": "0x" + "20" * 32,
            "to": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            "from": "0x" + "ff" * 20,
            "input": "0x" + tracked_calldata.hex(),
        }
        signals = build_signals_from_pending_tx_for_directory(
            relevant_tx, directory, tx_hash_cache=tx_cache, recv_ts_ms=1700000200
        )
        assert decode_calls == 1
        assert len(signals) == 1
        assert signals[0].leader_wallet == TRADER_EOA.lower()
        assert signals[0].provisional is True
        assert signals[0].side == "BUY"
        assert signals[0].usdc_size == Decimal("25")
        assert signals[0].size == Decimal("50")

        # 4. Rebroadcast of same pending tx.hash within TTL -> dropped BEFORE ABI decode!
        rebroadcast = build_signals_from_pending_tx_for_directory(
            relevant_tx, directory, tx_hash_cache=tx_cache, recv_ts_ms=1700000205
        )
        assert rebroadcast == []
        assert decode_calls == 1  # Still 1!

        # Advance past 60s TTL -> hash expires from cache
        now_mono += 61.0
        after_ttl = build_signals_from_pending_tx_for_directory(
            relevant_tx, directory, tx_hash_cache=tx_cache, recv_ts_ms=1700062000
        )
        assert len(after_ttl) == 1
        assert decode_calls == 2

        # 5. Safe execTransaction wrapping matchOrders (+8 hex-char inner word offset) matches pre-filter!
        safe_types = [
            "address",
            "uint256",
            "bytes",
            "uint8",
            "uint256",
            "uint256",
            "uint256",
            "address",
            "address",
            "bytes",
        ]
        safe_wrapped = calldata_decoder.SAFE_EXEC_TRANSACTION_SELECTOR + encode(
            safe_types,
            [
                POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
                0,
                tracked_calldata,
                0,
                0,
                0,
                0,
                "0x" + "00" * 20,
                "0x" + "00" * 20,
                b"\xaa" * 65,
            ],
        )
        assert (
            calldata_decoder.calldata_matches_tracked_users(
                "0x" + safe_wrapped.hex(), directory.hex40_without_prefix
            )
            is True
        )

        # 6. Legacy V1 matchOrders (4-arg 0xe60f0c05) decoded when include_legacy_v1=True
        v1_order_tuple = (
            "(uint256,address,address,address,uint256,uint256,uint256,"
            "uint256,uint256,uint256,uint8,uint8,bytes)"
        )
        v1_taker = (
            1,
            TRADER_PROXY,
            TRADER_EOA,
            "0x" + "00" * 20,
            888999,
            30_000_000,
            60_000_000,
            0,
            0,
            0,
            0,  # BUY
            2,
            b"\x55" * 65,
        )
        v1_maker = (
            2,
            untracked_b,
            untracked_b,
            "0x" + "00" * 20,
            888999,
            60_000_000,
            30_000_000,
            0,
            0,
            0,
            1,  # SELL
            0,
            b"\x66" * 65,
        )
        v1_calldata = calldata_decoder.MATCH_ORDERS_V1_4ARG_SELECTOR + encode(
            [v1_order_tuple, f"{v1_order_tuple}[]", "uint256", "uint256[]"],
            [v1_taker, [v1_maker], 30_000_000, [60_000_000]],
        )
        v1_tx = {
            "hash": "0x" + "30" * 32,
            "to": POLYMARKET_CTF_EXCHANGE_V1_ADDRESS,
            "from": "0x" + "ff" * 20,
            "input": "0x" + v1_calldata.hex(),
        }
        assert (
            build_signals_from_pending_tx_for_directory(v1_tx, directory, include_legacy_v1=False)
            == []
        )
        v1_signals = build_signals_from_pending_tx_for_directory(
            v1_tx, directory, include_legacy_v1=True, recv_ts_ms=1700070000
        )
        assert len(v1_signals) == 1
        assert v1_signals[0].leader_wallet == TRADER_EOA.lower()
        assert v1_signals[0].token_id == "888999"
        assert v1_signals[0].side == "BUY"
        assert v1_signals[0].usdc_size == Decimal("30")
        assert v1_signals[0].size == Decimal("60")


class TestSlice6StateMachineAutoDowngradeAndRuntime:
    """Slice 6: confirmed+mempool_reserved state machine, AlchemyDetectionRuntime, auto-downgrade."""

    def test_mempool_reservation_state_machine_promote_and_ghost_fill_expiry(self) -> None:
        from pmcopy.services.onchain_detection.mempool_reservation_state import (
            MempoolReservationStateMachine,
            ReservationStatus,
        )
        from pmcopy.services.onchain_detection.trade_detection_models import (
            DetectionConfidence,
            OnchainTradeSignal,
        )

        now = 1000.0
        expired_callbacks: list[str] = []
        sm = MempoolReservationStateMachine(
            ttl_seconds=15.0,
            clock=lambda: now,
            on_ghost_fill_expired=lambda r: expired_callbacks.append(r.pending_signal.tx_hash),
        )

        pending_sig = OnchainTradeSignal(
            leader_wallet=TRADER_EOA.lower(),
            token_id="555666",
            side="BUY",
            size=Decimal("50"),
            price=Decimal("0.5"),
            usdc_size=Decimal("25"),
            tx_hash="0x" + "aa" * 32,
            confidence=DetectionConfidence.PENDING_PROBABLE,
            maker_side=0,
            watched_role="maker",
            event_ts=1700000000,
            log_index=None,
            condition_id="0x" + "77" * 32,
            provisional=True,
            exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
            order_hash=None,
        )

        # 1. Pending sighting creates MEMPOOL_RESERVED once; duplicate sighting is idempotent
        res1, created1 = sm.reserve_pending(pending_sig)
        res2, created2 = sm.reserve_pending(pending_sig)
        assert created1 is True
        assert created2 is False
        assert res1 is res2
        assert res1.status is ReservationStatus.MEMPOOL_RESERVED
        assert sm.active_reservation_count == 1

        # 2. Confirmed OrderFilled log within 15s promotes MEMPOOL_RESERVED -> CONFIRMED once
        now += 4.0
        confirmed_sig = OnchainTradeSignal(
            leader_wallet=TRADER_EOA.lower(),
            token_id="555666",
            side="BUY",
            size=Decimal("50"),
            price=Decimal("0.5"),
            usdc_size=Decimal("25"),
            tx_hash="0x" + "aa" * 32,
            confidence=DetectionConfidence.MINED_AUTHORITATIVE,
            maker_side=0,
            watched_role="maker",
            event_ts=1700004000,
            log_index=3,
            condition_id="",  # Empty on log: state machine enriches from pre-warmed pending!
            provisional=False,
            exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
            order_hash="0x" + "cc" * 32,
        )
        decision = sm.promote_confirmed(confirmed_sig)
        assert decision.should_dispatch is True
        assert decision.promoted_from_mempool is True
        assert decision.signal.condition_id == "0x" + "77" * 32
        assert res1.status is ReservationStatus.CONFIRMED
        assert sm.active_reservation_count == 0

        # Duplicate confirmed log must NOT dispatch twice
        dup_decision = sm.promote_confirmed(confirmed_sig)
        assert dup_decision.should_dispatch is False

        # 3. Ghost Fill: reverted/dropped mempool tx expires cleanly after 15s TTL
        ghost_sig = OnchainTradeSignal(
            leader_wallet=TRADER_EOA.lower(),
            token_id="999000",
            side="SELL",
            size=Decimal("100"),
            price=Decimal("0.6"),
            usdc_size=Decimal("60"),
            tx_hash="0x" + "bb" * 32,
            confidence=DetectionConfidence.PENDING_PROBABLE,
            maker_side=1,
            watched_role="taker",
            event_ts=1700010000,
            log_index=None,
            condition_id="0x" + "88" * 32,
            provisional=True,
            exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
            order_hash=None,
        )
        ghost_res, created_ghost = sm.reserve_pending(ghost_sig)
        assert created_ghost is True
        now += 15.5
        expired = sm.expire_stale()
        assert len(expired) == 1
        assert ghost_res.status is ReservationStatus.EXPIRED_GHOST_FILL
        assert expired_callbacks == ["0x" + "bb" * 32]
        assert sm.expired_ghost_fill_count == 1
        assert sm.active_reservation_count == 0

    @pytest.mark.asyncio
    async def test_alchemy_detection_runtime_singleton_promote_and_circuit_breaker_downgrade(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging
        from eth_abi import encode
        from pmcopy.orchestrators.detection.alchemy_detection_runtime import AlchemyDetectionRuntime
        from pmcopy.services.onchain_detection import calldata_decoder
        from pmcopy.services.onchain_detection.order_filled_detector import (
            ORDER_FILLED_V2_TOPIC0,
        )
        from pmcopy.services.onchain_detection.trade_detection_models import (
            OnchainTradeSignal,
        )

        fake_ws = _FakeDuplexWs()
        connect_calls = 0

        def _connect_factory(url: str, **kwargs: object) -> _FakeConnectContext:
            nonlocal connect_calls
            connect_calls += 1
            return _FakeConnectContext(fake_ws, None)

        dispatched: list[OnchainTradeSignal] = []
        dispatched_event = asyncio.Event()

        class _Sink:
            async def dispatch(self, signal: OnchainTradeSignal) -> None:
                dispatched.append(signal)
                dispatched_event.set()

        cfg = load_config(
            overrides=_base_overrides(
                detection={
                    "mode": "on_chain_confirmed+mempool_reserved",
                    "onchain": {
                        "alchemy_wss_url": ALCHEMY_WSS,
                        "max_hourly_ws_mb": "0.006",  # ~6,291 bytes ceiling for test
                        "subscription_debounce_seconds": 0.05,
                    },
                },
                copy_trading={
                    "wallets": [
                        {
                            "wallet": TRADER_EOA,
                            "eoa_address": TRADER_EOA,
                            "proxy_address": TRADER_PROXY,
                        }
                    ]
                },
            )
        )

        # Start with empty roster -> ZERO WS connections opened!
        runtime = AlchemyDetectionRuntime.compose(
            config=cfg,
            wallets=(),
            sink=_Sink(),
            use_default_condition_resolver=False,
            connect_factory=_connect_factory,
            telemetry_interval_seconds=60.0,
        )
        await runtime.start()
        try:
            await asyncio.sleep(0.03)
            assert connect_calls == 0
            assert runtime.ws.active_subscription_count == 0
            assert runtime.effective_mode is DetectionMode.CONFIRMED_MEMPOOL_RESERVED

            # Dynamically add TRADER_EOA -> 1 WS connection opened with 3 subscriptions
            runtime.update_watched_wallets([TRADER_EOA])
            await asyncio.sleep(0.12)
            assert connect_calls == 1
            assert runtime.ws.active_subscription_count == 3
            assert runtime.ws.active_slots == frozenset(
                {"logs_maker", "logs_taker", "pending_exchange"}
            )

            # Find subscription IDs assigned to each slot from sent frames
            sub_frames = [f for f in fake_ws.sent_frames if f.get("method") == "eth_subscribe"]
            assert len(sub_frames) == 3
            maker_sub_id = "0xsub1"
            taker_sub_id = "0xsub2"
            pending_sub_id = "0xsub3"

            # 1. Push pending matchOrders for TRADER_PROXY -> reserves, does NOT dispatch!
            order_tuple_v2 = (
                "(uint256,address,address,uint256,uint256,uint256,"
                "uint8,uint8,uint256,bytes32,bytes32,bytes)"
            )
            match_orders_v2_type = (
                f"(bytes32,{order_tuple_v2},{order_tuple_v2}[],uint256,uint256[],uint256,uint256[])"
            )
            untracked = "0x" + "44" * 20
            calldata = calldata_decoder.MATCH_ORDERS_SELECTOR + encode(
                [match_orders_v2_type],
                [
                    (
                        b"\x55" * 32,
                        (
                            1,
                            untracked,
                            untracked,
                            333444,
                            40_000_000,
                            20_000_000,
                            1,
                            0,
                            1700000000,
                            b"\x00" * 32,
                            b"\x00" * 32,
                            b"\x11" * 4,
                        ),
                        [
                            (
                                2,
                                TRADER_PROXY,
                                TRADER_EOA,
                                333444,
                                20_000_000,
                                40_000_000,
                                0,
                                2,
                                1700000000,
                                b"\x00" * 32,
                                b"\x00" * 32,
                                b"\x22" * 4,
                            )
                        ],
                        40_000_000,
                        [20_000_000],
                        0,
                        [0],
                    )
                ],
            )
            tx_hash = "0x" + "ef" * 32
            await fake_ws.push_subscription_event(
                pending_sub_id,
                {
                    "hash": tx_hash,
                    "to": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
                    "from": "0x" + "99" * 20,
                    "input": "0x" + calldata.hex(),
                },
            )
            await asyncio.sleep(0.05)
            assert dispatched == []
            assert runtime.reservation_state.active_reservation_count == 1

            # 2. Push matching confirmed OrderFilled log on BOTH maker_sub_id and taker_sub_id
            words = [0, 333444, 20_000_000, 40_000_000, 0, 0, 0]
            mined_log = {
                "address": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
                "topics": [
                    "0x" + ORDER_FILLED_V2_TOPIC0.hex(),
                    "0x" + "12" * 32,
                    "0x" + TRADER_PROXY[2:].lower().rjust(64, "0"),
                    "0x" + untracked[2:].rjust(64, "0"),
                ],
                "data": "0x" + "".join(f"{w:064x}" for w in words),
                "transactionHash": tx_hash,
                "logIndex": "0x2",
                "blockNumber": "0x200",
            }
            await fake_ws.push_subscription_event(maker_sub_id, mined_log)
            await fake_ws.push_subscription_event(taker_sub_id, mined_log)
            await asyncio.wait_for(dispatched_event.wait(), timeout=2.0)
            await asyncio.sleep(0.03)

            # Dispatched ONCE with canonical TRADER_EOA and pre-warmed condition_id!
            assert len(dispatched) == 1
            assert dispatched[0].leader_wallet == TRADER_EOA.lower()
            assert dispatched[0].provisional is False
            assert dispatched[0].condition_id == "0x" + "55" * 32
            assert runtime.reservation_state.active_reservation_count == 0

            # 3. Breach PMCOPY__MAX_HOURLY_WS_MB -> CRITICAL alert + eth_unsubscribe pending + degrade to on_chain_logs_only!
            with caplog.at_level(logging.CRITICAL):
                await fake_ws.push_subscription_event(
                    pending_sub_id,
                    {"hash": "0x" + "77" * 32, "padding": "X" * 5000},
                )
                for _ in range(40):
                    if runtime.effective_mode is DetectionMode.ON_CHAIN_LOGS_ONLY:
                        break
                    await asyncio.sleep(0.02)

            assert runtime.effective_mode is DetectionMode.ON_CHAIN_LOGS_ONLY
            assert connect_calls == 1
            assert runtime.ws.active_slots == frozenset({"logs_maker", "logs_taker"})
            assert runtime.ws.active_subscription_count == 2
            assert any(
                rec.levelno == logging.CRITICAL
                and "alchemy_ws_bandwidth_circuit_breaker_tripped" in rec.message
                for rec in caplog.records
            )
        finally:
            await runtime.stop()

    def test_legacy_session_retires_newheads_and_unfiltered_logs(self) -> None:
        from pmcopy.adapters.polygon.alchemy_json_rpc import AlchemySubscriptionOptions
        from pmcopy.orchestrators.bootstrap.polygon_bootstrap import PolygonSourceRuntimeConfig

        # Unfiltered logs without watched_wallets must be rejected
        with pytest.raises(ValueError, match="watched_wallets"):
            AlchemySubscriptionOptions(
                chain_id=137,
                pending_targets=(POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,),
                log_addresses=(POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,),
                watched_wallets=(),
            )

        # PolygonSourceRuntimeConfig.pending_targets locks strictly to Exchange addresses
        cfg = PolygonSourceRuntimeConfig(
            chain_id=137,
            websocket_url=ALCHEMY_WSS,
            exchange_addresses=(POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,),
            watched_wallets=frozenset({TRADER_EOA, TRADER_PROXY}),
        )
        assert cfg.pending_targets == (POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),)
