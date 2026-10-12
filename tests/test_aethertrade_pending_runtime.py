"""One bounded pending-only socket cannot own mined execution or cross-region I/O."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt


class Clock:
    """An explicitly advanced receipt clock with no wall-clock dependency."""

    def capture(self):
        return TransportReceipt(1, 1, "test", 1_000_001)


class TestPendingRuntime:
    """Subscription admission, ownership validation and isolation are observable seams."""

    @pytest.mark.parametrize(
        "provider,enabled,expected",
        [
            ("alchemy", True, True),
            ("alchemy", False, False),
            ("infura", True, False),
        ],
    )
    async def test_production_composition_only_creates_enabled_supported_sampler(
        self,
        tmp_path,
        monkeypatch,
        provider,
        enabled,
        expected,
    ):
        from pmcopy.config.native_detection import NativeDetectionConfig
        from pmcopy.domain.grouped_detection import TraderDetectionPolicy
        from pmcopy.orchestrators.native_detection_runtime import NativeDetectionRuntime

        wallet = "0x" + "11" * 20
        owner = SimpleNamespace(
            primary="alchemy",
            gateway=Mock(),
            budget=Mock(),
            endpoints=SimpleNamespace(http_url=lambda _: "https://synthetic.invalid", infura=""),
        )
        monkeypatch.setattr("pmcopy.orchestrators.native_detection_runtime.SystemReceiptClock", Clock)
        runtime = NativeDetectionRuntime.compose(
            config=NativeDetectionConfig(
                state_path=str(tmp_path / "source.sqlite"),
                pending_enabled=enabled,
                traders=(
                    TraderDetectionPolicy(
                        wallet,
                        mode="pending" if provider == "alchemy" else "mined",
                        provider=provider,
                    ),
                ),
            ),
            provider_runtime=owner,
            wallets=(wallet,),
            sink=AsyncMock(),
            raw=Mock(),
            status_changed=Mock(),
        )
        try:
            assert (runtime._pending is not None) is expected
            owner.gateway.call_observed.assert_not_called()
            owner.budget.admit_rpc.assert_not_called()
        finally:
            await runtime.stop()

    async def test_later_inflight_failure_cannot_erase_budget_pause_reason(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        _, evaluator = self.dependencies()
        evaluator.stop_requested, evaluator.stop_reason = True, "budget_stopped"
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=Mock(),
            admission=Mock(),
            clock=Clock(),
            evidence_sink=Mock(),
            status_changed=Mock(),
        )
        await runtime._evaluate("first", Clock().capture(), "owned")
        evaluator.observe_frame.side_effect = OSError("later read failed")
        await runtime._evaluate("second", Clock().capture(), "owned")
        assert runtime.status == "budget_stopped"

    async def test_fifth_inflight_body_is_charged_then_stops_without_unbounded_queue(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()
        frame = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": "s", "result": "hash"},
            }
        )
        frames = ['{"jsonrpc":"2.0","id":1,"result":"s"}'] + [frame] * 5

        async def observe(raw, receipt, **kwargs):
            if kwargs["expected_subscription"]:
                await asyncio.Event().wait()

        evaluator.observe_frame.side_effect = observe
        admission = SimpleNamespace(admit_rpc=Mock(), charge_stream=Mock())
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=AsyncMock(return_value=socket)),
            admission=admission,
            clock=Clock(),
            evidence_sink=Mock(),
            status_changed=Mock(),
            receiver=SimpleNamespace(receive=AsyncMock(side_effect=frames)),
        )
        await runtime.start()
        await runtime.wait()
        assert runtime.status == "capacity_stopped"
        assert evaluator.observe_frame.await_count == 5
        admission.charge_stream.assert_called_once_with(
            "alchemy", len(frame.encode()), category="pending"
        )

    async def test_subscription_send_has_injected_deadline_and_cleanup(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()
        sender = SimpleNamespace(send=AsyncMock(side_effect=TimeoutError))
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=AsyncMock(return_value=socket)),
            admission=SimpleNamespace(admit_rpc=Mock()),
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock(), invalidate=Mock()),
            sender=sender,
            status_changed=Mock(),
        )
        await runtime.start()
        await runtime.wait()
        assert runtime.status == "source_unavailable"
        sender.send.assert_awaited_once()
        socket.close.assert_awaited_once()

    async def test_close_failure_preserves_budget_reason_and_exposes_cleanup_failure(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()
        socket.close.side_effect = OSError("private transport details")

        async def observe(*args, **kwargs):
            evaluator.stop_requested, evaluator.stop_reason = True, "budget_stopped"

        evaluator.observe_frame.side_effect = observe
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=AsyncMock(return_value=socket)),
            admission=SimpleNamespace(admit_rpc=Mock()),
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock(), invalidate=Mock()),
            receiver=SimpleNamespace(
                receive=AsyncMock(return_value='{"jsonrpc":"2.0","id":1,"result":"s"}')
            ),
            status_changed=Mock(),
        )
        await runtime.start()
        await runtime.wait()
        assert runtime.status == "budget_stopped"
        assert runtime.cleanup_failed is True

    async def test_unrecorded_gap_invalidates_local_evidence(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        _, evaluator = self.dependencies()
        sink = SimpleNamespace(
            append_event=Mock(side_effect=ValueError("invalid evidence")), invalidate=Mock()
        )
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=AsyncMock(side_effect=OSError)),
            admission=SimpleNamespace(admit_rpc=Mock()),
            clock=Clock(),
            evidence_sink=sink,
            status_changed=Mock(),
        )
        await runtime.start()
        await runtime.wait()
        sink.invalidate.assert_called_once_with("storage_failure")

    async def test_slow_body_does_not_delay_receipt_of_next_hash_frame(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()
        body_entered, next_received = asyncio.Event(), asyncio.Event()
        frames = [
            '{"jsonrpc":"2.0","id":1,"result":"s"}',
            '{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"s","result":"one"}}',
            '{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"s","result":"two"}}',
        ]

        async def receive(*args):
            if frames:
                frame = frames.pop(0)
                if not frames:
                    next_received.set()
                return frame
            await asyncio.Event().wait()

        async def observe(raw, receipt, **kwargs):
            if kwargs["expected_subscription"]:
                body_entered.set()
                await asyncio.Event().wait()

        evaluator.observe_frame.side_effect = observe
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=AsyncMock(return_value=socket)),
            admission=SimpleNamespace(admit_rpc=Mock()),
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock(), invalidate=Mock()),
            receiver=SimpleNamespace(receive=receive),
            status_changed=Mock(),
        )
        await runtime.start()
        try:
            await body_entered.wait()
            for _ in range(5):
                await asyncio.sleep(0)
            assert next_received.is_set()
        finally:
            await runtime.stop()

    @staticmethod
    def dependencies():
        socket = SimpleNamespace(send=AsyncMock(), close=AsyncMock())
        evaluator = SimpleNamespace(
            stop_requested=False,
            stop_reason=None,
            expire=Mock(),
            aclose=AsyncMock(),
            observe_frame=AsyncMock(return_value="ignored"),
        )
        return socket, evaluator

    async def test_one_grouped_subscription_is_charged_before_open_and_every_frame_is_observed(
        self,
    ):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()
        sequence = []
        frames = [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": "owned"}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {"subscription": "owned", "result": "0x" + "11" * 32},
                }
            ),
        ]

        async def receive(_socket, timeout_s):
            return frames.pop(0)

        async def observe(raw, receipt, **kwargs):
            if kwargs["expected_subscription"] == "owned":
                evaluator.stop_requested, evaluator.stop_reason = True, "capacity_stopped"

        async def open_socket(provider):
            sequence.append("open")
            return socket

        evaluator.observe_frame.side_effect = observe
        admission = SimpleNamespace(
            admit_rpc=Mock(side_effect=lambda *a, **k: sequence.append("admit"))
        )
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=open_socket),
            admission=admission,
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock()),
            receiver=SimpleNamespace(receive=receive),
            status_changed=Mock(),
        )
        await runtime.start()
        await runtime.wait()
        assert sequence == ["admit", "open"]
        admission.admit_rpc.assert_called_once_with("alchemy", "eth_subscribe", category="pending")
        subscription = json.loads(socket.send.call_args.args[0])
        assert subscription["params"][0] == "alchemy_pendingTransactions"
        assert subscription["params"][1]["hashesOnly"] is True
        assert len(subscription["params"][1]["toAddress"]) == 2
        assert evaluator.observe_frame.await_count == 2
        assert evaluator.observe_frame.call_args_list[0].kwargs["expected_subscription"] == ""
        assert runtime.status == "capacity_stopped"
        socket.close.assert_awaited_once()

    @pytest.mark.parametrize("fallback,mode", [("mined", "mined"), ("pause", "paused")])
    async def test_native_owner_starts_stops_and_applies_explicit_budget_policy(
        self, tmp_path, fallback, mode
    ):
        from pmcopy.domain.grouped_detection import TraderDetectionPolicy
        from pmcopy.orchestrators.native_detection_runtime import NativeDetectionRuntime
        from pmcopy.storage.detection_state import SqliteDetectionState

        wallet = "0x" + "11" * 20
        state = SqliteDetectionState(tmp_path / "state")
        pending = SimpleNamespace(
            status="evaluating", start=AsyncMock(), stop=AsyncMock(), request_stop=Mock()
        )
        ready = asyncio.Event()

        class Session:
            connected = False

            async def run(self):
                self.connected = True
                state.set_effective((wallet,), "mined", "pending_disabled")
                runtime._source_changed()
                ready.set()
                await asyncio.Event().wait()

        observer = Mock()
        runtime = NativeDetectionRuntime(
            policies=(TraderDetectionPolicy(wallet, mode="pending", on_budget_exhausted=fallback),),
            state=state,
            session_factory=lambda groups: Session(),
            observer=observer,
            status_changed=Mock(),
            pending_runtime=pending,
        )
        try:
            await runtime.start()
            await ready.wait()
            pending.start.assert_awaited_once()
            assert runtime.states()[0].effective_mode == "mined"
            assert runtime.states()[0].degraded_reason == "pending_evaluation_only"
            pending.status = "budget_stopped"
            runtime._source_changed()
            assert runtime.states()[0].effective_mode == mode
            assert observer.retain_wallets.call_args.args[0] == (
                () if mode == "paused" else (wallet,)
            )
            runtime.update_watched_wallets(())
            pending.request_stop.assert_called_once()
        finally:
            await runtime.stop()
            pending.stop.assert_awaited_once()
            state.close()

    async def test_subscription_budget_failure_never_opens_any_socket(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        _, evaluator = self.dependencies()
        connector = SimpleNamespace(open=AsyncMock())
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=connector,
            admission=SimpleNamespace(admit_rpc=Mock(side_effect=BudgetExhausted("quota"))),
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock()),
            status_changed=Mock(),
        )
        await runtime.start()
        await runtime.wait()
        assert runtime.status == "budget_stopped"
        connector.open.assert_not_awaited()

    async def test_idle_receive_timeout_expires_without_retry_or_extra_connection(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()

        def expire():
            evaluator.stop_requested, evaluator.stop_reason = True, "deadline_unknown"

        evaluator.expire.side_effect = expire
        connector = SimpleNamespace(open=AsyncMock(return_value=socket))
        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=connector,
            admission=SimpleNamespace(admit_rpc=Mock()),
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock()),
            receiver=SimpleNamespace(receive=AsyncMock(side_effect=TimeoutError)),
            status_changed=Mock(),
        )
        await runtime.start()
        await runtime.wait()
        assert runtime.status == "deadline_unknown"
        connector.open.assert_awaited_once()

    async def test_roster_change_cancels_only_optional_pending_owner(self):
        from pmcopy.orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime

        socket, evaluator = self.dependencies()
        entered = asyncio.Event()

        async def receive(socket, timeout_s):
            entered.set()
            await asyncio.Event().wait()

        runtime = PendingEvaluationRuntime(
            evaluator=evaluator,
            connector=SimpleNamespace(open=AsyncMock(return_value=socket)),
            admission=SimpleNamespace(admit_rpc=Mock()),
            clock=Clock(),
            evidence_sink=SimpleNamespace(append_event=Mock()),
            receiver=SimpleNamespace(receive=receive),
            status_changed=Mock(),
        )
        await runtime.start()
        await entered.wait()
        runtime.request_stop()
        await runtime.stop()
        assert runtime.status == "stopped"
        evaluator.aclose.assert_awaited_once()
        socket.close.assert_awaited_once()
