"""Runtime composition remains locally durable while Hungary is unavailable."""

import asyncio
from types import SimpleNamespace

from pydantic import SecretStr
import pytest

from tests.test_aethertrade_evidence_outbox import FixedClock, event
from tests.test_aethertrade_evidence_publisher import FakeTransport


def config(tmp_path, **changes):
    from config.evidence import EvidenceConfig

    return SimpleNamespace(
        evidence=EvidenceConfig(
            outbox_path=str(tmp_path / "evidence.sqlite3"),
            token=SecretStr("private-test-token" * 3),
            **changes,
        )
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"outbox_path": ":memory:"},
        {"outbox_path": ""},
        {"max_events": True},
        {"max_events": 0},
        {"max_bytes": -1},
        {"batch_size": 101},
        {"timeout_seconds": 0},
        {"retry_delay_seconds": float("nan")},
    ],
)
def test_runtime_config_rejects_unsafe_bounds(tmp_path, changes):
    from config.evidence import EvidenceConfig

    with pytest.raises(ValueError):
        EvidenceConfig(**changes)


def test_secret_rejected_in_yaml_and_never_exposed_in_repr(tmp_path):
    from config.evidence import EvidenceSecretBoundary

    value = config(tmp_path).evidence
    assert value.token.get_secret_value() not in repr(value)
    with pytest.raises(ValueError) as caught:
        EvidenceSecretBoundary.reject_yaml({"evidence": {"token": "secret-credential"}})
    assert "secret-credential" not in str(caught.value)


@pytest.mark.parametrize("enabled,token", [(False, "x" * 32), (True, "")])
def test_runtime_has_no_disabled_or_unconfigured_noop(tmp_path, enabled, token):
    from config.evidence import EvidenceConfig
    from orchestrators.evidence_runtime import EvidenceRuntime

    cfg = SimpleNamespace(
        evidence=EvidenceConfig(
            outbox_path=str(tmp_path / "box"), enabled=enabled, token=SecretStr(token)
        )
    )
    with pytest.raises(ValueError):
        EvidenceRuntime.from_config(
            cfg, clock=FixedClock(), transport=FakeTransport(TimeoutError())
        )
    assert not (tmp_path / "box").exists()


async def test_runtime_persists_before_start_and_close_cancels_stalled_network(tmp_path):
    from orchestrators.evidence_runtime import EvidenceRuntime

    entered, cancelled = asyncio.Event(), asyncio.Event()

    class StalledTransport:
        closed = False

        async def post(self, body):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async def aclose(self):
            self.closed = True

    transport = StalledTransport()
    runtime = EvidenceRuntime.from_config(config(tmp_path), clock=FixedClock(), transport=transport)
    runtime.append_event(event().wire())
    assert not entered.is_set() and runtime.health().pending == 1
    await runtime.start()
    await entered.wait()
    runtime.append_event(event(2).wire())
    assert runtime.health().valid and runtime.health().pending == 2
    await runtime.aclose()
    assert cancelled.is_set() and transport.closed
    reopened = EvidenceRuntime.from_config(
        config(tmp_path), clock=FixedClock(), transport=FakeTransport(TimeoutError())
    )
    assert reopened.health().valid and reopened.health().pending == 2
    await reopened.aclose()


async def test_runtime_clockless_record_and_overflow_stay_invalid_after_restart(tmp_path):
    from orchestrators.evidence_runtime import EvidenceRuntime

    cfg = config(tmp_path, max_events=1)
    runtime = EvidenceRuntime.from_config(
        cfg, clock=FixedClock(), transport=FakeTransport(TimeoutError())
    )
    runtime.append_event(event().wire())
    with pytest.raises(ValueError):
        runtime.append_event(event(2).wire())
    assert not runtime.health().valid
    await runtime.aclose()
    reopened = EvidenceRuntime.from_config(
        cfg, clock=FixedClock(), transport=FakeTransport(TimeoutError())
    )
    assert not reopened.health().valid and reopened.health().pending == 2
    with pytest.raises(ValueError):
        reopened.record(
            "bot",
            "gap",
            {"reason": "disconnected"},
            SimpleNamespace(wall_timestamp_ns=None),
            recovered=False,
        )
    await reopened.aclose()


async def test_runtime_closes_after_unexpected_worker_failure(tmp_path):
    from orchestrators.evidence_runtime import EvidenceRuntime

    failed = asyncio.Event()

    class BrokenTransport:
        closed = False

        async def post(self, body):
            failed.set()
            raise RuntimeError("private upstream detail")

        async def aclose(self):
            self.closed = True

    transport = BrokenTransport()
    runtime = EvidenceRuntime.from_config(config(tmp_path), clock=FixedClock(), transport=transport)
    runtime.append_event(event().wire())
    await runtime.start()
    await failed.wait()
    await runtime.aclose()
    assert transport.closed
    assert runtime.delivery_status == "closed"
    reopened = EvidenceRuntime.from_config(
        config(tmp_path), clock=FixedClock(), transport=FakeTransport(TimeoutError())
    )
    assert reopened.health().pending >= 1
    await reopened.aclose()


async def test_runtime_exposes_sticky_explicit_invalidation(tmp_path):
    from orchestrators.evidence_runtime import EvidenceRuntime

    runtime = EvidenceRuntime.from_config(
        config(tmp_path), clock=FixedClock(), transport=FakeTransport(TimeoutError())
    )
    runtime.invalidate("invalid_payload")
    assert not runtime.health().valid and runtime.health().pending == 1
    await runtime.aclose()
