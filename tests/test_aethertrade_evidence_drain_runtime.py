"""Fill evidence drains independently and releases database access before shutdown."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


class TestEvidenceDrainRuntime:
    """Explicit event barriers keep retry and cancellation tests deterministic."""

    async def test_retry_does_not_drop_rows_or_end_worker(self):
        from pmcopy.orchestrators.evidence.evidence_drain_runtime import EvidenceDrainRuntime

        waiting = asyncio.Event()
        released = asyncio.Event()
        drain = SimpleNamespace(drain_once=AsyncMock(side_effect=RuntimeError("private details")))

        class Waiter:
            async def wait(self, stop, seconds):
                waiting.set()
                await released.wait()

        runtime = EvidenceDrainRuntime(drain, waiter=Waiter())
        await runtime.start()
        await waiting.wait()
        assert runtime.status == "retry"
        drain.drain_once.assert_awaited_once()
        await runtime.aclose()
        assert runtime.status == "closed"

    async def test_close_cancels_stalled_database_operation(self):
        from pmcopy.orchestrators.evidence.evidence_drain_runtime import EvidenceDrainRuntime

        entered, cancelled = asyncio.Event(), asyncio.Event()

        async def blocked():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        runtime = EvidenceDrainRuntime(SimpleNamespace(drain_once=blocked))
        await runtime.start()
        await entered.wait()
        await runtime.aclose()
        await runtime.aclose()
        assert cancelled.is_set()

    def test_user_state_passes_atomic_projection_to_actual_fill_repository(self, monkeypatch):
        from pmcopy.config.app_config import AppConfig
        from pmcopy.orchestrators.bootstrap import runtime_bootstrap
        from pmcopy.services.accounts.multi_user_config import RuntimeUser

        seen = []
        projection = object()

        def fills(pool, **kwargs):
            seen.append(kwargs)
            raise LookupError("repository composed")

        monkeypatch.setattr(runtime_bootstrap, "FillsRepo", fills)
        with pytest.raises(LookupError, match="repository composed"):
            runtime_bootstrap.build_user_state(RuntimeUser("u", AppConfig().wallet), Mock(), Mock(),
                                               AppConfig(), evidence_projection=projection)
        assert seen[0]["evidence_projection"] is projection

    async def test_shutdown_stops_drain_before_database(self, monkeypatch):
        import pmcopy.app as app

        events = []

        async def stop_drain():
            events.append("drain")

        async def stop_database():
            events.append("database")

        monkeypatch.setattr("pmcopy.utils.fetch_data.close_session", AsyncMock())
        monkeypatch.setattr("pmcopy.storage.postgres.connection.shutdown_pool", stop_database)
        await app._shutdown_runtime(monitor_tasks=[], supervised=[], market_data_runtimes={},
                                    user_data_runtimes=[], metrics_runner=None,
                                    evidence_drain=SimpleNamespace(aclose=stop_drain))
        assert events == ["drain", "database"]

    async def test_early_startup_failure_closes_async_only_resources(self, monkeypatch):
        import pmcopy.app as app

        evidence = SimpleNamespace(aclose=AsyncMock())
        drain = SimpleNamespace(aclose=AsyncMock())

        async def fail_after_registration(owners, **kwargs):
            owners.extend([evidence, drain])
            raise RuntimeError("startup interrupted")

        monkeypatch.setattr(app, "_run_application", fail_after_registration)
        monkeypatch.setattr("pmcopy.utils.fetch_data.close_session", AsyncMock())
        with pytest.raises(RuntimeError, match="startup interrupted"):
            await app.main()
        drain.aclose.assert_awaited_once()
        evidence.aclose.assert_awaited_once()
