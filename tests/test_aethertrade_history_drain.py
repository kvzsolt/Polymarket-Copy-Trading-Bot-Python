"""History jobs share the application's one durable publisher and bounded scheduler."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from config.app_config import AppConfig
from config.evidence import EvidenceConfig
from tests.test_aethertrade_evidence_composition import (
    TestEvidenceComposition as CompositionFixture,
)


class TestHistoryDrain:
    """No historical job is opened or polled unless explicitly configured."""

    def test_default_is_disabled_and_invalid_paths_are_rejected(self):
        assert EvidenceConfig().history_journal_path is None
        for path in ("", ":memory:", "bad\x00path", 4):
            with pytest.raises(ValueError):
                EvidenceConfig(history_journal_path=path)

    async def test_drain_delegates_one_bounded_batch_and_closes_journal(self):
        from orchestrators.history_evidence_drain_runtime import HistoryEvidenceDrainRuntime

        transfer = SimpleNamespace(publish_local_once=Mock(return_value=3))
        journal = SimpleNamespace(close=Mock())
        runtime = HistoryEvidenceDrainRuntime(journal, transfer)
        assert await runtime.drain_once() == 3
        transfer.publish_local_once.assert_called_once_with()
        await runtime.aclose()
        await runtime.aclose()
        journal.close.assert_called_once_with()

    async def test_app_registers_history_after_existing_evidence_owner(self, monkeypatch):
        from orchestrators.history_evidence_drain_runtime import HistoryEvidenceDrainRuntime

        app, config, provider, policy = CompositionFixture.prepare(monkeypatch)
        evidence = SimpleNamespace(start=AsyncMock(), aclose=AsyncMock())
        history = SimpleNamespace(start=AsyncMock(), aclose=AsyncMock())
        factory = Mock(return_value=history)
        monkeypatch.setattr(HistoryEvidenceDrainRuntime, "from_config", factory)
        monkeypatch.setattr(app, "build_app", AsyncMock(side_effect=LookupError("composed")))
        owners = []
        with pytest.raises(LookupError, match="composed"):
            await app._run_application(owners, startup_policy=policy, evidence_runtime=evidence)
        factory.assert_called_once_with(config.evidence, evidence)
        history.start.assert_awaited_once()
        assert owners[:3] == [provider, evidence, history]

    def test_disabled_composition_never_opens_a_journal(self):
        from orchestrators.history_evidence_drain_runtime import HistoryEvidenceDrainRuntime

        assert HistoryEvidenceDrainRuntime.from_config(AppConfig().evidence, Mock()) is None
