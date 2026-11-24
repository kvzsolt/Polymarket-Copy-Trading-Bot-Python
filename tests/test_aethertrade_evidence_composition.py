"""Real configuration and startup wire durable evidence before source consumers."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.config.app_config import AppConfig, dump_config_payload, load_config
from tests.test_redemption_config import TestRedemptionConfig as RedeemConfigFixture


class TestEvidenceComposition:
    """Isolated deployment directories and fake factories perform no external I/O."""

    def test_private_environment_loads_redacted_token(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PMCOPY__EVIDENCE__TOKEN", "synthetic-token" * 3)
        config = load_config(RedeemConfigFixture.overrides(), config_dir=tmp_path)
        assert config.evidence.token.get_secret_value() == "synthetic-token" * 3
        assert "synthetic-token" not in repr(config)
        assert "synthetic-token" not in str(dump_config_payload(config))

    @pytest.mark.parametrize("filename", ["evidence.yml", "config/runtime.local.yml"])
    def test_yaml_token_rejected_without_disclosure(self, tmp_path, monkeypatch, filename):
        monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(tmp_path))
        path = tmp_path / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("evidence:\n  token: forbidden-token\n" if filename.endswith("runtime.local.yml") else "token: forbidden-token\n")
        with pytest.raises(ValueError) as caught:
            load_config(RedeemConfigFixture.overrides(), config_dir=tmp_path)
        assert "forbidden-token" not in str(caught.value)

    @staticmethod
    def prepare(monkeypatch):
        import pmcopy.app as app

        config = AppConfig()
        provider = SimpleNamespace(aclose=AsyncMock())
        monkeypatch.setattr(app, "load_config", lambda: config)
        monkeypatch.setattr(app, "configure_logging", lambda **kwargs: None)
        monkeypatch.setattr(app, "_validate_detection_confidence_floor", lambda config: None)
        monkeypatch.setattr("pmcopy.orchestrators.bootstrap.provider_runtime.ProviderRuntime.from_config", lambda config: provider)
        policy = SimpleNamespace(validate=lambda config: None, effective_config=lambda config: config)
        return app, config, provider, policy

    async def test_default_runtime_starts_before_consumers_and_is_owned(self, monkeypatch):
        app, config, provider, policy = self.prepare(monkeypatch)
        evidence = SimpleNamespace(start=AsyncMock(), aclose=AsyncMock())
        monkeypatch.setattr("pmcopy.orchestrators.evidence.evidence_runtime.EvidenceRuntime.from_config", lambda config: evidence)

        async def stop_after_composition(**kwargs):
            evidence.start.assert_awaited_once()
            raise LookupError("composition verified")

        monkeypatch.setattr(app, "build_app", stop_after_composition)
        owners = []
        with pytest.raises(LookupError, match="composition verified"):
            await app._run_application(owners, startup_policy=policy)
        assert owners[:2] == [provider, evidence]

    async def test_invalid_evidence_stops_before_database_or_source(self, monkeypatch):
        app, config, provider, policy = self.prepare(monkeypatch)
        from pmcopy.domain.evidence import EvidenceError

        def unavailable(config):
            raise EvidenceError("unsafe synthetic-token detail")

        monkeypatch.setattr("pmcopy.orchestrators.evidence.evidence_runtime.EvidenceRuntime.from_config", unavailable)
        build = AsyncMock(side_effect=AssertionError("database should not open"))
        monkeypatch.setattr(app, "build_app", build)
        assert await app._run_application([], startup_policy=policy) == 1
        build.assert_not_awaited()

    @staticmethod
    def disabled(monkeypatch):
        """Return the same fixture with evidence publication explicitly turned off.

        ``evidence.enabled`` is the only supported opt-out. It stays false by
        default in the dataclass, so a deployment that omits the file, or that
        deletes the key, still fails closed on a missing private credential.
        """
        from dataclasses import replace

        from pmcopy.config.evidence import EvidenceConfig

        app, config, provider, policy = TestEvidenceComposition.prepare(monkeypatch)
        off = replace(config, evidence=EvidenceConfig(enabled=False))
        monkeypatch.setattr(app, "load_config", lambda: off)
        return app, off, provider, policy

    async def test_disabled_evidence_starts_without_a_sink_or_owned_runtime(self, monkeypatch):
        """An explicit opt-out must not block startup on a private credential."""

        def forbidden(_config):
            raise AssertionError("disabled evidence must not be composed")

        app, config, provider, policy = self.disabled(monkeypatch)
        monkeypatch.setattr(
            "pmcopy.orchestrators.evidence.evidence_runtime.EvidenceRuntime.from_config", forbidden
        )

        async def stop_after_composition(**kwargs):
            raise LookupError("composition verified")

        monkeypatch.setattr(app, "build_app", stop_after_composition)
        owners = []
        with pytest.raises(LookupError, match="composition verified"):
            await app._run_application(owners, startup_policy=policy)
        assert owners == [provider]

    async def test_disabled_evidence_still_refuses_a_configured_history_journal(
        self, monkeypatch, tmp_path, capsys
    ):
        """A reviewed history job needs a durable sink, so the gate stays closed.

        The refusal must come from the journal requirement itself, not from a
        credential check that an opt-out already waived, and it must name the
        setting the operator has to change.
        """
        from dataclasses import replace

        from pmcopy.config.evidence import EvidenceConfig

        app, config, provider, policy = self.disabled(monkeypatch)
        with_journal = replace(
            config,
            evidence=EvidenceConfig(
                enabled=False, history_journal_path=str(tmp_path / "history.sqlite3")
            ),
        )
        monkeypatch.setattr(app, "load_config", lambda: with_journal)
        opened = AsyncMock(side_effect=AssertionError("journal must not be opened"))
        monkeypatch.setattr(
            "pmcopy.orchestrators.evidence.history_evidence_drain_runtime."
            "HistoryEvidenceDrainRuntime.from_config",
            opened,
        )
        build = AsyncMock(side_effect=AssertionError("database should not open"))
        monkeypatch.setattr(app, "build_app", build)

        assert await app._run_application([], startup_policy=policy) == 1
        build.assert_not_awaited()
        opened.assert_not_awaited()
        assert "history_journal_path" in capsys.readouterr().out
