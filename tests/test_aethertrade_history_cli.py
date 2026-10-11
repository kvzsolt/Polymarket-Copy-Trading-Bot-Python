"""Explicit read-only command safety; no environment or provider requests in preview."""

import json

import pytest

from tests.test_aethertrade_history_import import HistoryGateway, plan_dict


def test_registered_command_defaults_to_preview_without_config_or_journal(
    tmp_path, monkeypatch, capsys
):
    import bot_cli.main as entry
    from bot_cli.commands.history_import import HistoryImportCommand

    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan_dict()))

    def forbidden(*args, **kwargs):
        raise AssertionError("preview touched runtime")

    monkeypatch.setattr(HistoryImportCommand, "execute", forbidden)
    monkeypatch.setattr("sys.argv", ["pmcopy", "history-import", "--plan", str(path)])
    assert entry.main() == 0
    value = json.loads(capsys.readouterr().out)
    assert value["read_only"] is True and value["execute"] is False
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.asyncio
async def test_explicit_collection_uses_shared_provider_runtime_and_never_publisher(tmp_path):
    from bot_cli.commands.history_import import HistoryImportCommand

    class Provider:
        gateway = HistoryGateway()
        closed = False

        def close(self):
            self.closed = True

    provider = Provider()
    loaded = []
    command = HistoryImportCommand(lambda: loaded.append("load") or object(), lambda cfg: provider)
    from domain.history_import import HistoryImportPlan

    result = await command.execute(HistoryImportPlan.from_dict(plan_dict()), tmp_path / "journal")
    assert result.complete and provider.closed and loaded == ["load"]
    assert all(call[3] == "backfill" for call in provider.gateway.calls)


def test_drain_open_requires_existing_validated_job_and_rejects_changed_plan(tmp_path):
    from storage.history_import import HistoryImportJournal
    from tests.test_aethertrade_history_import import plan

    missing = tmp_path / "missing"
    with pytest.raises(ValueError):
        HistoryImportJournal.open_existing(missing)
    assert not missing.exists()
    path = tmp_path / "job"
    journal = HistoryImportJournal(path, plan())
    journal.close()
    reopened = HistoryImportJournal.open_existing(path)
    assert reopened.cursor == 15
    reopened.close()
    with pytest.raises(ValueError):
        HistoryImportJournal(path, plan(to_block=18))
