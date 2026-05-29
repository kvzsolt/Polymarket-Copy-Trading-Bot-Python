from __future__ import annotations

from services.discovery.runtime_env_editor import RuntimeEnvEditor


def test_runtime_env_editor_replaces_only_leader_wallets(tmp_path):
    env_file = tmp_path / "runtime.local.env"
    env_file.write_text(
        "# keep comment\n"
        "FOO=bar\n"
        "LEADER_WALLETS=0x1111111111111111111111111111111111111111\n",
        encoding="utf-8",
    )
    editor = RuntimeEnvEditor(env_file)

    backup = editor.write_leaders(["0x2222222222222222222222222222222222222222"])

    assert backup.exists()
    text = env_file.read_text(encoding="utf-8")
    assert "# keep comment" in text
    assert "FOO=bar" in text
    assert "LEADER_WALLETS=0x2222222222222222222222222222222222222222" in text

