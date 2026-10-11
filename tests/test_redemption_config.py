"""Redemption credentials are optional, secret, and deployment-local."""

from pathlib import Path

import pytest

from config.app_config import EnvValidationError, dump_config_payload, load_config
from config.env import secret_value


class TestRedemptionConfig:
    """Exercise the real layered loader with isolated deployment files."""

    @staticmethod
    def overrides() -> dict[str, object]:
        """Return valid paper settings without signing credentials."""
        return {
            "database": {"dsn": "postgresql://test:test@localhost/test"},
            "wallet": {"funder_address": "0x" + "1" * 40},
            "polymarket": {"rpc_url": "https://rpc.example"},
        }

    def test_redemption_credentials_are_optional(self, tmp_path: Path) -> None:
        """Normal startup and previews require no Relayer key."""
        config = load_config(self.overrides(), config_dir=tmp_path)
        assert secret_value(config.redemption.relayer_api_key) == ""
        assert config.redemption.relayer_api_key_address == ""

    def test_secret_file_loads_redacted_credentials(self, tmp_path: Path) -> None:
        """The actual secret boundary preserves the key but masks serialization."""
        secret = "synthetic-redemption-secret"
        path = tmp_path / "secrets.env"
        path.write_text(
            f"PMCOPY__REDEMPTION__RELAYER_API_KEY={secret}\n"
            f"PMCOPY__REDEMPTION__RELAYER_API_KEY_ADDRESS=0x{'2' * 40}\n"
        )
        path.chmod(0o600)
        config = load_config(self.overrides(), config_dir=tmp_path, secrets_path=path)
        assert secret_value(config.redemption.relayer_api_key) == secret
        assert config.redemption.relayer_api_key_address == "0x" + "2" * 40
        assert secret not in repr(config)
        assert secret not in str(dump_config_payload(config))

    def test_process_environment_overrides_secret_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Existing configuration precedence also applies to settlement."""
        path = tmp_path / "secrets.env"
        path.write_text("PMCOPY__REDEMPTION__RELAYER_API_KEY=file-key\n")
        path.chmod(0o600)
        monkeypatch.setenv("PMCOPY__REDEMPTION__RELAYER_API_KEY", "process-key")
        config = load_config(self.overrides(), config_dir=tmp_path, secrets_path=path)
        assert secret_value(config.redemption.relayer_api_key) == "process-key"

    @pytest.mark.parametrize("filename", ["redemption.yml", "config/runtime.local.yml"])
    def test_rejects_relayer_secret_in_yaml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filename: str
    ) -> None:
        """A YAML credential cannot silently become a tracked signing secret."""
        monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(tmp_path))
        path = tmp_path / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        content = "relayer_api_key: synthetic-yaml-secret\n"
        if filename.endswith("runtime.local.yml"):
            content = "redemption:\n  relayer_api_key: synthetic-yaml-secret\n"
        path.write_text(content)
        with pytest.raises(EnvValidationError, match="secrets.env") as error:
            load_config(self.overrides(), config_dir=tmp_path)
        assert "synthetic-yaml-secret" not in str(error.value)
