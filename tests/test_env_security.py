from __future__ import annotations

import os
import stat
import sys
from importlib import reload
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _eth_addr(seed: str) -> str:
    return "0x" + (seed * 40)[:40]


class TestEnvSecurity:
    def test_private_key_file_loads(self, tmp_path):
        from config.env import get_private_key

        key_file = tmp_path / "pk.txt"
        key = "0x" + ("ab" * 32)
        key_file.write_text(key, encoding="utf-8")
        key_file.chmod(0o600)

        with patch.dict(os.environ, {"PRIVATE_KEY_FILE": str(key_file), "PRIVATE_KEY": ""}):
            assert get_private_key() == key

    def test_private_key_file_permissions_enforced(self, tmp_path):
        from config.env import get_private_key, EnvValidationError

        key_file = tmp_path / "pk.txt"
        key_file.write_text("0x" + ("ab" * 32), encoding="utf-8")
        key_file.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)  # 0640

        with patch.dict(os.environ, {"PRIVATE_KEY_FILE": str(key_file), "PRIVATE_KEY": ""}):
            with pytest.raises(EnvValidationError):
                get_private_key()

    def test_runtime_validation_rejects_inline_comments(self):
        from config.env import validate_runtime_env, EnvValidationError

        env = {
            "USER_ADDRESS": _eth_addr("1"),
            "LEADER_WALLETS": _eth_addr("2"),
            "PRIVATE_KEY": "0x" + ("ab" * 32),
            "PRIVATE_KEY_FILE": "",
            "RPC_URL": "http://localhost:8545 # inline",
            "PNL_LOG_FILE": "log/pnl.jsonl",
            "CLOB_HTTP_URL": "https://clob.polymarket.com",
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(EnvValidationError):
                validate_runtime_env()

    def test_rehydrate_env_refreshes_snapshot_in_place(self):
        from config.env import ENV, rehydrate_env

        env_ref = ENV
        address = _eth_addr("4")

        with patch.dict(
            os.environ,
            {"LEADER_WALLETS": "", "USER_ADDRESS": address, "ENABLE_RISK_MGMT": "false"},
            clear=False,
        ):
            refreshed = rehydrate_env()
            assert refreshed is env_ref
            assert ENV is env_ref
            assert ENV["USER_ADDRESS"] == address
            assert ENV["LEADER_WALLETS"] == [address]
            assert ENV["ENABLE_RISK_MGMT"] is False

        # Reset snapshot for any later tests in this process.
        rehydrate_env()

    def test_rehydrate_env_redacts_private_key_file_in_snapshot(self, tmp_path):
        from config.env import ENV, rehydrate_env

        key_file = tmp_path / "pk.txt"
        key_file.write_text("0x" + ("ab" * 32), encoding="utf-8")
        key_file.chmod(0o600)

        with patch.dict(
            os.environ,
            {"PRIVATE_KEY_FILE": str(key_file), "PRIVATE_KEY": ""},
            clear=False,
        ):
            rehydrate_env()
            assert ENV["PRIVATE_KEY"] == "***REDACTED***"
            assert ENV["PRIVATE_KEY_FILE"] == "***REDACTED***"

        rehydrate_env()

    def test_rehydrate_env_keeps_private_key_fields_empty_when_unset(self):
        from config.env import ENV, rehydrate_env

        with patch.dict(
            os.environ,
            {"PRIVATE_KEY_FILE": "", "PRIVATE_KEY": ""},
            clear=False,
        ):
            rehydrate_env()
            assert ENV["PRIVATE_KEY"] == ""
            assert ENV["PRIVATE_KEY_FILE"] == ""

        rehydrate_env()

    def test_rehydrate_env_does_not_redact_private_key_with_inline_comment(self):
        from config.env import ENV, rehydrate_env

        with patch.dict(
            os.environ,
            {"PRIVATE_KEY": "0x" + ("ab" * 32) + " # inline", "PRIVATE_KEY_FILE": ""},
            clear=False,
        ):
            rehydrate_env()
            assert ENV["PRIVATE_KEY"] == ""
            assert ENV["PRIVATE_KEY_FILE"] == ""

        rehydrate_env()

    def test_rehydrate_env_does_not_redact_private_key_file_with_inline_comment(self):
        from config.env import ENV, rehydrate_env

        with patch.dict(
            os.environ,
            {"PRIVATE_KEY": "", "PRIVATE_KEY_FILE": "/tmp/pk.txt # inline"},
            clear=False,
        ):
            rehydrate_env()
            assert ENV["PRIVATE_KEY"] == ""
            assert ENV["PRIVATE_KEY_FILE"] == ""

        rehydrate_env()

    def test_leader_auto_alias_enables_discovery_and_promotion(self):
        from config.env import ENV, rehydrate_env

        env_ref = ENV

        with patch.dict(
            os.environ,
            {
                "LEADER_AUTO": "true",
                "LEADER_AUTO_COUNT": "2",
                "AUTO_DISCOVER_LEADERS": "false",
                "DISCOVERY_PROMOTE_TO_ACTIVE": "false",
                "ENABLE_TRADER_DISCOVERY": "false",
            },
            clear=False,
        ):
            refreshed = rehydrate_env()
            assert refreshed is env_ref
            assert ENV["LEADER_AUTO"] is True
            assert ENV["LEADER_AUTO_COUNT"] == 2
            assert ENV["AUTO_DISCOVER_LEADERS"] is True
            assert ENV["DISCOVERY_PROMOTE_TO_ACTIVE"] is True
            assert ENV["ENABLE_TRADER_DISCOVERY"] is True
            assert ENV["AUTO_DISCOVER_TOP_N"] == 2

        rehydrate_env()

    def test_runtime_env_load_order_supports_local_public_override_and_secret_override(self, tmp_path):
        from config import runtime_env

        shared = tmp_path / "runtime.env"
        local_public = tmp_path / "runtime.local.env"
        secrets = tmp_path / ".env"

        shared.write_text("LEADER_WALLETS=0xshared\nRPC_URL=https://shared\n", encoding="utf-8")
        local_public.write_text("LEADER_WALLETS=0xlocal\nFUNDER_ADDRESS=0xabc\n", encoding="utf-8")
        secrets.write_text("RPC_URL=https://secret\nPRIVATE_KEY=0x" + ("ab" * 32) + "\n", encoding="utf-8")

        with patch.dict(os.environ, {}, clear=True):
            reload(runtime_env)
            runtime_env.load_runtime_env(
                non_sensitive_path=shared,
                local_non_sensitive_path=local_public,
                secrets_path=secrets,
                override_secrets=True,
            )
            assert os.environ["LEADER_WALLETS"] == "0xlocal"
            assert os.environ["FUNDER_ADDRESS"] == "0xabc"
            assert os.environ["RPC_URL"] == "https://secret"
            assert os.environ["PRIVATE_KEY"] == "0x" + ("ab" * 32)
