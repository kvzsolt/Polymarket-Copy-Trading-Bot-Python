from __future__ import annotations

import os
import stat
import sys
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
            "PROXY_WALLET": _eth_addr("3"),
            "PRIVATE_KEY": "0x" + ("ab" * 32),
            "PRIVATE_KEY_FILE": "",
            "RPC_URL": "http://localhost:8545 # inline",
            "PNL_LOG_FILE": "log/pnl.jsonl",
            "CLOB_HTTP_URL": "https://clob.polymarket.com",
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(EnvValidationError):
                validate_runtime_env()
