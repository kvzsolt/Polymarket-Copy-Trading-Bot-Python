"""Release-gate: ``bot config print`` redacts secrets (Issue #76).

Confirms that even with a populated PRIVATE_KEY in the environment, the
default ``bot config print`` invocation never echoes it back. Defends the
operator-facing safety contract that secrets cannot leak through ops tooling.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import config.app_config as app_config_mod
from bot_cli.commands.config import _config_to_dict, _redact_value, run_config
from tests.config_helpers import load_test_config


_SECRET_KEY = "0x" + "a" * 64
_SECRET_FUNDER = "0x" + "b" * 40
_SECRET_USER = "0x" + "c" * 40


def _env() -> dict[str, str]:
    return {
        "PRIVATE_KEY": _SECRET_KEY,
        "FUNDER_ADDRESS": _SECRET_FUNDER,
        "RPC_URL": "https://rpc.example.com",
        "CHAIN_ID": "137",
        "USER_ADDRESS": _SECRET_USER,
    }


def _load_cfg():
    """Load AppConfig while bypassing the runtime-version guard so the test
    runs on any Python the harness happens to provide (dev may be 3.13;
    CI is 3.11/3.12). The redaction contract is version-independent."""
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(_env())


def test_redact_value_masks_private_key() -> None:
    assert _redact_value("PRIVATE_KEY", _SECRET_KEY) == "***REDACTED***"
    assert _redact_value("private_key", _SECRET_KEY) == "***REDACTED***"
    assert _redact_value("CLOB_API_KEY", "abc") == "***REDACTED***"
    assert _redact_value("RPC_URL", "https://rpc") == "https://rpc"


def test_config_to_dict_redacts_secrets() -> None:
    cfg = _load_cfg()
    out = _config_to_dict(cfg, redacted=True)
    rendered = json.dumps(out)
    assert _SECRET_KEY not in rendered
    assert "***REDACTED***" in rendered


def test_run_config_print_does_not_leak_secret(capsys, monkeypatch) -> None:
    cfg = _load_cfg()

    monkeypatch.setattr(
        "bot_cli.commands.config.load_config",
        lambda *_args, **_kwargs: cfg,
    )
    args = SimpleNamespace(
        config_subcommand="print",
        no_redact=False,
    )
    rc = run_config(args)
    assert rc == 0
    captured = capsys.readouterr().out
    assert _SECRET_KEY not in captured
    assert "***REDACTED***" in captured
