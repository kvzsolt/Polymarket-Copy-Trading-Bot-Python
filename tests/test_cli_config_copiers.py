"""Task 22 — ``pmcopy config validate/print`` must cover the schema v2 roster.

``config validate`` validates the FULL copiers configuration including
secret-ref presence (values are never read for display, never printed);
``config print`` shows copiers safely. Canaries are built at runtime.
"""

from __future__ import annotations

import argparse
import io
import json
import uuid
from contextlib import redirect_stdout
from unittest.mock import patch

import pmcopy.config.app_config as app_config_mod
from pmcopy.bot_cli.commands import config as config_command
from tests.config_helpers import load_test_config

LEADER_1 = "0x" + "1" * 40
FUNDER_A = "0x" + "c" * 40


def _canary_hex(length: int = 64) -> str:
    return (uuid.uuid4().hex + uuid.uuid4().hex)[:length]


def _roster_yaml(mode: str = "paper", key_env: str = "PRIMARY_PRIVATE_KEY") -> str:
    return f"""schema_version: 2
copiers:
  - id: primary
    enabled: true
    mode: {mode}
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: {key_env}
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        enabled: true
        sizing:
          mode: fixed
          quote_amount: "2.00"
"""


def _config(tmp_path, monkeypatch, *, trading_mode: str = "paper", roster: str | None = None):
    roster = roster if roster is not None else _roster_yaml()
    path = tmp_path / "copiers.yml"
    path.write_text(roster, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    env = {
        "PRIVATE_KEY": "",
        "FUNDER_ADDRESS": "",
        "RPC_URL": "https://polygon-rpc.example.com",
        "TRADING_MODE": trading_mode,
        "COPIERS_CONFIG_FILE": str(path),
    }
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _capture(fn, args=None) -> tuple[int, str]:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        code = fn(args if args is not None else argparse.Namespace())
    return code, buffer.getvalue()


class TestConfigValidate:
    def test_valid_roster_prints_secret_free_summary(self, tmp_path, monkeypatch):
        canary = "0x" + _canary_hex()
        monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
        monkeypatch.setenv("PRIMARY_PRIVATE_KEY", canary)
        cfg = _config(tmp_path, monkeypatch)
        monkeypatch.setattr(config_command, "load_config", lambda *a, **k: cfg)

        code, out = _capture(config_command._run_config_validate)

        assert code == 0
        payload = json.loads(out)
        assert payload["valid"] is True
        summary = payload["copiers"]
        assert [c["id"] for c in summary] == ["primary"]
        assert summary[0]["mode"] == "paper"
        # Account caps flow to the follow whitelist as exact decimal text
        # (fixed sizing: quote_amount is the per-order cap).
        follow = summary[0]["follows"][0]
        assert follow["max_trade_usdc"] == "2.00"
        assert follow["max_market_exposure_usdc"] == "50.00"
        assert len(summary[0]["follows"]) == 1
        # Secret VALUES never appear — even though the reference resolved.
        assert canary not in out

    def test_invalid_roster_reports_errors(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
        roster = _roster_yaml().replace("enabled: true", "enabled: yes", 1)
        cfg = _config(tmp_path, monkeypatch, roster=roster)
        monkeypatch.setattr(config_command, "load_config", lambda *a, **k: cfg)

        code, out = _capture(config_command._run_config_validate)

        assert code == 1
        payload = json.loads(out)
        assert payload["valid"] is False
        assert any("enabled" in error for error in payload["errors"])

    def test_live_missing_secret_names_variable_not_value(self, tmp_path, monkeypatch):
        canary = "0x" + _canary_hex()
        monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
        monkeypatch.setenv("PRIMARY_PRIVATE_KEY", canary)
        roster = _roster_yaml(mode="live", key_env="PRIMARY_LIVE_KEY")
        cfg = _config(tmp_path, monkeypatch, trading_mode="live", roster=roster)
        monkeypatch.setattr(config_command, "load_config", lambda *a, **k: cfg)

        code, out = _capture(config_command._run_config_validate)

        assert code == 1
        assert "PRIMARY_LIVE_KEY" in out
        assert canary not in out


class TestConfigPrint:
    def test_print_includes_secret_free_copiers_section(self, tmp_path, monkeypatch):
        canary = "0x" + _canary_hex()
        monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
        monkeypatch.setenv("PRIMARY_PRIVATE_KEY", canary)
        cfg = _config(tmp_path, monkeypatch)
        monkeypatch.setattr(config_command, "load_config", lambda *a, **k: cfg)

        code, out = _capture(
            config_command._run_config_print, argparse.Namespace(redacted=True)
        )

        assert code == 0
        payload = json.loads(out)
        assert "copiers" in payload
        assert [c["id"] for c in payload["copiers"]] == ["primary"]
        assert payload["copiers"][0]["funder_address"] == FUNDER_A
        assert canary not in out

    def test_print_fails_closed_on_invalid_roster(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
        roster = _roster_yaml().replace('max_order_quote: "5.00"', "max_order_quote: \"-5.00\"")
        cfg = _config(tmp_path, monkeypatch, roster=roster)
        monkeypatch.setattr(config_command, "load_config", lambda *a, **k: cfg)

        code, out = _capture(
            config_command._run_config_print, argparse.Namespace(redacted=True)
        )

        assert code == 1
        payload = json.loads(out)
        assert payload["error"] == "copiers_config_invalid"
        assert "max_order_quote" in payload["details"]
