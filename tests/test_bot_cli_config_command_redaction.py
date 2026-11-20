"""``pmcopy config print`` must redact secrets (Task 5 redactor integration).

Canaries are built at runtime (never committed literals). The command must:
- replace credential-shaped KEYS (the shared ``is_secret_key`` engine, not a
  command-local list), and
- pass string VALUES through ``redact_text`` so provider keys embedded in
  URLs (``/v2/<key>``, query parameters, userinfo) never print in cleartext.
"""

from __future__ import annotations

import argparse
import io
import json
import uuid
from contextlib import redirect_stdout

import pytest

from pmcopy.bot_cli.commands import config as config_command
from pmcopy.config.app_config import load_config
from pmcopy.utils.log_redaction import REDACTED_PLACEHOLDER

ADDRESS = "0x" + "4" * 40


def _canary_hex(length: int = 32) -> str:
    return (uuid.uuid4().hex + uuid.uuid4().hex)[:length]


def _config(overrides: dict | None = None) -> object:
    merged: dict = {
        "database": {"dsn": "postgresql://probe:probe@localhost/probe"},
        "wallet": {"funder_address": ADDRESS},
        "polymarket": {"rpc_url": "https://polygon-mainnet.g.alchemy.com/v2/test-key-123"},
        "detection": {
            "onchain": {"alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key-123"}
        },
    }
    if overrides:
        for section, payload in overrides.items():
            merged.setdefault(section, {}).update(payload)
    return load_config(merged)


class TestKeyRedactionEngine:
    """The command must use the shared credential-key engine."""

    @pytest.mark.parametrize(
        "key",
        [
            "postgres_password",
            "db_passwd",
            "client_credentials",
            "authorization_header",
            "wallet_private_key",
        ],
    )
    def test_shared_engine_keys_are_redacted(self, key: str) -> None:
        value = _canary_hex()
        assert config_command._redact_value(key, value) == REDACTED_PLACEHOLDER

    def test_market_identity_keys_stay_readable(self) -> None:
        assert config_command._redact_value("token_id", "123") == "123"
        assert config_command._redact_value("condition_ids", "abc") == "abc"
        assert config_command._redact_value("tokens", ["a", "b"]) == ["a", "b"]


class TestPatternRedactionOfValues:
    def test_rpc_url_provider_path_key_is_masked(self) -> None:
        canary = _canary_hex(32)
        cfg = _config(
            {"polymarket": {"rpc_url": f"https://polygon-mainnet.g.alchemy.com/v2/{canary}"}}
        )
        rendered = json.dumps(config_command._config_to_dict(cfg, redacted=True))

        assert canary not in rendered
        assert REDACTED_PLACEHOLDER in rendered

    def test_url_query_parameter_secret_is_masked(self) -> None:
        canary = _canary_hex()
        cfg = _config({"wallet": {"polygon_rpc_url": f"https://rpc.invalid/?api_key={canary}"}})
        rendered = json.dumps(config_command._config_to_dict(cfg, redacted=True))

        assert canary not in rendered

    def test_dsn_password_is_masked_in_printed_value(self) -> None:
        canary = _canary_hex()
        cfg = _config({"database": {"dsn": f"postgresql://probe:{canary}@localhost/probe"}})
        rendered = json.dumps(config_command._config_to_dict(cfg, redacted=True))

        assert canary not in rendered


class TestPrintCommandEndToEnd:
    def _print(self, cfg: object) -> str:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code = config_command._run_config_print(argparse.Namespace(redacted=True))
        assert exit_code == 0
        return buffer.getvalue()

    def test_runtime_canaries_never_survive_config_print(self, monkeypatch) -> None:
        dsn_canary = _canary_hex()
        path_key_canary = _canary_hex(32)
        cfg = _config(
            {
                "database": {"dsn": f"postgresql://u:{dsn_canary}@localhost/db"},
                "polymarket": {
                    "rpc_url": f"https://polygon-mainnet.g.alchemy.com/v2/{path_key_canary}"
                },
            }
        )
        # Swap the loader for the command's process-level call.
        monkeypatch.setattr(config_command, "load_config", lambda *a, **k: cfg)

        output = self._print(cfg)

        assert dsn_canary not in output
        assert path_key_canary not in output
        assert json.loads(output)  # output remains valid JSON

    def test_secret_fields_are_masked_by_key(self) -> None:
        cfg = _config()
        payload = config_command._config_to_dict(cfg, redacted=True)
        text = json.dumps(payload, default=str)

        assert payload["database"]["dsn"] == REDACTED_PLACEHOLDER
        assert payload["wallet"]["private_key"] == REDACTED_PLACEHOLDER
        assert REDACTED_PLACEHOLDER in text

    def test_unredacted_mode_is_reserved_and_still_masks_secretstr(self) -> None:
        """``--redacted`` is the only mode; SecretStr never leaks either way."""
        cfg = _config()
        payload = config_command._config_to_dict(cfg, redacted=False)
        text = json.dumps(payload, default=str)
        assert "SecretStr" not in text  # pydantic masks values by default


class TestListElementRedaction:
    """Follow-up F3-4: list values must run string elements through redaction.

    Scalar string fields already pass through ``redact_text``; a list of
    strings (tags, endpoint allowlists, notes) currently skips value
    redaction entirely, so a provider key embedded in a URL inside a list
    element printed in cleartext.
    """

    def test_list_string_elements_pattern_redacted(self) -> None:
        canary = _canary_hex(32)
        value = [f"https://rpc.probe.invalid/v2/{canary}", "https://rpc.invalid"]
        out = config_command._redact_value("tags", value)

        assert isinstance(out, list)
        rendered = json.dumps(out)
        assert canary not in rendered
        assert REDACTED_PLACEHOLDER in rendered
        # Non-secret list elements stay readable.
        assert out[1] == "https://rpc.invalid"

    def test_list_query_parameter_secret_redacted(self) -> None:
        canary = _canary_hex()
        out = config_command._redact_value("endpoints", [f"https://x.test/?token={canary}"])
        assert canary not in json.dumps(out)

    def test_list_non_string_elements_unchanged(self) -> None:
        value = [1, 2.5, None, True]
        assert config_command._redact_value("weights", value) == value

    def test_nested_list_elements_redacted(self) -> None:
        canary = _canary_hex(32)
        value = [["https://rpc.probe.invalid/v2/" + canary]]
        out = config_command._redact_value("groups", value)
        assert canary not in json.dumps(out)

    def test_dataclass_list_field_redacted_through_config_to_dict(self) -> None:
        import dataclasses

        canary = _canary_hex(32)

        @dataclasses.dataclass
        class _Probe:
            tags: list[str]

        rendered = json.dumps(
            config_command._config_to_dict(_Probe([f"https://rpc.probe.invalid/v2/{canary}"]))
        )
        assert canary not in rendered
        assert REDACTED_PLACEHOLDER in rendered

    def test_unredacted_mode_leaves_list_elements_alone(self) -> None:
        import dataclasses

        canary = _canary_hex(32)

        @dataclasses.dataclass
        class _Probe:
            tags: list[str]

        url = f"https://rpc.probe.invalid/v2/{canary}"
        assert config_command._config_to_dict(_Probe([url]), redacted=False) == {"tags": [url]}
