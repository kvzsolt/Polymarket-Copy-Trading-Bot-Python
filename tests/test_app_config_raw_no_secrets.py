"""Regression: AppConfig.raw must not carry credential values.

Pre-fix ``raw=dict(env)`` copied the entire process environment into
the in-memory AppConfig.raw mapping. Multiple call sites
(runtime_summary, trade_pipeline, executionService) accessed
``config.raw`` for structured logging and debug surfacing; any of
them serialising the mapping wholesale leaked the PRIVATE_KEY and
CLOB credentials. Filter at the load_config boundary so the
sensitive values are physically absent from the object.
"""
from __future__ import annotations


from config.app_config import _RAW_SENSITIVE_KEYS, load_config


def _valid_env(extras: dict[str, str] | None = None) -> dict[str, str]:
    base = {
        "PRIVATE_KEY": "0x" + "1" * 64,
        "FUNDER_ADDRESS": "0x" + "a" * 40,
        "USER_ADDRESS": "0x" + "a" * 40,
        "RPC_URL": "https://rpc.example",
        "LEADER_WALLETS": "0x" + "b" * 40,
        "CLOB_API_KEY": "supersecret-key",
        "CLOB_SECRET": "supersecret-secret",
        "CLOB_PASSPHRASE": "supersecret-passphrase",
        "FALCON_API_TOKEN": "supersecret-falcon",
        "DATABASE_URL": "postgresql://user:pass@localhost/db",
        "PAPER_TRADE_ONLY": "true",
    }
    if extras:
        base.update(extras)
    return base


def test_private_key_stripped_from_raw():
    cfg = load_config(_valid_env())
    assert "PRIVATE_KEY" not in cfg.raw, (
        "PRIVATE_KEY must be physically absent from AppConfig.raw"
    )
    assert cfg.raw.get("PRIVATE_KEY") is None
    # And the public-info keys must still survive.
    assert cfg.raw.get("FUNDER_ADDRESS") == "0x" + "a" * 40


def test_clob_credentials_stripped_from_raw():
    cfg = load_config(_valid_env())
    for sensitive in (
        "CLOB_API_KEY",
        "CLOB_SECRET",
        "CLOB_PASSPHRASE",
        "FALCON_API_TOKEN",
        "DATABASE_URL",
    ):
        assert sensitive not in cfg.raw, (
            f"{sensitive} must not leak into AppConfig.raw"
        )


def test_clob_credentials_loaded_into_typed_config():
    cfg = load_config(_valid_env())
    assert cfg.clob.api_key == "supersecret-key"
    assert cfg.clob.api_secret == "supersecret-secret"
    assert cfg.clob.api_passphrase == "supersecret-passphrase"


def test_sensitive_set_documented():
    """The sensitive-key set itself is part of the contract — drift here
    needs an explicit decision (and probably a new test)."""
    expected = {
        "PRIVATE_KEY",
        "PRIVATE_KEY_FILE",
        "CLOB_API_KEY",
        "CLOB_SECRET",
        "CLOB_PASSPHRASE",
            "FALCON_API_TOKEN",
            "DATABASE_URL",
            "RPC_URL",
            "FALLBACK_RPC_URL",
            "ONCHAIN_WS_URL",
            "PENDING_TX_WS_URL",
        }
    assert set(_RAW_SENSITIVE_KEYS) == expected
