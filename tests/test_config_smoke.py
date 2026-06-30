"""P0 config smoke test (deep-research): the shipped runtime env files must load
through python-dotenv + dummy secrets into a valid AppConfig.

This is the regression guard for the original risk-pre-reconcile crash — a
comment-shaped value in the env files reaching ``load_config`` and raising
``EnvValidationError`` at startup. It loads the env files exactly as the runtime
loader does (dotenv semantics) so a malformed committed default fails CI here
rather than at live startup.

``runtime.env`` and ``runtime.local.env`` are gitignored (operator-supplied), so
in a fresh CI checkout only ``runtime.local.example`` exists. The test loads
whichever files are present and always exercises the full ``load_config`` path
with dummy (non-secret) credentials.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values

from config.app_config import load_config

_ROOT = Path(__file__).resolve().parents[1]
# Loader precedence: runtime.env, then runtime.local.env overrides it; the
# tracked example is the CI fallback when neither operator file is present.
_OPERATOR_ENV_FILES = (
    _ROOT / "config" / "runtime.env",
    _ROOT / "config" / "runtime.local.env",
)
_EXAMPLE_ENV_FILE = _ROOT / "config" / "runtime.local.example"

# Non-secret stand-ins so the credentialed branches of load_config run without
# touching real secrets. Mirrors the deep-research reproduction fixture.
_DUMMY_SECRETS = {
    "PRIVATE_KEY": "0x" + "1" * 64,
    "FUNDER_ADDRESS": "0x" + "2" * 40,
    "RPC_URL": "https://polygon-bor-rpc.publicnode.com",
    "POLYGON_RPC_URL": "https://polygon-bor-rpc.publicnode.com",
    "CLOB_API_KEY": "dummy",
    "CLOB_SECRET": "dummy",
    "CLOB_PASSPHRASE": "dummy",
}


def _present_env_files() -> list[Path]:
    operator = [p for p in _OPERATOR_ENV_FILES if p.exists()]
    if operator:
        return operator
    return [p for p in (_EXAMPLE_ENV_FILE,) if p.exists()]


def _merged_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for env_file in _present_env_files():
        for key, value in dotenv_values(env_file).items():
            if value is not None:
                env[key] = value
    # Dummy secrets applied last so they win over any blank template values.
    env.update(_DUMMY_SECRETS)
    return env


def test_runtime_env_files_load_into_appconfig_with_dummy_secrets() -> None:
    cfg = load_config(_merged_env())
    assert cfg is not None
    # Spot-check a couple of typed surfaces actually populated.
    assert cfg.network.ctf_exchange_v2_address
    assert cfg.network.neg_risk_ctf_exchange_v2_address


def test_smoke_covers_at_least_the_tracked_template() -> None:
    # Guards against the test silently degrading to "no files loaded".
    assert _present_env_files(), "no runtime env file present to smoke-test"
