"""Phase 1 tests — strongly-typed AppConfig and load_config factory.

Validates the contract of ``config.app_config``:
    * Happy-path load from a minimal valid env dict.
    * Missing / malformed required values produce descriptive errors.
    * Cross-field semantic invariants are enforced.
    * Dataclass immutability is enforced.
    * Multiple problems aggregate into a single bullet-listed error.
"""

from __future__ import annotations

import os
import re
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from config.app_config import (
    AppConfig,
    EnvValidationError,
    SignatureType,
    load_config,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _minimal_valid_env(**overrides: str) -> dict[str, str]:
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "USER_ADDRESS": _VALID_USER,
    }
    env.update(overrides)
    return env


@pytest.fixture(autouse=True)
def _supported_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "config.app_config.sys.version_info",
        SimpleNamespace(major=3, minor=12),
    )


@pytest.fixture
def valid_env() -> dict[str, str]:
    return _minimal_valid_env()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_load_from_minimal_env(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        assert isinstance(cfg, AppConfig)

    def test_wallet_typed_correctly(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        assert cfg.wallet.private_key == _VALID_PRIVATE_KEY
        assert cfg.wallet.funder_address == _VALID_FUNDER
        assert cfg.wallet.chain_id == 137
        assert cfg.wallet.polygon_rpc_url == "https://polygon-rpc.example.com"

    def test_defaults_are_applied(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        # Sizing defaults straight from spec.
        assert cfg.sizing.mode == "proportional"
        assert cfg.sizing.percentage == pytest.approx(2.0)
        assert cfg.sizing.max_position_pct == pytest.approx(0.45)
        # Polling defaults.
        assert cfg.polling.fetch_interval == pytest.approx(0.25)
        # Execution defaults.
        assert cfg.execution.max_retries == 3
        assert cfg.execution.paper_trade_only is False
        # Logging default path.
        assert cfg.logging.pnl_log_file.endswith("pnl.jsonl")

    def test_leader_wallets_parsed_from_csv(self, valid_env: dict[str, str]) -> None:
        valid_env["LEADER_WALLETS"] = f"{_VALID_FUNDER},{_VALID_USER}"
        cfg = load_config(valid_env)
        assert cfg.leader.wallets == (_VALID_FUNDER, _VALID_USER)

    def test_leader_auto_cascade_activates_discovery(self, valid_env: dict[str, str]) -> None:
        valid_env["LEADER_AUTO"] = "true"
        cfg = load_config(valid_env)
        assert cfg.leader.leader_auto is True
        assert cfg.leader.auto_discover_leaders is True
        assert cfg.discovery.enable_trader_discovery is True
        assert cfg.discovery.discovery_promote_to_active is True

    def test_discovery_defaults_include_min_active_positions(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        assert cfg.discovery.min_active_positions == 1

    def test_raw_env_preserved_for_transition_excludes_secrets(
        self, valid_env: dict[str, str]
    ) -> None:
        cfg = load_config(valid_env)
        # PRIVATE_KEY is in the documented sensitive set and must NOT
        # land in AppConfig.raw — typed accessors (wallet.private_key)
        # carry the secret instead.
        assert "PRIVATE_KEY" not in cfg.raw
        # Public-info keys (wallet addresses, RPC endpoint, etc.) stay
        # in .raw for legacy callers still transitioning to typed config.
        assert cfg.raw["FUNDER_ADDRESS"] == _VALID_FUNDER

    def test_network_contract_prefers_collateral_then_pusd(self, valid_env: dict[str, str]) -> None:
        valid_env["USDC_CONTRACT_ADDRESS"] = "0x" + "1" * 40
        valid_env["PUSD_CONTRACT_ADDRESS"] = "0x" + "2" * 40
        valid_env["COLLATERAL_CONTRACT_ADDRESS"] = "0x" + "3" * 40
        cfg = load_config(valid_env)
        assert cfg.network.usdc_contract_address == "0x" + "3" * 40

    def test_network_contract_falls_back_to_pusd_before_legacy_usdc(self, valid_env: dict[str, str]) -> None:
        valid_env["USDC_CONTRACT_ADDRESS"] = "0x" + "1" * 40
        valid_env["PUSD_CONTRACT_ADDRESS"] = "0x" + "2" * 40
        cfg = load_config(valid_env)
        assert cfg.network.usdc_contract_address == "0x" + "2" * 40

    def test_load_without_env_reads_os_environ(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Seed os.environ with the minimal valid set, nuke everything else that might leak in.
        for key in list(os.environ.keys()):
            if key in _minimal_valid_env() or key.startswith(
                (
                    "LEADER_",
                    "COPY_",
                    "TRADE_",
                    "RISK_",
                    "BALANCE_",
                    "DISCOVERY_",
                    "AUTO_",
                    "TIER_",
                    "EXECUTION_",
                    "NEAR_",
                    "PROFIT_",
                    "MAX_",
                    "MIN_",
                    "ENABLE_",
                    "STAGED_",
                )
            ):
                monkeypatch.delenv(key, raising=False)
        for k, v in _minimal_valid_env().items():
            monkeypatch.setenv(k, v)
        cfg = load_config()  # no env arg -> os.environ
        assert isinstance(cfg, AppConfig)
        assert cfg.wallet.private_key == _VALID_PRIVATE_KEY


# ---------------------------------------------------------------------------
# Missing required keys
# ---------------------------------------------------------------------------


class TestMissingRequired:
    def test_missing_private_key(self, valid_env: dict[str, str]) -> None:
        valid_env.pop("PRIVATE_KEY")
        with pytest.raises(EnvValidationError, match="PRIVATE_KEY is required"):
            load_config(valid_env)

    def test_missing_funder_address(self, valid_env: dict[str, str]) -> None:
        valid_env.pop("FUNDER_ADDRESS")
        with pytest.raises(EnvValidationError, match="FUNDER_ADDRESS is required"):
            load_config(valid_env)

    def test_missing_rpc_url(self, valid_env: dict[str, str]) -> None:
        valid_env["RPC_URL"] = ""
        with pytest.raises(EnvValidationError, match="RPC_URL is required"):
            load_config(valid_env)


# ---------------------------------------------------------------------------
# Malformed values
# ---------------------------------------------------------------------------


class TestMalformedValues:
    def test_invalid_private_key_format(self, valid_env: dict[str, str]) -> None:
        valid_env["PRIVATE_KEY"] = "not-a-hex-key"
        with pytest.raises(EnvValidationError, match="PRIVATE_KEY must be a 66-char hex key"):
            load_config(valid_env)

    def test_private_key_file_requires_exact_0600_permissions(
        self, tmp_path, monkeypatch, valid_env: dict[str, str]
    ) -> None:
        # Path-traversal guard requires the key file to live under $HOME
        # or cwd; chdir into tmp_path so the test exercises the mode
        # branch rather than the location branch.
        monkeypatch.chdir(tmp_path)
        key_path = tmp_path / "pk.txt"
        key_path.write_text(_VALID_PRIVATE_KEY, encoding="utf-8")
        key_path.chmod(0o700)
        valid_env["PRIVATE_KEY"] = ""
        valid_env["PRIVATE_KEY_FILE"] = str(key_path)
        with pytest.raises(EnvValidationError, match="permissions 0600 exactly"):
            load_config(valid_env)

    def test_invalid_funder_address_format(self, valid_env: dict[str, str]) -> None:
        valid_env["FUNDER_ADDRESS"] = "0xNOT_A_VALID_ADDRESS"
        with pytest.raises(EnvValidationError, match="Invalid FUNDER_ADDRESS"):
            load_config(valid_env)

    def test_invalid_leader_wallet_in_csv(self, valid_env: dict[str, str]) -> None:
        valid_env["LEADER_WALLETS"] = f"{_VALID_FUNDER},0xBADADDR"
        with pytest.raises(EnvValidationError, match="Invalid leader wallet"):
            load_config(valid_env)

    def test_inline_comment_rejected(self, valid_env: dict[str, str]) -> None:
        valid_env["CHAIN_ID"] = "137 # mainnet"
        with pytest.raises(EnvValidationError, match="inline comment"):
            load_config(valid_env)


    def test_bad_chain_id(self, valid_env: dict[str, str]) -> None:
        valid_env["CHAIN_ID"] = "1"
        with pytest.raises(EnvValidationError, match="Unsupported CHAIN_ID"):
            load_config(valid_env)

    def test_non_numeric_int_field(self, valid_env: dict[str, str]) -> None:
        valid_env["MAX_OPEN_POSITIONS"] = "not-a-number"
        with pytest.raises(
            EnvValidationError,
            match="MAX_OPEN_POSITIONS must be a valid int",
        ):
            load_config(valid_env)


# ---------------------------------------------------------------------------
# Semantic / cross-field invariants
# ---------------------------------------------------------------------------


class TestSemanticInvariants:
    def test_runtime_python_supported_rejects_313_plus(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "config.app_config.sys.version_info",
            SimpleNamespace(major=3, minor=13),
        )
        from config.app_config import _runtime_python_supported
        assert _runtime_python_supported() is False

    def test_bot_state_db_enabled_overrides_legacy_db_enabled(self, valid_env: dict[str, str]) -> None:
        valid_env["DB_ENABLED"] = "false"
        valid_env["BOT_STATE_DB_ENABLED"] = "true"
        cfg = load_config(valid_env)
        assert cfg.database.enabled is True

    def test_max_share_price_must_not_be_below_min_share_price(self, valid_env: dict[str, str]) -> None:
        valid_env["MIN_SHARE_PRICE"] = "0.4"
        valid_env["MAX_SHARE_PRICE"] = "0.3"
        with pytest.raises(EnvValidationError, match="MAX_SHARE_PRICE must be >= MIN_SHARE_PRICE"):
            load_config(valid_env)

    @pytest.mark.parametrize("bad_value", ["0", "1.5"])
    def test_max_position_pct_out_of_range(self, valid_env: dict[str, str], bad_value: str) -> None:
        valid_env["MAX_POSITION_PCT"] = bad_value
        with pytest.raises(EnvValidationError, match=r"MAX_POSITION_PCT must be in \(0, 1\]"):
            load_config(valid_env)

    def test_near_max_price_warn_gt_exit(self, valid_env: dict[str, str]) -> None:
        valid_env["NEAR_MAX_PRICE_WARN"] = "0.98"
        valid_env["NEAR_MAX_PRICE_EXIT"] = "0.95"
        with pytest.raises(
            EnvValidationError,
            match="NEAR_MAX_PRICE_WARN must be <= NEAR_MAX_PRICE_EXIT",
        ):
            load_config(valid_env)

    def test_trust_size_multiplier_min_gt_max(self, valid_env: dict[str, str]) -> None:
        valid_env["TRUST_SIZE_MULTIPLIER_MIN"] = "2.0"
        valid_env["TRUST_SIZE_MULTIPLIER_MAX"] = "1.0"
        with pytest.raises(
            EnvValidationError,
            match=r"TRUST_SIZE_MULTIPLIER_MIN must be <= TRUST_SIZE_MULTIPLIER_MAX",
        ):
            load_config(valid_env)

    def test_adaptive_basis_invalid(self, valid_env: dict[str, str]) -> None:
        valid_env["ADAPTIVE_SIZING_BASIS"] = "moonphase"
        with pytest.raises(
            EnvValidationError,
            match="ADAPTIVE_SIZING_BASIS must be one of: trade_size, performance",
        ):
            load_config(valid_env)

    def test_leader_score_weights_all_zero(self, valid_env: dict[str, str]) -> None:
        valid_env["LEADER_SCORE_WEIGHT_BALANCE"] = "0"
        valid_env["LEADER_SCORE_WEIGHT_ANALYTICS"] = "0"
        valid_env["LEADER_SCORE_WEIGHT_ACTIVITY"] = "0"
        with pytest.raises(
            EnvValidationError, match=r"LEADER_SCORE_WEIGHT_\* values must sum to > 0"
        ):
            load_config(valid_env)

    def test_min_active_positions_must_be_positive(self, valid_env: dict[str, str]) -> None:
        valid_env["MIN_ACTIVE_POSITIONS"] = "0"
        with pytest.raises(EnvValidationError, match="MIN_ACTIVE_POSITIONS must be > 0"):
            load_config(valid_env)


# ---------------------------------------------------------------------------
# Aggregation of multiple errors
# ---------------------------------------------------------------------------


class TestAggregatedErrors:
    def test_multiple_errors_all_reported(self, valid_env: dict[str, str]) -> None:
        valid_env.pop("PRIVATE_KEY")
        valid_env["MAX_POSITION_PCT"] = "1.5"
        valid_env["NEAR_MAX_PRICE_WARN"] = "0.99"
        valid_env["NEAR_MAX_PRICE_EXIT"] = "0.50"
        with pytest.raises(EnvValidationError) as exc:
            load_config(valid_env)
        message = str(exc.value)
        assert "PRIVATE_KEY is required" in message
        assert "MAX_POSITION_PCT" in message
        assert "NEAR_MAX_PRICE_WARN must be <= NEAR_MAX_PRICE_EXIT" in message
        # All errors are bullet-listed.
        assert re.search(r"\n- .+\n- ", message)


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


class TestImmutability:
    def test_app_config_frozen(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        with pytest.raises(FrozenInstanceError):
            cfg.sizing.max_position_pct = 0.99  # type: ignore[misc]

    def test_wallet_config_frozen(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        with pytest.raises(FrozenInstanceError):
            cfg.wallet.private_key = "clobbered"  # type: ignore[misc]

    def test_section_replacement_also_frozen(self, valid_env: dict[str, str]) -> None:
        cfg = load_config(valid_env)
        with pytest.raises(FrozenInstanceError):
            cfg.database = None  # type: ignore[misc]


class TestMarketDataFreshnessConfig:
    """Market-data execution freshness + WSS liveness knobs are env-tunable."""

    def test_defaults(self, valid_env: dict[str, str]) -> None:
        md = load_config(valid_env).market_data
        assert md.executable_quote_max_age_seconds == 2.0
        assert md.executable_book_max_age_seconds == 2.0
        assert md.executable_tick_max_age_seconds == 300.0
        assert md.wss_connection_max_silence_seconds == 20.0
        assert md.rest_bootstrap_max_spread_bps == 300

    def test_overrides(self, valid_env: dict[str, str]) -> None:
        env = dict(valid_env)
        env.update(
            {
                "MARKET_DATA_EXECUTABLE_QUOTE_MAX_AGE_SECONDS": "5.5",
                "MARKET_DATA_EXECUTABLE_BOOK_MAX_AGE_SECONDS": "6.0",
                "MARKET_DATA_EXECUTABLE_TICK_MAX_AGE_SECONDS": "120.0",
                "MARKET_DATA_WSS_CONNECTION_MAX_SILENCE_SECONDS": "45.0",
                "MARKET_DATA_REST_BOOTSTRAP_MAX_SPREAD_BPS": "800",
            }
        )
        md = load_config(env).market_data
        assert md.executable_quote_max_age_seconds == 5.5
        assert md.executable_book_max_age_seconds == 6.0
        assert md.executable_tick_max_age_seconds == 120.0
        assert md.wss_connection_max_silence_seconds == 45.0
        assert md.rest_bootstrap_max_spread_bps == 800

    def test_rejects_non_positive_silence_window(self, valid_env: dict[str, str]) -> None:
        env = dict(valid_env)
        env["MARKET_DATA_WSS_CONNECTION_MAX_SILENCE_SECONDS"] = "0"
        with pytest.raises(EnvValidationError):
            load_config(env)
