from __future__ import annotations

import logging
import os
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml
from pydantic import SecretStr, ValidationError

from config.app_config import DiscoveryConfig, EnvValidationError, load_config
from config.env import SignatureType, secret_value

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "1" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": ADDRESS,
            "polygon_rpc_url": "https://rpc.invalid",
        },
        "network": {"rpc_url": "https://rpc.invalid"},
    }


def test_checked_in_yaml_loads_as_immutable_typed_config() -> None:
    config = load_config(_overrides())
    assert config.execution.trading_mode == "paper"
    assert config.execution.paper_trade_only is True
    assert config.dry_run is True
    # Characterizes the shipped deployment value: 250 has been the packaged
    # trader_discovery.yaml value (and the ShortlistConfig default) since the
    # discovery-architecture rewrite (93988d4); the previous 150 here was drift.
    assert config.discovery.shortlist.candidate_pool_size == 250
    with pytest.raises(FrozenInstanceError):
        config.database.dsn = "changed"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        config.discovery.scheduler.enabled = True  # type: ignore[misc]


def test_checked_in_yaml_keeps_pending_onchain_detection_opt_in() -> None:
    """Pending/mempool signals are provisional — never on by default.

    ``app.main()`` reads only ``detection.mode``; the deprecated boolean is
    consulted by ``DetectionModeResolver`` when no layer sets ``mode`` and
    derives ``on_chain_confirmed``. A checked-in ``true`` would therefore turn
    on the on-chain source for every operator who configures a WS endpoint,
    so the base file must keep it ``false`` (see
    ``test_checked_in_yaml_keeps_detection_mode_polling`` for the mode
    invariant itself).
    """
    config = load_config(_overrides())

    assert config.detection.onchain.enable_onchain_pending_detection is False


def test_funder_address_is_required_in_paper_mode() -> None:
    """Paper has no SDK-resolved wallet, so the funder IS the identity.

    ``create_polymarket_client(authenticated=False)`` returns a public client
    whose ``wallet_address`` is None, so nothing back-fills the funder later.
    An empty value would reach ``get_my_balance("")`` and fail readiness with
    no actionable error, so this must fail closed at config validation.
    """
    overrides = _overrides()
    overrides["wallet"].pop("funder_address")

    with pytest.raises(EnvValidationError, match="FUNDER_ADDRESS is required"):
        load_config(overrides)


def test_funder_address_stays_optional_in_live_mode() -> None:
    """Live derives the Deposit Wallet from PRIVATE_KEY — one secret, as designed.

    ``create_polymarket_client`` resolves the wallet through the SDK and only
    cross-checks a configured funder, so requiring one here would break the
    documented single-secret auth chain.
    """
    overrides = _overrides()
    overrides["wallet"].pop("funder_address")
    overrides["wallet"]["private_key"] = "0x" + "a" * 64
    overrides["execution"] = {"trading_mode": "live"}

    config = load_config(overrides)

    assert config.wallet.funder_address == ""
    assert config.execution.paper_trade_only is False


def test_funder_address_still_rejects_malformed_values() -> None:
    overrides = _overrides()
    overrides["wallet"]["funder_address"] = "0xnope"

    with pytest.raises(EnvValidationError, match="Invalid FUNDER_ADDRESS"):
        load_config(overrides)


# ---------------------------------------------------------------------------
# Non-finite numeric rejection. A NaN threshold makes every comparison against
# it False, silently disabling the limit it encodes; an infinite one pins the
# limit open. Both must fail closed at load, whatever layer supplied them.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("literal", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize(
    ("section", "payload", "location"),
    [
        # Frozen-dataclass section: inherits the shell's allow_inf_nan.
        ("sizing", {"percentage": None}, "sizing.percentage"),
        ("detection", {"fetch_interval": None}, "detection.fetch_interval"),
        ("risk", {"max_market_exposure_usdc": None}, "risk.max_market_exposure_usdc"),
        # Pydantic BaseModel section: carries its own model_config, so it only
        # rejects because _FrozenSettingsModel sets allow_inf_nan=False.
        (
            "discovery",
            {"shortlist": {"min_pnl_15d": None}},
            "discovery.shortlist.min_pnl_15d",
        ),
    ],
)
def test_non_finite_numbers_are_rejected_in_every_section(
    literal: float,
    section: str,
    payload: dict,
    location: str,
) -> None:
    def _fill(node: dict) -> dict:
        return {
            key: _fill(value) if isinstance(value, dict) else literal for key, value in node.items()
        }

    overrides = _overrides()
    overrides[section] = _fill(payload)

    with pytest.raises(EnvValidationError) as excinfo:
        load_config(overrides)

    message = str(excinfo.value)
    assert f"{location}: Input should be a finite number" in message
    # Aggregated + redacted: the rejected input is never echoed back.
    assert "Invalid application configuration:" in message
    assert repr(literal) not in message


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf"])
def test_non_finite_numbers_are_rejected_from_environment_strings(
    raw: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Env values arrive as strings and still coerce to non-finite floats."""
    monkeypatch.setenv("PMCOPY__SIZING__PERCENTAGE", raw)

    with pytest.raises(EnvValidationError, match="Input should be a finite number"):
        load_config(_overrides())


@pytest.mark.parametrize("raw", [".nan", ".inf", "-.inf"])
def test_non_finite_numbers_are_rejected_from_yaml_literals(
    raw: str,
    tmp_path: Path,
) -> None:
    """``.nan``/``.inf`` are valid YAML floats, not strings."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    sizing = config_dir / "sizing.yml"
    sizing.write_text(f"percentage: {raw}\n", encoding="utf-8")

    assert isinstance(yaml.safe_load(sizing.read_text())["percentage"], float)

    with pytest.raises(EnvValidationError, match="Input should be a finite number"):
        load_config(_overrides(), config_dir=config_dir)


def test_finite_numeric_values_still_load() -> None:
    """Preservation: the constraint must only reject non-finite values."""
    overrides = _overrides()
    overrides["sizing"] = {"percentage": 12.5}
    overrides["discovery"] = {"shortlist": {"min_pnl_15d": -250.0}}

    config = load_config(overrides)

    assert config.sizing.percentage == 12.5
    assert config.discovery.shortlist.min_pnl_15d == -250.0


def test_packaged_yaml_pending_detection_matches_typed_default() -> None:
    """The YAML layer must not silently re-enable a fail-closed code default."""
    packaged = yaml.safe_load((ROOT / "config" / "detection.yml").read_text())

    assert packaged["onchain"]["enable_onchain_pending_detection"] is False


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (0, SignatureType.EOA),
        (1, SignatureType.POLY_PROXY),
        (2, SignatureType.GNOSIS_SAFE),
        (3, SignatureType.DEPOSIT_WALLET),
    ],
)
def test_all_wallet_signature_hints_are_accepted(
    raw_value: int,
    expected: SignatureType,
) -> None:
    overrides = _overrides()
    overrides["wallet"]["signature_type"] = raw_value

    config = load_config(overrides)

    assert config.wallet.signature_type == expected


def test_legacy_local_shim_overrides_canonical_service_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    (deployment / "config" / "runtime.local.yml").write_text(
        yaml.safe_dump(
            {
                "wallet": {
                    "funder_address": ADDRESS,
                    "polygon_rpc_url": "https://local-rpc.invalid",
                },
                "network": {"rpc_url": "https://local-rpc.invalid"},
                "database": {"dsn": "postgresql://test:test@localhost/test"},
                "sizing": {"percentage": 7.5},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config()

    assert config.sizing.percentage == 7.5
    assert config.wallet.polygon_rpc_url == "https://local-rpc.invalid"


def test_default_local_and_secrets_paths_follow_deployment_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "runtime.local.yml").write_text(
        yaml.safe_dump({"sizing": {"percentage": 9.5}}),
        encoding="utf-8",
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=deployment-token\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    monkeypatch.chdir(tmp_path)
    # This test exercises the CWD walk-up itself, so drop the hermetic
    # deployment-root pin installed by the autouse conftest fixture.
    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)

    config = load_config(_overrides())

    assert config.sizing.percentage == 9.5
    assert config.discovery.api.token.get_secret_value() == "deployment-token"


def test_process_env_beats_mode_0600_secrets_and_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__SIZING__PERCENTAGE=6\nPMCOPY__DISCOVERY__API__TOKEN=top-secret-value\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    monkeypatch.setenv("PMCOPY__SIZING__PERCENTAGE", "8")
    config = load_config(_overrides(), secrets_path=secrets)
    assert config.sizing.percentage == 8
    assert config.discovery.api.has_token is True
    assert "top-secret-value" not in repr(config.discovery.api.token)


def test_wallet_private_key_is_secret_and_clob_credentials_are_removed() -> None:
    private_key = "0x" + "a" * 64
    overrides = _overrides()
    overrides["wallet"]["private_key"] = private_key

    config = load_config(overrides)

    assert isinstance(config.wallet.private_key, SecretStr)
    assert secret_value(config.wallet.private_key) == private_key
    assert not hasattr(config, "clob")
    assert private_key not in repr(config)


def test_obsolete_clob_credentials_fail_with_migration_error() -> None:
    overrides = _overrides()
    overrides["clob"] = {"api_key": "obsolete"}

    with pytest.raises(EnvValidationError, match="clob.api_key"):
        load_config(overrides)


@pytest.mark.parametrize(
    "key",
    ["venue_backend", "allow_experimental_ccxt_live", "shadow_ccxt"],
)
def test_obsolete_venue_backend_keys_are_rejected(key: str) -> None:
    """Removed venue selection must fail closed, not silently drop."""

    overrides = _overrides()
    overrides["execution"] = {**overrides.get("execution", {}), key: "sdk"}

    with pytest.raises(EnvValidationError, match=key):
        load_config(overrides)


def test_execution_config_no_longer_carries_venue_selection_fields() -> None:
    """CCXT is the only transport; no selector or experimental gate exists."""

    from config.app_config import ExecutionConfig

    config = ExecutionConfig()
    assert not hasattr(config, "venue_backend")
    assert not hasattr(config, "allow_experimental_ccxt_live")
    assert not hasattr(config, "shadow_ccxt")


def test_empty_secret_entries_do_not_erase_yaml_values(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__WALLET__POLYGON_RPC_URL=\n"
        "PMCOPY__NETWORK__RPC_URL=   \n"
        "PMCOPY__DISCOVERY__API__TOKEN=\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    overrides = _overrides()
    overrides["discovery"] = {"api": {"token": "yaml-token"}}

    config = load_config(overrides, secrets_path=secrets)

    assert config.wallet.polygon_rpc_url == "https://rpc.invalid"
    assert config.polymarket.rpc_url == "https://rpc.invalid"
    assert config.discovery.api.token.get_secret_value() == "yaml-token"


def test_secrets_file_requires_exact_0600(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__DISCOVERY__API__TOKEN=secret\n", encoding="utf-8")
    secrets.chmod(0o640)
    with pytest.raises(EnvValidationError, match="0600"):
        load_config(_overrides(), secrets_path=secrets)


@pytest.mark.skipif(os.name == "nt", reason="POSIX O_NOFOLLOW hardening")
def test_secrets_file_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.env"
    target.write_text("PMCOPY__SIZING__PERCENTAGE=6\n", encoding="utf-8")
    target.chmod(0o600)
    secrets = tmp_path / "secrets.env"
    secrets.symlink_to(target)

    with pytest.raises(EnvValidationError, match="non-symlink"):
        load_config(_overrides(), secrets_path=secrets)


def test_secrets_read_uses_validated_file_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__SIZING__PERCENTAGE=6\n", encoding="utf-8")
    secrets.chmod(0o600)
    replacement = tmp_path / "replacement.env"
    replacement.write_text("PMCOPY__SIZING__PERCENTAGE=9\n", encoding="utf-8")
    replacement.chmod(0o600)
    real_fstat = os.fstat
    swapped = False

    def replace_path_after_open(fd: int) -> os.stat_result:
        nonlocal swapped
        info = real_fstat(fd)
        if not swapped:
            os.replace(replacement, secrets)
            swapped = True
        return info

    monkeypatch.setattr("config.app_config.os.fstat", replace_path_after_open)

    config = load_config(_overrides(), secrets_path=secrets)

    assert swapped is True
    assert config.sizing.percentage == 6


def test_secrets_reject_duplicate_and_non_nested_application_keys(
    tmp_path: Path,
) -> None:
    duplicate = tmp_path / "duplicate.env"
    duplicate.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=one\nPMCOPY__DISCOVERY__API__TOKEN=two\n",
        encoding="utf-8",
    )
    duplicate.chmod(0o600)
    with pytest.raises(EnvValidationError, match="Duplicate key"):
        load_config(_overrides(), secrets_path=duplicate)

    flat = tmp_path / "flat.env"
    flat.write_text("PRIVATE_KEY=never-accepted\n", encoding="utf-8")
    flat.chmod(0o600)
    with pytest.raises(EnvValidationError, match="PMCOPY__SECTION__FIELD"):
        load_config(_overrides(), secrets_path=flat)


def test_unknown_yaml_key_fails_closed(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    payload = yaml.safe_load((ROOT / "config/execution.yml").read_text())
    payload["tradeing_mode"] = "live"
    (config_dir / "execution.yml").write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(EnvValidationError, match="tradeing_mode"):
        load_config(
            _overrides(),
            config_dir=config_dir,
            secrets_path=tmp_path / "missing.env",
        )


def test_unknown_nested_process_key_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PMCOPY__EXECUTION__TRADEING_MODE", "live")
    with pytest.raises(EnvValidationError, match="tradeing_mode"):
        load_config(_overrides())


def test_nested_process_json_decodes_wallet_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wallet = "0x" + "2" * 40
    monkeypatch.setenv("PMCOPY__LEADER__WALLETS", f'["{wallet}"]')
    config = load_config(_overrides())
    # Legacy env alias routes to the copy_trading owner at the loader boundary.
    assert config.copy_trading.wallets == (wallet,)


def test_runtime_polymarket_rpc_url_is_required() -> None:
    overrides = _overrides()
    overrides["network"]["rpc_url"] = ""
    with pytest.raises(EnvValidationError, match="polymarket.rpc_url is required"):
        load_config(overrides)


def test_secrets_template_omits_runtime_derived_clob_auth() -> None:
    template = (ROOT / "secrets.env.example").read_text(encoding="utf-8")
    assert template.endswith("\n")
    assert "PMCOPY__WALLET__PRIVATE_KEY=" in template
    assert "PMCOPY__WALLET__FUNDER_ADDRESS=" not in template
    assert "PMCOPY__CLOB__API_KEY=" not in template
    assert "PMCOPY__CLOB__API_SECRET=" not in template
    assert "PMCOPY__CLOB__API_PASSPHRASE=" not in template


def test_validation_error_never_echoes_secret(
    tmp_path: Path,
) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=never-print-this\n"
        "PMCOPY__DISCOVERY__API__MAX_RETRIES=not-an-int\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    with pytest.raises(EnvValidationError) as raised:
        load_config(_overrides(), secrets_path=secrets)
    assert "never-print-this" not in str(raised.value)


def test_flat_legacy_override_is_rejected() -> None:
    with pytest.raises(EnvValidationError, match="Flat environment overrides"):
        load_config({"FUNDER_ADDRESS": ADDRESS})


def test_scoring_and_longitudinal_weights_are_validated() -> None:
    with pytest.raises(ValidationError, match="weights must sum to 1.0"):
        DiscoveryConfig.model_validate({"scoring": {"weights": {"falcon_quality": 0.99}}})


def test_trading_mode_is_single_immutable_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PMCOPY__EXECUTION__TRADING_MODE", "live")
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64
    config = load_config(overrides)
    assert config.execution.trading_mode == "live"
    assert config.execution.paper_trade_only is False
    assert config.dry_run is False


# ---------------------------------------------------------------------------
# detection.mode selector (Slice 0)
# ---------------------------------------------------------------------------


def _onchain_overrides(mode: str, **onchain: object) -> dict:
    overrides = _overrides()
    section: dict = {"mode": mode}
    if onchain:
        section["onchain"] = dict(onchain)
    overrides["detection"] = section
    return overrides


def test_checked_in_yaml_keeps_detection_mode_polling() -> None:
    """Base YAML must never select an on-chain source; polling is the default."""
    from domain.detection import DetectionMode

    packaged = yaml.safe_load((ROOT / "config" / "detection.yml").read_text())
    # The base file must not pin ``mode`` at all: an explicit base value would
    # shadow the deprecated-boolean derivation in every real deployment (I1).
    assert packaged.get("mode", "polling") == "polling"

    config = load_config(_overrides())
    assert config.detection.mode == "polling"
    assert config.detection.detection_mode is DetectionMode.POLLING


def test_legacy_pending_flag_in_higher_layer_derives_on_chain_confirmed(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legacy ``enable_onchain_pending_detection: true`` above the base layer
    still derives ``on_chain_confirmed`` (with the deprecation WARN) through the
    real loader — the base YAML must not shadow the derivation."""
    import config.app_config as app_config_mod
    from domain.detection import DetectionMode

    monkeypatch.setattr(
        app_config_mod, "_DETECTION_MODE_WARNINGS", app_config_mod.DetectionModeWarningEmitter()
    )
    overrides = _overrides()
    overrides["detection"] = {
        "onchain": {
            "enable_onchain_pending_detection": True,
            "pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted",
        }
    }
    with caplog.at_level(logging.WARNING, logger="AppConfig"):
        config = load_config(overrides, environment="paper")

    assert config.detection.detection_mode is DetectionMode.ON_CHAIN_CONFIRMED
    warnings = [
        r.getMessage()
        for r in caplog.records
        if "detection_legacy_flags_deprecated" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "derived_mode=on_chain_confirmed" in warnings[0]
    assert "from=enable_onchain_pending_detection" in warnings[0]
    # Lazy logging: the record carries the template and args, not a pre-rendered string.
    record = next(
        r for r in caplog.records if "detection_legacy_flags_deprecated" in r.getMessage()
    )
    assert "%s" in str(record.msg)
    assert record.args


def test_detection_mode_deprecation_warning_is_emitted_once_per_process(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import config.app_config as app_config_mod

    monkeypatch.setattr(
        app_config_mod, "_DETECTION_MODE_WARNINGS", app_config_mod.DetectionModeWarningEmitter()
    )
    overrides = _overrides()
    overrides["detection"] = {
        "onchain": {
            "enable_onchain_pending_detection": True,
            "pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted",
        }
    }
    with caplog.at_level(logging.WARNING, logger="AppConfig"):
        load_config(overrides, environment="paper")
        load_config(overrides, environment="paper")

    warnings = [r for r in caplog.records if "detection_legacy_flags_deprecated" in r.getMessage()]
    assert len(warnings) == 1


def test_detection_mode_env_override_selects_on_chain_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from domain.detection import DetectionMode

    monkeypatch.setenv("PMCOPY__DETECTION__MODE", "on_chain_pending")
    monkeypatch.setenv(
        "PMCOPY__DETECTION__ONCHAIN__PENDING_TX_WS_URL",
        "wss://polygon.example.invalid/v2/redacted",
    )
    config = load_config(_overrides())
    assert config.detection.detection_mode is DetectionMode.ON_CHAIN_PENDING


def test_on_chain_mode_without_ws_url_fails_closed() -> None:
    with pytest.raises(EnvValidationError, match="detection.mode=on_chain_confirmed requires"):
        load_config(_onchain_overrides("on_chain_confirmed"))


def test_on_chain_mode_with_non_alchemy_provider_fails_closed() -> None:
    with pytest.raises(EnvValidationError, match="pending_tx_provider"):
        load_config(
            _onchain_overrides(
                "on_chain_confirmed",
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_tx_provider="infura",
            )
        )


def test_on_chain_pending_with_hashes_only_fails_closed() -> None:
    with pytest.raises(EnvValidationError, match="pending_tx_hashes_only"):
        load_config(
            _onchain_overrides(
                "on_chain_pending",
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_tx_hashes_only=True,
            )
        )


def test_on_chain_pending_rejects_confidence_above_pending_probable() -> None:
    """A floor above ``pending_probable`` silently drops every pending signal.

    ``PendingTxDetector`` emits at most ``PENDING_PROBABLE`` for a mempool
    sighting, so requiring ``mined_authoritative``/``confirmed_final`` in
    ``on_chain_pending`` mode is a mode that detects nothing at all.
    """
    with pytest.raises(EnvValidationError) as excinfo:
        load_config(
            _onchain_overrides(
                "on_chain_pending",
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_tx_detection_confidence_required="mined_authoritative",
            )
        )
    message = str(excinfo.value)
    assert "pending_tx_detection_confidence_required" in message
    assert "drops every pending signal" in message


def test_on_chain_pending_accepts_market_unattributed_confidence() -> None:
    """The looser floor is still a usable pending configuration."""
    config = load_config(
        _onchain_overrides(
            "on_chain_pending",
            pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
            pending_tx_detection_confidence_required="market_unattributed",
        )
    )
    onchain = config.detection.onchain
    assert onchain.pending_tx_detection_confidence_required == "market_unattributed"


def test_on_chain_confirmed_allows_confidence_above_pending_probable() -> None:
    """The confirmed source is receipt-backed; the pending floor does not bind."""
    config = load_config(
        _onchain_overrides(
            "on_chain_confirmed",
            pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
            pending_tx_detection_confidence_required="mined_authoritative",
        )
    )
    onchain = config.detection.onchain
    assert onchain.pending_tx_detection_confidence_required == "mined_authoritative"


def test_on_chain_pending_requires_positive_confirmation_deadline() -> None:
    """A non-positive deadline would mark every tracked copy unconfirmed at once."""
    with pytest.raises(
        EnvValidationError, match="pending_copy_confirmation_deadline_seconds must be > 0"
    ):
        load_config(
            _onchain_overrides(
                "on_chain_pending",
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_copy_confirmation_deadline_seconds=0,
            )
        )


def test_on_chain_pending_requires_positive_confirmation_max_tracked() -> None:
    """A non-positive ceiling would drop every tracked pending copy."""
    with pytest.raises(
        EnvValidationError, match="pending_copy_confirmation_max_tracked must be > 0"
    ):
        load_config(
            _onchain_overrides(
                "on_chain_pending",
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_copy_confirmation_max_tracked=0,
            )
        )


def test_pending_copy_confirmation_defaults_load_from_packaged_yaml() -> None:
    """The packaged base file carries the tracker defaults the typed model uses."""
    packaged = yaml.safe_load((ROOT / "config" / "detection.yml").read_text())
    onchain_yaml = packaged["onchain"]

    assert onchain_yaml["pending_copy_confirmation_enabled"] is True
    assert onchain_yaml["pending_copy_confirmation_deadline_seconds"] == 60.0
    assert onchain_yaml["pending_copy_confirmation_max_tracked"] == 2048
    assert onchain_yaml["pending_copy_auto_unwind_enabled"] is False

    onchain = load_config(_overrides()).detection.onchain
    assert onchain.pending_copy_confirmation_enabled is True
    assert onchain.pending_copy_confirmation_deadline_seconds == 60.0
    assert onchain.pending_copy_confirmation_max_tracked == 2048
    assert onchain.pending_copy_auto_unwind_enabled is False


def test_pending_copy_confirmation_keys_are_overridable() -> None:
    """Operators can retune the tracker without touching the base file."""
    config = load_config(
        _onchain_overrides(
            "on_chain_pending",
            pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
            pending_copy_confirmation_enabled=False,
            pending_copy_confirmation_deadline_seconds=30.5,
            pending_copy_confirmation_max_tracked=64,
            pending_copy_auto_unwind_enabled=True,
        )
    )
    onchain = config.detection.onchain
    assert onchain.pending_copy_confirmation_enabled is False
    assert onchain.pending_copy_confirmation_deadline_seconds == 30.5
    assert onchain.pending_copy_confirmation_max_tracked == 64
    assert onchain.pending_copy_auto_unwind_enabled is True


def test_invalid_detection_mode_rejected() -> None:
    """``DetectionConfig.mode`` is a ``Literal``: pydantic rejects an unknown
    mode before semantic validation, so ``detection_mode`` is total."""
    with pytest.raises(EnvValidationError, match="detection.mode: Input should be 'polling'"):
        load_config(_onchain_overrides("mempool"))


def test_on_chain_mode_with_copy_tasks_enabled_fails_closed() -> None:
    """I5: the v1 copy-task route has no cross-source dedup yet."""
    overrides = _onchain_overrides(
        "on_chain_confirmed",
        pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
    )
    overrides["copy_trading"] = {"copy_tasks": {"enabled": True}}
    with pytest.raises(
        EnvValidationError,
        match="detection.mode=on_chain_confirmed is not supported with copy_trading.copy_tasks.enabled=true",
    ):
        load_config(overrides)


def test_polling_mode_with_copy_tasks_enabled_is_allowed() -> None:
    overrides = _overrides()
    overrides["copy_trading"] = {"copy_tasks": {"enabled": True}}
    config = load_config(overrides)
    assert config.copy_trading.copy_tasks.enabled is True
    assert config.detection.mode == "polling"


def test_live_environment_selects_on_chain_confirmed() -> None:
    """The checked-in live environment opts into the receipt-confirmed source."""
    from domain.detection import DetectionMode

    packaged = yaml.safe_load(
        (ROOT / "config" / "environments" / "live" / "detection.yml").read_text()
    )
    assert packaged["mode"] == "on_chain_confirmed"

    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64
    overrides["detection"] = {
        "onchain": {"pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted"}
    }
    config = load_config(overrides, environment="live")

    assert config.environment == "live"
    assert config.execution.trading_mode == "live"
    assert config.detection.detection_mode is DetectionMode.ON_CHAIN_CONFIRMED
    assert config.detection.onchain.pending_tx_provider == "alchemy"


def test_live_environment_without_ws_url_fails_closed() -> None:
    """Selecting an on-chain mode in live without a WS endpoint is a startup error."""
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64
    with pytest.raises(EnvValidationError, match="detection.mode=on_chain_confirmed requires"):
        load_config(overrides, environment="live")
