"""Test-only adapter from historical flat fixture dictionaries to nested config."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from pmcopy.config.app_config import SERVICE_CONFIG_FILES, load_config

ROOT = Path(__file__).resolve().parents[1]


def _leaf_paths() -> dict[str, list[tuple[str, ...]]]:
    payload: dict[str, Any] = {}
    for service in SERVICE_CONFIG_FILES:
        path = ROOT / "src" / "pmcopy" / "config" / f"{service}.yml"
        if not path.exists():
            continue
        payload[service] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    output: dict[str, list[tuple[str, ...]]] = {}

    def walk(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                walk(child, (*path, str(key)))
            return
        output.setdefault(path[-1].upper(), []).append(path)
        output.setdefault("_".join(path).upper(), []).append(path)

    walk(payload, ())
    return output


_PATHS = _leaf_paths()
_ALIASES: dict[str, tuple[tuple[str, ...], ...]] = {
    "DATABASE_URL": (("database", "dsn"),),
    # wallet.yml ships no funder_address key, so the leaf-path scan cannot
    # discover this one; without an explicit alias the value fixtures already
    # pass would be silently dropped.
    "FUNDER_ADDRESS": (("wallet", "funder_address"),),
    "ALCHEMY_WSS_URL": (("detection", "onchain", "alchemy_wss_url"),),
    "RPC_URL": (("wallet", "polygon_rpc_url"), ("polymarket", "rpc_url")),
    "CHAIN_ID": (("wallet", "chain_id"), ("polymarket", "chain_id")),
    "LEADER_WALLETS": (("copy_trading", "wallets"),),
    "USER_ADDRESS": (("copy_trading", "wallets"),),
    "TRADING_MODE": (("execution", "trading_mode"),),
    "METRICS_PORT": (("app", "metrics_port"),),
    "METRICS_BIND_HOST": (("app", "metrics_bind_host"),),
}


def _parsed(key: str, value: Any) -> Any:
    if key in {"LEADER_WALLETS", "USER_ADDRESS"}:
        return [item.strip() for item in str(value).split(",") if item.strip()]
    if not isinstance(value, str):
        return value
    if value.startswith("0x") or key in {
        "PRIVATE_KEY",
        "FUNDER_ADDRESS",
        "PROXY_WALLET",
        "STAGED_TP_LEVELS",
        "STAGED_TP_FRACTIONS",
        "TIERED_TRADE_MULTIPLIERS",
        "TOKEN_DELAY_OVERRIDES",
        "CONDITION_DELAY_OVERRIDES",
    }:
        return value
    try:
        return yaml.safe_load(value)
    except yaml.YAMLError:
        return value


def _assign(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    cursor = target
    for part in path[:-1]:
        cursor = cursor.setdefault(part, {})
    cursor[path[-1]] = value


def nested_test_overrides(flat: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for raw_key, raw_value in flat.items():
        key = str(raw_key).upper()
        paths = _ALIASES.get(key)
        if paths is None:
            candidates = list(dict.fromkeys(_PATHS.get(key, [])))
            paths = tuple(candidates) if len(candidates) == 1 else ()
        for path in paths:
            _assign(output, path, _parsed(key, raw_value))
    return output


def load_test_config(flat: Mapping[str, Any] | None = None, **nested: Any):
    overrides = nested_test_overrides(flat or {})
    overrides.setdefault("database", {})["dsn"] = overrides.get("database", {}).get(
        "dsn", "postgresql://test:test@localhost/test"
    )
    # Required in paper mode (where it is the wallet identity). Defaulted here
    # — like database.dsn above — so fixtures exercising unrelated behaviour
    # need not restate it. Fixtures that pass one keep theirs.
    overrides.setdefault("wallet", {}).setdefault("funder_address", "0x" + "1" * 40)
    for key, value in nested.items():
        if key in overrides and isinstance(overrides[key], dict) and isinstance(value, Mapping):
            overrides[key] = {**overrides[key], **value}
        else:
            overrides[key] = value
    det = overrides.setdefault("detection", {})
    if isinstance(det, dict):
        onchain = det.setdefault("onchain", {})
        if (
            isinstance(onchain, dict)
            and "alchemy_wss_url" not in onchain
            and "onchain_ws_url" not in onchain
        ):
            onchain["alchemy_wss_url"] = (
                "wss://polygon-mainnet.g.alchemy.com/v2/test-fixture-key-12345"
            )
    placeholder_rpcs = {
        "https://polygon-rpc.example.com",
        "https://rpc.example.com",
        "https://rpc.invalid",
    }
    default_alchemy_http = "https://polygon-mainnet.g.alchemy.com/v2/test-fixture-key-12345"
    polymarket_section = overrides.get("polymarket")
    if (
        isinstance(polymarket_section, dict)
        and polymarket_section.get("rpc_url") in placeholder_rpcs
    ):
        polymarket_section["rpc_url"] = default_alchemy_http
    wallet_section = overrides.get("wallet")
    if (
        isinstance(wallet_section, dict)
        and wallet_section.get("polygon_rpc_url") in placeholder_rpcs
    ):
        wallet_section["polygon_rpc_url"] = default_alchemy_http
    return load_config(overrides)


__all__ = ["load_test_config", "nested_test_overrides"]
