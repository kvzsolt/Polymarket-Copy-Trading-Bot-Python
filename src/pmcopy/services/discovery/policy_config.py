"""Tolerant config loader for Falcon discovery policy.

The repository has used both mapping-backed and Pydantic/namespace-backed
configuration objects.  This module keeps the integration independent of the
concrete config library while still validating every money-path threshold.

Tuple settings are coerced against the element type of the dataclass default,
so integer tuples such as the Wallet360 windows and string tuples such as the
toxic market flags both accept a YAML list or a comma-separated string.
"""

from __future__ import annotations

from dataclasses import fields
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from pmcopy.services.discovery.bankroll_compatibility import BankrollCompatibilityPolicy
from pmcopy.services.discovery.falcon_candidate_service import FalconDiscoveryPolicy

_MISSING = object()
_ALLOWED_WALLET360_WINDOWS = {1, 3, 7, 15}


def _get(source: Any, name: str, default: Any = _MISSING) -> Any:
    if source is None:
        return default
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _coerce(value: Any, default: Any, *, name: str) -> Any:
    if isinstance(default, Decimal):
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be a decimal") from exc
        if not result.is_finite():
            raise ValueError(f"{name} must be finite")
        return result
    if isinstance(default, bool):
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"{name} must be a boolean")
    if isinstance(default, int):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be an integer") from exc
    if isinstance(default, tuple):
        if isinstance(value, str):
            parts = [part.strip() for part in value.split(",") if part.strip()]
        else:
            try:
                parts = list(value)
            except TypeError as exc:
                raise ValueError(f"{name} must be a sequence") from exc
        if default and isinstance(default[0], str):
            return tuple(str(part).strip() for part in parts if str(part).strip())
        try:
            return tuple(int(part) for part in parts)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must contain integers") from exc
    return value


def _load_dataclass_settings(source: Any, default_instance: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in fields(default_instance):
        name = item.name
        if name == "bankroll":
            continue
        raw = _get(source, name)
        if raw is _MISSING or raw is None:
            continue
        result[name] = _coerce(raw, getattr(default_instance, name), name=name)
    return result


def discovery_policy_from_config(config: Any) -> FalconDiscoveryPolicy:
    """Build a validated Falcon policy from mapping or object config."""
    discovery = _get(config, "discovery", config)
    falcon = _get(discovery, "falcon", discovery)
    policy_node = _get(falcon, "policy", falcon)

    default_bankroll = BankrollCompatibilityPolicy()
    bankroll_node = _get(policy_node, "bankroll", _get(falcon, "bankroll", None))
    bankroll_kwargs = _load_dataclass_settings(bankroll_node, default_bankroll)
    bankroll = BankrollCompatibilityPolicy(**bankroll_kwargs)

    default_policy = FalconDiscoveryPolicy()
    policy_kwargs = _load_dataclass_settings(policy_node, default_policy)
    windows = policy_kwargs.get("wallet360_windows", default_policy.wallet360_windows)
    if not windows or any(window not in _ALLOWED_WALLET360_WINDOWS for window in windows):
        raise ValueError("Wallet360 windows must be selected from 1, 3, 7 and 15")
    policy_kwargs["wallet360_windows"] = tuple(dict.fromkeys(windows))
    policy_kwargs["bankroll"] = bankroll
    return FalconDiscoveryPolicy(**policy_kwargs)


__all__ = ["discovery_policy_from_config"]
