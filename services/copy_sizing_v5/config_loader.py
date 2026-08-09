"""Load the V5 addendum without breaking the branch's current AppConfig schema."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml


def load_copy_sizing_v5_mapping(path: str | Path = "config/copy_sizing_v5.yaml") -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError("copy sizing V5 config must be a mapping")
    section = payload.get("sizing_v5", payload)
    if not isinstance(section, Mapping):
        raise ValueError("sizing_v5 must be a mapping")
    result = dict(section)
    result.pop("balances", None)
    return result


def load_balance_cache_mapping(path: str | Path = "config/copy_sizing_v5.yaml") -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    section = payload.get("sizing_v5", payload) if isinstance(payload, Mapping) else {}
    balances = section.get("balances", {}) if isinstance(section, Mapping) else {}
    if not isinstance(balances, Mapping):
        raise ValueError("sizing_v5.balances must be a mapping")
    return dict(balances)


__all__ = ["load_copy_sizing_v5_mapping", "load_balance_cache_mapping"]
