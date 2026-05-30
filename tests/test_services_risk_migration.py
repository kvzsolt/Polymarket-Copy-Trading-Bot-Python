"""Import checks for the canonical services.risk package migration."""

from __future__ import annotations

import importlib
import pytest


def test_canonical_risk_engine_path_exports_engine() -> None:
    canonical = importlib.import_module("services.risk.risk_engine")

    assert canonical.RiskEngine.__name__ == "RiskEngine"
    assert canonical.headroom_for_buy.__name__ == "headroom_for_buy"


def test_canonical_risk_monitor_path_exports_monitor() -> None:
    canonical = importlib.import_module("services.risk.risk_monitor")

    assert canonical.RiskPriceCache.__name__ == "RiskPriceCache"
    assert canonical.risk_monitor_loop_for_contexts.__name__ == "risk_monitor_loop_for_contexts"


def test_legacy_camelcase_risk_modules_are_removed() -> None:
    for module_name in ("services.riskEngine", "services.riskMonitor"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module_name)
