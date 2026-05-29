"""Compliance service — thin facade re-exporting from the flat compliance module."""
from __future__ import annotations

# Load the flat compliance.py directly to avoid circular import
import os
import importlib.util

_flat_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "compliance.py")
_spec = importlib.util.spec_from_file_location("services._compliance_flat", _flat_path)
_flat_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_flat_mod)

check_geoblock = _flat_mod.check_geoblock
from domain.models import ComplianceDecision

__all__ = ["check_geoblock", "ComplianceDecision"]
