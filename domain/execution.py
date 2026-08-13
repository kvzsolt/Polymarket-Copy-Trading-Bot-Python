"""Immutable execution policy values independent of venue adapters."""

from __future__ import annotations

from enum import StrEnum


class ExecutionOrderType(StrEnum):
    """Supported execution policies for copy orders."""

    FAK = "FAK"


__all__ = ["ExecutionOrderType"]
