"""Env-patch model for the strategy profiler / env tuner (PR3).

A single proposed change to ``config/runtime.local.env``. Patches are
computed by the tuner and applied (only with ``--apply``) by the
:class:`EnvPatchWriter`. Protected keys (secrets, credentials, RPC, leaders)
are never produced by the tuner and never written by the writer.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EnvPatch:
    """One proposed env-key change.

    Attributes:
        key: Upper-case env key, e.g. ``BUY_MAX_AMOUNT``.
        old_value: Current value in the target file (``None`` if the key is
            new). Never populated from a protected key — the writer rejects
            protected keys before reading them.
        new_value: Proposed new value as a string.
        reason: Human-readable explanation of why the change is suggested.
        confidence: Profiler confidence in the suggestion, in ``[0.0, 1.0]``.
    """

    key: str
    old_value: str | None
    new_value: str
    reason: str
    confidence: float

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("EnvPatch.key must be non-empty")
        if not self.key.isupper() and not all(c.isalnum() or c == "_" for c in self.key):
            # Best-effort sanity: env keys are upper-case identifiers.
            raise ValueError(f"EnvPatch.key looks malformed: {self.key!r}")
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(
                f"EnvPatch.confidence must be in [0.0, 1.0]; got {self.confidence}"
            )