"""Canonical position identity (issue #67).

Position identity throughout the runtime is the triple
``(user_id, condition_id, token_id)``. The optional ``outcome`` label is
carried for display / logging only and is **not** part of identity — two
``PositionKey`` instances that share the triple compare equal regardless of
outcome.

This module is the single source of truth. ``services.positionActionCoordinator``
re-exports :class:`PositionKey` as ``PositionScope`` for back-compat with
existing call sites.

The factory constructors normalize id casing (hex ids lowercased, whitespace
stripped) so callers that read from heterogeneous sources (REST snapshots,
WSS frames, SQLite rows) cannot accidentally fork a single position into
two distinct lock keys.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


def _normalize_id(value: Any) -> str:
    """Lowercase hex-prefixed ids, strip whitespace; leave non-hex strings as-is."""
    s = str(value or "").strip()
    return s.lower() if s[:2].lower() == "0x" else s


@dataclass(frozen=True, slots=True)
class PositionKey:
    """Canonical position identity ``(user_id, condition_id, token_id)``.

    ``outcome`` is optional metadata for human-readable logging; it is not
    part of the identity tuple and does not affect equality or hashing.
    """

    user_id: str
    condition_id: str
    token_id: str
    outcome: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "user_id", _normalize_id(self.user_id))
        object.__setattr__(self, "condition_id", _normalize_id(self.condition_id))
        object.__setattr__(self, "token_id", _normalize_id(self.token_id))
        if self.outcome is not None:
            normalized_outcome = str(self.outcome).strip()
            object.__setattr__(self, "outcome", normalized_outcome or None)
        if not self.user_id or not self.condition_id or not self.token_id:
            raise ValueError(
                "PositionKey requires non-empty user_id, condition_id, token_id"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PositionKey):
            return NotImplemented
        return self.identity() == other.identity()

    def __hash__(self) -> int:
        return hash(self.identity())

    def identity(self) -> tuple[str, str, str]:
        """Identity tuple — outcome is excluded by design."""
        return (self.user_id, self.condition_id, self.token_id)

    def log_fields(self) -> dict[str, str]:
        """Structured log fields for emission via ``logger.*(..., extra=...)``."""
        out: dict[str, str] = {
            "position_key.user_id": self.user_id,
            "position_key.condition_id": self.condition_id,
            "position_key.token_id": self.token_id,
        }
        if self.outcome:
            out["position_key.outcome"] = self.outcome
        return out

    # --- Factory constructors (issue #67 checklist item 2) -------------------

    @classmethod
    def from_signal_event(cls, event: Any, user_id: str) -> "PositionKey":
        """Build from a copy-trade signal event (dict or attr-bag)."""
        if isinstance(event, dict):
            def _get(key: str, default: Any = None) -> Any:
                return event.get(key, default)
        else:
            def _get(key: str, default: Any = None) -> Any:
                return getattr(event, key, default)

        return cls(
            user_id=user_id,
            condition_id=_get("condition_id") or _get("conditionId") or "",
            token_id=(
                _get("asset")
                or _get("token_id")
                or _get("tokenId")
                or _get("asset_id")
                or ""
            ),
            outcome=_get("outcome"),
        )

    @classmethod
    def from_wss_event(cls, event: dict, user_id: str) -> "PositionKey":
        """Build from a Polymarket WSS frame.

        WSS market frames use ``market`` (== condition id) and ``asset_id``
        (== token id); user-channel frames use the same vocabulary.
        """
        return cls(
            user_id=user_id,
            condition_id=(
                event.get("market")
                or event.get("condition_id")
                or ""
            ),
            token_id=(
                event.get("asset_id")
                or event.get("asset")
                or event.get("token_id")
                or ""
            ),
            outcome=event.get("outcome"),
        )

    @classmethod
    def from_stored_position(cls, row: dict) -> "PositionKey":
        """Build from a sqlite/positions row (column shape: user_id, condition_id, asset, outcome)."""
        return cls(
            user_id=row.get("user_id") or row.get("user") or "",
            condition_id=row.get("condition_id") or "",
            token_id=row.get("asset") or row.get("token_id") or "",
            outcome=row.get("outcome"),
        )

    @classmethod
    def from_execution_request(cls, req: Any) -> "PositionKey":
        """Build from an execution-layer request (dict or attr-bag)."""
        if isinstance(req, dict):
            def _get(key: str, default: Any = None) -> Any:
                return req.get(key, default)
        else:
            def _get(key: str, default: Any = None) -> Any:
                return getattr(req, key, default)

        return cls(
            user_id=_get("user_id") or _get("user") or "",
            condition_id=_get("condition_id") or "",
            token_id=_get("token_id") or _get("asset") or "",
            outcome=_get("outcome"),
        )

    @classmethod
    def from_reconciliation_record(cls, record: dict) -> "PositionKey":
        """Build from a pending-orders reconciliation record."""
        return cls(
            user_id=record.get("user_id") or "",
            condition_id=record.get("condition_id") or "",
            token_id=record.get("asset") or record.get("token_id") or "",
            outcome=record.get("outcome"),
        )


__all__ = ["PositionKey"]
