"""Typed identity for one copier (follower) account.

A ``CopierAccountId`` is a thin value object around the persisted
``user_id`` string. It exists so account ownership at composition seams
(the account runtime, the cross-process lease) is carried by a type instead
of an anonymous ``str``, without any database or ledger change:

* ``value`` is the EXACT persisted ``user_id`` — byte-identical, never
  case-folded, never re-spaced. Every existing table (orders, fills,
  positions, reservations, risk blocks, pnl ledgers) keys on that string;
  the wrapper must not mint a second spelling of the same account.
* There is no display-name component. ``RuntimeUser`` has no display name —
  the configured ``user_id`` IS the stable identity — so renaming display or
  wallet surfaces (deposit-wallet re-binding changes the funder address, not
  a named user's id) can never create a new ledger identity.
* Construction validates fail-closed: empty or whitespace-padded values are
  rejected rather than silently normalized (normalizing would split ledgers
  between the padded and stripped spellings).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CopierAccountId:
    """The stable identity of one copier account (persisted ``user_id``)."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value.strip():
            raise ValueError("CopierAccountId requires a non-empty user_id string")
        if self.value != self.value.strip():
            raise ValueError(
                "CopierAccountId rejects whitespace-padded user_id values; "
                "normalizing them would split ledgers between spellings"
            )

    @classmethod
    def from_runtime_user(cls, runtime_user: Any) -> "CopierAccountId":
        """Derive the account identity from a runtime user.

        Duck-typed on purpose: production passes ``RuntimeUser`` while
        composition tests may pass a namespace double carrying ``user_id``.
        Only the stable id is read — never the wallet, never any display
        surface.
        """
        user_id = getattr(runtime_user, "user_id", None)
        if not isinstance(user_id, str):
            raise TypeError("runtime user is missing a string user_id")
        return cls(user_id)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value


__all__ = ["CopierAccountId"]
