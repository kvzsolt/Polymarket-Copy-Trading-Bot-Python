"""Canonical position identity for the phased coordinator rewrite.

``PositionKey`` is the **immutable, hashable** identity of a single position
in the copy-trading bot.  It is keyed on the full tuple
``(user, condition_id, token_id[, outcome])`` so that YES and NO positions
under the same condition are naturally distinct.

This module is intentionally lightweight and import-safe — it depends only
on the standard library.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PositionKey:
    """Immutable identity of a single scoped position.

    Parameters
    ----------
    user:
        The wallet/user address that owns the position.
    condition_id:
        The on-chain condition identifier (market).
    token_id:
        The CLOB token identifier (outcome token).
    outcome:
        Optional human-readable outcome label (e.g. ``"Yes"``, ``"No"``).
        Two keys that differ only in ``outcome`` are considered **distinct**;
        this lets YES/NO positions under the same condition co-exist without
        lock collisions.
    """

    user: str
    condition_id: str
    token_id: str
    outcome: str | None = None
