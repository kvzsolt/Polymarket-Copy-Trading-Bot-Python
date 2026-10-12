"""Fail-closed version gate for the installed CCXT prediction namespace.

Version policy:

- Only stable ``MAJOR.MINOR.PATCH`` releases inside the supported
  ``>=4.5.73,<5`` window pass the gate.
- A pre-release NEVER satisfies a stable minimum. A pre-release of the
  same numeric release (``4.5.73rc1``) sorts below that release, and a
  pre-release of an older release (``4.5.7rc3``) additionally fails the
  numeric floor. Components are parsed strictly leading-numeric — the
  digits of a suffix are never joined into the numeric value.
- Any deviation from a plain three-numeric-component release (``rc``,
  ``.dev``, ``.post``, local segments, junk after the first component)
  marks the version non-stable and fails closed. This is deliberately
  stricter than PEP 440 for post-releases (``4.5.73.post1`` would sort
  above ``4.5.73`` there): the gate is a safety floor and CCXT ships
  plain three-component releases.

Why not ``packaging.version``: ``packaging`` reaches the development
virtualenv only as a transitive dependency of ``pytest`` and is absent
from ``requirements.runtime.txt`` (the production install path), so
importing it here would break production on a dev-only dependency. The
minimal comparator below is exhaustively tested instead. Version checks
supplement — never replace — capability/behavioral tests.
"""

from __future__ import annotations

from typing import Any

from pmcopy.integrations.polymarket.ccxt_compat.exceptions import CcxtPredictionUnavailable

_MINIMUM_CCXT_VERSION = (4, 5, 73)
_MAXIMUM_EXCLUSIVE_MAJOR = 5
_VERSION_COMPONENTS = 3
_ASCII_DIGITS = frozenset("0123456789")


def _leading_digits(component: str) -> str:
    """Return the leading ASCII-digit prefix of ``component``."""

    count = 0
    for ch in component:
        if ch not in _ASCII_DIGITS:
            break
        count += 1
    return component[:count]


def _parse_version_state(value: Any) -> tuple[tuple[int, ...], bool] | None:
    """Return ``(release, is_prerelease)`` or ``None`` when unparseable.

    The release tuple holds up to three integers from the purely numeric
    leading dot-components (extra numeric components are truncated, e.g.
    ``4.5.71.1`` -> ``(4, 5, 71)``). A component with a numeric prefix and
    a suffix (``73rc1``) contributes its prefix and marks a pre-release;
    a non-numeric component after the first (``dev1``) also marks a
    pre-release. Only a first component without any leading digits makes
    the whole value unparseable.
    """

    text = str(value or "").strip()
    if not text:
        return None

    release: list[int] = []
    is_prerelease = False
    for index, component in enumerate(text.split(".")):
        leading = _leading_digits(component)
        if leading and leading == component:
            if len(release) < _VERSION_COMPONENTS:
                release.append(int(leading))
            continue
        if not leading:
            if index == 0:
                return None
            is_prerelease = True
            break
        if len(release) < _VERSION_COMPONENTS:
            release.append(int(leading))
        is_prerelease = True
        break

    if not release:
        return None
    return tuple(release), is_prerelease


def parse_ccxt_version(value: Any) -> tuple[int, ...]:
    """Return the strict leading-numeric release components.

    Digits are never joined across a suffix: ``"4.5.7rc3"`` parses to
    ``(4, 5, 7)``. This helper drops the pre-release flag, so gating
    decisions must go through :func:`ensure_supported_ccxt_version`.
    """

    state = _parse_version_state(value)
    return state[0] if state is not None else ()


def ensure_supported_ccxt_version(version: Any) -> None:
    """Reject CCXT installs outside the supported ``>=4.5.73,<5`` window.

    Pre-releases and other non-stable builds never satisfy the stable
    minimum (see the module docstring for the full policy).

    Raises:
        CcxtPredictionUnavailable: When the version is missing, is not a
            stable release, is older than the required minimum, or is
            at/above the next breaking major.
    """

    state = _parse_version_state(version)
    if state is None:
        raise CcxtPredictionUnavailable(
            f"CCXT version could not be determined from {version!r}; >= 4.5.73, < 5 is required"
        )
    release, is_prerelease = state
    if is_prerelease:
        raise CcxtPredictionUnavailable(
            f"CCXT {version} is not a stable release; a stable >= 4.5.73, < 5 release is required"
        )
    if release < _MINIMUM_CCXT_VERSION:
        raise CcxtPredictionUnavailable(f"CCXT {version} is too old; >= 4.5.73, < 5 is required")
    if release[0] >= _MAXIMUM_EXCLUSIVE_MAJOR:
        raise CcxtPredictionUnavailable(
            f"CCXT {version} is unsupported; >= 4.5.73, < 5 is required"
        )


__all__ = [
    "ensure_supported_ccxt_version",
    "parse_ccxt_version",
]
