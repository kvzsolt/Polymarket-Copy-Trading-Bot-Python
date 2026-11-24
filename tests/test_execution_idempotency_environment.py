"""Multi-copier addon §5: execution idempotency key distinguishes the environment.

An execution idempotency key must distinguish venue, execution
mode/environment, canonical copier account, and the canonical economic source
intent. The copier account (``user``) and the economic source intent
(``intent_id``) are already components of ``compute_effective_key``; the venue
is structurally fixed to the single CCXT Polymarket adapter (repository
mandate: no venue selector exists, so two venues can never share a database
row space). The execution mode axis (paper vs live) was missing: a paper soak
and a live run for the same copier and the same source intent previously
derived the SAME key, letting a paper ``orders`` row (carrying a simulated
order id) answer a live submission's duplicate check — fabricating live
submission evidence from simulated state. These tests pin the environment
axis of the derivation.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pmcopy.services.execution.execution_service import _effective_idempotency_key
from pmcopy.services.execution.idempotency import compute_effective_identity, compute_effective_key

_BASE_KWARGS = dict(
    user="copier-primary",
    condition_id="0xcond",
    token_id="0xtoken",
    side="BUY",
    effective_size=Decimal("5"),
    effective_price=Decimal("0.50"),
    tick_size=Decimal("0.01"),
    intent_id="leader-tx|log:3",
)


def test_environment_axis_separates_paper_and_live_keys() -> None:
    """Same copier, same source intent, different mode -> different keys."""
    paper_key = compute_effective_key(environment="paper", **_BASE_KWARGS)
    live_key = compute_effective_key(environment="live", **_BASE_KWARGS)
    assert paper_key != live_key


def test_environment_axis_does_not_rekey_the_legacy_derivation() -> None:
    """Omitted/empty environment reproduces the historical key byte-for-byte.

    Rows written by processes predating this axis must keep colliding with
    recomputations from an environment-unaware caller, so the default stays
    the legacy payload. This is what keeps the upgrade window safe: nothing
    that used to dedup stops deduping.
    """
    legacy_key = compute_effective_key(**_BASE_KWARGS)
    assert legacy_key == compute_effective_key(environment="", **_BASE_KWARGS)
    assert legacy_key not in {
        compute_effective_key(environment="paper", **_BASE_KWARGS),
        compute_effective_key(environment="live", **_BASE_KWARGS),
    }


def test_identity_wrapper_carries_the_environment_axis() -> None:
    """``compute_effective_identity`` forwards the axis for callers that persist fields."""
    identity = compute_effective_identity(environment="live", **_BASE_KWARGS)
    assert identity.idempotency_key == compute_effective_key(
        environment="live", **_BASE_KWARGS
    )
    assert identity.idempotency_key != compute_effective_key(
        environment="paper", **_BASE_KWARGS
    )


def test_execution_service_helper_forwards_environment() -> None:
    """The ExecutionService derivation helper threads the axis through."""
    live_key = _effective_idempotency_key(environment="live", **_BASE_KWARGS)
    paper_key = _effective_idempotency_key(environment="paper", **_BASE_KWARGS)
    assert live_key == compute_effective_key(environment="live", **_BASE_KWARGS)
    assert paper_key == compute_effective_key(environment="paper", **_BASE_KWARGS)
    assert live_key != paper_key
