"""Discovery repository port contract — runnable without Docker.

``tests/test_storage.py`` already asserts this registration, but that module
spins a live Postgres container, and ``tests/conftest.py`` marks *every* test
in any file whose source mentions the Docker container helpers as
``integration``. Its ABC-registration class therefore never runs in the
portable unit tier, which is how ``DiscoveryRepo`` came to be unregistered
from its port unnoticed.

Nothing here touches a pool. Keep this module free of any reference to those
container helpers — including in comments, since the marker is applied by a
raw source-text match — as that is what keeps these checks in the default tier.
"""

from __future__ import annotations

import inspect

from pmcopy.domain.ports import AbstractDiscoveryRepository
from pmcopy.services.discovery.models import DiscoveryRepository
from pmcopy.storage.postgres.discovery_repo import DiscoveryRepo

# Methods the pre-rewrite port declared, none of which the single-generation
# pipeline ever implemented. Re-adding one would make the port a fiction again.
_OBSOLETE_PORT_METHODS = frozenset(
    {
        "upsert_trader",
        "get_trader",
        "list_traders",
        "upsert_market",
        "get_market",
        "insert_trade",
        "list_trades",
        "insert_position_snapshot",
        "latest_position_snapshot",
        "insert_orderbook_snapshot",
    }
)


def _public_methods(obj: type) -> set[str]:
    return {
        name for name, _ in inspect.getmembers(obj, inspect.isfunction) if not name.startswith("_")
    }


def test_discovery_repo_is_registered_against_its_port() -> None:
    assert issubclass(DiscoveryRepo, AbstractDiscoveryRepository)


def test_discovery_repo_implements_every_abstract_method() -> None:
    """A missing method would leave the class abstract and uninstantiable."""
    assert DiscoveryRepo.__abstractmethods__ == frozenset()
    assert isinstance(DiscoveryRepo(None), AbstractDiscoveryRepository)  # type: ignore[arg-type]


def test_port_declares_no_obsolete_methods() -> None:
    resurrected = _public_methods(AbstractDiscoveryRepository) & _OBSOLETE_PORT_METHODS
    assert resurrected == set()


# NOTE: there is deliberately no "port <= _public_methods(DiscoveryRepo)" test.
# ``DiscoveryRepo`` *inherits* the port, so ``inspect.getmembers`` reports every
# port method on it whether or not the adapter implements one — the subset holds
# even for a subclass whose body is ``pass``. That assert cannot fail, and
# ``test_discovery_repo_implements_every_abstract_method`` above is the real
# runtime check: an unimplemented method stays in ``__abstractmethods__`` and
# makes the class uninstantiable. The non-emptiness guard that test carried is
# kept below, where it stops the signature sweep from passing vacuously.


def test_port_signatures_match_the_concrete_repo() -> None:
    """Parameter names/kinds/defaults must agree, so callers are portable.

    Return/param *annotations* may differ: the port stays free of adapter
    imports and types candidate payloads as ``Any``.
    """

    def shape(method) -> list[tuple[str, object, object]]:
        return [
            (parameter.name, parameter.kind, parameter.default)
            for parameter in inspect.signature(method).parameters.values()
        ]

    port = _public_methods(AbstractDiscoveryRepository)
    # Without this, emptying the port would make the sweep below iterate over
    # nothing and pass.
    assert port, "port must declare the consumed surface"
    for name in port:
        assert shape(getattr(DiscoveryRepo, name)) == shape(
            getattr(AbstractDiscoveryRepository, name)
        ), f"signature drift on {name}"


def test_concrete_repo_satisfies_the_funnel_protocol() -> None:
    """The funnel's structural port and the ABC describe one adapter.

    ``DiscoveryRepository`` is not ``@runtime_checkable``, and an isinstance
    check against one only compares method *names* anyway. Compare the surface
    directly so a signature drift between the two ports is caught here.
    """
    protocol = _public_methods(DiscoveryRepository)
    assert protocol, "funnel protocol must declare the surface it consumes"
    assert protocol <= _public_methods(DiscoveryRepo)
    # The funnel's subset must also be declared by the ABC — two ports over
    # one adapter must not disagree about what that adapter offers.
    assert protocol <= _public_methods(AbstractDiscoveryRepository)


def test_latest_candidates_accepts_a_config_hash_scope() -> None:
    """Rescore relies on this kwarg to exclude stale-config snapshots."""
    for owner in (AbstractDiscoveryRepository, DiscoveryRepo, DiscoveryRepository):
        parameter = inspect.signature(owner.latest_candidates).parameters["config_hash"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None
