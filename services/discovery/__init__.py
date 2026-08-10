"""Heisenberg-backed trader discovery and ranking.

The live discovery service is ``HeisenbergDiscoveryService`` (bankroll-aware,
Heisenberg analytics provider). The persisted candidate row and the discovery
config hash live in ``services.discovery.models``.
"""

from services.discovery.heisenberg_discovery_service import HeisenbergDiscoveryService
from services.discovery.models import Candidate, DiscoveryRepository, discovery_config_hash

__all__ = [
    "Candidate",
    "DiscoveryRepository",
    "HeisenbergDiscoveryService",
    "discovery_config_hash",
]
