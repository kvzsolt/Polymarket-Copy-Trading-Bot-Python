"""Falcon-backed trader discovery and ranking.

The live discovery service is ``FalconDiscoveryService`` (bankroll-aware,
Falcon analytics provider). The persisted candidate row and the discovery
config hash live in ``services.discovery.models``.
"""

from services.discovery.falcon_discovery_service import FalconDiscoveryService
from services.discovery.models import Candidate, DiscoveryRepository, discovery_config_hash

__all__ = [
    "Candidate",
    "DiscoveryRepository",
    "FalconDiscoveryService",
    "discovery_config_hash",
]
