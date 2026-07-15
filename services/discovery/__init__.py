"""Single-generation trader discovery and longitudinal ranking."""

from services.discovery.funnel import DiscoveryFunnel
from services.discovery.models import Candidate
from services.discovery.scoring import CompositeScorer

__all__ = ["Candidate", "CompositeScorer", "DiscoveryFunnel"]
