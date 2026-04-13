"""Domain ports — abstract interfaces that the application core depends on.

Concrete implementations live in ``storage/`` (persistence adapters) and
``clients/`` (HTTP adapters). Application services declare their dependencies
against these ABCs only, never against concrete classes.
"""

from domain.ports.clients import (
    AbstractAnalyticsClient,
    AbstractBalanceClient,
    AbstractHttpClient,
    AbstractMarketClient,
    AbstractOrderGateway,
)
from domain.ports.notifiers import AbstractNotifier
from domain.ports.repositories import (
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractLeaderRepository,
    AbstractOrderRepository,
    AbstractPositionRepository,
    AbstractRedemptionRepository,
)

__all__ = [
    "AbstractAnalyticsClient",
    "AbstractBalanceClient",
    "AbstractCursorRepository",
    "AbstractDiscoveryRepository",
    "AbstractFillRepository",
    "AbstractHttpClient",
    "AbstractLeaderRepository",
    "AbstractMarketClient",
    "AbstractNotifier",
    "AbstractOrderGateway",
    "AbstractOrderRepository",
    "AbstractPositionRepository",
    "AbstractRedemptionRepository",
]
