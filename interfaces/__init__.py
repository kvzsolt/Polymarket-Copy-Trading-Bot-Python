"""Interface dataclass definitions.

Note (issue #66): the legacy ``UserActivityInterface`` / ``UserPositionInterface``
dataclasses depend on ``bson.ObjectId`` from a previously-vendored MongoDB
driver. They predate the Polymarket rewrite and are scheduled for quarantine
in issue #77. We tolerate a missing ``bson`` install at import time so that
production gateway / Protocol modules under this package remain importable.
"""

from typing import Optional
from dataclasses import dataclass

try:
    from bson import ObjectId
except ModuleNotFoundError:  # pragma: no cover — legacy dependency
    ObjectId = str  # type: ignore[assignment,misc]


@dataclass
class UserActivityInterface:
    _id: "ObjectId"
    proxyWallet: str
    timestamp: int
    conditionId: str
    type: str
    size: float
    usdcSize: float
    transactionHash: str
    price: float
    asset: str
    side: str
    outcomeIndex: int
    title: str
    slug: str
    icon: str
    eventSlug: str
    outcome: str
    name: str
    pseudonym: str
    bio: str
    profileImage: str
    profileImageOptimized: str
    bot: bool
    botExcutedTime: int
    question: Optional[str] = None


@dataclass
class UserPositionInterface:
    _id: "ObjectId"
    proxyWallet: str
    asset: str
    conditionId: str
    size: float
    avgPrice: float
    initialValue: float
    currentValue: float
    cashPnl: float
    percentPnl: float
    totalBought: float
    realizedPnl: float
    percentRealizedPnl: float
    curPrice: float
    redeemable: bool
    mergeable: bool
    title: str
    slug: str
    icon: str
    eventSlug: str
    outcome: str
    outcomeIndex: int
    oppositeOutcome: str
    oppositeAsset: str
    endDate: str
    negativeRisk: bool


__all__ = [
    "UserActivityInterface",
    "UserPositionInterface",
]
