"""Typed composite-scoring output models.

These sit between the per-family ``MetricFamilyRecord`` (Phase 4) and
future promotion / permission policy (not yet built).

Design rules
------------
- Each metric family produces an independent ``FamilyContribution`` that
  carries its own availability flag and a normalised 0-1 contribution.
- When a family is **unavailable**, its contribution is ``None`` and
  ``available`` is ``False``.  The composite score is computed only from
  the families that *are* present — it is **never** fabricated.
- If **no** family is available the composite is ``None`` — not zero.
- ``CompositeScoreRecord`` exposes every family contribution for
  downstream auditability.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from domain.scoring_models import PositionKey


# ---------------------------------------------------------------------------
# Availability of the composite itself
# ---------------------------------------------------------------------------

class CompositeAvailability(Enum):
    """Whether a composite score could be computed."""
    AVAILABLE = "available"
    PARTIAL = "partial"  # some families unavailable; score is degraded
    UNAVAILABLE = "unavailable"  # zero families available


# ---------------------------------------------------------------------------
# Per-family contribution
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FamilyContribution:
    """One metric family's contribution to the composite score.

    ``normalised_value`` is in [0.0, 1.0] when ``available`` is True.
    When unavailable, ``normalised_value`` is None and
    ``unavailable_reason`` explains why.
    """
    family_name: str
    available: bool
    normalised_value: float | None = None
    weight: float = 0.0
    weighted_value: float | None = None
    unavailable_reason: str | None = None


# ---------------------------------------------------------------------------
# Composite score record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FamilyWeights:
    """Configurable weight set for the three metric families.

    Weights are relative — they will be re-normalised at scoring time so
    that only the *available* families share 1.0.
    """
    entry_alpha: float = 0.40
    delay_survival: float = 0.35
    slippage_copyability: float = 0.25


DEFAULT_WEIGHTS = FamilyWeights()


@dataclass(frozen=True)
class CompositeScoreRecord:
    """Composite-score-ready output for one position.

    ``composite_score`` is a weighted average of available family
    contributions in [0.0, 1.0], or ``None`` when nothing is available.
    """
    trader_wallet: str
    condition_id: str
    token_id: str

    composite_score: float | None = None
    availability: CompositeAvailability = CompositeAvailability.UNAVAILABLE

    entry_alpha_contribution: FamilyContribution = field(
        default_factory=lambda: FamilyContribution(
            family_name="entry_alpha", available=False,
            unavailable_reason="not computed",
        )
    )
    delay_survival_contribution: FamilyContribution = field(
        default_factory=lambda: FamilyContribution(
            family_name="delay_survival", available=False,
            unavailable_reason="not computed",
        )
    )
    slippage_copyability_contribution: FamilyContribution = field(
        default_factory=lambda: FamilyContribution(
            family_name="slippage_copyability", available=False,
            unavailable_reason="not computed",
        )
    )

    families_available: int = 0
    families_total: int = 3

    weights_used: FamilyWeights = field(default_factory=lambda: DEFAULT_WEIGHTS)

    @property
    def key(self) -> PositionKey:
        return (self.trader_wallet, self.condition_id, self.token_id)

    @property
    def contributions(self) -> tuple[FamilyContribution, ...]:
        return (
            self.entry_alpha_contribution,
            self.delay_survival_contribution,
            self.slippage_copyability_contribution,
        )
