"""Read-only bridge: MetricFamilyRecord → CompositeScoreRecord.

This module translates per-family metric outputs into a composite score
with decomposed, individually-visible family contributions.

It does **not**:
- Fabricate values when a family is unavailable.
- Infer bankroll or position sizing.
- Apply promotion / permission policy.

Each family normaliser maps the family's available metrics into a [0, 1]
normalised contribution.  The composite is a weighted average of the
contributions that are actually available; unavailable families are
excluded (weight is re-distributed).
"""
from __future__ import annotations


from domain.composite_score_models import (
    CompositeAvailability,
    CompositeScoreRecord,
    DEFAULT_WEIGHTS,
    FamilyContribution,
    FamilyWeights,
)
from domain.metric_models import (
    DelaySurvivalMetrics,
    EntryAlphaMetrics,
    MetricAvailability,
    MetricFamilyRecord,
    SlippageCopyabilityMetrics,
)
from domain.scoring_models import PositionKey


# ---------------------------------------------------------------------------
# Per-family normalisers (MetricFamily → 0-1 float | None)
# ---------------------------------------------------------------------------

def _normalise_entry_alpha(ea: EntryAlphaMetrics) -> float | None:
    """Normalise entry-alpha family into [0, 1].

    Heuristic: proportion of time-windows that are available, weighted by
    recency (5m window is the most valuable signal).
    Window weights: 5m=0.50, 30m=0.30, 4h=0.20.
    """
    if not ea.any_window_available:
        return None

    window_weights = {
        ea.window_5m: 0.50,
        ea.window_30m: 0.30,
        ea.window_4h: 0.20,
    }
    total = 0.0
    for window, w in window_weights.items():
        if window.availability == MetricAvailability.AVAILABLE:
            total += w
    # Scale so that having all windows = 1.0
    return min(total / 1.0, 1.0)


def _normalise_delay_survival(ds: DelaySurvivalMetrics) -> float | None:
    """Normalise delay-survival family into [0, 1].

    Heuristic:
    - Base 0.5 for having any data.
    - +0.25 if position is still open (signal survived copy delay).
    - +0.25 if reconstruction confidence is sufficient and no mismatch.
    """
    if ds.availability != MetricAvailability.AVAILABLE:
        return None

    score = 0.5
    if ds.position_still_open is True:
        score += 0.25
    if ds.confidence_sufficient and not ds.has_mismatch:
        score += 0.25
    return min(score, 1.0)


def _normalise_slippage_copyability(sc: SlippageCopyabilityMetrics) -> float | None:
    """Normalise slippage-copyability family into [0, 1].

    Heuristic:
    - Penalise wide spreads: score = max(0, 1 - spread_bps/500).
    - Penalise stale snapshots: multiply by 0.5 if stale.
    - Require book depth for full credit.
    """
    if sc.availability != MetricAvailability.AVAILABLE:
        return None

    spread_bps = sc.spread_bps if sc.spread_bps is not None else 0
    base = max(0.0, 1.0 - spread_bps / 500.0)

    if sc.snapshot_stale:
        base *= 0.5
    if not sc.book_depth_available:
        base *= 0.75

    return min(max(base, 0.0), 1.0)


# ---------------------------------------------------------------------------
# Bridge service
# ---------------------------------------------------------------------------

class CompositeScoringBridge:
    """Stateless bridge: ``MetricFamilyRecord`` → ``CompositeScoreRecord``.

    Instantiate with optional custom ``FamilyWeights``; defaults are sane.
    """

    def __init__(self, weights: FamilyWeights | None = None) -> None:
        self._weights = weights or DEFAULT_WEIGHTS

    # -- public API ----------------------------------------------------------

    def score(self, mfr: MetricFamilyRecord) -> CompositeScoreRecord:
        """Produce a composite-score-ready record from metric families."""
        ea_contrib = self._contribution_entry_alpha(mfr.entry_alpha)
        ds_contrib = self._contribution_delay_survival(mfr.delay_survival)
        sc_contrib = self._contribution_slippage_copyability(
            mfr.slippage_copyability,
        )

        contributions = [ea_contrib, ds_contrib, sc_contrib]
        available = [c for c in contributions if c.available]
        n_available = len(available)

        composite, availability = self._compute_composite(available, n_available)

        return CompositeScoreRecord(
            trader_wallet=mfr.trader_wallet,
            condition_id=mfr.condition_id,
            token_id=mfr.token_id,
            composite_score=composite,
            availability=availability,
            entry_alpha_contribution=ea_contrib,
            delay_survival_contribution=ds_contrib,
            slippage_copyability_contribution=sc_contrib,
            families_available=n_available,
            families_total=3,
            weights_used=self._weights,
        )

    def score_batch(
        self,
        records: dict[PositionKey, MetricFamilyRecord],
    ) -> dict[PositionKey, CompositeScoreRecord]:
        """Score a batch of metric-family records."""
        return {k: self.score(mfr) for k, mfr in records.items()}

    # -- per-family contribution builders ------------------------------------

    def _contribution_entry_alpha(
        self, ea: EntryAlphaMetrics,
    ) -> FamilyContribution:
        norm = _normalise_entry_alpha(ea)
        if norm is None:
            return FamilyContribution(
                family_name="entry_alpha",
                available=False,
                unavailable_reason="no available entry-alpha windows",
            )
        w = self._weights.entry_alpha
        return FamilyContribution(
            family_name="entry_alpha",
            available=True,
            normalised_value=norm,
            weight=w,
            weighted_value=norm * w,
        )

    def _contribution_delay_survival(
        self, ds: DelaySurvivalMetrics,
    ) -> FamilyContribution:
        norm = _normalise_delay_survival(ds)
        if norm is None:
            reason = ds.unavailable_reason or "delay-survival metrics unavailable"
            return FamilyContribution(
                family_name="delay_survival",
                available=False,
                unavailable_reason=reason,
            )
        w = self._weights.delay_survival
        return FamilyContribution(
            family_name="delay_survival",
            available=True,
            normalised_value=norm,
            weight=w,
            weighted_value=norm * w,
        )

    def _contribution_slippage_copyability(
        self, sc: SlippageCopyabilityMetrics,
    ) -> FamilyContribution:
        norm = _normalise_slippage_copyability(sc)
        if norm is None:
            reason = sc.unavailable_reason or "slippage-copyability unavailable"
            return FamilyContribution(
                family_name="slippage_copyability",
                available=False,
                unavailable_reason=reason,
            )
        w = self._weights.slippage_copyability
        return FamilyContribution(
            family_name="slippage_copyability",
            available=True,
            normalised_value=norm,
            weight=w,
            weighted_value=norm * w,
        )

    # -- composite computation -----------------------------------------------

    @staticmethod
    def _compute_composite(
        available: list[FamilyContribution],
        n_available: int,
    ) -> tuple[float | None, CompositeAvailability]:
        """Weighted average of available contributions, re-normalised."""
        if n_available == 0:
            return None, CompositeAvailability.UNAVAILABLE

        total_weight = sum(c.weight for c in available)
        if total_weight <= 0:
            return None, CompositeAvailability.UNAVAILABLE

        composite = sum(
            (c.weighted_value or 0.0) for c in available
        ) / total_weight

        avail = (
            CompositeAvailability.AVAILABLE
            if n_available == 3
            else CompositeAvailability.PARTIAL
        )
        return composite, avail
