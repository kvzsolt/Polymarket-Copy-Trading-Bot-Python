"""Cached research evidence remains optional, scoped, and unable to submit orders."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.cached_assessment import (
    AssessmentConclusion,
    AssessmentScope,
    CachedCopyabilityAssessment,
)

NOW = datetime(2026, 9, 8, 12, tzinfo=UTC)


def test_cached_assessment_requires_exact_sizing_delay_scope_and_fresh_evidence():
    """An assessment for another stake or delay cannot certify this copy policy."""
    scope = AssessmentScope("0x" + "1" * 40, "sports", "fixed", Decimal("5"), Decimal("2"))
    assessment = CachedCopyabilityAssessment(
        scope,
        NOW - timedelta(hours=1),
        NOW + timedelta(hours=1),
        "policy-v1",
        "method-v1",
        AssessmentConclusion.INSUFFICIENT_EVIDENCE,
    )
    assert assessment.applies_to(scope, now=NOW)
    assert assessment.score is None
    assert assessment.conclusion is AssessmentConclusion.INSUFFICIENT_EVIDENCE
    assert not assessment.applies_to(scope, now=NOW + timedelta(hours=1))
    assert not assessment.applies_to(scope, now=NOW - timedelta(hours=2))
    assert not assessment.applies_to(replace(scope, collateral_amount=Decimal("50")), now=NOW)
    assert not assessment.applies_to(replace(scope, assumed_delay_seconds=Decimal("20")), now=NOW)


@pytest.mark.parametrize("invalid", [Decimal("NaN"), Decimal("Infinity"), True, "5"])
def test_cached_assessment_scope_rejects_inexact_or_nonfinite_money(invalid):
    """The cache boundary must parse financial strings before constructing domain evidence."""
    with pytest.raises(ValueError):
        AssessmentScope("0x" + "1" * 40, "sports", "fixed", invalid, Decimal("2"))
