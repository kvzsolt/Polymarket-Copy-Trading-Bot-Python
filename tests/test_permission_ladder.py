"""Tests for permission ladder tier evaluation."""
from __future__ import annotations


from services.discovery.permission_ladder import (
    FollowTier,
    PromotionThresholds,
    evaluate_tier,
)

W = "0xleader"
T = PromotionThresholds()


class TestWatchlistPromotion:
    def test_no_score_stays_watchlist(self):
        d = evaluate_tier(W, FollowTier.WATCHLIST, None, False, 0)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert not d.changed

    def test_insufficient_coverage_stays_watchlist(self):
        d = evaluate_tier(W, FollowTier.WATCHLIST, 0.80, False, 0)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert not d.changed

    def test_low_score_stays_watchlist(self):
        d = evaluate_tier(W, FollowTier.WATCHLIST, 0.30, True, 0)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert not d.changed

    def test_score_above_paper_threshold_promotes(self):
        d = evaluate_tier(W, FollowTier.WATCHLIST, 0.45, True, 0)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW
        assert d.changed
        assert d.promoted

    def test_exact_threshold_promotes(self):
        d = evaluate_tier(W, FollowTier.WATCHLIST, 0.40, True, 0)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW


class TestPaperFollowTransitions:
    def test_score_drop_demotes_to_watchlist(self):
        d = evaluate_tier(W, FollowTier.PAPER_FOLLOW, 0.20, True, 1000)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert d.demoted

    def test_adequate_score_stays_paper(self):
        d = evaluate_tier(W, FollowTier.PAPER_FOLLOW, 0.45, True, 1000)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW
        assert not d.changed

    def test_high_score_but_insufficient_time_stays_paper(self):
        d = evaluate_tier(W, FollowTier.PAPER_FOLLOW, 0.70, True, 3600)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW
        assert not d.changed
        assert "paper_time" in d.reason

    def test_high_score_and_sufficient_time_promotes_to_live(self):
        d = evaluate_tier(W, FollowTier.PAPER_FOLLOW, 0.70, True, 86400)
        assert d.recommended_tier == FollowTier.LIVE_FOLLOW
        assert d.promoted

    def test_no_score_demotes_to_watchlist(self):
        d = evaluate_tier(W, FollowTier.PAPER_FOLLOW, None, True, 99999)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert d.demoted


class TestLiveFollowTransitions:
    def test_adequate_score_stays_live(self):
        d = evaluate_tier(W, FollowTier.LIVE_FOLLOW, 0.60, True, 999999)
        assert d.recommended_tier == FollowTier.LIVE_FOLLOW
        assert not d.changed

    def test_score_drop_below_demote_to_paper(self):
        d = evaluate_tier(W, FollowTier.LIVE_FOLLOW, 0.30, True, 99999)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW
        assert d.demoted

    def test_no_score_demotes_to_watchlist(self):
        d = evaluate_tier(W, FollowTier.LIVE_FOLLOW, None, True, 99999)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert d.demoted

    def test_insufficient_coverage_demotes_to_watchlist(self):
        d = evaluate_tier(W, FollowTier.LIVE_FOLLOW, 0.80, False, 99999)
        assert d.recommended_tier == FollowTier.WATCHLIST
        assert d.demoted


class TestHysteresis:
    """Score between demote_to_paper (0.35) and promote_to_live (0.55)
    should NOT cause oscillation."""

    def test_live_with_borderline_score_stays_live(self):
        # 0.45 is above demote_to_paper (0.35) → stays live
        d = evaluate_tier(W, FollowTier.LIVE_FOLLOW, 0.45, True, 99999)
        assert d.recommended_tier == FollowTier.LIVE_FOLLOW

    def test_paper_with_borderline_score_stays_paper(self):
        # 0.50 is above demote_to_watchlist but below promote_to_live → stays paper
        d = evaluate_tier(W, FollowTier.PAPER_FOLLOW, 0.50, True, 99999)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW


class TestCustomThresholds:
    def test_custom_thresholds_respected(self):
        custom = PromotionThresholds(
            promote_to_paper=0.60,
            promote_to_live=0.80,
            min_paper_seconds=3600,
        )
        # 0.50 < 0.60 → stays watchlist
        d = evaluate_tier(W, FollowTier.WATCHLIST, 0.50, True, 0, custom)
        assert d.recommended_tier == FollowTier.WATCHLIST

        # 0.65 >= 0.60 → promotes to paper
        d = evaluate_tier(W, FollowTier.WATCHLIST, 0.65, True, 0, custom)
        assert d.recommended_tier == FollowTier.PAPER_FOLLOW
