"""Tests for VideoScorer — velocity and subscriber-relative reach signals."""

import pytest

from src.scoring import VideoScorer

WEIGHTS = {
    "popularity": 0.25,
    "engagement": 0.25,
    "freshness": 0.20,
    "velocity": 0.15,
    "reach": 0.15,
}


class TestVelocitySignal:
    """velocity = log10(views / age_days) — rewards fast-growing videos."""

    @pytest.mark.unit
    def test_younger_video_scores_higher_velocity(self):
        """Same views, fewer days → higher score."""
        scorer = VideoScorer(WEIGHTS, half_life_days=90)
        young = {"view_count": 100000, "published_at": "2026-03-30T00:00:00Z"}
        old = {"view_count": 100000, "published_at": "2025-01-01T00:00:00Z"}
        assert scorer.score_video(young) > scorer.score_video(old)

    @pytest.mark.unit
    def test_velocity_zero_weight_has_no_effect(self):
        """When velocity weight is 0, score matches legacy behavior."""
        legacy_weights = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}
        zero_vel_weights = {**legacy_weights, "velocity": 0.0, "reach": 0.0}
        legacy_scorer = VideoScorer(legacy_weights, 90)
        new_scorer = VideoScorer(zero_vel_weights, 90)
        video = {"view_count": 50000, "like_count": 2000, "comment_count": 100,
                 "published_at": "2026-01-15T00:00:00Z"}
        assert legacy_scorer.score_video(video) == pytest.approx(
            new_scorer.score_video(video), abs=0.001
        )

    @pytest.mark.edge_cases
    def test_velocity_with_no_published_at(self):
        """Missing published_at should not crash — uses fallback age."""
        scorer = VideoScorer(WEIGHTS, 90)
        video = {"view_count": 10000}
        score = scorer.score_video(video)
        assert score > 0

    @pytest.mark.edge_cases
    def test_velocity_brand_new_video(self):
        """Video published today (age < 1 day) should not crash or produce inf."""
        import math
        scorer = VideoScorer(WEIGHTS, 90)
        video = {"view_count": 5000, "published_at": "2026-03-31T12:00:00Z"}
        score = scorer.score_video(video)
        assert math.isfinite(score)


class TestReachSignal:
    """reach = log10(views / subscriber_count) — rewards over-performing videos."""

    @pytest.mark.unit
    def test_high_reach_scores_higher(self):
        """Same views, fewer subscribers → higher reach → higher score."""
        scorer = VideoScorer(WEIGHTS, 90)
        video = {"view_count": 100000, "like_count": 5000, "comment_count": 500,
                 "published_at": "2026-03-01T00:00:00Z"}
        small_channel = scorer.score_video(video, subscriber_count=10000)
        big_channel = scorer.score_video(video, subscriber_count=10000000)
        assert small_channel > big_channel

    @pytest.mark.unit
    def test_reach_zero_weight_has_no_effect(self):
        """When reach weight is 0, subscriber_count doesn't matter."""
        weights = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30, "reach": 0.0}
        scorer = VideoScorer(weights, 90)
        video = {"view_count": 50000, "like_count": 2000,
                 "published_at": "2026-01-15T00:00:00Z"}
        score_a = scorer.score_video(video, subscriber_count=1000)
        score_b = scorer.score_video(video, subscriber_count=10000000)
        assert score_a == pytest.approx(score_b, abs=0.001)

    @pytest.mark.unit
    def test_reach_without_subscriber_count(self):
        """No subscriber_count passed → reach signal is 0 (no crash)."""
        scorer = VideoScorer(WEIGHTS, 90)
        video = {"view_count": 50000, "published_at": "2026-01-15T00:00:00Z"}
        score = scorer.score_video(video)
        assert score > 0

    @pytest.mark.edge_cases
    def test_reach_with_zero_subscribers(self):
        """subscriber_count=0 should not crash."""
        import math
        scorer = VideoScorer(WEIGHTS, 90)
        video = {"view_count": 50000, "published_at": "2026-01-15T00:00:00Z"}
        score = scorer.score_video(video, subscriber_count=0)
        assert math.isfinite(score)
