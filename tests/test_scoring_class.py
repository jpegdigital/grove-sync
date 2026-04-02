"""Tests for VideoScorer class — scoring, ESE, and canonical set logic."""

import pytest

from src.scoring import VideoScorer


class TestVideoScorerInit:
    @pytest.mark.unit
    def test_stores_weights_and_half_life(self):
        weights = {"popularity": 0.4, "engagement": 0.3, "freshness": 0.3}
        s = VideoScorer(weights, half_life_days=60)
        assert s.weights == weights
        assert s.half_life_days == 60


class TestVideoScorerScoring:
    WEIGHTS = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}

    @pytest.mark.unit
    def test_score_video_uses_instance_weights(self):
        s = VideoScorer(self.WEIGHTS, 90)
        video = {"view_count": 100000, "like_count": 5000, "comment_count": 500}
        score = s.score_video(video)
        assert score > 0

    @pytest.mark.unit
    def test_ese_score_uses_instance_state(self):
        s = VideoScorer(self.WEIGHTS, 90)
        video = {"view_count": 100000, "like_count": 5000, "comment_count": 500, "duration_seconds": 300}
        ese = s.ese_score(video, alpha=0.3)
        raw = s.score_video(video)
        assert ese > raw  # ESE divides by gb^0.3, which is < 1 for small videos


class TestSelectCanonical:
    """Tests for select_canonical() — returns full universe, no budget filtering."""

    WEIGHTS = {"popularity": 0.25, "engagement": 0.25, "freshness": 0.15, "velocity": 0.20, "reach": 0.15}

    def _make_candidate(self, video_id, duration=300, published_at="2026-03-01T00:00:00Z",
                        views=10000, likes=500, comments=50):
        return {
            "video_id": video_id,
            "duration_seconds": duration,
            "published_at": published_at,
            "view_count": views,
            "like_count": likes,
            "comment_count": comments,
            "source_tags": [],
        }

    @pytest.mark.unit
    def test_returns_all_eligible_candidates(self):
        s = VideoScorer(self.WEIGHTS, 90)
        candidates = [self._make_candidate(f"v{i}") for i in range(10)]
        result = s.select_canonical(candidates, alpha=0.3, min_duration_s=60, max_duration_s=None)
        assert len(result) == 10

    @pytest.mark.unit
    def test_adds_score_fields(self):
        s = VideoScorer(self.WEIGHTS, 90)
        candidates = [self._make_candidate("v1")]
        result = s.select_canonical(candidates, alpha=0.3, min_duration_s=60, max_duration_s=None)
        assert "score" in result[0]
        assert "ese_score" in result[0]
        assert result[0]["score"] > 0

    @pytest.mark.unit
    def test_duration_filter_applied(self):
        s = VideoScorer(self.WEIGHTS, 90)
        candidates = [
            self._make_candidate("short", duration=30),   # below min
            self._make_candidate("ok", duration=300),
            self._make_candidate("long", duration=7200),   # above max
        ]
        result = s.select_canonical(candidates, alpha=0.3, min_duration_s=60, max_duration_s=3600)
        ids = [v["video_id"] for v in result]
        assert "short" not in ids
        assert "long" not in ids
        assert "ok" in ids

    @pytest.mark.unit
    def test_no_budget_filtering(self):
        """select_canonical returns ALL eligible regardless of size."""
        s = VideoScorer(self.WEIGHTS, 90)
        candidates = [self._make_candidate(f"v{i}", duration=3600) for i in range(50)]
        result = s.select_canonical(candidates, alpha=0.3, min_duration_s=60, max_duration_s=None)
        assert len(result) == 50
