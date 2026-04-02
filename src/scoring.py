"""Video scoring and duration filtering.

Score = engagement-storage efficiency (ESE):
    log10(likes * 0.7 + comments * 0.3 + 1) / estimated_gb ^ alpha

Alpha controls storage sensitivity per channel (from curated_channels):
    0.0 = pure engagement, ignore storage cost
    0.3 = moderate preference for storage-efficient content (default)
    0.5 = aggressive storage penalty
    1.0 = pure value-per-GB

All methods are pure — no I/O, no side effects.
"""

from __future__ import annotations

import math

# HLS storage estimate: measured ~1.8 Mbps median across channels
# (actual varies 0.8-3.0 based on content type and available source quality)
BITRATE_MBPS = 1.8


class VideoScorer:
    """Scores videos by engagement-storage efficiency."""

    def __init__(self, alpha_default: float = 0.3):
        self.alpha_default = alpha_default

    def score_video(self, video: dict, alpha: float | None = None) -> float:
        """Compute ESE score for a video.

        ESE = log10(likes * 0.7 + comments * 0.3 + 1) / gb ^ alpha

        Higher is better. Short, highly-liked videos score highest.
        """
        a = alpha if alpha is not None else self.alpha_default

        likes = _safe_int(video.get("like_count", 0))
        comments = _safe_int(video.get("comment_count", 0))
        duration_s = _safe_int(video.get("duration_seconds", 0))

        engagement = likes * 0.7 + comments * 0.3
        if engagement <= 0:
            return 0.0

        gb = estimate_gb(duration_s)
        return math.log10(engagement + 1) / (gb ** a)

    def select_canonical(
        self,
        candidates: list[dict],
        alpha: float,
        min_duration_s: int,
        max_duration_s: int | None,
    ) -> list[dict]:
        """Filter by duration, score, and return all eligible candidates.

        No budget filtering — returns ALL candidates that pass duration filters,
        each annotated with a score. Budget decisions happen in the processor.
        """
        eligible = [v for v in candidates if passes_duration_filter(v, min_duration_s, max_duration_s)]

        for v in eligible:
            v["score"] = self.score_video(v, alpha)

        return eligible


def estimate_gb(duration_seconds: int) -> float:
    """Estimate HLS storage in GB for a video of given duration."""
    return max(duration_seconds * BITRATE_MBPS / 8 / 1024, 0.001)


def passes_duration_filter(video: dict, min_duration_s: int, max_duration_s: int | None = None) -> bool:
    """Check if a video passes the duration rules."""
    duration = video.get("duration_seconds", 0)
    if duration < min_duration_s:
        return False
    if max_duration_s and duration > max_duration_s:
        return False
    return True


def _safe_int(val: object) -> int:
    try:
        f = float(val)
        return int(f) if math.isfinite(f) else 0
    except (ValueError, TypeError):
        return 0
