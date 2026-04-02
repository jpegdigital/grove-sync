"""Tests for ProcessCommand — video-at-a-time budget loop architecture."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config import AppConfig
from src.models import DownloadResult


def _mock_supabase():
    mock = MagicMock()
    mock.table.return_value = mock
    mock.select.return_value = mock
    mock.delete.return_value = mock
    mock.update.return_value = mock
    mock.insert.return_value = mock
    mock.eq.return_value = mock
    mock.neq.return_value = mock
    mock.in_.return_value = mock
    mock.filter.return_value = mock
    mock.range.return_value = mock
    mock.order.return_value = mock
    mock.limit.return_value = mock
    mock.upsert.return_value = mock
    mock.rpc.return_value = mock
    mock.execute.return_value = SimpleNamespace(data=[])
    return mock


def _make_job(video_id, action="download", channel_id="UC1", **overrides):
    base = {
        "id": f"job-{video_id}",
        "video_id": video_id,
        "channel_id": channel_id,
        "action": action,
        "status": "pending",
        "metadata": {"title": f"Video {video_id}", "handle": "@test",
                      "published_at": "2026-03-15T00:00:00Z", "source_tags": ["recent"]},
        "score": 5.0,
        "storage_bytes": None,
        "priority": 10 if action == "remove" else 0,
        "attempts": 0,
    }
    base.update(overrides)
    return base


def _make_download_result(video_id, staging_dir, **overrides):
    defaults = {
        "video_id": video_id,
        "channel_id": "UC1",
        "score": 5.0,
        "storage_bytes": 100_000_000,  # 100 MB
        "staging_dir": staging_dir / video_id,
        "job_id": f"job-{video_id}",
        "published_at": "2026-03-15T00:00:00Z",
        "info_data": {"title": f"Video {video_id}", "handle": "@test",
                      "source_tags": ["recent"]},
        "remuxed_tiers": [{"label": "720p", "hls_dir": str(staging_dir / video_id)}],
        "sidecar_files": [],
    }
    defaults.update(overrides)
    return DownloadResult(**defaults)


DEFAULT_STAGING_BYTES = 100_000_000  # 100 MB — default mock size for _measure_staging_bytes


@pytest.fixture
def config():
    return AppConfig()


@pytest.fixture
def db():
    from src.services.db import SyncDatabase

    return SyncDatabase(_mock_supabase())


@pytest.fixture
def storage():
    mock = MagicMock()
    mock.delete_video_objects.return_value = (True, None)
    mock.upload_hls_package.return_value = {"master": "test/master.m3u8"}
    return mock


@pytest.fixture
def hls():
    mock = MagicMock()
    mock.download_video_tiers.return_value = (
        [{"label": "720p", "source_path": "/tmp/v.mp4"}],
        {"info_json": Path("/tmp/info.json")},
    )
    mock.remux_to_hls.return_value = [{"label": "720p", "hls_dir": "/tmp/hls/720p"}]
    mock.extract_tier_metadata.return_value = {
        "bandwidth": 2500000, "resolution": "1280x720", "codecs": "avc1.64001f,mp4a.40.2",
    }
    mock.generate_master_playlist.return_value = "#EXTM3U\n#EXT-X-STREAM-INF\n720p/playlist.m3u8"
    mock.parse_info_json.return_value = {
        "title": "Test", "published_at": "2026-03-15T00:00:00Z",
        "handle": "@test", "source_tags": ["recent"],
    }
    mock.hls_cfg = {"min_tiers": 1}
    return mock


class TestProcessCommandPurge:
    @pytest.mark.unit
    def test_purge_claims_removal_jobs(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        removal_jobs = [
            _make_job("v1", action="remove", metadata={"media_path": "test/v1/master.m3u8"}),
            _make_job("v2", action="remove", metadata={"media_path": "test/v2/master.m3u8"}),
        ]
        db.claim_removal_jobs = MagicMock(return_value=removal_jobs)
        db.delete_video_record = MagicMock()
        db.complete_job = MagicMock()

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        count = cmd._purge(dry_run=False, verbose=False)

        assert count == 2
        assert storage.delete_video_objects.call_count == 2
        assert db.complete_job.call_count == 2

    @pytest.mark.unit
    def test_purge_dry_run_no_side_effects(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        db.claim_removal_jobs = MagicMock(return_value=[_make_job("v1", action="remove")])
        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._purge(dry_run=True, verbose=False)
        storage.delete_video_objects.assert_not_called()


class TestDownloadOne:
    @pytest.mark.unit
    def test_returns_download_result_on_success(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        job = _make_job("v1")
        db.update_job_storage_bytes = MagicMock()
        db.update_job_status = MagicMock()

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        result = cmd._download_one(job, max_attempts=3, dry_run=False, verbose=False)

        assert result is not None
        assert result.video_id == "v1"
        assert result.storage_bytes >= 0

    @pytest.mark.unit
    def test_returns_none_on_failure(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        job = _make_job("v1", attempts=0)
        hls.download_video_tiers.side_effect = RuntimeError("download failed")
        db.fail_job = MagicMock()

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        result = cmd._download_one(job, max_attempts=3, dry_run=False, verbose=False)

        assert result is None
        db.fail_job.assert_called_once()

    @pytest.mark.unit
    def test_permanent_failure_on_max_attempts(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        job = _make_job("v1", attempts=2)
        hls.download_video_tiers.side_effect = RuntimeError("download failed")
        db.mark_job_failed_permanent = MagicMock()

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        result = cmd._download_one(job, max_attempts=3, dry_run=False, verbose=False)

        assert result is None
        db.mark_job_failed_permanent.assert_called_once()


class TestUploadOne:
    @pytest.mark.unit
    def test_uploads_and_completes_job(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        staging = tmp_path / "v1"
        staging.mkdir()
        dl = _make_download_result("v1", tmp_path)

        db.upsert_video_record = MagicMock()
        db.complete_job = MagicMock()
        db.resolve_channel_handle = MagicMock(return_value="@test")

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        success = cmd._upload_one(dl, dry_run=False, verbose=False)

        assert success is True
        storage.upload_hls_package.assert_called_once()
        db.upsert_video_record.assert_called_once()
        db.complete_job.assert_called_once_with("job-v1")

    @pytest.mark.unit
    def test_dry_run_no_upload(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        staging = tmp_path / "v1"
        staging.mkdir()
        dl = _make_download_result("v1", tmp_path)

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        success = cmd._upload_one(dl, dry_run=True, verbose=False)

        assert success is True
        storage.upload_hls_package.assert_not_called()


class TestEvictOne:
    @pytest.mark.unit
    def test_evicts_from_r2_and_db(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        r2_data = {"media_path": "test/v_old/master.m3u8"}
        db.delete_video_record = MagicMock()

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        success = cmd._evict_one("v_old", r2_data, dry_run=False, verbose=False)

        assert success is True
        storage.delete_video_objects.assert_called_once_with(r2_data)
        db.delete_video_record.assert_called_once_with("v_old")

    @pytest.mark.unit
    def test_dry_run_no_eviction(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        success = cmd._evict_one("v_old", {}, dry_run=True, verbose=False)

        assert success is True
        storage.delete_video_objects.assert_not_called()


class TestProcessTierLoop:
    """Tests for the per-tier video-at-a-time budget loop."""

    def _setup_cmd(self, config, db, storage, hls, tmp_path, staging_bytes=DEFAULT_STAGING_BYTES):
        from src.commands.process import ProcessCommand

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._measure_staging_bytes = MagicMock(return_value=staging_bytes)
        db.update_job_storage_bytes = MagicMock()
        db.update_job_status = MagicMock()
        db.upsert_video_record = MagicMock()
        db.complete_job = MagicMock()
        db.fail_job = MagicMock()
        db.delete_video_record = MagicMock()
        db.resolve_channel_handle = MagicMock(return_value="@test")
        db.delete_channel_pending_jobs = MagicMock()
        return cmd

    @pytest.mark.unit
    def test_fresh_empty_r2_uploads(self, config, db, storage, hls, tmp_path):
        """Empty R2, under budget — download + upload."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 1_000_000_000  # 1 GB

        # One job, empty R2
        job = _make_job("v1", metadata={
            "title": "V1", "handle": "@test",
            "published_at": "2026-03-15T00:00:00Z", "source_tags": ["recent"],
        })
        claim_returns = [job, None]
        db.claim_next_pending_job = MagicMock(side_effect=claim_returns)
        db.fetch_existing_r2_with_bytes = MagicMock(return_value={})

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 1
        assert stats["evicted"] == 0

    @pytest.mark.unit
    def test_fresh_newer_evicts_oldest(self, config, db, storage, hls, tmp_path):
        """New video newer than oldest, over budget — evict oldest, upload new."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 150_000_000  # 150 MB

        job = _make_job("v_new", metadata={
            "title": "New", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["recent"],
        })
        claim_returns = [job, None]
        db.claim_next_pending_job = MagicMock(side_effect=claim_returns)

        # R2 has one old video taking up most of the budget
        db.fetch_existing_r2_with_bytes = MagicMock(return_value={
            "v_old": {
                "storage_bytes": 100_000_000,
                "published_at": "2026-01-01T00:00:00Z",
                "score": 3.0,
                "source_tags": ["recent"],
                "duration_seconds": 600,
                "media_path": "test/v_old/master.m3u8",
            },
        })

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 1
        assert stats["evicted"] == 1
        db.delete_video_record.assert_called_with("v_old")

    @pytest.mark.unit
    def test_fresh_multiple_evictions(self, config, db, storage, hls, tmp_path):
        """Need to evict 2+ oldest to fit new video."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 150_000_000  # 150 MB

        job = _make_job("v_new", metadata={
            "title": "New", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["recent"],
        })
        claim_returns = [job, None]
        db.claim_next_pending_job = MagicMock(side_effect=claim_returns)

        # R2 has two old videos filling the budget
        r2_state = {
            "v_old1": {
                "storage_bytes": 60_000_000,
                "published_at": "2026-01-01T00:00:00Z",
                "score": 2.0,
                "source_tags": ["recent"],
                "duration_seconds": 300,
                "media_path": "test/v_old1/master.m3u8",
            },
            "v_old2": {
                "storage_bytes": 60_000_000,
                "published_at": "2026-01-15T00:00:00Z",
                "score": 3.0,
                "source_tags": ["recent"],
                "duration_seconds": 300,
                "media_path": "test/v_old2/master.m3u8",
            },
        }
        db.fetch_existing_r2_with_bytes = MagicMock(return_value=r2_state)

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 1
        assert stats["evicted"] == 2

    @pytest.mark.unit
    def test_fresh_older_fits(self, config, db, storage, hls, tmp_path):
        """Older video, under budget — upload."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 500_000_000  # 500 MB

        job = _make_job("v_old", metadata={
            "title": "Old", "handle": "@test",
            "published_at": "2025-01-01T00:00:00Z", "source_tags": ["recent"],
        })
        claim_returns = [job, None]
        db.claim_next_pending_job = MagicMock(side_effect=claim_returns)

        # R2 has a newer video, plenty of room
        db.fetch_existing_r2_with_bytes = MagicMock(return_value={
            "v_existing": {
                "storage_bytes": 100_000_000,
                "published_at": "2026-03-01T00:00:00Z",
                "score": 5.0,
                "source_tags": ["recent"],
                "duration_seconds": 600,
                "media_path": "test/v_existing/master.m3u8",
            },
        })

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 1
        assert stats["evicted"] == 0

    @pytest.mark.unit
    def test_fresh_older_no_room_breaks(self, config, db, storage, hls, tmp_path):
        """Older video, no room — break, no more jobs claimed."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 150_000_000  # 150 MB

        job = _make_job("v_old", metadata={
            "title": "Old", "handle": "@test",
            "published_at": "2025-01-01T00:00:00Z", "source_tags": ["recent"],
        })
        # If loop didn't break, it would try to claim another — should only claim once
        db.claim_next_pending_job = MagicMock(side_effect=[job])

        db.fetch_existing_r2_with_bytes = MagicMock(return_value={
            "v_existing": {
                "storage_bytes": 100_000_000,
                "published_at": "2026-03-01T00:00:00Z",
                "score": 5.0,
                "source_tags": ["recent"],
                "duration_seconds": 600,
                "media_path": "test/v_existing/master.m3u8",
            },
        })

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 0
        assert stats["skipped"] == 1
        # Should have only claimed one job (broke after it)
        assert db.claim_next_pending_job.call_count == 1

    @pytest.mark.unit
    def test_catalog_higher_score_evicts_lowest(self, config, db, storage, hls, tmp_path):
        """Catalog tier: new video with higher score evicts lowest-scoring."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 150_000_000  # 150 MB

        job = _make_job("v_best", score=10.0, metadata={
            "title": "Best", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["popular"],
        })
        claim_returns = [job, None]
        db.claim_next_pending_job = MagicMock(side_effect=claim_returns)

        db.fetch_existing_r2_with_bytes = MagicMock(return_value={
            "v_low": {
                "storage_bytes": 100_000_000,
                "published_at": "2026-02-01T00:00:00Z",
                "score": 2.0,
                "source_tags": ["popular"],
                "duration_seconds": 600,
                "media_path": "test/v_low/master.m3u8",
            },
        })

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="catalog", sort_key="score",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 1
        assert stats["evicted"] == 1
        db.delete_video_record.assert_called_with("v_low")

    @pytest.mark.unit
    def test_catalog_lower_score_fits(self, config, db, storage, hls, tmp_path):
        """Catalog tier: lower score but room in budget — upload."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 500_000_000  # 500 MB

        job = _make_job("v_low", score=2.0, metadata={
            "title": "Low", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["rated"],
        })
        claim_returns = [job, None]
        db.claim_next_pending_job = MagicMock(side_effect=claim_returns)

        db.fetch_existing_r2_with_bytes = MagicMock(return_value={
            "v_high": {
                "storage_bytes": 100_000_000,
                "published_at": "2026-02-01T00:00:00Z",
                "score": 8.0,
                "source_tags": ["popular"],
                "duration_seconds": 600,
                "media_path": "test/v_high/master.m3u8",
            },
        })

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="catalog", sort_key="score",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 1
        assert stats["evicted"] == 0

    @pytest.mark.unit
    def test_catalog_lower_score_no_room_breaks(self, config, db, storage, hls, tmp_path):
        """Catalog tier: lower score, no room — break."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 150_000_000

        job = _make_job("v_low", score=2.0, metadata={
            "title": "Low", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["rated"],
        })
        db.claim_next_pending_job = MagicMock(side_effect=[job])

        db.fetch_existing_r2_with_bytes = MagicMock(return_value={
            "v_high": {
                "storage_bytes": 100_000_000,
                "published_at": "2026-02-01T00:00:00Z",
                "score": 8.0,
                "source_tags": ["popular"],
                "duration_seconds": 600,
                "media_path": "test/v_high/master.m3u8",
            },
        })

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="catalog", sort_key="score",
            dry_run=False, verbose=False,
        )

        assert stats["uploaded"] == 0
        assert stats["skipped"] == 1

    @pytest.mark.unit
    def test_oversized_skipped_continues(self, config, db, storage, hls, tmp_path):
        """Single video > tier budget — skip, continue to next."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 50_000_000  # 50 MB

        # First download is 100MB (oversized), second is 30MB (fits)
        cmd._measure_staging_bytes = MagicMock(side_effect=[100_000_000, 30_000_000])

        job_big = _make_job("v_big", metadata={
            "title": "Big", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["recent"],
        })
        job_small = _make_job("v_small", metadata={
            "title": "Small", "handle": "@test",
            "published_at": "2026-03-19T00:00:00Z", "source_tags": ["recent"],
        })

        db.claim_next_pending_job = MagicMock(side_effect=[job_big, job_small, None])
        db.fetch_existing_r2_with_bytes = MagicMock(return_value={})

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["skipped"] >= 1
        assert stats["uploaded"] == 1

    @pytest.mark.unit
    def test_download_failure_continues(self, config, db, storage, hls, tmp_path):
        """Download fails — retry logic runs, loop continues to next job."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)
        budget_bytes = 1_000_000_000

        job_fail = _make_job("v_fail", metadata={
            "title": "Fail", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["recent"],
        })
        job_ok = _make_job("v_ok", metadata={
            "title": "OK", "handle": "@test",
            "published_at": "2026-03-19T00:00:00Z", "source_tags": ["recent"],
        })

        db.claim_next_pending_job = MagicMock(side_effect=[job_fail, job_ok, None])
        db.fetch_existing_r2_with_bytes = MagicMock(return_value={})

        ok_tiers = [{"label": "720p", "source_path": "/tmp/v.mp4"}]
        ok_sidecar = {"info_json": Path("/tmp/info.json")}
        hls.download_video_tiers.side_effect = [
            RuntimeError("download failed"),
            (ok_tiers, ok_sidecar),
        ]

        stats = cmd._process_tier(
            "UC1", budget_bytes, tier="fresh", sort_key="published_at",
            dry_run=False, verbose=False,
        )

        assert stats["downloaded"] == 1  # only the successful one
        assert stats["uploaded"] == 1
        db.fail_job.assert_called_once()


class TestProcessChannel:
    """Tests for _process_channel orchestration."""

    def _setup_cmd(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._measure_staging_bytes = MagicMock(return_value=DEFAULT_STAGING_BYTES)
        db.update_job_storage_bytes = MagicMock()
        db.update_job_status = MagicMock()
        db.upsert_video_record = MagicMock()
        db.complete_job = MagicMock()
        db.fail_job = MagicMock()
        db.delete_video_record = MagicMock()
        db.resolve_channel_handle = MagicMock(return_value="@test")
        db.delete_channel_pending_jobs = MagicMock()
        db.fetch_existing_r2_with_bytes = MagicMock(return_value={})
        return cmd

    @pytest.mark.unit
    def test_archive_uploads_everything(self, config, db, storage, hls, tmp_path):
        """Archive mode — no budget checks, upload all."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)

        ch_cfg = {
            "channel_id": "UC1",
            "storage_budget_gb": 1.0,
            "catalog_fraction": 0.6,
            "sync_mode": "archive",
        }

        job1 = _make_job("v1", metadata={
            "title": "V1", "handle": "@test",
            "published_at": "2026-03-20T00:00:00Z", "source_tags": ["recent"],
        })
        job2 = _make_job("v2", metadata={
            "title": "V2", "handle": "@test",
            "published_at": "2026-03-19T00:00:00Z", "source_tags": ["popular"],
        })
        # Simulate jobs being consumed: first call returns 2, second returns 1, third returns 0
        db.fetch_pending_download_jobs = MagicMock(
            side_effect=[[job1, job2], [job2], []]
        )

        stats = cmd._process_channel("UC1", ch_cfg, dry_run=False, verbose=False)

        assert stats["uploaded"] == 2
        assert stats["evicted"] == 0

    @pytest.mark.unit
    def test_zero_budget_skips(self, config, db, storage, hls, tmp_path):
        """Zero budget channel — skip entirely, delete pending jobs."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)

        ch_cfg = {
            "channel_id": "UC1",
            "storage_budget_gb": 0,
            "catalog_fraction": 0.6,
            "sync_mode": "sync",
        }

        stats = cmd._process_channel("UC1", ch_cfg, dry_run=False, verbose=False)

        assert stats["uploaded"] == 0
        assert stats["downloaded"] == 0
        db.delete_channel_pending_jobs.assert_called_once_with("UC1")

    @pytest.mark.unit
    def test_cleanup_pending_on_exit(self, config, db, storage, hls, tmp_path):
        """Remaining pending jobs deleted after loop exits."""
        cmd = self._setup_cmd(config, db, storage, hls, tmp_path)

        ch_cfg = {
            "channel_id": "UC1",
            "storage_budget_gb": 10.0,
            "catalog_fraction": 0.6,
            "sync_mode": "sync",
        }

        # No jobs to process
        db.claim_next_pending_job = MagicMock(return_value=None)

        cmd._process_channel("UC1", ch_cfg, dry_run=False, verbose=False)

        db.delete_channel_pending_jobs.assert_called_with("UC1")


class TestRunOrchestration:
    """Tests for run() top-level orchestration."""

    @pytest.mark.unit
    def test_purge_then_channels(self, config, db, storage, hls, tmp_path):
        """Purge runs first, then per-channel processing."""
        from src.commands.process import ProcessCommand

        call_order = []

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._purge = MagicMock(side_effect=lambda **kw: (call_order.append("purge"), 0)[1])
        cmd._process_channel = MagicMock(
            side_effect=lambda *a, **kw: (
                call_order.append("process_channel"),
                {"downloaded": 0, "uploaded": 0, "evicted": 0, "skipped": 0},
            )[1]
        )

        db.fetch_curated_channels = MagicMock(return_value=[
            {"channel_id": "UC1", "storage_budget_gb": 10.0,
             "catalog_fraction": 0.6, "sync_mode": "sync"},
        ])

        cmd.run(dry_run=False, verbose=False)

        assert call_order == ["purge", "process_channel"]

    @pytest.mark.unit
    def test_removals_only_skips_processing(self, config, db, storage, hls, tmp_path):
        from src.commands.process import ProcessCommand

        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._purge = MagicMock(return_value=0)
        cmd._process_channel = MagicMock()

        cmd.run(dry_run=False, verbose=False, removals_only=True)

        cmd._purge.assert_called_once()
        cmd._process_channel.assert_not_called()
