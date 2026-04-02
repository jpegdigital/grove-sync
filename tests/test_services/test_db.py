"""Tests for current SyncDatabase behavior."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.services.db import SyncDatabase


def _mock_supabase():
    mock = MagicMock()
    mock.table.return_value = mock
    mock.select.return_value = mock
    mock.delete.return_value = mock
    mock.update.return_value = mock
    mock.insert.return_value = mock
    mock.upsert.return_value = mock
    mock.eq.return_value = mock
    mock.neq.return_value = mock
    mock.in_.return_value = mock
    mock.filter.return_value = mock
    mock.range.return_value = mock
    mock.order.return_value = mock
    mock.limit.return_value = mock
    return mock


class TestSyncDatabaseInit:
    @pytest.mark.unit
    def test_stores_client_and_config(self):
        client = MagicMock()
        db = SyncDatabase(client, page_size=500, enqueue_batch_size=50)
        assert db.client is client
        assert db.page_size == 500
        assert db.enqueue_batch_size == 50


class TestSyncDatabaseUpdateFullRefreshTimestamp:
    @pytest.mark.integration
    def test_dry_run_skips(self):
        client = _mock_supabase()
        db = SyncDatabase(client)

        db.update_full_refresh_timestamp(["c1"], dry_run=True)

        client.table.assert_not_called()


class TestSyncDatabaseUpdateVideoScores:
    @pytest.mark.integration
    def test_dry_run_skips(self):
        client = _mock_supabase()
        db = SyncDatabase(client)

        db.update_video_scores({"v1": 1.2}, dry_run=True)

        client.table.assert_not_called()

    @pytest.mark.integration
    def test_batches_upserts(self):
        client = _mock_supabase()
        client.execute.return_value = SimpleNamespace(data=[])
        db = SyncDatabase(client, enqueue_batch_size=2)

        db.update_video_scores({"v1": 1.0, "v2": 2.0, "v3": 3.0})

        assert client.upsert.call_count == 2
        first_payload = client.upsert.call_args_list[0].args[0]
        second_payload = client.upsert.call_args_list[1].args[0]
        assert first_payload == [
            {"youtube_id": "v1", "score": 1.0},
            {"youtube_id": "v2", "score": 2.0},
        ]
        assert second_payload == [{"youtube_id": "v3", "score": 3.0}]
        assert all(c.kwargs["on_conflict"] == "youtube_id" for c in client.upsert.call_args_list)


class TestSyncDatabaseReplaceChannelJobs:
    @pytest.mark.integration
    def test_dry_run_prints_jobs_without_writes(self, capsys):
        client = _mock_supabase()
        db = SyncDatabase(client)

        db.replace_channel_jobs("UC1", [{"video_id": "v1", "channel_id": "UC1"}], dry_run=True)

        assert "DRY RUN" in capsys.readouterr().out
        client.table.assert_not_called()

    @pytest.mark.integration
    def test_replaces_only_pending_jobs_and_skips_in_flight_duplicates(self):
        client = _mock_supabase()
        client.execute.side_effect = [
            SimpleNamespace(data=[{"video_id": "v_inflight"}]),
            SimpleNamespace(data=[]),
            SimpleNamespace(data=[]),
        ]
        db = SyncDatabase(client, enqueue_batch_size=10)

        jobs = [
            {"video_id": "v_inflight", "channel_id": "UC1", "metadata": {}, "score": 9.0, "published_at": "2026-03-01T00:00:00Z"},
            {"video_id": "v_new", "channel_id": "UC1", "metadata": {"title": "New"}, "score": 4.0, "published_at": "2026-03-02T00:00:00Z"},
        ]

        db.replace_channel_jobs("UC1", jobs)

        client.delete.assert_called_once()
        eq_calls = [c.args for c in client.eq.call_args_list]
        assert ("channel_id", "UC1") in eq_calls
        assert ("status", "pending") in eq_calls
        client.insert.assert_called_once_with([
            {
                "video_id": "v_new",
                "channel_id": "UC1",
                "metadata": {"title": "New"},
                "score": 4.0,
                "published_at": "2026-03-02T00:00:00Z",
                "status": "pending",
            }
        ])


class TestSyncDatabaseFetchExistingR2WithBytes:
    @pytest.mark.integration
    def test_returns_dict_with_storage_bytes(self):
        client = _mock_supabase()
        client.execute.return_value = SimpleNamespace(data=[
            {
                "youtube_id": "v1",
                "storage_bytes": 1000,
                "score": 5.0,
                "sync_tier": "fresh",
                "duration_seconds": 120,
                "published_at": "2026-03-01T00:00:00Z",
                "media_path": "foo/master.m3u8",
            },
        ])
        db = SyncDatabase(client)

        result = db.fetch_existing_r2_with_bytes("UC1")

        assert result == {
            "v1": {
                "storage_bytes": 1000,
                "published_at": "2026-03-01T00:00:00Z",
                "score": 5.0,
                "sync_tier": "fresh",
                "duration_seconds": 120,
                "media_path": "foo/master.m3u8",
            }
        }


class TestSyncDatabaseResolveHandle:
    @pytest.mark.integration
    def test_from_metadata(self):
        db = SyncDatabase(_mock_supabase())
        assert db.resolve_channel_handle({"metadata": {"handle": "@chan"}}) == "@chan"

    @pytest.mark.integration
    def test_fallback_to_db(self):
        client = _mock_supabase()
        client.execute.return_value = SimpleNamespace(data=[{"custom_url": "@fromdb"}])
        db = SyncDatabase(client)

        assert db.resolve_channel_handle({"metadata": {}, "channel_id": "UC1"}) == "@fromdb"

    @pytest.mark.integration
    def test_returns_unknown(self):
        client = _mock_supabase()
        client.execute.return_value = SimpleNamespace(data=[])
        db = SyncDatabase(client)

        assert db.resolve_channel_handle({"metadata": {}, "channel_id": "UC1"}) == "unknown"
