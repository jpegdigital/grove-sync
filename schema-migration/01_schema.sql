-- ============================================================================
-- PradoTube Schema Migration: 01_schema.sql
-- Creates the pradotube schema, all tables, indexes, and foreign keys.
-- ============================================================================

BEGIN;

-- Create schema
CREATE SCHEMA IF NOT EXISTS pradotube;

-- ── channels ────────────────────────────────────────────────────────────────
CREATE TABLE pradotube.channels (
    youtube_id              text        PRIMARY KEY,
    title                   text        NOT NULL,
    description             text,
    custom_url              text,
    thumbnail_url           text,
    banner_url              text,
    subscriber_count        bigint      DEFAULT 0,
    subscriber_count_hidden boolean     DEFAULT false,
    video_count             bigint      DEFAULT 0,
    view_count              bigint      DEFAULT 0,
    published_at            timestamptz,
    fetched_at              timestamptz DEFAULT now(),
    videos_fetched_at       timestamptz,
    created_at              timestamptz DEFAULT now()
);

-- ── creators ────────────────────────────────────────────────────────────────
CREATE TABLE pradotube.creators (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name              text        NOT NULL,
    slug              text        NOT NULL UNIQUE,
    avatar_channel_id text        REFERENCES pradotube.channels(youtube_id),
    cover_channel_id  text        REFERENCES pradotube.channels(youtube_id),
    display_order     integer     NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT now(),
    priority          integer     NOT NULL DEFAULT 50,
    sort_name         text
);

CREATE INDEX idx_creators_display_order ON pradotube.creators (display_order);
CREATE INDEX idx_creators_slug          ON pradotube.creators (slug);
CREATE INDEX idx_creators_sort_name     ON pradotube.creators (sort_name);

-- ── curated_channels ────────────────────────────────────────────────────────
CREATE TABLE pradotube.curated_channels (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id            text        NOT NULL UNIQUE REFERENCES pradotube.channels(youtube_id),
    display_order         integer     DEFAULT 0,
    notes                 text,
    created_at            timestamptz DEFAULT now(),
    creator_id            uuid        REFERENCES pradotube.creators(id),
    priority              integer     NOT NULL DEFAULT 50,
    date_range_override   text,
    min_duration_override integer,
    last_full_refresh_at  timestamptz,
    max_videos_override   integer,
    sync_mode             text        NOT NULL DEFAULT 'sync',
    storage_budget_gb     numeric     NOT NULL DEFAULT 10.0,
    catalog_fraction      numeric     NOT NULL DEFAULT 0.60,
    scoring_alpha         numeric     NOT NULL DEFAULT 0.30,
    min_duration_seconds  integer     NOT NULL DEFAULT 60,
    max_duration_seconds  integer     NOT NULL DEFAULT 3600
);

CREATE INDEX idx_curated_creator       ON pradotube.curated_channels (creator_id);
CREATE INDEX idx_curated_display_order ON pradotube.curated_channels (display_order);

-- ── channel_calibration ─────────────────────────────────────────────────────
CREATE TABLE pradotube.channel_calibration (
    channel_id                text        PRIMARY KEY REFERENCES pradotube.channels(youtube_id),
    calibrated_at             timestamptz NOT NULL DEFAULT now(),
    total_videos_sampled      integer     NOT NULL DEFAULT 0,
    videos_in_date_range      integer     NOT NULL DEFAULT 0,
    posts_per_week            numeric     DEFAULT 0,
    avg_gap_days              numeric,
    median_gap_days           numeric,
    avg_duration_seconds      integer,
    median_duration_seconds   integer,
    passing_min60             integer     DEFAULT 0,
    passing_min60_max3600     integer     DEFAULT 0,
    passing_min300            integer     DEFAULT 0,
    passing_min300_max3600    integer     DEFAULT 0,
    duration_buckets          jsonb       DEFAULT '{}'::jsonb
);

-- ── videos ──────────────────────────────────────────────────────────────────
CREATE TABLE pradotube.videos (
    youtube_id          text        PRIMARY KEY,
    channel_id          text        NOT NULL REFERENCES pradotube.channels(youtube_id),
    title               text        NOT NULL,
    description         text,
    thumbnail_url       text,
    published_at        timestamptz,
    duration            text,
    view_count          bigint,
    fetched_at          timestamptz DEFAULT now(),
    created_at          timestamptz DEFAULT now(),
    is_downloaded       boolean     NOT NULL DEFAULT false,
    media_path          text,
    thumbnail_path      text,
    subtitle_path       text,
    duration_seconds    integer,
    like_count          bigint,
    comment_count       bigint,
    tags                text[],
    categories          text[],
    chapters            jsonb,
    width               integer,
    height              integer,
    fps                 real,
    language            text,
    webpage_url         text,
    handle              text,
    downloaded_at       timestamptz,
    info_json_synced_at timestamptz,
    r2_synced_at        timestamptz,
    source_tags         text[]      NOT NULL DEFAULT '{}'::text[],
    sync_tier           text,
    storage_bytes       bigint,
    score               real        DEFAULT 0
);

CREATE INDEX idx_videos_channel    ON pradotube.videos (channel_id, published_at DESC);
CREATE INDEX idx_videos_downloaded ON pradotube.videos (is_downloaded) WHERE (is_downloaded = true);
CREATE INDEX idx_videos_r2_pending ON pradotube.videos (is_downloaded) WHERE (r2_synced_at IS NULL AND is_downloaded = true);
CREATE INDEX idx_videos_r2_synced  ON pradotube.videos (r2_synced_at) WHERE (r2_synced_at IS NOT NULL);
CREATE INDEX idx_videos_source_tags ON pradotube.videos USING gin (source_tags);
CREATE INDEX idx_videos_sync_tier  ON pradotube.videos (channel_id, sync_tier) WHERE (r2_synced_at IS NOT NULL);

-- ── sync_queue ──────────────────────────────────────────────────────────────
CREATE TABLE pradotube.sync_queue (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id     text        NOT NULL,
    channel_id   text        NOT NULL,
    status       text        NOT NULL DEFAULT 'pending',
    metadata     jsonb,
    error        text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    started_at   timestamptz,
    attempts     integer     NOT NULL DEFAULT 0,
    storage_bytes bigint,
    score        numeric,
    published_at timestamptz
);

CREATE INDEX idx_sync_queue_status_created ON pradotube.sync_queue (status, created_at);
CREATE INDEX idx_sync_queue_claim          ON pradotube.sync_queue (channel_id, status, attempts) INCLUDE (published_at, score);

-- ── profiles ────────────────────────────────────────────────────────────────
CREATE TABLE pradotube.profiles (
    user_id      uuid        PRIMARY KEY,
    role         text        NOT NULL DEFAULT 'member',
    display_name text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- ── user_subscriptions ──────────────────────────────────────────────────────
CREATE TABLE pradotube.user_subscriptions (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    uuid        NOT NULL,
    creator_id uuid        NOT NULL REFERENCES pradotube.creators(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, creator_id)
);

CREATE INDEX idx_user_subscriptions_user    ON pradotube.user_subscriptions (user_id);
CREATE INDEX idx_user_subscriptions_creator ON pradotube.user_subscriptions (creator_id);

-- ── watch_sessions ──────────────────────────────────────────────────────────
CREATE TABLE pradotube.watch_sessions (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          uuid        NOT NULL,
    video_id         text        NOT NULL,
    watched_ranges   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    unique_seconds   integer     NOT NULL DEFAULT 0,
    total_watch_time integer     NOT NULL DEFAULT 0,
    duration_seconds integer,
    last_position    integer     NOT NULL DEFAULT 0,
    completed        boolean     NOT NULL DEFAULT false,
    source           text        NOT NULL DEFAULT 'feed',
    previous_video_id text,
    session_start    timestamptz NOT NULL DEFAULT now(),
    session_end      timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_watch_sessions_user_video      ON pradotube.watch_sessions (user_id, video_id);
CREATE INDEX idx_watch_sessions_user_recent     ON pradotube.watch_sessions (user_id, session_start DESC);
CREATE INDEX idx_watch_sessions_user_incomplete ON pradotube.watch_sessions (user_id) WHERE (completed = false AND last_position > 0);

COMMIT;
