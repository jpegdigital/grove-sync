-- ============================================================================
-- PradoTube Schema Migration: 02_functions.sql
-- All functions scoped to the pradotube schema with search_path set.
-- ============================================================================

BEGIN;

-- ── merge_ranges ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.merge_ranges(p_ranges jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
DECLARE
    sorted JSONB;
    merged JSONB := '[]'::jsonb;
    current_start INT;
    current_end   INT;
    r JSONB;
    s INT;
    e INT;
BEGIN
    SELECT jsonb_agg(elem ORDER BY (elem->0)::int)
    INTO sorted
    FROM jsonb_array_elements(p_ranges) AS elem;

    IF sorted IS NULL OR jsonb_array_length(sorted) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    current_start := (sorted->0->0)::int;
    current_end   := (sorted->0->1)::int;

    FOR i IN 1..jsonb_array_length(sorted) - 1 LOOP
        r := sorted->i;
        s := (r->0)::int;
        e := (r->1)::int;
        IF s <= current_end + 1 THEN
            current_end := GREATEST(current_end, e);
        ELSE
            merged := merged || jsonb_build_array(jsonb_build_array(current_start, current_end));
            current_start := s;
            current_end   := e;
        END IF;
    END LOOP;

    merged := merged || jsonb_build_array(jsonb_build_array(current_start, current_end));
    RETURN merged;
END;
$$;

-- ── compute_unique_seconds ──────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.compute_unique_seconds(p_ranges jsonb)
RETURNS integer
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
DECLARE
    total INT := 0;
    r JSONB;
BEGIN
    FOR r IN SELECT * FROM jsonb_array_elements(p_ranges) LOOP
        total := total + ((r->1)::int - (r->0)::int);
    END LOOP;
    RETURN total;
END;
$$;

-- ── claim_next_job ──────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.claim_next_job(
    p_channel_id text,
    p_max_attempts integer,
    p_sort_key text DEFAULT 'published_at'
)
RETURNS SETOF pradotube.sync_queue
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
BEGIN
  RETURN QUERY
  UPDATE sync_queue
  SET status = 'processing', started_at = now()
  WHERE id = (
    SELECT id FROM sync_queue
    WHERE channel_id = p_channel_id
      AND status = 'pending'
      AND attempts < p_max_attempts
    ORDER BY
      CASE WHEN p_sort_key = 'score' THEN score END DESC NULLS LAST,
      CASE WHEN p_sort_key != 'score' THEN published_at END DESC NULLS LAST
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  )
  RETURNING *;
END;
$$;

-- ── fail_job_atomic ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.fail_job_atomic(p_job_id uuid, p_error text)
RETURNS void
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
BEGIN
  UPDATE sync_queue
  SET status = 'pending',
      started_at = NULL,
      attempts = attempts + 1,
      error = left(p_error, 1000)
  WHERE id = p_job_id;
END;
$$;

-- ── is_admin ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.is_admin()
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pradotube
AS $$
  SELECT EXISTS (
    SELECT 1 FROM pradotube.profiles
    WHERE user_id = auth.uid() AND role = 'admin'
  );
$$;

-- ── custom_access_token_hook ────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.custom_access_token_hook(event jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pradotube
AS $$
DECLARE
  claims JSONB;
  user_role TEXT;
BEGIN
  claims := event->'claims';

  SELECT role INTO user_role
  FROM pradotube.profiles
  WHERE user_id = (event->>'user_id')::UUID;

  IF user_role IS NULL THEN
    user_role := 'member';
  END IF;

  claims := jsonb_set(
    claims,
    '{app_metadata}',
    COALESCE(claims->'app_metadata', '{}'::JSONB)
  );
  claims := jsonb_set(
    claims,
    '{app_metadata,role}',
    to_jsonb(user_role)
  );

  event := jsonb_set(event, '{claims}', claims);
  RETURN event;
END;
$$;

-- Grant execute on the hook to supabase_auth_admin (required for auth hooks)
GRANT USAGE ON SCHEMA pradotube TO supabase_auth_admin;
GRANT EXECUTE ON FUNCTION pradotube.custom_access_token_hook TO supabase_auth_admin;
REVOKE EXECUTE ON FUNCTION pradotube.custom_access_token_hook FROM authenticated, anon, public;

-- ── get_distinct_video_channel_ids ──────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.get_distinct_video_channel_ids()
RETURNS TABLE(channel_id text)
LANGUAGE sql
SET search_path = pradotube
AS $$
    SELECT DISTINCT v.channel_id
    FROM videos v
    WHERE v.channel_id IS NOT NULL;
$$;

-- ── video_counts_by_channel ─────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.video_counts_by_channel()
RETURNS TABLE(channel_id text, downloaded bigint, uploaded bigint)
LANGUAGE sql
SET search_path = pradotube
AS $$
  SELECT v.channel_id,
    COUNT(*) FILTER (WHERE v.is_downloaded = true) AS downloaded,
    COUNT(*) FILTER (WHERE v.r2_synced_at IS NOT NULL) AS uploaded
  FROM videos v
  WHERE v.channel_id IS NOT NULL
  GROUP BY v.channel_id;
$$;

-- ── feed_for_user ───────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.feed_for_user(
    p_user_id uuid,
    p_limit integer DEFAULT 50,
    p_offset integer DEFAULT 0,
    p_creator_id uuid DEFAULT NULL,
    p_search_text text DEFAULT NULL
)
RETURNS TABLE(
    video_id text, title text, thumbnail_url text, media_path text,
    duration_seconds integer, published_at timestamptz, channel_title text,
    creator_id uuid, creator_name text, creator_slug text,
    creator_avatar_url text, creator_priority integer,
    view_count bigint, like_count bigint, comment_count bigint,
    width integer, height integer, fps real, tags text[],
    score double precision
)
LANGUAGE sql
SET search_path = pradotube
AS $$
  WITH subscribed_videos AS (
    SELECT
      v.youtube_id,
      v.title,
      v.thumbnail_url        AS video_thumbnail_url,
      v.media_path,
      v.duration_seconds,
      v.published_at,
      ch.title                AS channel_title,
      cr.id                   AS cr_id,
      cr.name                 AS cr_name,
      cr.slug                 AS cr_slug,
      av_ch.thumbnail_url     AS cr_avatar_url,
      cr.priority             AS creator_priority,
      cc.priority             AS channel_priority,
      v.view_count,
      v.like_count,
      v.comment_count,
      v.width,
      v.height,
      v.fps,
      v.tags,
      ROW_NUMBER() OVER (PARTITION BY v.channel_id ORDER BY v.published_at DESC) AS recency_rank,
      COUNT(*)     OVER (PARTITION BY v.channel_id)                              AS channel_total,
      CASE WHEN v.published_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1.0 ELSE 0.0 END AS freshness
    FROM user_subscriptions us
    JOIN creators cr           ON cr.id = us.creator_id
    JOIN curated_channels cc   ON cc.creator_id = cr.id
    JOIN videos v              ON v.channel_id = cc.channel_id
    JOIN channels ch           ON ch.youtube_id = v.channel_id
    LEFT JOIN channels av_ch   ON av_ch.youtube_id = cr.avatar_channel_id
    WHERE us.user_id = p_user_id
      AND v.r2_synced_at IS NOT NULL
      AND (p_creator_id IS NULL OR cr.id = p_creator_id)
      AND (p_search_text IS NULL OR p_search_text = '' OR (
        v.title ILIKE '%' || p_search_text || '%'
        OR cr.name ILIKE '%' || p_search_text || '%'
        OR ch.title ILIKE '%' || p_search_text || '%'
        OR EXISTS (SELECT 1 FROM unnest(v.tags) t WHERE t ILIKE '%' || p_search_text || '%')
      ))
  ),
  scored AS (
    SELECT
      sv.*,
      CASE
        WHEN sv.channel_total <= 1 THEN 1.0
        ELSE 1.0 - ((sv.recency_rank - 1)::double precision / (sv.channel_total - 1)::double precision)
      END AS relative_recency,
      (sv.channel_priority::double precision / 100.0) * (sv.creator_priority::double precision / 100.0) AS priority_score,
      (('x' || substr(md5(to_char(CURRENT_DATE, 'YYYY-MM-DD') || ':' || sv.youtube_id), 1, 8))::bit(32)::bigint
        & x'7FFFFFFF'::bigint)::double precision / 2147483647.0 AS jitter
    FROM subscribed_videos sv
  ),
  final_scored AS (
    SELECT
      s.*,
      0.3 * s.relative_recency
        + 0.5 * s.priority_score
        + 0.1 * s.jitter
        + 0.1 * s.freshness AS final_score
    FROM scored s
  ),
  diversified AS (
    SELECT
      fs.*,
      ROW_NUMBER() OVER (PARTITION BY fs.cr_id ORDER BY fs.final_score DESC) AS creator_rank
    FROM final_scored fs
  )
  SELECT
    d.youtube_id    AS video_id,
    d.title,
    d.video_thumbnail_url AS thumbnail_url,
    d.media_path,
    d.duration_seconds,
    d.published_at,
    d.channel_title,
    d.cr_id         AS creator_id,
    d.cr_name       AS creator_name,
    d.cr_slug       AS creator_slug,
    d.cr_avatar_url AS creator_avatar_url,
    d.creator_priority,
    d.view_count,
    d.like_count,
    d.comment_count,
    d.width,
    d.height,
    d.fps,
    d.tags,
    d.final_score   AS score
  FROM diversified d
  ORDER BY
    CASE WHEN p_creator_id IS NULL THEN d.creator_rank ELSE 1 END ASC,
    d.final_score DESC
  LIMIT p_limit
  OFFSET p_offset;
$$;

-- ── search_videos ───────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.search_videos(
    p_query text,
    p_limit integer DEFAULT 20
)
RETURNS TABLE(
    video_id text, title text, thumbnail_url text, media_path text,
    duration_seconds integer, published_at timestamptz, channel_title text,
    creator_id uuid, creator_name text, creator_slug text,
    creator_avatar_url text, creator_priority integer,
    view_count bigint, like_count bigint, comment_count bigint,
    width integer, height integer, fps real, tags text[],
    score double precision
)
LANGUAGE sql
SET search_path = pradotube
AS $$
  SELECT
    v.youtube_id      AS video_id,
    v.title,
    v.thumbnail_url,
    v.media_path,
    v.duration_seconds,
    v.published_at,
    ch.title          AS channel_title,
    cr.id             AS creator_id,
    cr.name           AS creator_name,
    cr.slug           AS creator_slug,
    av_ch.thumbnail_url AS creator_avatar_url,
    cr.priority       AS creator_priority,
    v.view_count,
    v.like_count,
    v.comment_count,
    v.width,
    v.height,
    v.fps,
    v.tags,
    (CASE
      WHEN v.title ILIKE p_query || '%' THEN 1.0
      WHEN v.title ILIKE '%' || p_query || '%' THEN 0.8
      ELSE 0.5
    END * (cr.priority::double precision / 100.0)) AS score
  FROM videos v
  JOIN channels ch           ON ch.youtube_id = v.channel_id
  JOIN curated_channels cc   ON cc.channel_id = v.channel_id
  JOIN creators cr           ON cr.id = cc.creator_id
  LEFT JOIN channels av_ch   ON av_ch.youtube_id = cr.avatar_channel_id
  WHERE v.r2_synced_at IS NOT NULL
    AND (
      v.title ILIKE '%' || p_query || '%'
      OR EXISTS (
        SELECT 1 FROM unnest(v.tags) t
        WHERE t ILIKE '%' || p_query || '%'
      )
    )
  ORDER BY score DESC, v.published_at DESC
  LIMIT p_limit;
$$;

-- ── search_creators ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.search_creators(
    p_query text,
    p_limit integer DEFAULT 10
)
RETURNS TABLE(id uuid, name text, slug text, avatar_url text, priority integer)
LANGUAGE sql
SET search_path = pradotube
AS $$
  SELECT
    cr.id,
    cr.name,
    cr.slug,
    av_ch.thumbnail_url AS avatar_url,
    cr.priority
  FROM creators cr
  LEFT JOIN channels av_ch ON av_ch.youtube_id = cr.avatar_channel_id
  WHERE
    p_query = '' OR
    cr.name ILIKE '%' || p_query || '%' OR
    cr.slug ILIKE '%' || p_query || '%'
  ORDER BY
    CASE WHEN p_query != '' AND cr.sort_name ILIKE p_query || '%' THEN 0 ELSE 1 END,
    cr.priority DESC,
    cr.sort_name
  LIMIT p_limit;
$$;

-- ── upsert_watch_heartbeat ──────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.upsert_watch_heartbeat(
    p_user_id uuid,
    p_video_id text,
    p_session_id uuid DEFAULT NULL,
    p_new_range jsonb DEFAULT NULL,
    p_elapsed integer DEFAULT 0,
    p_position integer DEFAULT 0,
    p_duration integer DEFAULT NULL,
    p_source text DEFAULT 'feed',
    p_previous_video_id text DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
DECLARE
    v_session_id UUID;
    v_ranges     JSONB;
    v_unique     INT;
    v_total      INT;
    v_completed  BOOLEAN;
    v_duration   INT;
BEGIN
    IF p_session_id IS NULL THEN
        INSERT INTO watch_sessions (user_id, video_id, duration_seconds, source, previous_video_id)
        VALUES (p_user_id, p_video_id, p_duration, p_source, p_previous_video_id)
        RETURNING id INTO v_session_id;
    ELSE
        v_session_id := p_session_id;
    END IF;

    SELECT watched_ranges, total_watch_time, duration_seconds
    INTO v_ranges, v_total, v_duration
    FROM watch_sessions
    WHERE id = v_session_id;

    IF v_duration IS NULL AND p_duration IS NOT NULL THEN
        v_duration := p_duration;
    END IF;

    IF p_new_range IS NOT NULL AND jsonb_typeof(p_new_range) = 'array' AND jsonb_array_length(p_new_range) = 2 THEN
        v_ranges := merge_ranges(v_ranges || jsonb_build_array(p_new_range));
    END IF;

    v_unique := compute_unique_seconds(v_ranges);
    v_completed := CASE
        WHEN v_duration IS NOT NULL AND v_duration > 0
        THEN (v_unique::float / v_duration::float) >= 0.85
        ELSE false
    END;

    UPDATE watch_sessions SET
        watched_ranges   = v_ranges,
        unique_seconds   = v_unique,
        total_watch_time = v_total + COALESCE(p_elapsed, 0),
        last_position    = p_position,
        duration_seconds = COALESCE(v_duration, duration_seconds),
        completed        = v_completed,
        session_end      = now(),
        updated_at       = now()
    WHERE id = v_session_id;

    RETURN v_session_id;
END;
$$;

-- ── continue_watching_for_user ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.continue_watching_for_user(
    p_user_id uuid,
    p_limit integer DEFAULT 10
)
RETURNS TABLE(
    video_id text, title text, thumbnail_url text, media_path text,
    duration_seconds integer, last_position integer, unique_seconds integer,
    coverage_pct double precision, creator_name text, creator_avatar_url text,
    creator_id uuid, session_id uuid, session_start timestamptz
)
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (ws.video_id)
        ws.video_id,
        v.title,
        v.thumbnail_url,
        v.media_path,
        ws.duration_seconds,
        ws.last_position,
        ws.unique_seconds,
        CASE
            WHEN ws.duration_seconds > 0
            THEN ROUND((ws.unique_seconds::float / ws.duration_seconds::float) * 100, 1)
            ELSE 0
        END::FLOAT       AS coverage_pct,
        c.name           AS creator_name,
        c.avatar_url     AS creator_avatar_url,
        c.id             AS creator_id,
        ws.id            AS session_id,
        ws.session_start
    FROM watch_sessions ws
    JOIN videos v   ON v.video_id = ws.video_id
    JOIN creators c ON c.id = v.creator_id
    WHERE ws.user_id = p_user_id
      AND ws.completed = false
      AND ws.last_position > 10
    ORDER BY ws.video_id, ws.session_start DESC
    LIMIT p_limit;
END;
$$;

-- ── watch_history_for_user ──────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pradotube.watch_history_for_user(
    p_user_id uuid,
    p_limit integer DEFAULT 50,
    p_offset integer DEFAULT 0
)
RETURNS TABLE(
    session_id uuid, video_id text, title text, thumbnail_url text,
    creator_name text, creator_avatar_url text,
    duration_seconds integer, unique_seconds integer, total_watch_time integer,
    last_position integer, completed boolean, source text,
    session_start timestamptz, session_end timestamptz,
    coverage_pct double precision, watched_ranges jsonb
)
LANGUAGE plpgsql
SET search_path = pradotube
AS $$
BEGIN
    RETURN QUERY
    WITH latest_per_video AS (
        SELECT DISTINCT ON (ws.video_id)
            ws.id,
            ws.video_id,
            ws.duration_seconds,
            ws.unique_seconds,
            ws.total_watch_time,
            ws.last_position,
            ws.completed,
            ws.source,
            ws.session_start,
            ws.session_end
        FROM watch_sessions ws
        WHERE ws.user_id = p_user_id
        ORDER BY ws.video_id, ws.session_start DESC
    ),
    aggregated_ranges AS (
        SELECT
            ws.video_id,
            jsonb_agg(range_elem) AS all_ranges
        FROM watch_sessions ws,
             jsonb_array_elements(ws.watched_ranges) AS range_elem
        WHERE ws.user_id = p_user_id
        GROUP BY ws.video_id
    )
    SELECT
        lv.id              AS session_id,
        lv.video_id,
        v.title,
        v.thumbnail_url,
        cr.name            AS creator_name,
        av_ch.thumbnail_url AS creator_avatar_url,
        lv.duration_seconds,
        lv.unique_seconds,
        lv.total_watch_time,
        lv.last_position,
        lv.completed,
        lv.source,
        lv.session_start,
        lv.session_end,
        CASE
            WHEN lv.duration_seconds > 0
            THEN ROUND((lv.unique_seconds::numeric / lv.duration_seconds::numeric) * 100, 1)::double precision
            ELSE 0
        END                AS coverage_pct,
        COALESCE(ar.all_ranges, '[]'::jsonb) AS watched_ranges
    FROM latest_per_video lv
    JOIN videos v              ON v.youtube_id = lv.video_id
    JOIN curated_channels cc   ON cc.channel_id = v.channel_id
    JOIN creators cr           ON cr.id = cc.creator_id
    LEFT JOIN channels av_ch   ON av_ch.youtube_id = cr.avatar_channel_id
    LEFT JOIN aggregated_ranges ar ON ar.video_id = lv.video_id
    ORDER BY lv.session_start DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

-- ── rls_auto_enable (event trigger) ─────────────────────────────────────────
-- NOTE: This event trigger auto-enables RLS on new tables.
-- Updated to fire on the pradotube schema instead of public.
CREATE OR REPLACE FUNCTION pradotube.rls_auto_enable()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN
    SELECT *
    FROM pg_event_trigger_ddl_commands()
    WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
      AND object_type IN ('table','partitioned table')
  LOOP
     IF cmd.schema_name IS NOT NULL AND cmd.schema_name IN ('pradotube') AND cmd.schema_name NOT IN ('pg_catalog','information_schema') AND cmd.schema_name NOT LIKE 'pg_toast%' AND cmd.schema_name NOT LIKE 'pg_temp%' THEN
      BEGIN
        EXECUTE format('alter table if exists %s enable row level security', cmd.object_identity);
        RAISE LOG 'rls_auto_enable: enabled RLS on %', cmd.object_identity;
      EXCEPTION
        WHEN OTHERS THEN
          RAISE LOG 'rls_auto_enable: failed to enable RLS on %', cmd.object_identity;
      END;
     ELSE
        RAISE LOG 'rls_auto_enable: skip % (either system schema or not in enforced list: %.)', cmd.object_identity, cmd.schema_name;
     END IF;
  END LOOP;
END;
$$;

-- NOTE: The event trigger itself must be created by a superuser or
-- supabase_admin. If you get a permission error, run this via the
-- Supabase SQL Editor (which runs as supabase_admin):
--
-- CREATE EVENT TRIGGER pradotube_ensure_rls ON ddl_command_end
--   EXECUTE FUNCTION pradotube.rls_auto_enable();

COMMIT;
