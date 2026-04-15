-- ============================================================================
-- PradoTube Schema Migration: 03_policies.sql
-- Enables RLS on all tables, creates all policies, and grants access
-- to Supabase roles (anon, authenticated, service_role).
-- ============================================================================

BEGIN;

-- ── Enable RLS ──────────────────────────────────────────────────────────────
ALTER TABLE pradotube.channel_calibration ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.channels            ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.creators            ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.curated_channels    ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.profiles            ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.sync_queue          ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.user_subscriptions  ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.videos              ENABLE ROW LEVEL SECURITY;
ALTER TABLE pradotube.watch_sessions      ENABLE ROW LEVEL SECURITY;

-- ── Grant schema usage ──────────────────────────────────────────────────────
GRANT USAGE ON SCHEMA pradotube TO anon, authenticated, service_role;

-- ── Grant table access ──────────────────────────────────────────────────────
-- service_role bypasses RLS, but still needs table grants
GRANT ALL ON ALL TABLES IN SCHEMA pradotube TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA pradotube TO anon, authenticated, service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pradotube TO anon, authenticated, service_role;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA pradotube
  GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA pradotube
  GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA pradotube
  GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;

-- ── channels policies ───────────────────────────────────────────────────────
CREATE POLICY channels_select_authed ON pradotube.channels
    FOR SELECT TO authenticated USING (true);

CREATE POLICY channels_insert_admin ON pradotube.channels
    FOR INSERT TO authenticated
    WITH CHECK (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY channels_update_admin ON pradotube.channels
    FOR UPDATE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY channels_delete_admin ON pradotube.channels
    FOR DELETE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

-- ── creators policies ───────────────────────────────────────────────────────
CREATE POLICY creators_select_authed ON pradotube.creators
    FOR SELECT TO authenticated USING (true);

CREATE POLICY creators_insert_admin ON pradotube.creators
    FOR INSERT TO authenticated
    WITH CHECK (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY creators_update_admin ON pradotube.creators
    FOR UPDATE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY creators_delete_admin ON pradotube.creators
    FOR DELETE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

-- ── curated_channels policies ───────────────────────────────────────────────
CREATE POLICY curated_channels_select_authed ON pradotube.curated_channels
    FOR SELECT TO authenticated USING (true);

CREATE POLICY curated_channels_insert_admin ON pradotube.curated_channels
    FOR INSERT TO authenticated
    WITH CHECK (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY curated_channels_update_admin ON pradotube.curated_channels
    FOR UPDATE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY curated_channels_delete_admin ON pradotube.curated_channels
    FOR DELETE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

-- ── videos policies ─────────────────────────────────────────────────────────
CREATE POLICY videos_select_authed ON pradotube.videos
    FOR SELECT TO authenticated USING (true);

CREATE POLICY videos_insert_admin ON pradotube.videos
    FOR INSERT TO authenticated
    WITH CHECK (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY videos_update_admin ON pradotube.videos
    FOR UPDATE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY videos_delete_admin ON pradotube.videos
    FOR DELETE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

-- ── profiles policies ───────────────────────────────────────────────────────
CREATE POLICY profiles_select_own ON pradotube.profiles
    FOR SELECT TO authenticated
    USING (auth.uid() = user_id);

CREATE POLICY profiles_select_admin ON pradotube.profiles
    FOR SELECT TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY profiles_insert_admin ON pradotube.profiles
    FOR INSERT TO authenticated
    WITH CHECK (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY profiles_update_admin ON pradotube.profiles
    FOR UPDATE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

-- ── sync_queue policies ─────────────────────────────────────────────────────
CREATE POLICY "Allow all access to sync_queue" ON pradotube.sync_queue
    FOR ALL TO public
    USING (true)
    WITH CHECK (true);

-- ── user_subscriptions policies ─────────────────────────────────────────────
CREATE POLICY subscriptions_select_own ON pradotube.user_subscriptions
    FOR SELECT TO authenticated
    USING (auth.uid() = user_id);

CREATE POLICY subscriptions_select_admin ON pradotube.user_subscriptions
    FOR SELECT TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY subscriptions_insert_admin ON pradotube.user_subscriptions
    FOR INSERT TO authenticated
    WITH CHECK (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

CREATE POLICY subscriptions_delete_admin ON pradotube.user_subscriptions
    FOR DELETE TO authenticated
    USING (((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin');

-- ── watch_sessions policies ─────────────────────────────────────────────────
CREATE POLICY "Users can read their own sessions" ON pradotube.watch_sessions
    FOR SELECT TO public
    USING (auth.uid() = user_id);

CREATE POLICY "Users can insert their own sessions" ON pradotube.watch_sessions
    FOR INSERT TO public
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update their own sessions" ON pradotube.watch_sessions
    FOR UPDATE TO public
    USING (auth.uid() = user_id);

COMMIT;
