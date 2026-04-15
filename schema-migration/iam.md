# IAM: Shared Identity & Access Management

## Overview

A centralized, AD-style group membership system for a shared Supabase project. Multiple apps share one Supabase project (and one `auth.users` table). Each app needs its own access control, but group management is centralized in a shared `iam` schema.

The model is intentionally flat: users belong to groups, the JWT carries all group memberships, and each app checks for the specific group names it recognizes. No app registry, no role indirection, no namespacing. This mirrors how Active Directory security groups work.

## Schema

Two tables in a dedicated `iam` schema:

```sql
CREATE SCHEMA iam;

CREATE TABLE iam.groups (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL UNIQUE,
    description text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE iam.group_members (
    group_id uuid NOT NULL REFERENCES iam.groups(id) ON DELETE CASCADE,
    user_id uuid NOT NULL,
    granted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);

CREATE INDEX idx_group_members_user ON iam.group_members (user_id);
```

## Group Naming Convention

Groups are named `{app}-{role}` by convention. The name is the full identity of the group; there is no separate app or role table.

Examples:
- `pradotube-admin` — Full admin access in PradoTube
- `pradotube-member` — Regular member in PradoTube
- `familyhub-admin` — Admin in a hypothetical second app
- `global-admin` — Cross-app superadmin (if ever needed)

Apps only check for the group names they recognize and ignore everything else.

## Auth Hook

Supabase allows one custom access token hook per project. This hook fires every time a JWT is issued (login, token refresh). It reads the user's group memberships from `iam` and injects them as a flat array into `app_metadata.groups`.

```sql
CREATE OR REPLACE FUNCTION iam.access_token_hook(event jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = iam
AS $$
DECLARE
    claims jsonb;
    user_uuid uuid;
    user_groups jsonb;
BEGIN
    claims := event->'claims';
    user_uuid := (event->>'user_id')::uuid;

    SELECT COALESCE(jsonb_agg(g.name), '[]'::jsonb)
    INTO user_groups
    FROM group_members gm
    JOIN groups g ON g.id = gm.group_id
    WHERE gm.user_id = user_uuid;

    claims := jsonb_set(
        claims,
        '{app_metadata}',
        COALESCE(claims->'app_metadata', '{}'::jsonb)
    );
    claims := jsonb_set(claims, '{app_metadata,groups}', user_groups);

    RETURN jsonb_set(event, '{claims}', claims);
END;
$$;
```

### Hook grants

The hook runs as `supabase_auth_admin`. It needs:

```sql
GRANT USAGE ON SCHEMA iam TO supabase_auth_admin;
GRANT SELECT ON iam.groups TO supabase_auth_admin;
GRANT SELECT ON iam.group_members TO supabase_auth_admin;
GRANT EXECUTE ON FUNCTION iam.access_token_hook TO supabase_auth_admin;

-- Prevent direct invocation by end users
REVOKE EXECUTE ON FUNCTION iam.access_token_hook FROM authenticated, anon, public;
```

### Hook registration

In the Supabase Dashboard: **Authentication > Hooks > Custom Access Token (JWT)**
- Enable the hook
- Schema: `iam`
- Function: `access_token_hook`

## Resulting JWT

After the hook runs, a user's decoded JWT looks like:

```json
{
  "sub": "382216bc-55c8-42a1-aad3-93933987fd75",
  "email": "jose@example.com",
  "role": "authenticated",
  "app_metadata": {
    "groups": ["pradotube-admin", "global-admin"]
  }
}
```

A user with no group memberships gets an empty array:

```json
{
  "app_metadata": {
    "groups": []
  }
}
```

## How Apps Consume Groups

Each app's RLS policies use the `?` (contains) operator to check for specific group names in the JWT array:

```sql
-- Check if user is a pradotube admin
(auth.jwt() -> 'app_metadata' -> 'groups') ? 'pradotube-admin'
```

A helper function per app is optional but keeps policies readable:

```sql
CREATE OR REPLACE FUNCTION pradotube.is_app_admin()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pradotube
AS $$
    SELECT COALESCE(
        (auth.jwt() -> 'app_metadata' -> 'groups') ? 'pradotube-admin',
        false
    );
$$;
```

Then policies become:

```sql
CREATE POLICY channels_insert_admin ON pradotube.channels
    FOR INSERT TO authenticated
    WITH CHECK (pradotube.is_app_admin());
```

## Migration from Current State

PradoTube currently stores roles in `pradotube.profiles.role`. To migrate:

### 1. Seed the groups

```sql
INSERT INTO iam.groups (name, description) VALUES
    ('pradotube-admin', 'Full admin access in PradoTube'),
    ('pradotube-member', 'Regular member in PradoTube');
```

### 2. Migrate existing role assignments

```sql
INSERT INTO iam.group_members (group_id, user_id)
SELECT g.id, p.user_id
FROM pradotube.profiles p
JOIN iam.groups g ON g.name = 'pradotube-' || p.role;
```

This maps `profiles.role = 'admin'` to the `pradotube-admin` group and `profiles.role = 'member'` to `pradotube-member`.

### 3. Update PradoTube RLS policies

All 28 policies change from:

```sql
((auth.jwt() -> 'app_metadata') ->> 'role') = 'admin'
```

to:

```sql
(auth.jwt() -> 'app_metadata' -> 'groups') ? 'pradotube-admin'
```

### 4. Remove the old hook

The current `pradotube.custom_access_token_hook` is replaced by `iam.access_token_hook`. The old function can be dropped after the new hook is registered.

### 5. Drop the role column

The `role` column on `pradotube.profiles` is no longer needed. The `profiles` table itself stays (it holds `display_name` and `created_at`), but `role` can be removed:

```sql
ALTER TABLE pradotube.profiles DROP COLUMN role;
```

## IAM Schema RLS and Access

The `iam` tables need their own access policies. Recommended:

```sql
ALTER TABLE iam.groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE iam.group_members ENABLE ROW LEVEL SECURITY;

-- Everyone can read groups (for UI display)
CREATE POLICY groups_select ON iam.groups
    FOR SELECT TO authenticated USING (true);

-- Only global or app admins can manage groups (adjust as needed)
CREATE POLICY groups_manage ON iam.groups
    FOR ALL TO authenticated
    USING ((auth.jwt() -> 'app_metadata' -> 'groups') ? 'global-admin');

-- Users can see their own memberships
CREATE POLICY members_select_own ON iam.group_members
    FOR SELECT TO authenticated
    USING (auth.uid() = user_id);

-- Admins can see and manage all memberships
CREATE POLICY members_manage ON iam.group_members
    FOR ALL TO authenticated
    USING ((auth.jwt() -> 'app_metadata' -> 'groups') ? 'global-admin');
```

Grant schema and table access to Supabase roles:

```sql
GRANT USAGE ON SCHEMA iam TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA iam TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA iam TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA iam
    GRANT ALL ON TABLES TO anon, authenticated, service_role;
```

## Summary

| Concern | Approach |
|---------|----------|
| Where roles live | `iam.groups` + `iam.group_members` |
| How they reach the client | Single auth hook injects flat `groups` array into JWT |
| How apps enforce access | RLS policies check `(jwt -> 'app_metadata' -> 'groups') ? 'group-name'` |
| Group naming | `{app}-{role}` by convention, no enforced structure |
| Multi-app isolation | Each app only checks its own group names; ignores the rest |
| Management | Direct table operations via service_role or a future admin UI |
