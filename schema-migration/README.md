# PradoTube Schema Migration

Migrates the Grove Sync database from the `public` schema on a dedicated
Supabase project to the `pradotube` schema on a shared Supabase project.

## Files

| File | Purpose |
|------|---------|
| `01_schema.sql` | Creates `pradotube` schema, tables, indexes, foreign keys |
| `02_functions.sql` | All 15 PL/pgSQL functions, scoped to `pradotube` |
| `03_policies.sql` | Enables RLS, creates all 28 policies, grants for Supabase roles |
| `04_data_small_tables.sql` | INSERT statements for all tables except `videos` (small tables) |
| `export_import_videos.py` | Python script to export `videos` from old DB → import to new DB |

## Migration Steps

### 1. Prepare the new Supabase project

In the new project's Supabase Dashboard:
- Go to **Settings → API → Exposed schemas** and add `pradotube`
- This tells PostgREST to serve tables/RPCs from the `pradotube` schema

### 2. Run schema DDL

Connect to the new project's database (via `psql` or the SQL Editor in Dashboard)
and run the scripts **in order**:

```bash
psql "$NEW_DATABASE_URL" -f 01_schema.sql
psql "$NEW_DATABASE_URL" -f 02_functions.sql
psql "$NEW_DATABASE_URL" -f 03_policies.sql
```

### 3. Load small-table data

```bash
psql "$NEW_DATABASE_URL" -f 04_data_small_tables.sql
```

### 4. Migrate videos data

The `videos` table has ~7,700 rows, too large for inline SQL. Use the helper script:

```bash
# Export from old DB
python export_import_videos.py export \
  --database-url "$OLD_DATABASE_URL" \
  --schema public \
  --output videos_export.json

# Import to new DB
python export_import_videos.py import \
  --database-url "$NEW_DATABASE_URL" \
  --schema pradotube \
  --input videos_export.json
```

Requires `psycopg2` (or `psycopg2-binary`):
```bash
pip install psycopg2-binary
```

### 5. Register the Auth Hook

In the new project's Dashboard → **Auth → Hooks**:
- Enable the "Custom Access Token" hook
- Point it to `pradotube.custom_access_token_hook`

### 6. Update application configuration

#### Environment variables (`.env`)

Update these to the new project's values:
- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`
- `SUPABASE_SECRET_KEY`
- `DATABASE_URL`

R2 and YouTube keys stay the same (unless you're changing those too).

#### Python client — schema option

In the code that creates the Supabase client, pass the schema:

```python
from supabase import create_client, ClientOptions

client = create_client(
    url,
    key,
    options=ClientOptions(schema="pradotube"),
)
```

All `.table()` and `.rpc()` calls will then route to `pradotube` automatically.

#### Frontend client (if any)

```typescript
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(url, key, {
  db: { schema: 'pradotube' },
})
```

### 7. Re-create user accounts

Supabase Auth users live in `auth.users` and are project-scoped, so
they won't carry over automatically. You have 3 profiles:

| user_id | role | display_name |
|---------|------|--------------|
| `382216bc-...` | admin | Jose |
| `84fbc142-...` | member | Luis |
| `5818b086-...` | member | Ellie |

Options:
- Have each user sign up again on the new project (new UUIDs; update
  `profiles`, `user_subscriptions`, `watch_sessions` accordingly)
- Use the Supabase Admin API to create users with specific UUIDs to
  preserve the FK references in the exported data

### Notes

- The `rls_auto_enable` event trigger is included in `02_functions.sql`,
  updated to fire on the `pradotube` schema.
- The `custom_access_token_hook` references `pradotube.profiles` (updated).
- Functions use `SET search_path = pradotube` so unqualified table names
  resolve correctly.
- The `sync_queue` table is exported empty (transient job data).
