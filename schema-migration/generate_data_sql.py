"""Generate 04_data_small_tables.sql from the current Supabase database.

Usage:
    python generate_data_sql.py --database-url "$DATABASE_URL" > 04_data_small_tables.sql

Exports all tables except `videos` (which uses the separate export_import_videos.py).
Outputs INSERT statements targeting the pradotube schema.
"""

import argparse
import json
import sys

import psycopg2
import psycopg2.extras


TABLES_IN_ORDER = [
    "channels",
    "creators",
    "curated_channels",
    "channel_calibration",
    "profiles",
    "user_subscriptions",
    "watch_sessions",
]


JSONB_COLUMNS = {
    "watched_ranges", "duration_buckets", "chapters", "metadata",
}


def escape_sql_value(val, col_name=""):
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, dict):
        return "'" + json.dumps(val).replace("'", "''") + "'::jsonb"
    if isinstance(val, list):
        if col_name in JSONB_COLUMNS:
            return "'" + json.dumps(val).replace("'", "''") + "'::jsonb"
        if len(val) == 0:
            return "'{}'::text[]"
        inner = ",".join(
            '"' + str(v).replace("\\", "\\\\").replace('"', '\\"') + '"' for v in val
        )
        return "'{" + inner + "}'::text[]"
    s = str(val).replace("'", "''")
    return f"'{s}'"


def generate_inserts(cur, table_name, schema="public"):
    cur.execute(f"SELECT row_to_json(t) FROM {schema}.{table_name} t")
    rows = [r[0] for r in cur.fetchall()]
    if not rows:
        return f"-- {table_name}: no data\n"

    columns = list(rows[0].keys())
    col_list = ", ".join(columns)
    lines = [f"-- {table_name}: {len(rows)} rows"]
    lines.append(f"INSERT INTO pradotube.{table_name} ({col_list}) VALUES")

    value_rows = []
    for row in rows:
        vals = ", ".join(escape_sql_value(row[c], c) for c in columns)
        value_rows.append(f"  ({vals})")

    lines.append(",\n".join(value_rows) + ";")
    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--schema", default="public")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    conn = psycopg2.connect(args.database_url)
    cur = conn.cursor()

    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        out.write("-- ============================================================================\n")
        out.write("-- PradoTube Schema Migration: 04_data_small_tables.sql\n")
        out.write("-- Auto-generated data dump for all tables except videos.\n")
        out.write("-- ============================================================================\n")
        out.write("\n")
        out.write("BEGIN;\n")
        out.write("\n")

        for table in TABLES_IN_ORDER:
            out.write(generate_inserts(cur, table, args.schema))
            out.write("\n")

        out.write("COMMIT;\n")
    finally:
        if args.output:
            out.close()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
