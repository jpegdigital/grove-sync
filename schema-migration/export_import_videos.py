"""Export/import the videos table between Supabase projects.

Usage:
    # Export from old DB (public schema)
    python export_import_videos.py export \
        --database-url "$OLD_DATABASE_URL" \
        --schema public \
        --output videos_export.json

    # Import to new DB (pradotube schema)
    python export_import_videos.py import \
        --database-url "$NEW_DATABASE_URL" \
        --schema pradotube \
        --input videos_export.json

Requires: psycopg2-binary
"""

import argparse
import json
import sys

import psycopg2
import psycopg2.extras


BATCH_SIZE = 500


def export_videos(database_url, schema, output_path):
    conn = psycopg2.connect(database_url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(f"SELECT * FROM {schema}.videos ORDER BY youtube_id")
    rows = cur.fetchall()

    serializable = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        serializable.append(d)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=None)

    print(f"Exported {len(serializable)} videos to {output_path}")
    cur.close()
    conn.close()


def import_videos(database_url, schema, input_path):
    with open(input_path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not rows:
        print("No rows to import")
        return

    columns = list(rows[0].keys())
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict_col = "youtube_id"

    update_cols = [c for c in columns if c != conflict_col]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = (
        f"INSERT INTO {schema}.videos ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_col}) DO UPDATE SET {update_set}"
    )

    conn = psycopg2.connect(database_url)
    cur = conn.cursor()

    imported = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        values = [tuple(row[c] for c in columns) for row in batch]
        cur.executemany(sql, values)
        imported += len(batch)
        print(f"  Imported {imported}/{len(rows)} videos...")

    conn.commit()
    print(f"Successfully imported {imported} videos into {schema}.videos")
    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Export/import videos table")
    sub = parser.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export")
    exp.add_argument("--database-url", required=True)
    exp.add_argument("--schema", default="public")
    exp.add_argument("--output", required=True)

    imp = sub.add_parser("import")
    imp.add_argument("--database-url", required=True)
    imp.add_argument("--schema", default="pradotube")
    imp.add_argument("--input", required=True)

    args = parser.parse_args()

    if args.command == "export":
        export_videos(args.database_url, args.schema, args.output)
    elif args.command == "import":
        import_videos(args.database_url, args.schema, args.input)


if __name__ == "__main__":
    main()
