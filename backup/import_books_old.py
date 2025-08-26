#!/usr/bin/env python3
"""
Import a CSV of books into PostgreSQL (upsert by ISBN), loading DB settings from a .env file.

CSV expected headers (case-insensitive):
  isbn, title, authors, published_date, publisher, language, pages, categories, shelf, position

Notes:
- Read DB config from --env (default: .env). Supports DATABASE_URL or PG*/DB* variables.
- authors and categories can be semicolon-separated; stored as TEXT[].
- published_date: YYYY-MM-DD, YYYY-MM, or YYYY (month/day default to 1).
- Extra CSV columns go into JSONB 'metadata'.
- Use --create to create the table if it does not exist.
"""

import argparse
import csv
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from dotenv import load_dotenv  # pip install python-dotenv
import psycopg                  # pip install psycopg[binary]
from psycopg import sql
from psycopg.rows import dict_row

EXPECTED_HEADERS = [
    "isbn", "title", "authors", "published_date", "publisher",
    "language", "pages", "categories", "shelf", "position"
]

CREATE_TABLE_SQL_TMPL = sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    isbn           TEXT PRIMARY KEY,
    title          TEXT NOT NULL,
    authors        TEXT[],
    published_date DATE,
    publisher      TEXT,
    language       TEXT,
    pages          INTEGER,
    categories     TEXT[],
    shelf          TEXT,
    position       INTEGER,
    source_file    TEXT,
    metadata       JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
""")

UPSERT_SQL_TMPL = sql.SQL("""
INSERT INTO {table} (
    isbn, title, authors, published_date, publisher, language, pages,
    categories, shelf, position, source_file, metadata
) VALUES (
    %(isbn)s, %(title)s, %(authors)s, %(published_date)s, %(publisher)s, %(language)s, %(pages)s,
    %(categories)s, %(shelf)s, %(position)s, %(source_file)s, %(metadata)s
)
ON CONFLICT (isbn) DO UPDATE SET
    title          = EXCLUDED.title,
    authors        = EXCLUDED.authors,
    published_date = EXCLUDED.published_date,
    publisher      = EXCLUDED.publisher,
    language       = EXCLUDED.language,
    pages          = EXCLUDED.pages,
    categories     = EXCLUDED.categories,
    shelf          = EXCLUDED.shelf,
    position       = EXCLUDED.position,
    source_file    = EXCLUDED.source_file,
    metadata       = COALESCE({table}.metadata, '{{}}'::jsonb) || COALESCE(EXCLUDED.metadata, '{{}}'::jsonb),
    updated_at     = NOW();
""")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import a CSV of books into PostgreSQL (upsert by ISBN).")
    p.add_argument("--csv", required=True, help="Path to the input CSV file.")
    p.add_argument("--table", default="books",
                   help="Destination table name (default: books).")
    p.add_argument("--delimiter", default=",",
                   help="CSV delimiter (default: ,).")
    p.add_argument("--encoding", default="utf-8-sig",
                   help="CSV encoding (default: utf-8-sig).")
    p.add_argument("--create", action="store_true",
                   help="Create the table if it does not exist.")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Batch size for executemany (default: 1000).")
    p.add_argument("--env", default=".env",
                   help="Path to .env file with DB settings (default: .env).")
    return p.parse_args()


def normalize_header(h: str) -> str:
    return h.strip().lower()


def split_list(val: Optional[str]) -> Optional[List[str]]:
    if val is None:
        return None
    s = val.strip()
    if not s:
        return None
    parts = s.split(";") if ";" in s else s.split(",")
    cleaned = [p.strip() for p in parts if p.strip()]
    return cleaned or None


def parse_int(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    s = val.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_date(val: Optional[str]):
    if not val:
        return None
    s = val.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%Y":
                return dt.replace(month=1, day=1).date()
            if fmt == "%Y-%m":
                return dt.replace(day=1).date()
            return dt.date()
        except ValueError:
            continue
    return None


def validate_headers(fieldnames: List[str]) -> Tuple[bool, List[str]]:
    fields = [normalize_header(h) for h in fieldnames]
    missing = [h for h in EXPECTED_HEADERS if h not in fields]
    return (len(missing) == 0, missing)


def build_records(rows: List[Dict[str, str]], csv_path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    src = os.path.basename(csv_path)
    norm_expected = [normalize_header(h) for h in EXPECTED_HEADERS]
    records, errors = [], []

    for i, row in enumerate(rows, start=2):  # header is line 1
        lower_row = {normalize_header(k): v for k, v in row.items()}

        # Extra columns → metadata dict
        extras = {k: v for k, v in lower_row.items()
                  if k not in norm_expected and v not in (None, "",)}

        try:
            rec = {
                "isbn": (lower_row.get("isbn") or "").strip(),
                "title": (lower_row.get("title") or "").strip(),
                "authors": split_list(lower_row.get("authors")),
                "published_date": parse_date(lower_row.get("published_date")),
                "publisher": (lower_row.get("publisher") or None),
                "language": (lower_row.get("language") or None),
                "pages": parse_int(lower_row.get("pages")),
                "categories": split_list(lower_row.get("categories")),
                "shelf": (lower_row.get("shelf") or None),
                "position": parse_int(lower_row.get("position")),
                "source_file": src,
                "metadata": extras or None,  # psycopg3 adapts dict → JSON automatically
            }
            if not rec["isbn"] or not rec["title"]:
                raise ValueError("isbn and title are required")
            records.append(rec)
        except Exception as e:
            errors.append(f"Row {i}: {e}")

    return records, errors


def ensure_table(conn: psycopg.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL_TMPL.format(table=sql.Identifier(table)))
    conn.commit()


def safe_table_name(name: str) -> bool:
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))


def env_get(*names: str, default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


def load_db_config(env_path: str) -> Dict[str, Any]:
    # Load .env (does nothing if file missing)
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    dsn = os.getenv("DATABASE_URL") or os.getenv(
        "POSTGRES_URL") or os.getenv("DB_URL")

    if dsn:
        return {"dsn": dsn}

    host = env_get("PGHOST", "DB_HOST", default="localhost")
    port = int(env_get("PGPORT", "DB_PORT", default="5432"))
    dbname = env_get("PGDATABASE", "DB_NAME")
    user = env_get("PGUSER", "DB_USER")
    password = env_get("PGPASSWORD", "DB_PASSWORD")
    print(user, password)

    missing = [k for k, v in {"PGDATABASE/DB_NAME": dbname,
                              "PGUSER/DB_USER": user, "PGPASSWORD/DB_PASSWORD": password}.items() if not v]
    if missing:
        raise SystemExit(
            "Missing required DB settings in environment. Provide either DATABASE_URL or variables:\n"
            "  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD  (or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD)\n"
            f"Missing: {', '.join(missing)}\n"
            f"Loaded from: {env_path or '.env'}"
        )

    return {"kwargs": dict(host=host, port=port, dbname=dbname, user=user, password=password)}


def main():
    args = parse_args()

    if not safe_table_name(args.table):
        raise SystemExit(f"Unsafe table name: {args.table}")

    # Load DB config from .env
    db_cfg = load_db_config(args.env)

    # Read CSV
    with open(args.csv, "r", newline="", encoding=args.encoding) as f:
        rdr = csv.DictReader(f, delimiter=args.delimiter)
        if not rdr.fieldnames:
            raise SystemExit("CSV has no header row.")
        ok, missing = validate_headers(rdr.fieldnames)
        if not ok:
            exp = ", ".join(EXPECTED_HEADERS)
            mis = ", ".join(missing)
            raise SystemExit(
                f"CSV header mismatch.\nExpected at least: {exp}\nMissing: {mis}")
        rows = list(rdr)

    records, errors = build_records(rows, args.csv)
    if errors:
        print("Some rows were skipped due to errors:")
        for e in errors[:10]:
            print("  -", e)
        if len(errors) > 10:
            print(f"  ... and {len(errors)-10} more")
    if not records:
        print("No valid rows to import.")
        return

    # Connect
    conn = psycopg.connect(
        dsn=db_cfg["dsn"]) if "dsn" in db_cfg else psycopg.connect(**db_cfg["kwargs"])
    conn.row_factory = dict_row

    try:
        if args.create:
            ensure_table(conn, args.table)

        upsert_sql = UPSERT_SQL_TMPL.format(table=sql.Identifier(args.table))
        with conn.cursor() as cur:
            for i in range(0, len(records), args.batch_size):
                cur.executemany(upsert_sql, records[i:i + args.batch_size])
        conn.commit()
        print(f"Imported {len(records)} rows into table '{args.table}'.")
        if errors:
            print(f"Skipped {len(errors)} rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
