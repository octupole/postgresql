#!/usr/bin/env python3
"""
Show (and optionally snapshot) the most recently updated books.

Defaults replicate:
  SELECT isbn, title, authors, categories, shelf, position, source_file, updated_at
  FROM public.books
  ORDER BY updated_at DESC
  LIMIT 20;

Features:
- Reads DB settings from .env (DATABASE_URL or PG*/DB* vars)
- Prints an aligned table to stdout
- --limit to change number of rows
- --schema / --table to target a different source table
- --snapshot <name> to CREATE TABLE AS SELECT (optionally --replace)
- --out csv|json|md to write the result to a file with --outfile
"""

import argparse
import csv
import json
import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

# ---------- CLI ----------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Show recent books and optionally snapshot them to a table.")
    p.add_argument("--env", default=".env",
                   help="Path to .env with DB settings (default: .env)")
    p.add_argument("--schema", default="public",
                   help="Source schema (default: public)")
    p.add_argument("--table", default="books",
                   help="Source table (default: books)")
    p.add_argument("--limit", type=int, default=20,
                   help="How many rows to show (default: 20)")

    p.add_argument(
        "--snapshot", help="Create a table snapshot with this name (e.g., books_recent)")
    p.add_argument("--replace", action="store_true",
                   help="If snapshot exists, drop and recreate it")

    p.add_argument(
        "--out", choices=["csv", "json", "md"], help="Also export the result")
    p.add_argument(
        "--outfile", help="Path for export file (e.g., recent.csv / recent.json / recent.md)")
    return p.parse_args()

# ---------- ENV / DB ----------


def load_db_config(env_path: str) -> Dict[str, Any]:
    # Override existing env so your .env wins
    load_dotenv(env_path, override=True)

    dsn = os.getenv("DATABASE_URL") or os.getenv(
        "POSTGRES_URL") or os.getenv("DB_URL")
    if dsn:
        return {"dsn": dsn}

    def first(*names: str, default: Optional[str] = None) -> Optional[str]:
        for n in names:
            v = os.getenv(n)
            if v:
                return v
        return default

    host = first("PGHOST", "DB_HOST", default="localhost")
    port = int(first("PGPORT", "DB_PORT", default="5432"))
    dbname = first("PGDATABASE", "DB_NAME")
    user = first("PGUSER", "DB_USER")
    password = first("PGPASSWORD", "DB_PASSWORD")

    missing = [k for k, v in {"PGDATABASE/DB_NAME": dbname,
                              "PGUSER/DB_USER": user, "PGPASSWORD/DB_PASSWORD": password}.items() if not v]
    if missing:
        raise SystemExit(
            "Missing DB settings. Provide DATABASE_URL or:\n"
            "  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD)\n"
            f"Missing: {', '.join(missing)}\nLoaded from: {env_path or '.env'}"
        )

    return {"kwargs": dict(host=host, port=port, dbname=dbname, user=user, password=password)}

# ---------- QUERY ----------


COLUMNS = ["isbn", "title", "authors", "categories",
           "shelf", "position", "source_file", "updated_at"]


def build_select(schema: str, table: str, limit: int):
    return sql.SQL("""
        SELECT isbn, title, authors, categories, shelf, position, source_file, updated_at
        FROM {}.{}
        ORDER BY updated_at DESC
        LIMIT %s
    """).format(sql.Identifier(schema), sql.Identifier(table)), (limit,)


def run_query(conn: psycopg.Connection, schema: str, table: str, limit: int) -> List[Dict[str, Any]]:
    q, params = build_select(schema, table, limit)
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return rows

# ---------- OUTPUT FORMATTING ----------


def to_str(val: Any) -> str:
    if isinstance(val, list):
        return "; ".join(str(x) for x in val)
    return "" if val is None else str(val)


def print_table(rows: List[Dict[str, Any]], columns: List[str]) -> None:
    if not rows:
        print("(no rows)")
        return

    # compute column widths
    widths = {col: max(len(col), *(len(to_str(r.get(col)))
                       for r in rows)) for col in columns}

    # header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    sep = "-+-".join("-" * widths[col] for col in columns)
    print(header)
    print(sep)

    # rows
    for r in rows:
        line = " | ".join(to_str(r.get(col)).ljust(
            widths[col]) for col in columns)
        print(line)


def export(rows: List[Dict[str, Any]], columns: List[str], kind: str, path: str) -> None:
    if not path:
        raise SystemExit("--outfile is required when using --out")
    if kind == "csv":
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(columns)
            for r in rows:
                w.writerow([to_str(r.get(c)) for c in columns])
    elif kind == "json":
        # Keep arrays as-is for JSON
        with open(path, "w", encoding="utf-8") as f:
            json.dump([{c: r.get(c) for c in columns} for r in rows],
                      f, ensure_ascii=False, indent=2, default=str)
    elif kind == "md":
        # simple markdown table
        with open(path, "w", encoding="utf-8") as f:
            f.write("| " + " | ".join(columns) + " |\n")
            f.write("| " + " | ".join("---" for _ in columns) + " |\n")
            for r in rows:
                f.write("| " + " | ".join(to_str(r.get(c))
                        for c in columns) + " |\n")
    else:
        raise ValueError(f"Unknown out format: {kind}")

# ---------- SNAPSHOT (optional CREATE TABLE AS SELECT) ----------


def create_snapshot(conn: psycopg.Connection, src_schema: str, src_table: str, dst_schema: str, dst_table: str, limit: int, replace: bool):
    with conn.cursor() as cur:
        if replace:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(dst_schema), sql.Identifier(dst_table)))

        # CREATE TABLE AS SELECT ...
        sel, params = build_select(src_schema, src_table, limit)
        create_sql = sql.SQL("CREATE TABLE {}.{} AS ").format(sql.Identifier(
            dst_schema), sql.Identifier(dst_table)).join([sql.SQL(""), sel])
        cur.execute(create_sql, params)

        # (optional) add a primary key if isbn is unique in your data
        try:
            cur.execute(sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (isbn)").format(
                sql.Identifier(dst_schema), sql.Identifier(dst_table)))
        except Exception:
            conn.rollback()  # ignore if it fails (duplicates / not unique)
            conn.commit()
        else:
            conn.commit()

# ---------- MAIN ----------


def main():
    args = parse_args()
    db_cfg = load_db_config(args.env)

    conn = psycopg.connect(
        dsn=db_cfg["dsn"]) if "dsn" in db_cfg else psycopg.connect(**db_cfg["kwargs"])
    conn.row_factory = dict_row
    try:
        # 1) Run the query
        rows = run_query(conn, args.schema, args.table, args.limit)

        # 2) Print to terminal
        print_table(rows, COLUMNS)

        # 3) Optional export
        if args.out:
            export(rows, COLUMNS, args.out, args.outfile)

        # 4) Optional snapshot table
        if args.snapshot:
            # snapshot goes into the same schema by default
            create_snapshot(conn, args.schema, args.table,
                            args.schema, args.snapshot, args.limit, args.replace)
            print(f"\nSnapshot created: {args.schema}.{args.snapshot}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
