#!/usr/bin/env python3
"""
Generate PostgreSQL table schemas from column labels file or derive from existing tables.

Supports two modes:
1. Create schema from column labels file (text file with column names, one per line)
2. Derive schema from existing database table

Features:
- Smart type inference for column labels
- Full schema extraction from existing tables including constraints
- Output as SQL CREATE TABLE statement or Python dict
- Configurable table name and schema options
- Database connection via .env file
"""

import argparse
import os
import re
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg
from psycopg import sql
from psycopg.rows import dict_row


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate PostgreSQL table schema from column labels or existing table")

    # Mode selection
    mode = p.add_mutually_exclusive_group(required=False)
    mode.add_argument("--from-labels", metavar="FILE",
                      help="Create schema from column labels file")
    mode.add_argument("--from-table", metavar="TABLE",
                      help="Derive schema from existing table (format: [schema.]table)")

    # Common options
    p.add_argument("--table-name", default="new_table",
                   help="Name for the new table (default: new_table)")
    p.add_argument("--schema", default="public",
                   help="Target schema name (default: public)")
    p.add_argument("--env", default=".env",
                   help="Path to .env file with DB settings (default: .env)")

    # Output options
    p.add_argument("--output", choices=["sql", "dict"], default="sql",
                   help="Output format: sql or dict (default: sql)")
    p.add_argument("--out-file", help="Write output to file instead of stdout")

    # Database operations
    p.add_argument("--create", action="store_true",
                   help="Create the table in the database (requires DB connection)")
    p.add_argument("--drop", action="store_true",
                   help="Drop the table from the database (requires DB connection)")
    p.add_argument("--if-exists", choices=["fail", "replace", "skip"], default="fail",
                   help="What to do if table exists when creating (default: fail)")
    p.add_argument("--force", action="store_true",
                   help="Skip confirmation prompts for destructive operations")

    # Schema creation options (for --from-labels mode)
    p.add_argument("--primary-key",
                   help="Column name to use as primary key")
    p.add_argument("--not-null", nargs="*", default=[],
                   help="Column names that should be NOT NULL")

    return p.parse_args()


def load_db_config(env_path: str) -> Dict[str, Any]:
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
            f"Missing DB settings: {', '.join(missing)}\n"
            f"Loaded from: {env_path}")

    return {"kwargs": dict(host=host, port=port, dbname=dbname, user=user, password=password)}


def infer_column_type(column_name: str) -> str:
    """Infer PostgreSQL column type from column name patterns."""
    name_lower = column_name.lower()

    # Primary keys
    if name_lower in ("id", "pk") or name_lower.endswith("_id"):
        return "SERIAL PRIMARY KEY" if name_lower == "id" else "INTEGER"

    # Timestamps
    if any(x in name_lower for x in ["created_at", "updated_at", "timestamp", "_at"]):
        return "TIMESTAMPTZ"

    # Dates
    if any(x in name_lower for x in ["date", "_date", "birthday", "anniversary"]):
        return "DATE"

    # Boolean flags
    if any(x in name_lower for x in ["is_", "has_", "can_", "should_", "enabled", "active", "deleted"]):
        return "BOOLEAN"

    # Numeric fields
    if any(x in name_lower for x in ["price", "cost", "amount", "total", "count", "num", "quantity"]):
        return "NUMERIC(10,2)" if any(x in name_lower for x in ["price", "cost", "amount", "total"]) else "INTEGER"

    # Email, URL, phone
    if any(x in name_lower for x in ["email", "mail"]):
        return "TEXT"
    if any(x in name_lower for x in ["url", "link", "website"]):
        return "TEXT"
    if any(x in name_lower for x in ["phone", "mobile", "tel"]):
        return "VARCHAR(20)"

    # Arrays (plural forms)
    if name_lower.endswith("s") and not name_lower.endswith("ss"):
        singular = name_lower[:-1]
        if singular in ["tag", "category", "author", "keyword", "skill"]:
            return "TEXT[]"

    # JSON fields
    if any(x in name_lower for x in ["metadata", "config", "settings", "options", "data", "json"]):
        return "JSONB"

    # Default to TEXT for names, descriptions, etc.
    if any(x in name_lower for x in ["name", "title", "description", "comment", "note", "text", "content"]):
        return "TEXT"

    # Fallback
    return "TEXT"


def read_column_labels(file_path: str) -> List[str]:
    """Read column labels from file, one per line."""
    with open(file_path, "r", encoding="utf-8") as f:
        labels = [line.strip() for line in f if line.strip()
                  and not line.strip().startswith("#")]
    return labels


def create_schema_from_labels(labels: List[str], table_name: str, schema: str,
                              primary_key: Optional[str], not_null: List[str]) -> Dict[str, Any]:
    """Generate schema dictionary from column labels."""
    columns = []

    for label in labels:
        # Clean column name (replace spaces/special chars with underscores)
        col_name = re.sub(r'[^\w]+', '_', label.lower()).strip('_')
        col_type = infer_column_type(col_name)

        # Handle primary key override
        if primary_key and col_name == primary_key:
            if not col_type.endswith("PRIMARY KEY"):
                col_type = "SERIAL PRIMARY KEY"
        elif col_type.endswith("PRIMARY KEY") and primary_key and col_name != primary_key:
            # Remove auto-detected primary key if different one specified
            col_type = "INTEGER"

        # Handle NOT NULL constraint
        constraints = []
        if col_name in not_null or col_type.endswith("PRIMARY KEY"):
            constraints.append("NOT NULL")

        columns.append({
            "name": col_name,
            "type": col_type,
            "constraints": constraints,
            "original_label": label
        })

    return {
        "table_name": table_name,
        "schema": schema,
        "columns": columns
    }


def derive_schema_from_table(conn: psycopg.Connection, source_table: str) -> Dict[str, Any]:
    """Extract complete schema from existing database table."""

    # Parse schema.table
    if "." in source_table:
        src_schema, src_table = source_table.split(".", 1)
    else:
        src_schema, src_table = "public", source_table

    with conn.cursor() as cur:
        # Get column information
        cur.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (src_schema, src_table))

        col_info = cur.fetchall()
        if not col_info:
            raise SystemExit(f"Table {src_schema}.{src_table} not found")

        # Get constraints
        cur.execute("""
            SELECT 
                tc.constraint_type,
                kcu.column_name,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s AND tc.table_name = %s
        """, (src_schema, src_table))

        constraints_info = cur.fetchall()

    # Build constraints map
    constraints_map = {}
    for constraint in constraints_info:
        col_name = constraint["column_name"]
        if col_name not in constraints_map:
            constraints_map[col_name] = []
        constraints_map[col_name].append(constraint["constraint_type"])

    # Build columns list
    columns = []
    for col in col_info:
        name = col["column_name"]

        # Build type string
        data_type = col["data_type"].upper()
        if col["character_maximum_length"]:
            col_type = f"{data_type}({col['character_maximum_length']})"
        elif col["numeric_precision"] and col["numeric_scale"]:
            col_type = f"{data_type}({col['numeric_precision']},{col['numeric_scale']})"
        elif col["numeric_precision"]:
            col_type = f"{data_type}({col['numeric_precision']})"
        else:
            col_type = data_type

        # Handle special cases
        if data_type == "CHARACTER VARYING":
            col_type = col_type.replace("CHARACTER VARYING", "VARCHAR")

        # Build constraints
        constraints = []
        if name in constraints_map:
            if "PRIMARY KEY" in constraints_map[name]:
                constraints.append("PRIMARY KEY")
            if "UNIQUE" in constraints_map[name]:
                constraints.append("UNIQUE")

        if col["is_nullable"] == "NO":
            constraints.append("NOT NULL")

        if col["column_default"]:
            constraints.append(f"DEFAULT {col['column_default']}")

        columns.append({
            "name": name,
            "type": col_type,
            "constraints": constraints
        })

    return {
        "table_name": src_table,
        "schema": src_schema,
        "columns": columns,
        "source_table": f"{src_schema}.{src_table}"
    }


def schema_to_sql(schema_dict: Dict[str, Any], target_table: str, target_schema: str) -> str:
    """Convert schema dictionary to SQL CREATE TABLE statement."""

    lines = [f'CREATE TABLE {target_schema}.{target_table} (']

    column_lines = []
    for col in schema_dict["columns"]:
        parts = [f'    {col["name"]}', col["type"]]
        if col["constraints"]:
            parts.extend(col["constraints"])
        column_lines.append(" ".join(parts))

    lines.append(",\n".join(column_lines))
    lines.append(");")

    return "\n".join(lines)


def table_exists(conn: psycopg.Connection, table_name: str, schema_name: str) -> bool:
    """Check if table exists in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            ) AS table_exists
        """, (schema_name, table_name))
        result = cur.fetchone()
        return result["table_exists"] if isinstance(result, dict) else result[0]


def confirm_action(message: str, force: bool = False) -> bool:
    """Ask for user confirmation unless force is True."""
    if force:
        return True
    
    response = input(f"{message} (y/N): ").strip().lower()
    return response in ('y', 'yes')


def create_table_in_db(conn: psycopg.Connection, schema_dict: Dict[str, Any], 
                      target_table: str, target_schema: str, if_exists: str, force: bool) -> bool:
    """Create table in database with conflict handling."""
    
    exists = table_exists(conn, target_table, target_schema)
    
    if exists:
        if if_exists == "skip":
            print(f"Table {target_schema}.{target_table} already exists, skipping creation.")
            return False
        elif if_exists == "fail":
            raise SystemExit(f"Table {target_schema}.{target_table} already exists. Use --if-exists replace/skip or --drop first.")
        elif if_exists == "replace":
            if not confirm_action(f"Table {target_schema}.{target_table} exists. Replace it?", force):
                print("Operation cancelled.")
                return False
            drop_table_from_db(conn, target_table, target_schema, force=True)
    
    # Create the table
    create_sql = schema_to_sql(schema_dict, target_table, target_schema)
    
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    
    print(f"Table {target_schema}.{target_table} created successfully.")
    return True


def drop_table_from_db(conn: psycopg.Connection, table_name: str, schema_name: str, force: bool = False) -> bool:
    """Drop table from database with confirmation."""
    
    if not table_exists(conn, table_name, schema_name):
        print(f"Table {schema_name}.{table_name} does not exist.")
        return False
    
    if not confirm_action(f"Drop table {schema_name}.{table_name}? This will delete all data!", force):
        print("Operation cancelled.")
        return False
    
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE {}.{}").format(
            sql.Identifier(schema_name), sql.Identifier(table_name)))
    conn.commit()
    
    print(f"Table {schema_name}.{table_name} dropped successfully.")
    return True


def main():
    args = parse_args()

    # Validate arguments
    if not args.drop and not args.from_labels and not args.from_table:
        raise SystemExit("Error: Must specify --from-labels, --from-table, or --drop")

    # Handle drop operation first (standalone operation)
    if args.drop:
        if not args.from_table and not (args.from_labels or args.table_name != "new_table"):
            raise SystemExit("--drop requires either --from-table or --table-name to specify which table to drop")
        
        db_cfg = load_db_config(args.env)
        conn = psycopg.connect(
            dsn=db_cfg["dsn"]) if "dsn" in db_cfg else psycopg.connect(**db_cfg["kwargs"])
        conn.row_factory = dict_row
        
        try:
            target_table = args.from_table.split(".")[-1] if args.from_table else args.table_name
            target_schema = args.from_table.split(".")[0] if args.from_table and "." in args.from_table else args.schema
            drop_table_from_db(conn, target_table, target_schema, args.force)
        finally:
            conn.close()
        return

    # Schema generation modes
    if args.from_labels:
        # Mode 1: Create from column labels file
        labels = read_column_labels(args.from_labels)
        schema_dict = create_schema_from_labels(
            labels, args.table_name, args.schema, args.primary_key, args.not_null)
        db_conn = None

    else:
        # Mode 2: Derive from existing table
        db_cfg = load_db_config(args.env)
        db_conn = psycopg.connect(
            dsn=db_cfg["dsn"]) if "dsn" in db_cfg else psycopg.connect(**db_cfg["kwargs"])
        db_conn.row_factory = dict_row

        try:
            schema_dict = derive_schema_from_table(db_conn, args.from_table)
        except Exception as e:
            db_conn.close()
            raise e

    try:
        # Handle database table creation
        if args.create:
            if not db_conn:
                # Need DB connection for --from-labels mode
                db_cfg = load_db_config(args.env)
                db_conn = psycopg.connect(
                    dsn=db_cfg["dsn"]) if "dsn" in db_cfg else psycopg.connect(**db_cfg["kwargs"])
                db_conn.row_factory = dict_row
            
            create_table_in_db(db_conn, schema_dict, args.table_name, args.schema, 
                             args.if_exists, args.force)

        # Generate output (unless only creating in DB)
        if not args.create or args.output or args.out_file:
            if args.output == "sql":
                output = schema_to_sql(schema_dict, args.table_name, args.schema)
            else:  # dict format
                import json
                output = json.dumps(schema_dict, indent=2, ensure_ascii=False)

            # Write output
            if args.out_file:
                with open(args.out_file, "w", encoding="utf-8") as f:
                    f.write(output)
                print(f"Schema written to: {args.out_file}")
            elif not args.create:  # Don't print schema if only creating table
                print(output)

    finally:
        if db_conn:
            db_conn.close()


if __name__ == "__main__":
    main()
