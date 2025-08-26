#!/usr/bin/env python3
"""
General CSV importer for PostgreSQL with dynamic table creation.

Features:
1. Auto-detect schema from CSV headers with smart type inference
2. Use predefined column definitions file
3. Create table automatically or use existing table
4. Flexible data type mapping and constraint handling
5. Batch processing with upsert capability

CSV Mode Options:
- Auto-detect: Infers column types from CSV headers and sample data
- Column file: Uses column definitions file for precise schema control
- Existing table: Imports into pre-existing table structure

Notes:
- Reads DB config from .env file (DATABASE_URL or PG*/DB* variables)
- Extra CSV columns beyond schema are stored in JSONB 'metadata' column
- Supports upsert operations with configurable conflict resolution
- Use --create-table to automatically create table if it doesn't exist
"""

import argparse
import csv
import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

from dotenv import load_dotenv
import psycopg
from psycopg import sql
from psycopg.rows import dict_row


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import CSV data into PostgreSQL with dynamic table creation")
    
    p.add_argument("--csv", required=True, help="Path to the input CSV file")
    p.add_argument("--table", default="imported_data",
                   help="Destination table name (default: imported_data)")
    p.add_argument("--schema", default="public",
                   help="Target schema name (default: public)")
    
    # Schema definition modes
    schema_mode = p.add_mutually_exclusive_group()
    schema_mode.add_argument("--columns-file", 
                           help="File with column definitions (one per line, or JSON format)")
    schema_mode.add_argument("--auto-detect", action="store_true", default=True,
                           help="Auto-detect schema from CSV headers and data (default)")
    
    # Table creation options
    p.add_argument("--create-table", action="store_true",
                   help="Create table if it doesn't exist")
    p.add_argument("--if-exists", choices=["fail", "replace", "append"], default="append",
                   help="What to do if table exists (default: append)")
    p.add_argument("--primary-key", 
                   help="Column name to use as primary key (enables upsert)")
    
    # CSV parsing options
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    p.add_argument("--encoding", default="utf-8-sig", help="CSV encoding (default: utf-8-sig)")
    p.add_argument("--sample-rows", type=int, default=100,
                   help="Number of rows to sample for type detection (default: 100)")
    
    # Processing options
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Batch size for processing (default: 1000)")
    p.add_argument("--env", default=".env", help="Path to .env file (default: .env)")
    p.add_argument("--force", action="store_true", help="Skip confirmation prompts")
    
    return p.parse_args()


def load_db_config(env_path: str) -> Dict[str, Any]:
    """Load database configuration from .env file."""
    load_dotenv(env_path, override=True)
    
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or os.getenv("DB_URL")
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
        raise SystemExit(f"Missing DB settings: {', '.join(missing)}\nLoaded from: {env_path}")
    
    return {"kwargs": dict(host=host, port=port, dbname=dbname, user=user, password=password)}


def normalize_column_name(name: str) -> str:
    """Normalize column name for PostgreSQL (lowercase, underscores)."""
    # Replace non-alphanumeric with underscores, convert to lowercase
    normalized = re.sub(r'[^\w]+', '_', name.strip().lower())
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    # Ensure it doesn't start with a number
    if normalized and normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized or "unnamed_column"


def infer_postgresql_type(column_name: str, sample_values: List[str]) -> str:
    """Infer PostgreSQL column type from column name and sample values."""
    name_lower = column_name.lower()
    
    # Filter out empty values for analysis
    non_empty_values = [v.strip() for v in sample_values if v and v.strip()]
    if not non_empty_values:
        return "TEXT"
    
    # Type inference based on column name patterns (same as schema_generator.py)
    if name_lower in ("id", "pk") or name_lower.endswith("_id"):
        return "SERIAL PRIMARY KEY" if name_lower == "id" else "INTEGER"
    
    if any(x in name_lower for x in ["created_at", "updated_at", "timestamp", "_at"]):
        return "TIMESTAMPTZ"
    
    if any(x in name_lower for x in ["date", "_date", "birthday", "anniversary"]):
        return "DATE"
    
    if any(x in name_lower for x in ["is_", "has_", "can_", "should_", "enabled", "active", "deleted"]):
        return "BOOLEAN"
    
    # Analyze sample values for type detection
    numeric_count = 0
    integer_count = 0
    float_count = 0
    boolean_count = 0
    date_count = 0
    json_count = 0
    
    for val in non_empty_values[:20]:  # Sample first 20 values
        val_lower = val.lower()
        
        # Check for boolean values
        if val_lower in ('true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0'):
            boolean_count += 1
            continue
        
        # Check for numeric values
        try:
            if '.' in val or 'e' in val.lower():
                float(val)
                float_count += 1
                numeric_count += 1
            else:
                int(val)
                integer_count += 1
                numeric_count += 1
            continue
        except ValueError:
            pass
        
        # Check for date patterns
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m", "%Y"):
            try:
                datetime.strptime(val, fmt)
                date_count += 1
                break
            except ValueError:
                continue
        
        # Check for JSON-like content
        if val.startswith(('{', '[')) and val.endswith(('}', ']')):
            try:
                json.loads(val)
                json_count += 1
            except (json.JSONDecodeError, ValueError):
                pass
    
    total_samples = len(non_empty_values[:20])
    
    # Determine type based on sample analysis
    if boolean_count > total_samples * 0.7:
        return "BOOLEAN"
    elif integer_count > total_samples * 0.8:
        return "INTEGER"
    elif numeric_count > total_samples * 0.8:
        return "NUMERIC"
    elif date_count > total_samples * 0.7:
        return "DATE"
    elif json_count > total_samples * 0.7:
        return "JSONB"
    
    # Check for array-like content (semicolon or comma separated)
    if name_lower.endswith("s") and not name_lower.endswith("ss"):
        array_like = sum(1 for val in non_empty_values[:10] if ';' in val or ',' in val)
        if array_like > len(non_empty_values[:10]) * 0.5:
            return "TEXT[]"
    
    # Length-based TEXT vs VARCHAR decision
    max_length = max(len(val) for val in non_empty_values) if non_empty_values else 0
    if max_length <= 255:
        return f"VARCHAR({min(max_length * 2, 255)})"
    
    return "TEXT"


def read_columns_file(file_path: str) -> List[Dict[str, str]]:
    """Read column definitions from file (simple text or JSON format)."""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    
    # Try JSON format first
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "columns" in data:
            return data["columns"]
    except json.JSONDecodeError:
        pass
    
    # Simple text format - one column per line
    columns = []
    for line in content.split('\n'):
        line = line.strip()
        if line and not line.startswith('#'):
            # Support "column_name:type" or just "column_name"
            if ':' in line:
                name, col_type = line.split(':', 1)
                columns.append({"name": name.strip(), "type": col_type.strip()})
            else:
                columns.append({"name": line, "type": None})  # Will infer type
    
    return columns


def auto_detect_schema(csv_path: str, delimiter: str, encoding: str, sample_rows: int) -> List[Dict[str, str]]:
    """Auto-detect table schema from CSV file."""
    with open(csv_path, "r", newline="", encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            raise SystemExit("CSV file has no headers")
        
        # Sample data for type inference
        sample_data = {}
        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for field, value in row.items():
                if field not in sample_data:
                    sample_data[field] = []
                sample_data[field].append(value or "")
    
    # Generate schema
    schema = []
    for field in reader.fieldnames:
        normalized_name = normalize_column_name(field)
        sample_values = sample_data.get(field, [])
        col_type = infer_postgresql_type(normalized_name, sample_values)
        
        schema.append({
            "name": normalized_name,
            "type": col_type,
            "original_name": field
        })
    
    return schema


def create_table_sql(schema: List[Dict[str, str]], table_name: str, schema_name: str, primary_key: Optional[str]) -> str:
    """Generate CREATE TABLE SQL from schema definition."""
    
    lines = [f"CREATE TABLE {schema_name}.{table_name} ("]
    column_lines = []
    
    # Add metadata column by default
    has_metadata = any(col["name"] == "metadata" for col in schema)
    
    for col in schema:
        col_name = col["name"]
        col_type = col["type"]
        
        # Handle primary key
        if primary_key and col_name == primary_key:
            if not col_type.endswith("PRIMARY KEY"):
                col_type = f"{col_type} PRIMARY KEY"
        elif col_type.endswith("PRIMARY KEY") and primary_key and col_name != primary_key:
            # Remove auto-detected primary key if different one specified
            col_type = col_type.replace(" PRIMARY KEY", "")
        
        column_lines.append(f"    {col_name} {col_type}")
    
    # Add standard metadata columns
    if not has_metadata:
        column_lines.append("    metadata JSONB")
    
    # Add timestamps if not present
    has_created_at = any("created_at" in col["name"] for col in schema)
    has_updated_at = any("updated_at" in col["name"] for col in schema)
    
    if not has_created_at:
        column_lines.append("    created_at TIMESTAMPTZ DEFAULT NOW()")
    if not has_updated_at:
        column_lines.append("    updated_at TIMESTAMPTZ DEFAULT NOW()")
    
    lines.append(",\n".join(column_lines))
    lines.append(");")
    
    return "\n".join(lines)


def table_exists(conn: psycopg.Connection, table_name: str, schema_name: str) -> bool:
    """Check if table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            ) AS table_exists
        """, (schema_name, table_name))
        result = cur.fetchone()
        return result["table_exists"] if isinstance(result, dict) else result[0]


def get_table_columns(conn: psycopg.Connection, table_name: str, schema_name: str) -> Set[str]:
    """Get existing table column names."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (schema_name, table_name))
        return {row["column_name"] if isinstance(row, dict) else row[0] for row in cur.fetchall()}


def convert_value(value: str, col_type: str) -> Any:
    """Convert string value to appropriate Python type for PostgreSQL."""
    if not value or value.strip() == "":
        return None
    
    value = value.strip()
    col_type_upper = col_type.upper()
    
    try:
        if "INTEGER" in col_type_upper or "SERIAL" in col_type_upper:
            return int(value)
        elif "NUMERIC" in col_type_upper or "DECIMAL" in col_type_upper:
            return float(value)
        elif "BOOLEAN" in col_type_upper:
            return value.lower() in ('true', 't', 'yes', 'y', '1')
        elif "DATE" in col_type_upper:
            # Try various date formats
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m", "%Y"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
            return None
        elif "TIMESTAMP" in col_type_upper:
            # Try various timestamp formats
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            return None
        elif "TEXT[]" in col_type_upper:
            # Handle array types
            if ';' in value:
                return [item.strip() for item in value.split(';') if item.strip()]
            elif ',' in value:
                return [item.strip() for item in value.split(',') if item.strip()]
            else:
                return [value]
        elif "JSONB" in col_type_upper or "JSON" in col_type_upper:
            try:
                parsed = json.loads(value)
                return json.dumps(parsed)  # Return as JSON string for psycopg
            except json.JSONDecodeError:
                return json.dumps({"raw_value": value})
    except (ValueError, TypeError):
        pass
    
    return value  # Return as string if conversion fails


def process_csv_data(csv_path: str, schema: List[Dict[str, str]], delimiter: str, encoding: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Process CSV data according to schema."""
    
    # Create mapping from original names to normalized names
    name_mapping = {}
    type_mapping = {}
    
    for col in schema:
        original = col.get("original_name", col["name"])
        normalized = col["name"]
        name_mapping[original] = normalized
        type_mapping[normalized] = col["type"]
    
    records = []
    errors = []
    
    with open(csv_path, "r", newline="", encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        
        for i, row in enumerate(reader, start=2):  # Start at 2 (header is line 1)
            try:
                record = {}
                metadata = {}
                
                for original_name, value in row.items():
                    if original_name in name_mapping:
                        normalized_name = name_mapping[original_name]
                        col_type = type_mapping[normalized_name]
                        record[normalized_name] = convert_value(value, col_type)
                    else:
                        # Extra columns go to metadata
                        if value and value.strip():
                            metadata[original_name] = value.strip()
                
                # Add metadata if any extra columns
                if metadata:
                    record["metadata"] = json.dumps(metadata)  # Convert to JSON string
                
                # Add timestamps
                if "created_at" not in record:
                    record["created_at"] = datetime.now()
                if "updated_at" not in record:
                    record["updated_at"] = datetime.now()
                
                records.append(record)
                
            except Exception as e:
                errors.append(f"Row {i}: {e}")
    
    return records, errors


def create_upsert_sql(schema: List[Dict[str, str]], table_name: str, schema_name: str, primary_key: Optional[str]) -> Tuple[sql.SQL, List[str]]:
    """Create INSERT or UPSERT SQL statement."""
    
    # Get column names from schema plus standard columns
    columns = [col["name"] for col in schema]
    
    # Add standard columns if not present
    if "metadata" not in columns:
        columns.append("metadata")
    if "created_at" not in columns:
        columns.append("created_at")  
    if "updated_at" not in columns:
        columns.append("updated_at")
    
    placeholders = [f"%({col})s" for col in columns]
    
    if primary_key:
        # UPSERT with ON CONFLICT
        sql_template = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({pk}) DO UPDATE SET
                {updates},
                updated_at = NOW()
        """)
        
        updates = [
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
            for col in columns if col not in (primary_key, "created_at", "updated_at")
        ]
        
        query = sql_template.format(
            table=sql.Identifier(schema_name, table_name),
            columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            placeholders=sql.SQL(", ").join(sql.SQL(p) for p in placeholders),
            pk=sql.Identifier(primary_key),
            updates=sql.SQL(", ").join(updates)
        )
    else:
        # Simple INSERT
        query = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
        """).format(
            table=sql.Identifier(schema_name, table_name),
            columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            placeholders=sql.SQL(", ").join(sql.SQL(p) for p in placeholders)
        )
    
    return query, columns


def confirm_action(message: str, force: bool = False) -> bool:
    """Ask for user confirmation unless force is True."""
    if force:
        return True
    
    response = input(f"{message} (y/N): ").strip().lower()
    return response in ('y', 'yes')


def main():
    args = parse_args()
    
    # Load database configuration
    db_cfg = load_db_config(args.env)
    conn = psycopg.connect(
        dsn=db_cfg["dsn"]) if "dsn" in db_cfg else psycopg.connect(**db_cfg["kwargs"])
    conn.row_factory = dict_row
    
    try:
        # Check if table exists
        table_already_exists = table_exists(conn, args.table, args.schema)
        
        if table_already_exists and args.if_exists == "fail":
            raise SystemExit(f"Table {args.schema}.{args.table} already exists. Use --if-exists append/replace")
        
        # Determine schema
        if args.columns_file:
            print(f"Using column definitions from: {args.columns_file}")
            schema = read_columns_file(args.columns_file)
            
            # Get CSV headers to create mapping
            with open(args.csv, "r", newline="", encoding=args.encoding) as f:
                reader = csv.DictReader(f, delimiter=args.delimiter)
                csv_headers = reader.fieldnames or []
            
            # Create mapping from CSV headers to schema columns
            csv_to_schema = {}
            for header in csv_headers:
                normalized = normalize_column_name(header)
                # Find matching schema column
                for col in schema:
                    if col["name"] == normalized:
                        csv_to_schema[header] = col
                        col["original_name"] = header
                        break
            
            # Infer types for columns without explicit types
            if any(col.get("type") is None for col in schema):
                print("Inferring types for undefined columns...")
                auto_schema = auto_detect_schema(args.csv, args.delimiter, args.encoding, args.sample_rows)
                auto_types = {col["name"]: col["type"] for col in auto_schema}
                
                for col in schema:
                    if col.get("type") is None:
                        col["type"] = auto_types.get(col["name"], "TEXT")
        else:
            print(f"Auto-detecting schema from CSV: {args.csv}")
            schema = auto_detect_schema(args.csv, args.delimiter, args.encoding, args.sample_rows)
        
        # Display detected schema
        print("\nDetected Schema:")
        print("-" * 50)
        for col in schema:
            original = col.get("original_name", col["name"])
            if original != col["name"]:
                print(f"  {original} -> {col['name']}: {col['type']}")
            else:
                print(f"  {col['name']}: {col['type']}")
        print()
        
        # Handle table creation/replacement
        if args.if_exists == "replace" and table_already_exists:
            if confirm_action(f"Replace existing table {args.schema}.{args.table}?", args.force):
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE {}.{}").format(
                        sql.Identifier(args.schema), sql.Identifier(args.table)))
                conn.commit()
                table_already_exists = False
                print(f"Dropped existing table {args.schema}.{args.table}")
            else:
                print("Operation cancelled.")
                return
        
        # Create table if needed
        if not table_already_exists:
            if args.create_table or args.if_exists == "replace":
                create_sql = create_table_sql(schema, args.table, args.schema, args.primary_key)
                print("Creating table with SQL:")
                print(create_sql)
                print()
                
                if confirm_action("Create this table?", args.force):
                    with conn.cursor() as cur:
                        cur.execute(create_sql)
                    conn.commit()
                    print(f"Table {args.schema}.{args.table} created successfully.")
                else:
                    print("Operation cancelled.")
                    return
            else:
                raise SystemExit(f"Table {args.schema}.{args.table} does not exist. Use --create-table to create it.")
        
        # Verify table columns match schema
        existing_columns = get_table_columns(conn, args.table, args.schema)
        schema_columns = {col["name"] for col in schema}
        
        # Process CSV data
        print(f"Processing CSV data from: {args.csv}")
        records, errors = process_csv_data(args.csv, schema, args.delimiter, args.encoding)
        
        if errors:
            print("Errors encountered during processing:")
            for error in errors[:10]:
                print(f"  - {error}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more errors")
            print()
        
        if not records:
            print("No valid records to import.")
            return
        
        print(f"Processed {len(records)} records")
        if errors:
            print(f"Skipped {len(errors)} records due to errors")
        
        if not confirm_action(f"Import {len(records)} records into {args.schema}.{args.table}?", args.force):
            print("Import cancelled.")
            return
        
        # Create insert/upsert SQL
        insert_sql, column_order = create_upsert_sql(schema, args.table, args.schema, args.primary_key)
        
        # Execute batch inserts
        imported_count = 0
        with conn.cursor() as cur:
            for i in range(0, len(records), args.batch_size):
                batch = records[i:i + args.batch_size]
                
                # Ensure all records have all required columns
                for record in batch:
                    for col in column_order:
                        if col not in record:
                            record[col] = None
                
                cur.executemany(insert_sql, batch)
                imported_count += len(batch)
                
                if len(records) > args.batch_size:
                    print(f"Imported {imported_count}/{len(records)} records...")
        
        conn.commit()
        
        print(f"\n✅ Successfully imported {imported_count} records into {args.schema}.{args.table}")
        if errors:
            print(f"⚠️  Skipped {len(errors)} records due to errors")
        
        # Show sample of imported data
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT * FROM {}.{} LIMIT 3").format(
                sql.Identifier(args.schema), sql.Identifier(args.table)))
            sample_rows = cur.fetchall()
            
        if sample_rows:
            print("\nSample of imported data:")
            print("-" * 50)
            for i, row in enumerate(sample_rows, 1):
                print(f"Row {i}:")
                for key, value in row.items():
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, default=str)
                    print(f"  {key}: {value}")
                print()
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()