"""
Command-line interface for schema generator.

This module provides the CLI functionality for generating PostgreSQL schemas,
maintaining compatibility with the original schema_generator.py script.
"""

import argparse
import sys
from typing import Optional

from ..core.schema_generator import SchemaGenerator


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
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


def confirm_action(message: str, force: bool = False) -> bool:
    """Ask for user confirmation unless force is True."""
    if force:
        return True
    
    response = input(f"{message} (y/N): ").strip().lower()
    return response in ('y', 'yes')


def main():
    """Main CLI function."""
    args = parse_args()

    # Validate arguments
    if not args.drop and not args.from_labels and not args.from_table:
        print("Error: Must specify --from-labels, --from-table, or --drop", file=sys.stderr)
        sys.exit(1)

    try:
        with SchemaGenerator(env_path=args.env) as generator:
            
            # Handle drop operation first (standalone operation)
            if args.drop:
                if not args.from_table and args.table_name == "new_table":
                    print("Error: --drop requires --from-table or --table-name to specify which table to drop", file=sys.stderr)
                    sys.exit(1)
                
                target_table = args.from_table.split(".")[-1] if args.from_table else args.table_name
                target_schema = args.from_table.split(".")[0] if args.from_table and "." in args.from_table else args.schema
                
                if confirm_action(f"Drop table {target_schema}.{target_table}? This will delete all data!", args.force):
                    generator.db_manager.drop_table(target_table, target_schema)
                    print(f"Table {target_schema}.{target_table} dropped successfully.")
                else:
                    print("Operation cancelled.")
                return

            # Schema generation modes
            if args.from_labels:
                # Mode 1: Create from column labels file
                print(f"Generating schema from column labels: {args.from_labels}")
                schema = generator.from_labels_file(
                    args.from_labels, 
                    args.table_name, 
                    args.schema, 
                    primary_key=args.primary_key, 
                    not_null=args.not_null
                )
            else:
                # Mode 2: Derive from existing table
                print(f"Deriving schema from existing table: {args.from_table}")
                schema = generator.from_table(args.from_table, args.table_name, args.schema)

            # Display detected schema
            print(f"\nGenerated Schema for {schema.schema_name}.{schema.table_name}:")
            print("-" * 60)
            for col in schema.columns:
                constraints_str = " ".join(col.get("constraints", []))
                original = col.get("original_name", "")
                if original and original != col["name"]:
                    print(f"  {original} -> {col['name']}: {col['type']} {constraints_str}".strip())
                else:
                    print(f"  {col['name']}: {col['type']} {constraints_str}".strip())
            print()

            # Handle database table creation
            if args.create:
                if confirm_action(f"Create table {schema.schema_name}.{schema.table_name} in database?", args.force):
                    created = generator.create_table(schema, args.if_exists)
                    if created:
                        print(f"✅ Table {schema.schema_name}.{schema.table_name} created successfully.")
                    else:
                        print(f"ℹ️  Table {schema.schema_name}.{schema.table_name} already exists (skipped).")
                else:
                    print("Table creation cancelled.")

            # Generate output (unless only creating in DB)
            if not args.create or args.output or args.out_file:
                output = generator.export_schema(schema, args.output)

                # Write output
                if args.out_file:
                    generator.save_schema(schema, args.out_file, args.output)
                    print(f"Schema written to: {args.out_file}")
                elif not args.create:  # Don't print schema if only creating table
                    print("Generated SQL:")
                    print("-" * 40)
                    print(output)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()