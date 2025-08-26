"""
Command-line interface for CSV importer.

This module provides the CLI functionality for importing CSV files,
maintaining compatibility with the original csv_importer.py script.
"""

import argparse
import sys
from typing import Optional

from ..core.csv_importer import CSVImporter


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
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
                           help="File with column definitions")
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


def confirm_action(message: str, force: bool = False) -> bool:
    """Ask for user confirmation unless force is True."""
    if force:
        return True
    
    response = input(f"{message} (y/N): ").strip().lower()
    return response in ('y', 'yes')


def progress_callback(imported: int, total: int):
    """Progress callback for large imports."""
    if total > 1000:  # Only show progress for large imports
        print(f"Imported {imported}/{total} records...")


def main():
    """Main CLI function."""
    args = parse_args()
    
    try:
        with CSVImporter(env_path=args.env) as importer:
            # Display detected schema first
            print(f"Processing CSV: {args.csv}")
            
            if args.columns_file:
                print(f"Using column definitions from: {args.columns_file}")
            else:
                print("Auto-detecting schema from CSV headers and data...")
            
            # Perform import
            result = importer.import_csv(
                csv_path=args.csv,
                table=args.table,
                schema_name=args.schema,
                create_table=args.create_table,
                columns_file=args.columns_file,
                primary_key=args.primary_key,
                if_exists=args.if_exists,
                delimiter=args.delimiter,
                encoding=args.encoding,
                sample_rows=args.sample_rows,
                batch_size=args.batch_size,
                progress_callback=progress_callback if not args.force else None
            )
            
            # Display schema if table was created
            if result.table_created and result.schema_detected:
                print("\nCreated table with schema:")
                print("-" * 50)
                for col in result.schema_detected.columns:
                    original = col.get("original_name", col["name"])
                    if original != col["name"]:
                        print(f"  {original} -> {col['name']}: {col['type']}")
                    else:
                        print(f"  {col['name']}: {col['type']}")
                print()
            
            # Show results
            if result.error_count > 0:
                print("Errors encountered during processing:")
                for error in result.errors[:10]:
                    print(f"  - {error}")
                if len(result.errors) > 10:
                    print(f"  ... and {len(result.errors) - 10} more errors")
                print()
            
            if result.imported_count > 0:
                if not args.force:
                    # Ask for confirmation before import
                    if not confirm_action(f"Import {result.imported_count} records into {args.schema}.{args.table}?"):
                        print("Import cancelled.")
                        return
                
                print(f"\n✅ Successfully imported {result.imported_count} records into {args.schema}.{args.table}")
                if result.table_created:
                    print("🆕 Table created")
                if result.error_count > 0:
                    print(f"⚠️  Skipped {result.error_count} records due to errors")
                
                print(f"⏱️  Processing time: {result.processing_time:.1f} seconds")
                
            else:
                print("No valid records to import.")
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()