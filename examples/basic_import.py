#!/usr/bin/env python3
"""
Basic CSV Import Example

This example demonstrates the simplest way to import CSV data
using the PGTools library with automatic schema detection.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pgtools import CSVImporter

def main():
    """Import CSV data with automatic table creation."""
    
    # Initialize the CSV importer (reads .env for database config)
    importer = CSVImporter()
    
    print("🚀 Basic CSV Import Example")
    print("=" * 40)
    
    try:
        # Import CSV with automatic schema detection
        result = importer.import_csv(
            csv_path="examples/data/sample_data.csv",       # Your CSV file
            table="employees",                # Target table name
            create_table=True,               # Create table if it doesn't exist
            if_exists="replace"              # Replace if table already exists
        )
        
        # Display results
        print(f"\n✅ Import completed successfully!")
        print(f"   📊 Records imported: {result.imported_count}")
        print(f"   ❌ Records with errors: {result.error_count}")
        print(f"   🆕 Table created: {result.table_created}")
        print(f"   ⏱️  Processing time: {result.processing_time:.1f} seconds")
        
        # Show any errors
        if result.errors:
            print(f"\n⚠️  Errors encountered:")
            for error in result.errors[:5]:  # Show first 5 errors
                print(f"     • {error}")
            if len(result.errors) > 5:
                print(f"     ... and {len(result.errors) - 5} more")
        
        # Display the detected schema
        if result.schema_detected:
            print(f"\n📋 Auto-detected Schema:")
            for col in result.schema_detected.columns:
                original = col.get('original_name', col['name'])
                if original != col['name']:
                    print(f"     {original} → {col['name']}: {col['type']}")
                else:
                    print(f"     {col['name']}: {col['type']}")
    
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return 1
    
    finally:
        # Clean up connections
        importer.close()
    
    return 0


if __name__ == "__main__":
    exit(main())