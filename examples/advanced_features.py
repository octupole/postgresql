#!/usr/bin/env python3
"""
Advanced Features Example

This example demonstrates advanced features of PGTools including
upserts, batch processing, error handling, and progress tracking.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pgtools import CSVImporter, SchemaGenerator
import time

def progress_callback(current: int, total: int):
    """Progress callback for import operations."""
    percentage = (current / total) * 100
    print(f"   📈 Progress: {current}/{total} ({percentage:.1f}%)")

def main():
    """Demonstrate advanced PGTools features."""
    
    print("🚀 Advanced Features Example")
    print("=" * 40)
    
    with CSVImporter() as importer, SchemaGenerator() as generator:
        
        # Example 1: Upsert Operations (Insert/Update)
        print("\n1. 🔄 Upsert Operations")
        print("-" * 25)
        
        # Initial data import
        print("Importing initial data...")
        result1 = importer.import_csv(
            csv_path="tests/data/test_initial.csv",
            table="user_scores",
            create_table=True,
            primary_key="id",  # This enables upserts!
            if_exists="replace"
        )
        print(f"   ✅ Initial import: {result1.imported_count} records")
        
        # Update existing records and add new ones
        print("Importing updates...")
        result2 = importer.import_csv(
            csv_path="tests/data/test_updates.csv", 
            table="user_scores",
            primary_key="id"  # Existing records will be updated
        )
        print(f"   ✅ Updates import: {result2.imported_count} records processed")
        print("     (existing records updated, new records inserted)")
        
        
        # Example 2: Column Definitions File
        print("\n2. 📋 Using Column Definitions")
        print("-" * 30)
        
        # Import with predefined column types
        try:
            result3 = importer.import_csv(
                csv_path="tests/data/test_data_types.csv",
                table="typed_data", 
                columns_file="test_columns.txt",  # Explicit type definitions
                create_table=True,
                if_exists="replace"
            )
            print(f"   ✅ Typed import: {result3.imported_count} records")
            print("     Used predefined column types from file")
        except FileNotFoundError:
            print("   ⚠️  test_columns.txt not found, skipping")
        
        
        # Example 3: Batch Processing with Progress
        print("\n3. ⚡ Batch Processing & Progress")
        print("-" * 35)
        
        # Import with custom batch size and progress tracking
        result4 = importer.import_csv(
            csv_path="tests/data/test_data_types.csv",
            table="batch_test",
            create_table=True,
            batch_size=2,  # Small batch for demo
            progress_callback=progress_callback,  # Track progress
            if_exists="replace"
        )
        print(f"   ✅ Batch import completed: {result4.imported_count} records")
        
        
        # Example 4: Error Handling
        print("\n4. 🛡️  Error Handling")
        print("-" * 22)
        
        # Import data with intentional errors
        try:
            result5 = importer.import_csv(
                csv_path="tests/data/test_bad_data.csv",
                table="error_test",
                create_table=True,
                if_exists="replace"
            )
            
            print(f"   📊 Import results:")
            print(f"     • Successful: {result5.imported_count} records")
            print(f"     • Failed: {result5.error_count} records")
            
            if result5.errors:
                print(f"   ⚠️  Sample errors:")
                for error in result5.errors[:3]:  # Show first 3 errors
                    print(f"     • {error}")
                if len(result5.errors) > 3:
                    print(f"     • ... and {len(result5.errors) - 3} more")
        except FileNotFoundError:
            print("   ⚠️  test_bad_data.csv not found, skipping")
        
        
        # Example 5: Complex Data Types
        print("\n5. 🎭 Complex Data Types")
        print("-" * 25)
        
        # Import data with JSON, arrays, and complex types
        try:
            result6 = importer.import_csv(
                csv_path="tests/data/test_complex_types.csv",
                table="complex_data",
                create_table=True,
                if_exists="replace"
            )
            print(f"   ✅ Complex types import: {result6.imported_count} records")
            
            # Show detected schema for complex types
            if result6.schema_detected:
                print("   🔍 Detected complex types:")
                for col in result6.schema_detected.columns:
                    if col['type'] in ['JSONB', 'TEXT[]']:
                        print(f"     • {col['name']}: {col['type']}")
        except FileNotFoundError:
            print("   ⚠️  test_complex_types.csv not found, skipping")
        
        
        # Example 6: Schema Manipulation
        print("\n6. 🔧 Schema Manipulation")
        print("-" * 25)
        
        # Create and modify schema programmatically
        from pgtools.core.schema_generator import Schema
        
        # Start with auto-detected schema
        try:
            base_schema = generator.from_labels_file("example_columns.txt", "dynamic_table")
            
            # Add custom columns
            base_schema.add_column("created_by", "VARCHAR(50)", ["NOT NULL", "DEFAULT 'system'"])
            base_schema.add_column("last_updated", "TIMESTAMPTZ", ["DEFAULT NOW()"])
            base_schema.add_column("version", "INTEGER", ["DEFAULT 1"])
            
            # Remove a column if it exists
            if base_schema.get_column("phone_number"):
                base_schema.remove_column("phone_number") 
                print("   🗑️  Removed phone_number column")
            
            print(f"   🏗️  Modified schema for '{base_schema.table_name}':")
            for col in base_schema.columns[-3:]:  # Show last 3 columns we added
                constraints = " ".join(col.get("constraints", []))
                print(f"     • {col['name']}: {col['type']} {constraints}".strip())
            
            # Create the modified table
            generator.create_table(base_schema, if_exists="replace")
            print("   ✅ Dynamic table created with modifications")
            
        except FileNotFoundError:
            print("   ⚠️  example_columns.txt not found, creating manual schema")
            manual_schema = Schema("dynamic_table", "public")
            manual_schema.add_column("id", "SERIAL PRIMARY KEY")
            manual_schema.add_column("name", "TEXT", ["NOT NULL"])
            manual_schema.add_column("created_by", "VARCHAR(50)", ["DEFAULT 'system'"])
            generator.create_table(manual_schema, if_exists="replace")
            print("   ✅ Manual schema created")
        
        
        # Example 7: Performance Monitoring
        print("\n7. ⏱️  Performance Monitoring")
        print("-" * 30)
        
        start_time = time.time()
        
        # Large batch processing simulation
        result7 = importer.import_csv(
            csv_path="examples/data/sample_data.csv",
            table="performance_test", 
            create_table=True,
            batch_size=1000,  # Efficient batch size
            if_exists="replace"
        )
        
        end_time = time.time()
        
        print(f"   📊 Performance metrics:")
        print(f"     • Records processed: {result7.imported_count}")
        print(f"     • Total time: {result7.processing_time:.2f} seconds")
        print(f"     • Records/second: {result7.imported_count / result7.processing_time:.1f}")
        print(f"     • Wall clock time: {end_time - start_time:.2f} seconds")
        
        print(f"\n🎉 All advanced features demonstrated successfully!")
        print(f"   💡 Check your PostgreSQL database to see the created tables")


if __name__ == "__main__":
    main()