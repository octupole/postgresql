#!/usr/bin/env python3
"""
PGTools Library Demo

This script demonstrates the key features of the PGTools library
with simple, practical examples that showcase the main functionality.
"""

from pgtools import CSVImporter, SchemaGenerator, import_csv, generate_schema

def demo_quick_functions():
    """Demonstrate the convenience functions."""
    print("🚀 Quick Functions Demo")
    print("=" * 25)
    
    try:
        # Quick CSV import
        print("📥 Quick CSV import:")
        result = import_csv("examples/data/sample_data.csv", "quick_demo", create_table=True, if_exists="replace")
        print(f"   ✅ Imported {result.imported_count} records")
        
        # Quick schema generation
        print("\n📋 Quick schema generation:")
        try:
            schema = generate_schema("books_columns.txt", table_name="books_demo")
            print(f"   ✅ Generated schema for '{schema.table_name}' with {len(schema.columns)} columns")
        except FileNotFoundError:
            print("   ⚠️  books_columns.txt not found, creating manual schema")
            from pgtools import SchemaGenerator
            with SchemaGenerator() as gen:
                schema = gen.from_labels(["id", "title", "author", "pages"], "books_demo")
                print(f"   ✅ Generated manual schema with {len(schema.columns)} columns")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")

def demo_class_usage():
    """Demonstrate using the main classes."""
    print("\n🏗️  Class Usage Demo")
    print("=" * 20)
    
    # CSV Importer example
    print("📊 CSVImporter class:")
    with CSVImporter() as importer:
        try:
            result = importer.import_csv(
                "examples/data/sample_data.csv",
                "class_demo",
                create_table=True,
                primary_key="id",
                if_exists="replace"
            )
            print(f"   ✅ Imported {result.imported_count} records with upsert capability")
        except Exception as e:
            print(f"   ❌ Import error: {e}")
    
    # Schema Generator example  
    print("\n🏗️  SchemaGenerator class:")
    with SchemaGenerator() as generator:
        try:
            # Create schema from labels
            schema = generator.from_labels(
                ["user_id", "username", "email", "created_at", "is_premium"],
                "users_demo",
                primary_key="user_id"
            )
            
            # Create table
            created = generator.create_table(schema, if_exists="replace")
            print(f"   ✅ Created table '{schema.table_name}': {created}")
            
            # Export schema
            sql = generator.export_schema(schema, "sql")
            print(f"   📜 Generated SQL ({len(sql)} characters)")
            
        except Exception as e:
            print(f"   ❌ Schema error: {e}")

def demo_advanced_features():
    """Demonstrate advanced features."""
    print("\n⚡ Advanced Features Demo")
    print("=" * 27)
    
    # Type inference
    print("🧠 Type Inference:")
    from pgtools.utils import TypeInference
    
    sample_inferences = [
        ("user_id", "Expected: INTEGER with PRIMARY KEY"),
        ("email_address", "Expected: TEXT"),
        ("is_active", "Expected: BOOLEAN"),  
        ("created_at", "Expected: TIMESTAMPTZ"),
        ("price_amount", "Expected: NUMERIC"),
        ("metadata", "Expected: JSONB")
    ]
    
    for col_name, expected in sample_inferences:
        inferred_type = TypeInference.infer_type(col_name)
        print(f"   • {col_name}: {inferred_type}")
    
    # Data conversion
    print(f"\n🔄 Data Conversion:")
    from pgtools.utils import DataConverter
    
    test_conversions = [
        ("true", "BOOLEAN", "Boolean conversion"),
        ("123.45", "NUMERIC", "Numeric conversion"),
        ("tag1;tag2;tag3", "TEXT[]", "Array conversion"),
        ('{"key": "value"}', "JSONB", "JSON conversion")
    ]
    
    for value, pg_type, description in test_conversions:
        converted = DataConverter.convert_value(value, pg_type)
        print(f"   • {description}: '{value}' → {converted} ({type(converted).__name__})")

def main():
    """Run the library demo."""
    print("🎭 PGTools Library Demo")
    print("=" * 25)
    print("This demo shows the key features of the PGTools library")
    print("for PostgreSQL schema generation and CSV data import.\n")
    
    try:
        demo_quick_functions()
        demo_class_usage()
        demo_advanced_features()
        
        print(f"\n🎉 Demo completed successfully!")
        print(f"💡 Explore the examples/ directory for more detailed usage")
        print(f"📚 Check README.md for comprehensive documentation")
        
    except KeyboardInterrupt:
        print(f"\n👋 Demo interrupted by user")
    except Exception as e:
        print(f"\n💥 Demo failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())