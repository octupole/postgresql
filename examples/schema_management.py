#!/usr/bin/env python3
"""
Schema Management Example

This example demonstrates various ways to generate and manage
PostgreSQL table schemas using the PGTools library.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pgtools import SchemaGenerator

def main():
    """Demonstrate schema generation and management."""
    
    print("🏗️  Schema Management Example")
    print("=" * 40)
    
    # Initialize schema generator
    with SchemaGenerator() as generator:
        
        # Example 1: Generate schema from column labels
        print("\n1. 📝 Schema from Column Labels")
        print("-" * 35)
        
        labels = ["user_id", "full_name", "email_address", "birth_date", 
                 "is_active", "total_spent", "tags", "metadata"]
        
        schema = generator.from_labels(
            labels=labels,
            table_name="customers",
            primary_key="user_id",
            not_null=["full_name", "email_address"]
        )
        
        print(f"Generated schema for '{schema.table_name}':")
        for col in schema.columns:
            constraints = " ".join(col.get("constraints", []))
            print(f"  • {col['name']}: {col['type']} {constraints}".strip())
        
        # Create the table
        created = generator.create_table(schema, if_exists="replace")
        print(f"✅ Table created: {created}")
        
        
        # Example 2: Generate schema from labels file
        print("\n2. 📄 Schema from Labels File")
        print("-" * 35)
        
        try:
            schema2 = generator.from_labels_file(
                "books_columns.txt",
                table_name="library_books",
                primary_key="isbn"
            )
            
            print(f"Generated schema from file for '{schema2.table_name}':")
            for col in schema2.columns:
                print(f"  • {col['name']}: {col['type']}")
            
            # Export schema to SQL file
            generator.save_schema(schema2, "library_schema.sql", format="sql")
            print("💾 Schema saved to library_schema.sql")
            
        except FileNotFoundError:
            print("⚠️  books_columns.txt not found, skipping this example")
        
        
        # Example 3: Copy schema from existing table
        print("\n3. 📋 Copy Schema from Existing Table")
        print("-" * 35)
        
        try:
            # Copy the customers table we just created
            backup_schema = generator.from_table(
                "customers", 
                table_name="customers_backup"
            )
            
            print(f"Copied schema from 'customers' to '{backup_schema.table_name}':")
            for col in backup_schema.columns:
                constraints = " ".join(col.get("constraints", []))
                print(f"  • {col['name']}: {col['type']} {constraints}".strip())
            
            # Create backup table
            created = generator.create_table(backup_schema)
            print(f"✅ Backup table created: {created}")
            
        except Exception as e:
            print(f"⚠️  Could not copy table schema: {e}")
        
        
        # Example 4: Programmatic schema building
        print("\n4. 🔧 Programmatic Schema Building")
        print("-" * 35)
        
        from pgtools.core.schema_generator import Schema
        
        # Build schema programmatically
        custom_schema = Schema("orders", "public")
        custom_schema.add_column("id", "SERIAL", ["PRIMARY KEY", "NOT NULL"])
        custom_schema.add_column("customer_id", "INTEGER", ["NOT NULL"])
        custom_schema.add_column("order_date", "DATE", ["NOT NULL", "DEFAULT CURRENT_DATE"])
        custom_schema.add_column("total_amount", "NUMERIC(10,2)", ["NOT NULL"])
        custom_schema.add_column("status", "VARCHAR(20)", ["DEFAULT 'pending'"])
        custom_schema.add_column("items", "JSONB")
        
        print(f"Built custom schema for '{custom_schema.table_name}':")
        for col in custom_schema.columns:
            constraints = " ".join(col.get("constraints", []))
            print(f"  • {col['name']}: {col['type']} {constraints}".strip())
        
        # Generate SQL
        sql = custom_schema.to_sql()
        print(f"\n📜 Generated SQL:")
        print(sql)
        
        
        # Example 5: Schema export in different formats
        print("\n5. 📤 Schema Export Formats")
        print("-" * 35)
        
        # Export as SQL
        sql_export = generator.export_schema(schema, format="sql")
        print("SQL format (first 200 chars):")
        print(f"   {sql_export[:200]}...")
        
        # Export as JSON
        json_export = generator.export_schema(schema, format="json")
        print(f"\nJSON format (first 200 chars):")
        print(f"   {json_export[:200]}...")
        
        print(f"\n🎉 Schema management examples completed successfully!")


if __name__ == "__main__":
    main()