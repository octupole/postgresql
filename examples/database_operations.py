#!/usr/bin/env python3
"""
Database Operations Example

This example demonstrates direct database operations using
the DatabaseManager class for low-level database interactions.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pgtools import DatabaseManager, SchemaGenerator
import json

def main():
    """Demonstrate database operations and management."""
    
    print("💾 Database Operations Example")
    print("=" * 35)
    
    with DatabaseManager() as db:
        
        # Example 1: Basic Database Information
        print("\n1. 🔍 Database Inspection")
        print("-" * 25)
        
        # Check if tables exist
        tables_to_check = ["users", "products", "orders", "nonexistent_table"]
        for table in tables_to_check:
            exists = db.table_exists(table)
            status = "✅ EXISTS" if exists else "❌ NOT FOUND"
            print(f"   Table '{table}': {status}")
        
        
        # Example 2: Create Test Table for Operations
        print("\n2. 🏗️  Table Creation")
        print("-" * 22)
        
        # Define schema programmatically
        test_schema = [
            {"name": "id", "type": "SERIAL PRIMARY KEY", "constraints": []},
            {"name": "username", "type": "VARCHAR(50)", "constraints": ["NOT NULL", "UNIQUE"]},
            {"name": "email", "type": "TEXT", "constraints": ["NOT NULL"]},
            {"name": "age", "type": "INTEGER", "constraints": []},
            {"name": "is_active", "type": "BOOLEAN", "constraints": ["DEFAULT TRUE"]},
            {"name": "profile", "type": "JSONB", "constraints": []},
            {"name": "created_at", "type": "TIMESTAMPTZ", "constraints": ["DEFAULT NOW()"]}
        ]
        
        # Create table
        created = db.create_table("demo_users", test_schema, if_exists="replace")
        print(f"   ✅ Table 'demo_users' created: {created}")
        
        # Get table columns to verify
        columns = db.get_table_columns("demo_users")
        print(f"   📋 Columns created: {', '.join(sorted(columns))}")
        
        
        # Example 3: Insert Sample Data
        print("\n3. 📥 Data Insertion")
        print("-" * 20)
        
        # Insert some sample data
        sample_users = [
            ("alice", "alice@example.com", 28, True, '{"role": "admin", "department": "IT"}'),
            ("bob", "bob@example.com", 34, True, '{"role": "user", "department": "Sales"}'),
            ("charlie", "charlie@example.com", 22, False, '{"role": "user", "department": "Marketing"}')
        ]
        
        insert_query = """
            INSERT INTO demo_users (username, email, age, is_active, profile)
            VALUES (%s, %s, %s, %s, %s::jsonb)
        """
        
        for user_data in sample_users:
            rows_affected = db.execute_update(insert_query, user_data)
            print(f"   ✅ Inserted user '{user_data[0]}': {rows_affected} row affected")
        
        
        # Example 4: Query Data
        print("\n4. 📤 Data Querying")  
        print("-" * 19)
        
        # Query all users
        all_users = db.execute_query("SELECT * FROM demo_users ORDER BY id")
        print(f"   📊 Total users in table: {len(all_users)}")
        
        # Display users
        for user in all_users:
            profile = json.loads(user['profile']) if user['profile'] else {}
            print(f"     • {user['username']} ({user['email']}) - {profile.get('role', 'N/A')}")
        
        # Query with conditions
        active_users = db.execute_query(
            "SELECT username, email FROM demo_users WHERE is_active = %s",
            (True,)
        )
        print(f"   🟢 Active users: {len(active_users)}")
        for user in active_users:
            print(f"     • {user['username']}: {user['email']}")
        
        
        # Example 5: Table Schema Inspection
        print("\n5. 🔬 Schema Inspection")
        print("-" * 23)
        
        # Get detailed schema information
        try:
            schema_info = db.get_table_schema("demo_users")
            print(f"   📋 Detailed schema for 'demo_users':")
            for col in schema_info:
                constraints = ", ".join(col["constraints"]) if col["constraints"] else "None"
                print(f"     • {col['name']}: {col['type']} | Constraints: {constraints}")
        except Exception as e:
            print(f"   ❌ Could not get schema: {e}")
        
        
        # Example 6: Table Operations
        print("\n6. 🔧 Table Operations")
        print("-" * 21)
        
        # Create a backup table by copying schema
        with SchemaGenerator() as generator:
            try:
                # Copy schema from existing table
                backup_schema = generator.from_table("demo_users", "demo_users_backup")
                generator.create_table(backup_schema, if_exists="replace")
                print("   ✅ Backup table created with same schema")
                
                # Copy data to backup table
                copy_query = "INSERT INTO demo_users_backup SELECT * FROM demo_users"
                copied_rows = db.execute_update(copy_query)
                print(f"   📋 Copied {copied_rows} rows to backup table")
                
            except Exception as e:
                print(f"   ❌ Backup operation failed: {e}")
        
        
        # Example 7: Data Updates and Deletes
        print("\n7. ✏️  Data Modifications")
        print("-" * 25)
        
        # Update a user's information
        update_query = """
            UPDATE demo_users 
            SET age = %s, profile = profile || %s::jsonb 
            WHERE username = %s
        """
        
        updated_rows = db.execute_update(
            update_query, 
            (29, '{"last_updated": "2024-08-26"}', "alice")
        )
        print(f"   ✏️  Updated alice's record: {updated_rows} row affected")
        
        # Soft delete (deactivate) a user
        deactivate_query = "UPDATE demo_users SET is_active = FALSE WHERE username = %s"
        deactivated_rows = db.execute_update(deactivate_query, ("charlie",))
        print(f"   🔒 Deactivated charlie: {deactivated_rows} row affected")
        
        # Check final state
        final_count = db.execute_query("SELECT COUNT(*) as total FROM demo_users WHERE is_active = TRUE")
        active_count = final_count[0]['total']
        print(f"   📊 Active users remaining: {active_count}")
        
        
        # Example 8: Connection Information
        print("\n8. 🔌 Connection Information")
        print("-" * 27)
        
        # Get connection info (safely, without exposing passwords)
        print(f"   🔗 Database Manager: {repr(db)}")
        
        # Test connection with a simple query
        try:
            version_result = db.execute_query("SELECT version() as pg_version")
            pg_version = version_result[0]['pg_version']
            # Show only the first part of version string
            version_short = pg_version.split(' on ')[0]
            print(f"   🐘 PostgreSQL Version: {version_short}")
        except Exception as e:
            print(f"   ❌ Could not get version: {e}")
        
        
        # Example 9: Cleanup (Optional)
        print("\n9. 🧹 Cleanup")
        print("-" * 15)
        
        cleanup = input("   🗑️  Remove demo tables? (y/N): ").strip().lower()
        if cleanup in ('y', 'yes'):
            try:
                db.drop_table("demo_users", if_exists=True)
                db.drop_table("demo_users_backup", if_exists=True)
                print("   ✅ Demo tables removed")
            except Exception as e:
                print(f"   ❌ Cleanup failed: {e}")
        else:
            print("   📋 Demo tables preserved for inspection")
        
        print(f"\n🎉 Database operations example completed!")
        print(f"   💡 You can inspect the tables in your PostgreSQL database")


if __name__ == "__main__":
    main()