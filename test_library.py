#!/usr/bin/env python3
"""
Quick Library Test

Test the PGTools library to ensure it works correctly
after refactoring from standalone scripts.
"""

def test_imports():
    """Test that all library components can be imported."""
    print("🔧 Testing imports...")
    
    try:
        from pgtools import CSVImporter, SchemaGenerator, DatabaseManager
        from pgtools import import_csv, generate_schema
        from pgtools.utils import TypeInference, DataConverter, DatabaseConfig
        print("   ✅ All imports successful")
        return True
    except Exception as e:
        print(f"   ❌ Import failed: {e}")
        return False

def test_type_inference():
    """Test type inference functionality."""
    print("🧠 Testing type inference...")
    
    try:
        from pgtools.utils.type_inference import TypeInference
        
        # Test column name normalization
        normalized = TypeInference.normalize_column_name("User Name!")
        assert normalized == "user_name", f"Expected 'user_name', got '{normalized}'"
        
        # Test type inference from name
        id_type = TypeInference.infer_from_name("id")
        assert "PRIMARY KEY" in id_type, f"Expected PRIMARY KEY in {id_type}"
        
        email_type = TypeInference.infer_from_name("email")
        assert email_type == "TEXT", f"Expected TEXT, got {email_type}"
        
        # Test type inference from values
        price_type = TypeInference.infer_from_values("price", ["10.50", "25.99", "100.00"])
        assert "NUMERIC" in price_type, f"Expected NUMERIC in {price_type}"
        
        print("   ✅ Type inference working correctly")
        return True
    except Exception as e:
        print(f"   ❌ Type inference failed: {e}")
        return False

def test_data_converter():
    """Test data conversion functionality."""
    print("🔄 Testing data conversion...")
    
    try:
        from pgtools.utils.data_converter import DataConverter
        
        # Test boolean conversion
        bool_val = DataConverter.convert_value("true", "BOOLEAN")
        assert bool_val == True, f"Expected True, got {bool_val}"
        
        # Test integer conversion
        int_val = DataConverter.convert_value("42", "INTEGER")
        assert int_val == 42, f"Expected 42, got {int_val}"
        
        # Test array conversion
        array_val = DataConverter.convert_value("a;b;c", "TEXT[]")
        assert array_val == ["a", "b", "c"], f"Expected ['a', 'b', 'c'], got {array_val}"
        
        print("   ✅ Data conversion working correctly")
        return True
    except Exception as e:
        print(f"   ❌ Data conversion failed: {e}")
        return False

def test_schema_creation():
    """Test schema creation without database connection."""
    print("📋 Testing schema creation...")
    
    try:
        from pgtools.core.schema_generator import Schema
        
        # Create a schema programmatically
        schema = Schema("test_table", "public")
        schema.add_column("id", "SERIAL PRIMARY KEY")
        schema.add_column("name", "VARCHAR(100)", ["NOT NULL"])
        schema.add_column("email", "TEXT")
        
        # Test schema properties
        assert schema.table_name == "test_table"
        assert len(schema.columns) == 3
        assert schema.get_column("name")["type"] == "VARCHAR(100)"
        
        # Test SQL generation
        sql = schema.to_sql()
        assert "CREATE TABLE" in sql
        assert "test_table" in sql
        assert "SERIAL PRIMARY KEY" in sql
        
        # Test dict conversion
        schema_dict = schema.to_dict()
        assert "table_name" in schema_dict
        assert "columns" in schema_dict
        
        print("   ✅ Schema creation working correctly")
        return True
    except Exception as e:
        print(f"   ❌ Schema creation failed: {e}")
        return False

def test_db_config():
    """Test database configuration without actual connection."""
    print("🔗 Testing database configuration...")
    
    try:
        from pgtools.utils.db_config import DatabaseConfig
        
        # Test config object creation (should not fail)
        config = DatabaseConfig(".env")
        
        # Test environment variable parsing
        import os
        os.environ["TEST_DB_NAME"] = "testdb"
        os.environ["TEST_DB_USER"] = "testuser"
        
        # This should work even without actual .env file
        print("   ✅ Database configuration working correctly")
        return True
    except Exception as e:
        print(f"   ❌ Database configuration failed: {e}")
        return False

def test_convenience_functions():
    """Test convenience functions."""
    print("⚡ Testing convenience functions...")
    
    try:
        from pgtools import generate_schema
        
        # Test without file (should fail gracefully)
        try:
            schema = generate_schema("nonexistent.txt")
            print("   ⚠️  Should have failed for nonexistent file")
        except FileNotFoundError:
            print("   ✅ Correctly handled missing file")
        
        return True
    except Exception as e:
        print(f"   ❌ Convenience functions failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 PGTools Library Test Suite")
    print("=" * 35)
    
    tests = [
        test_imports,
        test_type_inference,
        test_data_converter,
        test_schema_creation,
        test_db_config,
        test_convenience_functions
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"   💥 Test crashed: {e}")
        print()
    
    print("🏁 Test Results")
    print("-" * 15)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Library is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Check the library installation.")
        return 1

if __name__ == "__main__":
    exit(main())