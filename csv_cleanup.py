#!/usr/bin/env python3
"""
CSV File Cleanup Utility

This script helps you decide which CSV files to keep based on your usage needs.
"""

import os
from typing import Dict, List

def analyze_csv_files() -> Dict[str, Dict]:
    """Analyze all CSV files and categorize them."""
    
    csv_files = {
        # Essential for library functionality
        "essential": {
            "examples/data/sample_data.csv": "Core demo data - used in examples and documentation"
        },
        
        # Comprehensive test suite files  
        "test_suite": {
            "tests/data/test_data_types.csv": "Core test data for type inference and basic functionality",
            "tests/data/test_initial.csv": "For testing upsert operations (initial data)",
            "tests/data/test_updates.csv": "For testing upsert operations (updates)",
            "tests/data/test_complex_types.csv": "For testing JSON, arrays, and complex data types",
            "tests/data/test_bad_data.csv": "For testing error handling with malformed data",
            "tests/data/test_nulls.csv": "For testing null and empty value handling", 
            "tests/data/test_unicode.csv": "For testing international character support",
            "tests/data/test_reserved_words.csv": "For testing PostgreSQL reserved word handling",
            "tests/data/test_additional.csv": "For testing append operations to existing tables",
            "tests/data/test_books_data.csv": "For testing book-specific schema operations"
        },
        
        # Already removed
        "removed": {
            "mybooks.csv": "Original test data (obsolete)",
            "update_data.csv": "Old test data (replaced by test_updates.csv)"
        }
    }
    
    return csv_files

def main():
    """Display CSV file analysis and cleanup options."""
    
    print("📊 CSV File Analysis")
    print("=" * 25)
    
    analysis = analyze_csv_files()
    
    print("\n✅ ESSENTIAL FILES (Keep these):")
    print("-" * 40)
    for file, desc in analysis["essential"].items():
        exists = "✓" if os.path.exists(file) else "✗"
        print(f"  {exists} {file}")
        print(f"    → {desc}")
    
    print("\n🧪 TEST SUITE FILES (Optional):")
    print("-" * 40)
    print("These are only needed if you want to run the comprehensive test suite.")
    for file, desc in analysis["test_suite"].items():
        exists = "✓" if os.path.exists(file) else "✗"
        print(f"  {exists} {file}")
        print(f"    → {desc}")
    
    print("\n🗑️  REMOVED FILES:")
    print("-" * 20)
    for file, desc in analysis["removed"].items():
        exists = "✓" if os.path.exists(file) else "✗ (removed)"
        print(f"  {exists} {file}")
        print(f"    → {desc}")
    
    # Count existing files
    essential_count = sum(1 for f in analysis["essential"] if os.path.exists(f))
    test_count = sum(1 for f in analysis["test_suite"] if os.path.exists(f))
    
    print(f"\n📈 SUMMARY:")
    print(f"  Essential files: {essential_count}/{len(analysis['essential'])}")
    print(f"  Test suite files: {test_count}/{len(analysis['test_suite'])}")
    
    print(f"\n💡 RECOMMENDATIONS:")
    if essential_count < len(analysis["essential"]):
        print("  ⚠️  Missing essential files! Library functionality may be limited.")
    else:
        print("  ✅ All essential files present - library will work perfectly.")
    
    if test_count > 0:
        print(f"  🧪 You have {test_count} test files - great for comprehensive testing!")
        print("     To remove test files and keep only essentials:")
        print("     rm -rf tests/data/")
    else:
        print("  📦 Minimal setup - only essential files present.")
    
    # Calculate total size of all CSV files
    total_size = 0
    for category in ["essential", "test_suite"]:
        for file_path in analysis[category]:
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
    
    print(f"  💾 Total CSV file size: {total_size:,} bytes ({total_size/1024:.1f} KB)")
    
    print(f"\n📁 ORGANIZATION:")
    print(f"  📂 examples/data/ - Example data files used in demos")
    print(f"  📂 tests/data/ - Test data files for comprehensive testing")
    print(f"  📦 Clean separation of concerns!")

if __name__ == "__main__":
    main()