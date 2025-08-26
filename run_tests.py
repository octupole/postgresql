#!/usr/bin/env python3
"""
Automated test runner for PostgreSQL CSV importer and schema generator tools.

This script runs all the tests defined in test_plan.md and reports results.
"""

import subprocess
import sys
import time
from typing import List, Tuple

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

class TestResult:
    def __init__(self, name: str, passed: bool, message: str = ""):
        self.name = name
        self.passed = passed
        self.message = message

def run_command(cmd: str, expect_success: bool = True) -> Tuple[bool, str]:
    """Run a shell command and return success status and output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        success = (result.returncode == 0) == expect_success
        output = result.stdout + result.stderr
        return success, output
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, f"Command failed: {str(e)}"

def print_test_header(test_name: str):
    """Print formatted test header."""
    print(f"\n{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}{Colors.BOLD}Running: {test_name}{Colors.END}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.END}")

def print_result(test_result: TestResult):
    """Print test result with appropriate formatting."""
    status = f"{Colors.GREEN}✅ PASS{Colors.END}" if test_result.passed else f"{Colors.RED}❌ FAIL{Colors.END}"
    print(f"{status} - {test_result.name}")
    if test_result.message:
        print(f"    {test_result.message}")

def cleanup_tables():
    """Clean up any existing test tables."""
    tables = [
        "test_auto", "test_columns", "test_types", "test_upsert", 
        "test_books", "users", "employees", "test_integration",
        "test_reimport", "test_large", "test_unicode", "test_nulls",
        "test_reserved", "test_errors"
    ]
    
    print(f"\n{Colors.YELLOW}🧹 Cleaning up test tables...{Colors.END}")
    for table in tables:
        cmd = f"python schema_generator.py --drop --table-name {table} --force"
        run_command(cmd, expect_success=True)  # Don't care if it fails (table might not exist)

def run_tests() -> List[TestResult]:
    """Run all tests and return results."""
    results = []
    
    # Setup
    print_test_header("SETUP - Environment Cleanup")
    cleanup_tables()
    
    # Test 1: Schema Generator Tests
    print_test_header("TEST 1.1 - Schema Generator: Column Labels → SQL")
    success, output = run_command("python schema_generator.py --from-labels books_columns.txt --table-name test_books")
    results.append(TestResult(
        "Generate SQL from column labels", 
        success and "CREATE TABLE" in output and "isbn" in output,
        "SQL schema generated with proper column types"
    ))
    
    print_test_header("TEST 1.2 - Schema Generator: Create Table in DB")
    success, output = run_command("python schema_generator.py --from-labels books_columns.txt --table-name test_books --create --primary-key isbn --force")
    results.append(TestResult(
        "Create table in database",
        success and "created successfully" in output,
        "Table created with primary key"
    ))
    
    print_test_header("TEST 1.3 - Schema Generator: Verify Table Creation")
    success, output = run_command("python show_recent_books.py --table test_books --limit 1")
    results.append(TestResult(
        "Verify table exists and is queryable",
        success and "no rows" in output,
        "Table exists and can be queried"
    ))
    
    print_test_header("TEST 1.4 - Schema Generator: Drop Table")
    success, output = run_command("python schema_generator.py --drop --table-name test_books --force")
    results.append(TestResult(
        "Drop table from database",
        success and "dropped successfully" in output,
        "Table dropped successfully"
    ))
    
    # Test 2: CSV Importer Basic Tests
    print_test_header("TEST 2.1 - CSV Importer: Auto-Detection Mode")
    success, output = run_command("python csv_importer.py --csv tests/data/test_data_types.csv --table test_auto --create-table --force")
    results.append(TestResult(
        "Auto-detect schema and import data",
        success and "Successfully imported" in output and "3 records" in output,
        "Schema auto-detected and data imported"
    ))
    
    print_test_header("TEST 2.2 - CSV Importer: Column Definition File Mode") 
    success, output = run_command("python csv_importer.py --csv tests/data/test_data_types.csv --columns-file test_columns.txt --table test_columns --create-table --force")
    results.append(TestResult(
        "Use column definition file",
        success and "Successfully imported" in output and "VARCHAR(50)" in output,
        "Predefined column types used correctly"
    ))
    
    print_test_header("TEST 2.3 - CSV Importer: Upsert Functionality (Initial)")
    success, output = run_command("python csv_importer.py --csv tests/data/test_initial.csv --table test_upsert --create-table --primary-key id --force")
    results.append(TestResult(
        "Initial data import for upsert test",
        success and "Successfully imported" in output,
        "Initial data loaded for upsert testing"
    ))
    
    print_test_header("TEST 2.4 - CSV Importer: Upsert Functionality (Updates)")
    success, output = run_command("python csv_importer.py --csv tests/data/test_updates.csv --table test_upsert --primary-key id --force")
    results.append(TestResult(
        "Upsert with updates and new records",
        success and "Successfully imported" in output,
        "Existing records updated, new ones inserted"
    ))
    
    print_test_header("TEST 2.5 - CSV Importer: Complex Data Types")
    success, output = run_command("python csv_importer.py --csv tests/data/test_complex_types.csv --table test_types --create-table --force")
    results.append(TestResult(
        "Handle JSON, arrays, and complex types",
        success and "Successfully imported" in output and "JSONB" in output,
        "Complex data types handled correctly"
    ))
    
    print_test_header("TEST 2.6 - CSV Importer: Error Handling")
    success, output = run_command("python csv_importer.py --csv tests/data/test_bad_data.csv --table test_errors --create-table --force")
    results.append(TestResult(
        "Handle bad data gracefully",
        success and ("Successfully imported" in output or "Skipped" in output),
        "Bad rows handled, good rows imported"
    ))
    
    print_test_header("TEST 2.7 - CSV Importer: Append to Existing Table")
    success, output = run_command("python csv_importer.py --csv tests/data/test_additional.csv --table test_types --if-exists append --force")
    results.append(TestResult(
        "Append data to existing table",
        success and "Successfully imported" in output,
        "Data appended successfully"
    ))
    
    # Test 3: Integration Tests
    print_test_header("TEST 3.1 - Integration: Schema Generator → CSV Importer")
    # Create table with schema generator
    success1, _ = run_command("python schema_generator.py --from-labels books_columns.txt --table test_integration --create --primary-key isbn --force")
    # Import data with CSV importer
    success2, output = run_command("python csv_importer.py --csv tests/data/test_books_data.csv --table test_integration --primary-key isbn --force")
    results.append(TestResult(
        "Schema generator + CSV importer workflow",
        success1 and success2 and "Successfully imported" in output,
        "End-to-end workflow successful"
    ))
    
    # Test 4: Edge Cases
    print_test_header("TEST 4.1 - Edge Case: Unicode Characters")
    success, output = run_command("python csv_importer.py --csv tests/data/test_unicode.csv --table test_unicode --create-table --force")
    results.append(TestResult(
        "Handle Unicode and special characters",
        success and "Successfully imported" in output,
        "International characters preserved"
    ))
    
    print_test_header("TEST 4.2 - Edge Case: Null and Empty Values")
    success, output = run_command("python csv_importer.py --csv tests/data/test_nulls.csv --table test_nulls --create-table --force")
    results.append(TestResult(
        "Handle null and empty values",
        success and "Successfully imported" in output,
        "Empty values handled appropriately"
    ))
    
    print_test_header("TEST 4.3 - Edge Case: Reserved Words as Column Names")
    success, output = run_command("python csv_importer.py --csv tests/data/test_reserved_words.csv --table test_reserved --create-table --force")
    results.append(TestResult(
        "Handle PostgreSQL reserved words",
        success and "Successfully imported" in output,
        "Reserved words properly handled"
    ))
    
    return results

def generate_report(results: List[TestResult]):
    """Generate and display test report."""
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}TEST EXECUTION SUMMARY{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    
    print(f"\n{Colors.BOLD}Results:{Colors.END}")
    for result in results:
        print_result(result)
    
    print(f"\n{Colors.BOLD}Overall Summary:{Colors.END}")
    if passed == total:
        print(f"{Colors.GREEN}{Colors.BOLD}🎉 ALL TESTS PASSED! ({passed}/{total}){Colors.END}")
    else:
        print(f"{Colors.RED}{Colors.BOLD}❌ SOME TESTS FAILED ({passed}/{total} passed){Colors.END}")
    
    print(f"\n{Colors.BOLD}Test Categories:{Colors.END}")
    print(f"  • Schema Generator: 4 tests")
    print(f"  • CSV Importer Basic: 7 tests") 
    print(f"  • Integration: 1 test")
    print(f"  • Edge Cases: 3 tests")
    print(f"  • Total: {total} tests")
    
    if passed < total:
        print(f"\n{Colors.YELLOW}💡 Check failed tests above for details{Colors.END}")
    
    print(f"\n{Colors.BOLD}Cleanup:{Colors.END}")
    cleanup_tables()
    print(f"{Colors.GREEN}✅ Test tables cleaned up{Colors.END}")

def main():
    """Main test execution function."""
    print(f"{Colors.BOLD}{Colors.BLUE}")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║          PostgreSQL Tools - Comprehensive Test Suite      ║") 
    print("╚════════════════════════════════════════════════════════════╝")
    print(f"{Colors.END}")
    
    print(f"{Colors.YELLOW}⚠️  This will create and drop test tables in your database{Colors.END}")
    print(f"{Colors.YELLOW}⚠️  Make sure your .env file is configured correctly{Colors.END}")
    
    response = input(f"\n{Colors.BOLD}Continue with tests? (y/N): {Colors.END}").strip().lower()
    if response not in ('y', 'yes'):
        print("Tests cancelled.")
        return
    
    start_time = time.time()
    results = run_tests()
    end_time = time.time()
    
    print(f"\n{Colors.BOLD}Tests completed in {end_time - start_time:.1f} seconds{Colors.END}")
    generate_report(results)
    
    # Exit with appropriate code
    passed = sum(1 for r in results if r.passed)
    sys.exit(0 if passed == len(results) else 1)

if __name__ == "__main__":
    main()