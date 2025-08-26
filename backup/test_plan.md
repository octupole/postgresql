# Comprehensive Test Plan for PostgreSQL Tools

## Overview
This test plan covers both `csv_importer.py` and `schema_generator.py` to ensure all functionality works correctly.

---

## Test Environment Setup

### Prerequisites
1. PostgreSQL database running with connection configured in `.env`
2. All Python dependencies installed (`psycopg`, `python-dotenv`)
3. Clean database (drop any existing test tables)

### Cleanup Before Testing
```bash
# Drop all test tables if they exist
python schema_generator.py --drop --table-name test_auto --force
python schema_generator.py --drop --table-name test_columns --force
python schema_generator.py --drop --table-name test_types --force
python schema_generator.py --drop --table-name test_upsert --force
python schema_generator.py --drop --table-name test_books --force
python schema_generator.py --drop --table-name users --force
python schema_generator.py --drop --table-name employees --force
```

---

## Part 1: Schema Generator Tests

### Test 1.1: Column Labels File → SQL Schema
**Purpose:** Test schema generation from column labels file

**Command:**
```bash
python schema_generator.py --from-labels books_columns.txt --table-name test_books
```

**Expected Output:**
- SQL CREATE TABLE statement
- Smart type inference (isbn→TEXT, pages→INTEGER, created_at→TIMESTAMPTZ)
- Proper PostgreSQL column names

**Success Criteria:** ✅ SQL syntax is valid, types are appropriate

### Test 1.2: Derive Schema from Existing Table
**Purpose:** Extract schema from existing database table

**Setup:** Use the books table created by import_books.py (if available)
**Command:**
```bash
python schema_generator.py --from-table books --table-name books_copy
```

**Expected Output:**
- Complete schema with all constraints
- Proper data types with precision/scale
- Primary keys and constraints preserved

**Success Criteria:** ✅ Schema matches original table structure

### Test 1.3: Create Table in Database
**Purpose:** Test actual table creation

**Command:**
```bash
python schema_generator.py --from-labels books_columns.txt --table-name test_books --create --primary-key isbn
```

**Expected Output:**
- Table created successfully
- Primary key set correctly
- All columns present with correct types

**Verification:**
```bash
python show_recent_books.py --table test_books --limit 5
```
**Success Criteria:** ✅ Table exists and shows "(no rows)"

### Test 1.4: Drop Table
**Purpose:** Test table deletion

**Command:**
```bash
python schema_generator.py --drop --table-name test_books --force
```

**Expected Output:**
- Table dropped successfully

**Success Criteria:** ✅ Table no longer exists

---

## Part 2: CSV Importer Tests

### Test 2.1: Auto-Detection Mode (Basic)
**Purpose:** Test automatic schema detection and table creation

**Command:**
```bash
python csv_importer.py --csv test_data_types.csv --table test_auto --create-table --force
```

**Expected Output:**
- Schema auto-detected correctly
- Table created with appropriate types
- All data imported successfully
- Sample data displayed

**Success Criteria:** ✅ All data types correctly inferred and imported

### Test 2.2: Column Definition File Mode
**Purpose:** Test using predefined column schema

**Command:**
```bash
python csv_importer.py --csv test_data_types.csv --columns-file test_columns.txt --table test_columns --create-table --force
```

**Expected Output:**
- Uses predefined column types
- Proper mapping from CSV headers to column names
- Data imported with correct types

**Success Criteria:** ✅ Schema matches definition file, data imported correctly

### Test 2.3: Upsert Functionality
**Purpose:** Test primary key-based upserts

**Setup:** Create table with initial data
**Commands:**
```bash
# Initial import
python csv_importer.py --csv test_initial.csv --table test_upsert --create-table --primary-key id --force

# Update with new/modified records
python csv_importer.py --csv test_updates.csv --table test_upsert --primary-key id --force
```

**Expected Output:**
- Existing records updated
- New records inserted
- created_at preserved, updated_at changed

**Success Criteria:** ✅ Upserts work correctly, timestamps handled properly

### Test 2.4: Complex Data Types
**Purpose:** Test JSON, arrays, and special types

**Command:**
```bash
python csv_importer.py --csv test_complex_types.csv --table test_types --create-table --force
```

**Expected Output:**
- JSON data properly parsed and stored
- Arrays correctly handled
- Dates and timestamps converted
- Boolean values recognized

**Success Criteria:** ✅ All complex types handled correctly

### Test 2.5: Error Handling
**Purpose:** Test error scenarios and recovery

**Command:**
```bash
python csv_importer.py --csv test_bad_data.csv --table test_errors --create-table --force
```

**Expected Output:**
- Errors reported for bad rows
- Good rows still imported
- Error summary provided

**Success Criteria:** ✅ Graceful error handling, partial import succeeds

### Test 2.6: Existing Table (Append Mode)
**Purpose:** Test importing to existing table

**Setup:** Use table from previous test
**Command:**
```bash
python csv_importer.py --csv test_additional.csv --table test_types --if-exists append --force
```

**Expected Output:**
- Data appended to existing table
- No schema conflicts
- All records imported

**Success Criteria:** ✅ Data appended successfully

### Test 2.7: Table Replace Mode
**Purpose:** Test replacing existing table

**Command:**
```bash
python csv_importer.py --csv sample_data.csv --table test_types --if-exists replace --force
```

**Expected Output:**
- Old table dropped
- New table created with new schema
- New data imported

**Success Criteria:** ✅ Table replaced completely

---

## Part 3: Integration Tests

### Test 3.1: Schema Generator → CSV Importer Chain
**Purpose:** Test using schema generator to create table, then import data

**Commands:**
```bash
# Generate and create table
python schema_generator.py --from-labels books_columns.txt --table test_integration --create --primary-key isbn

# Import data to existing table
python csv_importer.py --csv test_books_data.csv --table test_integration --primary-key isbn --force
```

**Success Criteria:** ✅ Seamless workflow from schema to data

### Test 3.2: Export and Reimport
**Purpose:** Test data consistency through export/import cycle

**Commands:**
```bash
# Export existing data
python show_recent_books.py --table test_integration --out csv --outfile exported.csv

# Create new table and import
python csv_importer.py --csv exported.csv --table test_reimport --create-table --force
```

**Success Criteria:** ✅ Data consistency maintained

---

## Part 4: Edge Cases and Stress Tests

### Test 4.1: Large File Processing
**Purpose:** Test batch processing with large files

**Setup:** Create large CSV (1000+ rows)
**Command:**
```bash
python csv_importer.py --csv large_test.csv --table test_large --create-table --batch-size 100 --force
```

**Success Criteria:** ✅ Large file processed efficiently

### Test 4.2: Unicode and Special Characters
**Purpose:** Test international character handling

**Command:**
```bash
python csv_importer.py --csv test_unicode.csv --table test_unicode --create-table --force
```

**Success Criteria:** ✅ Unicode characters preserved

### Test 4.3: Empty and Null Values
**Purpose:** Test handling of missing data

**Command:**
```bash
python csv_importer.py --csv test_nulls.csv --table test_nulls --create-table --force
```

**Success Criteria:** ✅ Nulls handled appropriately

### Test 4.4: Column Name Conflicts
**Purpose:** Test PostgreSQL reserved word handling

**Command:**
```bash
python csv_importer.py --csv test_reserved_words.csv --table test_reserved --create-table --force
```

**Success Criteria:** ✅ Reserved words properly escaped

---

## Part 5: Performance and Reliability Tests

### Test 5.1: Connection Recovery
**Purpose:** Test database connection handling

**Method:** Interrupt connection during import, verify recovery

### Test 5.2: Transaction Integrity
**Purpose:** Ensure atomic operations

**Method:** Force failure mid-import, verify no partial data

### Test 5.3: Memory Usage
**Purpose:** Test memory efficiency with large files

**Method:** Monitor memory usage during large file import

---

## Expected Test Results Summary

| Test Category | Test Count | Expected Pass Rate |
|---------------|------------|-------------------|
| Schema Generator | 4 | 100% |
| CSV Importer Basic | 7 | 100% |
| Integration | 2 | 100% |
| Edge Cases | 4 | 100% |
| Performance | 3 | 100% |
| **Total** | **20** | **100%** |

---

## Test Execution Order

1. **Setup:** Clean environment, verify database connection
2. **Schema Generator:** Tests 1.1-1.4
3. **CSV Importer Basic:** Tests 2.1-2.7  
4. **Integration:** Tests 3.1-3.2
5. **Edge Cases:** Tests 4.1-4.4
6. **Performance:** Tests 5.1-5.3
7. **Cleanup:** Drop all test tables

---

## Failure Investigation

If any test fails:
1. Check database connection and permissions
2. Verify .env file configuration
3. Check PostgreSQL logs for errors
4. Validate test data file formats
5. Confirm Python dependencies are installed

---

## Success Criteria

All tests pass when:
- ✅ No Python exceptions or crashes
- ✅ Database operations complete successfully  
- ✅ Data integrity maintained throughout
- ✅ Expected outputs match actual results
- ✅ Performance within acceptable limits