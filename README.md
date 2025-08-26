# PGTools - PostgreSQL Schema and Data Management Library

A comprehensive Python library for PostgreSQL schema generation and CSV data import with intelligent type inference, upsert capabilities, and flexible table management.

## 🚀 Features

- **Automatic Schema Detection**: Smart PostgreSQL type inference from column names and data samples
- **Flexible CSV Import**: Import any CSV with automatic table creation and data type conversion
- **Upsert Support**: Primary key-based insert/update operations
- **Multiple Input Sources**: Column label files, existing tables, or auto-detection from CSV
- **Type Safety**: Comprehensive data type conversion and validation
- **Batch Processing**: Efficient handling of large datasets
- **CLI Tools**: Command-line interfaces maintaining backward compatibility

## 📦 Installation

```bash
# Install dependencies
pip install psycopg python-dotenv

# The library is ready to use directly
# No additional installation required for local development
```

## 🏗️ Library Structure

```
pgtools/
├── __init__.py              # Main library interface
├── core/                    # Core functionality
│   ├── csv_importer.py      # CSV import engine
│   ├── schema_generator.py  # Schema generation engine
│   └── database_manager.py  # Database operations
├── utils/                   # Utility modules
│   ├── type_inference.py    # Type inference engine
│   ├── data_converter.py    # Data type conversion
│   └── db_config.py         # Database configuration
└── cli/                     # Command-line interfaces
    ├── csv_importer_cli.py  # CSV importer CLI
    └── schema_generator_cli.py # Schema generator CLI
```

## 🎯 Quick Start

### Library Usage

```python
from pgtools import CSVImporter, SchemaGenerator

# Quick CSV import with auto-detection
importer = CSVImporter()
result = importer.import_csv("data.csv", table="my_table", create_table=True)
print(f"Imported {result.imported_count} records")

# Generate schema from column labels
generator = SchemaGenerator()
schema = generator.from_labels(["id", "name", "email", "created_at"])
generator.create_table(schema)

# Copy table structure
source_schema = generator.from_table("existing_table")
generator.create_table(source_schema.table_name + "_backup", source_schema)
```

### Command Line Usage

```bash
# Import CSV with automatic schema detection
python -m pgtools.cli.csv_importer_cli --csv data.csv --table users --create-table

# Generate schema from column labels file
python -m pgtools.cli.schema_generator_cli --from-labels columns.txt --table-name users --create

# Copy existing table structure
python -m pgtools.cli.schema_generator_cli --from-table products --table-name products_backup --create
```

## 📊 Core Classes

### CSVImporter

Import CSV data with automatic schema detection and type conversion.

```python
from pgtools import CSVImporter

# Initialize with database configuration
importer = CSVImporter(env_path=".env")

# Import with various options
result = importer.import_csv(
    csv_path="employees.csv",
    table="employees",
    create_table=True,           # Create table if it doesn't exist
    primary_key="id",            # Enable upserts
    if_exists="append",          # append|replace|fail
    columns_file="schema.txt",   # Optional predefined schema
    batch_size=1000             # Batch processing size
)

print(f"Imported: {result.imported_count}")
print(f"Errors: {result.error_count}")
print(f"Table created: {result.table_created}")
```

### SchemaGenerator

Generate PostgreSQL table schemas from various sources.

```python
from pgtools import SchemaGenerator

generator = SchemaGenerator()

# From column labels
schema = generator.from_labels(
    labels=["user_id", "full_name", "email", "is_active", "created_at"],
    table_name="users",
    primary_key="user_id",
    not_null=["full_name", "email"]
)

# From existing table
schema = generator.from_table("source_table", "backup_table")

# From labels file
schema = generator.from_labels_file("columns.txt", "new_table")

# Create table in database
generator.create_table(schema, if_exists="replace")

# Export schema
sql = generator.export_schema(schema, format="sql")
generator.save_schema(schema, "schema.sql")
```

### DatabaseManager

High-level database operations and connection management.

```python
from pgtools import DatabaseManager

db = DatabaseManager()

# Check table existence
if db.table_exists("users"):
    print("Table exists")

# Get table schema
schema = db.get_table_schema("users")

# Execute queries
results = db.execute_query("SELECT * FROM users LIMIT 10")

# Table operations
db.create_table("new_table", schema_definition)
db.drop_table("old_table")
```

## 🛠️ Configuration

### Database Configuration (.env file)

```bash
# Option 1: Database URL (recommended)
DATABASE_URL=postgresql://user:password@localhost:5432/database

# Option 2: Individual components
PGHOST=localhost
PGPORT=5432
PGDATABASE=mydb
PGUSER=myuser
PGPASSWORD=mypassword

# Alternative naming convention
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=myuser
DB_PASSWORD=mypassword
```

### Column Definitions File

Text format:
```txt
# comments start with #
id:SERIAL PRIMARY KEY
name:VARCHAR(100)
email:TEXT
created_at:TIMESTAMPTZ
is_active:BOOLEAN
metadata:JSONB
```

JSON format:
```json
{
  "columns": [
    {"name": "id", "type": "SERIAL PRIMARY KEY"},
    {"name": "name", "type": "VARCHAR(100)"},
    {"name": "email", "type": "TEXT"},
    {"name": "metadata", "type": "JSONB"}
  ]
}
```

## 🎨 Type Inference

The library intelligently infers PostgreSQL types based on:

### Column Name Patterns
- `id`, `*_id` → INTEGER/SERIAL PRIMARY KEY
- `*_at`, `timestamp` → TIMESTAMPTZ  
- `*_date`, `birthday` → DATE
- `is_*`, `has_*`, `active` → BOOLEAN
- `price`, `amount`, `total` → NUMERIC(10,2)
- `count`, `quantity` → INTEGER
- `email`, `url` → TEXT
- `phone` → VARCHAR(20)
- `tags`, `categories` (plural) → TEXT[]
- `metadata`, `config`, `*_data` → JSONB

### Data Value Analysis
- Numeric strings → INTEGER/NUMERIC
- Boolean strings (true/false, y/n, 1/0) → BOOLEAN
- Date strings (various formats) → DATE/TIMESTAMPTZ
- JSON strings → JSONB
- Semicolon/comma separated → TEXT[]

## 📈 Advanced Features

### Upsert Operations

```python
# Enable upserts with primary key
result = importer.import_csv(
    "updates.csv", 
    table="users", 
    primary_key="id"  # Enables ON CONFLICT DO UPDATE
)
```

### Batch Processing

```python
# Process large files efficiently
result = importer.import_csv(
    "big_data.csv",
    table="large_table",
    batch_size=5000,  # Process in batches of 5000
    progress_callback=lambda current, total: print(f"{current}/{total}")
)
```

### Context Manager Usage

```python
# Automatic connection cleanup
with CSVImporter() as importer:
    result = importer.import_csv("data.csv", "table")
    
with SchemaGenerator() as generator:
    schema = generator.from_labels(["id", "name"])
    generator.create_table(schema)
```

### Error Handling

```python
result = importer.import_csv("messy_data.csv", "clean_table")

if result.error_count > 0:
    print(f"Skipped {result.error_count} bad rows:")
    for error in result.errors:
        print(f"  - {error}")
        
print(f"Successfully imported {result.imported_count} records")
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Run all tests
python run_tests.py

# Quick smoke test
python -c "from pgtools import import_csv; import_csv('sample_data.csv', 'test')"
```

## 📝 Migration from Standalone Scripts

The library maintains full backward compatibility with the original scripts:

```bash
# Old way
python csv_importer.py --csv data.csv --table users --create-table

# New way (same functionality)
python -m pgtools.cli.csv_importer_cli --csv data.csv --table users --create-table

# Or use the library directly
python -c "from pgtools import import_csv; import_csv('data.csv', 'users', create_table=True)"
```

## 🎭 Examples

See the `examples/` directory for comprehensive usage examples:
- `basic_import.py` - Simple CSV import
- `schema_management.py` - Schema generation and management
- `advanced_features.py` - Upserts, batch processing, error handling
- `database_operations.py` - Direct database operations

## 🤝 Contributing

1. All original functionality is preserved in the library
2. New features should include comprehensive tests
3. CLI interfaces maintain backward compatibility
4. Documentation should be updated for new features

## 📄 License

MIT License - see original file headers for details.

## 🐛 Troubleshooting

### Common Issues

**Import Error**: Ensure `psycopg` and `python-dotenv` are installed
```bash
pip install psycopg python-dotenv
```

**Connection Error**: Check your `.env` file configuration and database connectivity
```python
from pgtools.utils.db_config import DatabaseConfig
config = DatabaseConfig()
print(config)  # Shows loaded configuration
```

**Type Inference Issues**: Use explicit column definitions file for precise control
```python
# Use columns file for exact type control
result = importer.import_csv("data.csv", "table", columns_file="types.txt")
```

**Large File Performance**: Increase batch size and use progress callbacks
```python
result = importer.import_csv(
    "huge.csv", "table", 
    batch_size=10000,
    progress_callback=lambda c, t: print(f"Progress: {c/t*100:.1f}%")
)
```