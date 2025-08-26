"""
PGTools - PostgreSQL Schema and Data Management Library

A comprehensive Python library for PostgreSQL schema generation and CSV data import
with intelligent type inference, upsert capabilities, and flexible table management.

Main Classes:
    SchemaGenerator: Generate and manage PostgreSQL table schemas
    CSVImporter: Import CSV data with automatic schema detection
    DatabaseManager: High-level database operations and utilities

Quick Start:
    from pgtools import CSVImporter, SchemaGenerator
    
    # Import CSV with auto-detection
    importer = CSVImporter()
    importer.import_csv("data.csv", table="my_table", create_table=True)
    
    # Generate schema from column labels
    generator = SchemaGenerator()
    schema = generator.from_labels(["id", "name", "email"])
    generator.create_table("users", schema)

Version: 1.0.0
Author: PostgreSQL Tools
License: MIT
"""

from .core.schema_generator import SchemaGenerator
from .core.csv_importer import CSVImporter
from .core.database_manager import DatabaseManager
from .utils.type_inference import TypeInference
from .utils.data_converter import DataConverter
from .utils.db_config import DatabaseConfig

__version__ = "1.0.0"
__author__ = "PostgreSQL Tools"
__license__ = "MIT"

__all__ = [
    "SchemaGenerator",
    "CSVImporter", 
    "DatabaseManager",
    "TypeInference",
    "DataConverter",
    "DatabaseConfig"
]

# Convenience functions for quick usage
def import_csv(csv_path: str, table: str, **kwargs):
    """Quick CSV import function.
    
    Args:
        csv_path: Path to CSV file
        table: Target table name
        **kwargs: Additional options (create_table, primary_key, etc.)
    
    Returns:
        ImportResult object with statistics
    """
    importer = CSVImporter()
    return importer.import_csv(csv_path, table, **kwargs)

def generate_schema(source, **kwargs):
    """Quick schema generation function.
    
    Args:
        source: Column labels file path or existing table name
        **kwargs: Additional options (table_name, primary_key, etc.)
    
    Returns:
        Schema object
    """
    generator = SchemaGenerator()
    if source.endswith(('.txt', '.json')):
        return generator.from_labels_file(source, **kwargs)
    else:
        return generator.from_table(source, **kwargs)