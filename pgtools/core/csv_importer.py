"""
CSV import core module for PGTools.

This module provides the CSVImporter class for importing CSV data into
PostgreSQL tables with automatic schema detection, data type conversion,
and upsert capabilities.
"""

import csv
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

from psycopg import sql

from .database_manager import DatabaseManager
from .schema_generator import SchemaGenerator, Schema
from ..utils.type_inference import TypeInference
from ..utils.data_converter import DataConverter


class ImportResult:
    """Results from a CSV import operation."""
    
    def __init__(self):
        self.imported_count = 0
        self.error_count = 0
        self.errors = []
        self.table_created = False
        self.schema_detected = None
        self.processing_time = 0.0
    
    def __repr__(self) -> str:
        return (f"ImportResult(imported={self.imported_count}, "
                f"errors={self.error_count}, created={self.table_created})")


class CSVImporter:
    """CSV data importer with automatic schema detection and type conversion."""
    
    def __init__(self, env_path: Optional[str] = None, **connection_params):
        """Initialize CSV importer.
        
        Args:
            env_path: Path to .env file for database configuration
            **connection_params: Direct connection parameters
        """
        self.db_manager = DatabaseManager(env_path, **connection_params)
        self.schema_generator = SchemaGenerator(env_path, **connection_params)
    
    def import_csv(self, csv_path: str, table: str, schema_name: str = "public",
                  create_table: bool = False, columns_file: Optional[str] = None,
                  primary_key: Optional[str] = None, if_exists: str = "append",
                  delimiter: str = ",", encoding: str = "utf-8-sig",
                  sample_rows: int = 100, batch_size: int = 1000,
                  progress_callback: Optional[callable] = None) -> ImportResult:
        """Import CSV data into PostgreSQL table.
        
        Args:
            csv_path: Path to CSV file
            table: Target table name
            schema_name: Target schema name (default: public)
            create_table: Create table if it doesn't exist
            columns_file: Path to column definitions file
            primary_key: Column to use as primary key (enables upserts)
            if_exists: Action if table exists ('fail', 'append', 'replace')
            delimiter: CSV delimiter (default: ,)
            encoding: CSV encoding (default: utf-8-sig)
            sample_rows: Number of rows to sample for type detection
            batch_size: Batch size for database operations
            progress_callback: Optional progress callback function
            
        Returns:
            ImportResult with import statistics
        """
        start_time = datetime.now()
        result = ImportResult()
        
        # Check if table exists
        table_exists = self.db_manager.table_exists(table, schema_name)
        
        if table_exists and if_exists == "fail":
            raise SystemExit(f"Table {schema_name}.{table} already exists")
        
        # Determine schema
        if columns_file:
            # Use predefined column definitions
            table_schema = self._load_schema_from_file(
                columns_file, csv_path, table, schema_name, delimiter, encoding, sample_rows
            )
        else:
            # Auto-detect schema from CSV
            table_schema = self._auto_detect_schema(
                csv_path, table, schema_name, delimiter, encoding, sample_rows, primary_key
            )
        
        result.schema_detected = table_schema
        
        # Handle table creation/replacement
        if if_exists == "replace" and table_exists:
            self.db_manager.drop_table(table, schema_name)
            table_exists = False
        
        if not table_exists and (create_table or if_exists == "replace"):
            self.schema_generator.create_table(table_schema)
            result.table_created = True
        elif not table_exists:
            raise SystemExit(f"Table {schema_name}.{table} does not exist. Use create_table=True")
        
        # Process and import CSV data
        records, errors = self._process_csv_data(
            csv_path, table_schema, delimiter, encoding
        )
        
        result.error_count = len(errors)
        result.errors = errors
        
        if records:
            # Import data
            imported = self._import_records(
                records, table_schema, primary_key, batch_size, progress_callback
            )
            result.imported_count = imported
        
        result.processing_time = (datetime.now() - start_time).total_seconds()
        return result
    
    def _auto_detect_schema(self, csv_path: str, table_name: str, schema_name: str,
                           delimiter: str, encoding: str, sample_rows: int, 
                           primary_key: Optional[str] = None) -> Schema:
        """Auto-detect schema from CSV file."""
        with open(csv_path, "r", newline="", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if not reader.fieldnames:
                raise SystemExit("CSV file has no headers")
            
            # Sample data for type inference
            sample_data = {}
            for i, row in enumerate(reader):
                if i >= sample_rows:
                    break
                for field, value in row.items():
                    if field not in sample_data:
                        sample_data[field] = []
                    sample_data[field].append(value or "")
        
        # Generate schema
        schema = Schema(table_name, schema_name)
        
        for field in reader.fieldnames:
            normalized_name = TypeInference.normalize_column_name(field)
            sample_values = sample_data.get(field, [])
            col_type = TypeInference.infer_type(normalized_name, sample_values)
            
            # Handle primary key specification
            constraints = []
            if primary_key and normalized_name == primary_key:
                # Ensure the primary key column has appropriate type and constraint
                if not col_type.endswith("PRIMARY KEY"):
                    if col_type.startswith("SERIAL"):
                        col_type = "SERIAL PRIMARY KEY"
                    else:
                        constraints.append("PRIMARY KEY")
            
            schema.add_column(
                name=normalized_name,
                data_type=col_type,
                constraints=constraints,
                original_name=field
            )
        
        # Add standard metadata columns
        self._add_standard_columns(schema)
        
        schema.source_info = {
            "source_type": "csv_auto_detect",
            "csv_file": csv_path,
            "sample_rows": sample_rows
        }
        
        return schema
    
    def _load_schema_from_file(self, columns_file: str, csv_path: str, table_name: str,
                              schema_name: str, delimiter: str, encoding: str,
                              sample_rows: int) -> Schema:
        """Load schema from column definitions file."""
        # Read column definitions
        column_defs = self._read_column_definitions(columns_file)
        
        # Get CSV headers for mapping
        with open(csv_path, "r", newline="", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            csv_headers = reader.fieldnames or []
        
        schema = Schema(table_name, schema_name)
        
        # Create mapping and add columns
        for col_def in column_defs:
            col_name = col_def["name"]
            col_type = col_def.get("type")
            
            # Find matching CSV header
            original_name = None
            for header in csv_headers:
                if TypeInference.normalize_column_name(header) == col_name:
                    original_name = header
                    break
            
            # Infer type if not specified
            if not col_type and original_name:
                # Auto-detect type for this column
                auto_schema = self._auto_detect_schema(
                    csv_path, table_name, schema_name, delimiter, encoding, sample_rows
                )
                auto_col = auto_schema.get_column(col_name)
                col_type = auto_col["type"] if auto_col else "TEXT"
            elif not col_type:
                col_type = "TEXT"
            
            schema.add_column(
                name=col_name,
                data_type=col_type,
                original_name=original_name
            )
        
        # Add standard metadata columns
        self._add_standard_columns(schema)
        
        schema.source_info = {
            "source_type": "column_file",
            "columns_file": columns_file,
            "csv_file": csv_path
        }
        
        return schema
    
    def _read_column_definitions(self, file_path: str) -> List[Dict[str, Any]]:
        """Read column definitions from file."""
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        
        # Try JSON format first
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "columns" in data:
                return data["columns"]
        except json.JSONDecodeError:
            pass
        
        # Simple text format - one column per line
        columns = []
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                if ':' in line:
                    name, col_type = line.split(':', 1)
                    columns.append({"name": name.strip(), "type": col_type.strip()})
                else:
                    columns.append({"name": line, "type": None})
        
        return columns
    
    def _add_standard_columns(self, schema: Schema):
        """Add standard metadata columns to schema."""
        column_names = {col["name"] for col in schema.columns}
        
        if "metadata" not in column_names:
            schema.add_column("metadata", "JSONB")
        
        if "created_at" not in column_names:
            schema.add_column("created_at", "TIMESTAMPTZ", ["DEFAULT NOW()"])
        
        if "updated_at" not in column_names:
            schema.add_column("updated_at", "TIMESTAMPTZ", ["DEFAULT NOW()"])
    
    def _process_csv_data(self, csv_path: str, schema: Schema, delimiter: str, 
                         encoding: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Process CSV data according to schema."""
        # Create mappings
        name_mapping = {}
        type_mapping = {}
        
        for col in schema.columns:
            original = col.get("original_name", col["name"])
            normalized = col["name"]
            name_mapping[original] = normalized
            type_mapping[normalized] = col["type"]
        
        records = []
        errors = []
        
        with open(csv_path, "r", newline="", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            
            for i, row in enumerate(reader, start=2):  # Start at 2 (header is line 1)
                try:
                    record = {}
                    metadata = {}
                    
                    for original_name, value in row.items():
                        if original_name in name_mapping:
                            normalized_name = name_mapping[original_name]
                            col_type = type_mapping[normalized_name]
                            record[normalized_name] = DataConverter.convert_value(value, col_type)
                        else:
                            # Extra columns go to metadata
                            if value and value.strip():
                                metadata[original_name] = value.strip()
                    
                    # Add metadata if any extra columns
                    if metadata:
                        record["metadata"] = json.dumps(metadata)
                    
                    # Add timestamps
                    now = datetime.now()
                    if "created_at" not in record:
                        record["created_at"] = now
                    if "updated_at" not in record:
                        record["updated_at"] = now
                    
                    records.append(record)
                    
                except Exception as e:
                    errors.append(f"Row {i}: {e}")
        
        return records, errors
    
    def _import_records(self, records: List[Dict[str, Any]], schema: Schema,
                       primary_key: Optional[str], batch_size: int,
                       progress_callback: Optional[callable] = None) -> int:
        """Import records into database."""
        # Create insert/upsert SQL
        insert_sql, column_order = self._create_insert_sql(schema, primary_key)
        
        imported_count = 0
        conn = self.db_manager.get_connection()
        
        with conn.cursor() as cur:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Ensure all records have all required columns
                for record in batch:
                    for col in column_order:
                        if col not in record:
                            record[col] = None
                
                cur.executemany(insert_sql, batch)
                imported_count += len(batch)
                
                if progress_callback:
                    progress_callback(imported_count, len(records))
        
        conn.commit()
        return imported_count
    
    def _create_insert_sql(self, schema: Schema, primary_key: Optional[str]) -> Tuple[sql.SQL, List[str]]:
        """Create INSERT or UPSERT SQL statement."""
        columns = [col["name"] for col in schema.columns]
        placeholders = [f"%({col})s" for col in columns]
        
        # Check if the primary key column actually has a unique constraint
        can_upsert = False
        if primary_key:
            # Check if the primary key column exists and has appropriate constraints
            pk_column = None
            for col in schema.columns:
                if col["name"] == primary_key:
                    pk_column = col
                    break
            
            if pk_column:
                constraints = pk_column.get("constraints", [])
                # Check if column has PRIMARY KEY or UNIQUE constraint
                can_upsert = any("PRIMARY KEY" in str(c) or "UNIQUE" in str(c) for c in constraints)
                
                # If no constraint in schema, check if table exists and has the constraint
                if not can_upsert:
                    try:
                        # Check if the table exists and has a primary key on this column
                        existing_schema = self.db_manager.get_table_schema(schema.table_name, schema.schema_name)
                        for existing_col in existing_schema:
                            if existing_col["name"] == primary_key:
                                existing_constraints = existing_col.get("constraints", [])
                                can_upsert = any("PRIMARY KEY" in str(c) or "UNIQUE" in str(c) for c in existing_constraints)
                                break
                    except:
                        # If we can't check the existing table, don't use upsert
                        can_upsert = False
        
        if primary_key and can_upsert:
            # UPSERT with ON CONFLICT
            sql_template = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({pk}) DO UPDATE SET
                    {updates},
                    updated_at = NOW()
            """)
            
            updates = [
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                for col in columns if col not in (primary_key, "created_at", "updated_at")
            ]
            
            query = sql_template.format(
                table=sql.Identifier(schema.schema_name, schema.table_name),
                columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
                placeholders=sql.SQL(", ").join(sql.SQL(p) for p in placeholders),
                pk=sql.Identifier(primary_key),
                updates=sql.SQL(", ").join(updates)
            )
        else:
            # Simple INSERT
            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
            """).format(
                table=sql.Identifier(schema.schema_name, schema.table_name),
                columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
                placeholders=sql.SQL(", ").join(sql.SQL(p) for p in placeholders)
            )
        
        return query, columns
    
    def close(self):
        """Close database connections."""
        self.db_manager.close_connection()
        self.schema_generator.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()