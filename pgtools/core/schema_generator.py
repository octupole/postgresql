"""
Schema generation core module for PGTools.

This module provides the SchemaGenerator class for creating PostgreSQL
table schemas from various sources including column label files,
existing tables, and programmatic definitions.
"""

import json
import os
import re
from typing import Dict, Any, List, Optional, Union

from .database_manager import DatabaseManager
from ..utils.type_inference import TypeInference


class Schema:
    """Represents a PostgreSQL table schema."""
    
    def __init__(self, table_name: str, schema_name: str = "public", columns: Optional[List[Dict[str, Any]]] = None):
        """Initialize schema.
        
        Args:
            table_name: Name of the table
            schema_name: Database schema name (default: public)
            columns: List of column definitions
        """
        self.table_name = table_name
        self.schema_name = schema_name
        self.columns = columns or []
        self.source_info = {}
    
    def add_column(self, name: str, data_type: str, constraints: Optional[List[str]] = None, **kwargs):
        """Add a column to the schema.
        
        Args:
            name: Column name
            data_type: PostgreSQL data type
            constraints: List of constraints (NOT NULL, PRIMARY KEY, etc.)
            **kwargs: Additional column metadata
        """
        column = {
            "name": name,
            "type": data_type,
            "constraints": constraints or [],
            **kwargs
        }
        self.columns.append(column)
    
    def get_column(self, name: str) -> Optional[Dict[str, Any]]:
        """Get column definition by name."""
        for col in self.columns:
            if col["name"] == name:
                return col
        return None
    
    def remove_column(self, name: str) -> bool:
        """Remove column by name. Returns True if removed."""
        for i, col in enumerate(self.columns):
            if col["name"] == name:
                del self.columns[i]
                return True
        return False
    
    def to_sql(self) -> str:
        """Generate CREATE TABLE SQL statement."""
        lines = [f"CREATE TABLE {self.schema_name}.{self.table_name} ("]
        column_lines = []
        
        for col in self.columns:
            parts = [f'    {col["name"]}', col["type"]]
            if col.get("constraints"):
                parts.extend(col["constraints"])
            column_lines.append(" ".join(parts))
        
        lines.append(",\n".join(column_lines))
        lines.append(");")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "columns": self.columns,
            "source_info": self.source_info
        }
    
    def __repr__(self) -> str:
        """String representation."""
        return f"Schema(table='{self.schema_name}.{self.table_name}', columns={len(self.columns)})"


class SchemaGenerator:
    """PostgreSQL schema generator from various sources."""
    
    def __init__(self, env_path: Optional[str] = None, **connection_params):
        """Initialize schema generator.
        
        Args:
            env_path: Path to .env file for database configuration
            **connection_params: Direct connection parameters
        """
        self.db_manager = DatabaseManager(env_path, **connection_params)
    
    def from_labels(self, labels: List[str], table_name: str = "new_table", 
                   schema_name: str = "public", primary_key: Optional[str] = None,
                   not_null: Optional[List[str]] = None) -> Schema:
        """Generate schema from list of column labels.
        
        Args:
            labels: List of column label strings
            table_name: Target table name
            schema_name: Target schema name
            primary_key: Column to use as primary key
            not_null: Columns that should be NOT NULL
            
        Returns:
            Schema object
        """
        not_null = not_null or []
        schema = Schema(table_name, schema_name)
        
        for label in labels:
            # Normalize column name
            col_name = TypeInference.normalize_column_name(label)
            col_type = TypeInference.infer_from_name(col_name)
            
            # Handle primary key override
            if primary_key and col_name == primary_key:
                if not col_type.endswith("PRIMARY KEY"):
                    col_type = "SERIAL PRIMARY KEY"
            elif col_type.endswith("PRIMARY KEY") and primary_key and col_name != primary_key:
                # Remove auto-detected primary key if different one specified
                col_type = col_type.replace(" PRIMARY KEY", "").strip()
            
            # Handle NOT NULL constraint
            constraints = []
            if col_name in not_null or col_type.endswith("PRIMARY KEY"):
                constraints.append("NOT NULL")
            
            schema.add_column(
                name=col_name,
                data_type=col_type,
                constraints=constraints,
                original_label=label
            )
        
        schema.source_info = {
            "source_type": "labels",
            "labels": labels
        }
        
        return schema
    
    def from_labels_file(self, file_path: str, table_name: str = "new_table",
                        schema_name: str = "public", **kwargs) -> Schema:
        """Generate schema from column labels file.
        
        Args:
            file_path: Path to file with column labels
            table_name: Target table name
            schema_name: Target schema name
            **kwargs: Additional options (primary_key, not_null, etc.)
            
        Returns:
            Schema object
        """
        labels = self._read_labels_file(file_path)
        schema = self.from_labels(labels, table_name, schema_name, **kwargs)
        schema.source_info["source_file"] = file_path
        return schema
    
    def from_table(self, source_table: str, table_name: Optional[str] = None,
                  schema_name: str = "public") -> Schema:
        """Generate schema from existing database table.
        
        Args:
            source_table: Source table name (format: [schema.]table)
            table_name: Target table name (default: same as source)
            schema_name: Target schema name
            
        Returns:
            Schema object
        """
        # Parse source table name
        if "." in source_table:
            src_schema, src_table = source_table.split(".", 1)
        else:
            src_schema, src_table = "public", source_table
        
        target_table = table_name or src_table
        
        # Get schema from existing table
        table_schema = self.db_manager.get_table_schema(src_table, src_schema)
        
        schema = Schema(target_table, schema_name)
        for col_info in table_schema:
            schema.add_column(
                name=col_info["name"],
                data_type=col_info["type"],
                constraints=col_info["constraints"]
            )
        
        schema.source_info = {
            "source_type": "table",
            "source_table": f"{src_schema}.{src_table}"
        }
        
        return schema
    
    def create_table(self, schema: Union[Schema, str], if_exists: str = "fail") -> bool:
        """Create table in database from schema.
        
        Args:
            schema: Schema object or table name (for existing schemas)
            if_exists: Action if table exists ('fail', 'replace', 'skip')
            
        Returns:
            True if table was created, False if skipped
        """
        if isinstance(schema, str):
            # Assume it's a table name and we should use from_table
            schema_obj = self.from_table(schema)
        else:
            schema_obj = schema
        
        return self.db_manager.create_table(
            schema_obj.table_name,
            schema_obj.columns,
            schema_obj.schema_name,
            if_exists
        )
    
    def _read_labels_file(self, file_path: str) -> List[str]:
        """Read column labels from file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Labels file not found: {file_path}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        
        # Try JSON format first
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "columns" in data:
                return [col.get("name", col) if isinstance(col, dict) else col 
                       for col in data["columns"]]
        except json.JSONDecodeError:
            pass
        
        # Simple text format - one label per line
        labels = []
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                # Support "column_name:type" format, but just take the name
                if ':' in line:
                    labels.append(line.split(':', 1)[0].strip())
                else:
                    labels.append(line)
        
        return labels
    
    def export_schema(self, schema: Schema, format: str = "sql") -> str:
        """Export schema in specified format.
        
        Args:
            schema: Schema object to export
            format: Export format ('sql', 'json', 'dict')
            
        Returns:
            Formatted schema string
        """
        if format == "sql":
            return schema.to_sql()
        elif format == "json":
            return json.dumps(schema.to_dict(), indent=2, ensure_ascii=False)
        elif format == "dict":
            return str(schema.to_dict())
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def save_schema(self, schema: Schema, file_path: str, format: str = "sql"):
        """Save schema to file.
        
        Args:
            schema: Schema object to save
            file_path: Output file path
            format: Export format ('sql', 'json')
        """
        content = self.export_schema(schema, format)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
    
    def close(self):
        """Close database connection."""
        self.db_manager.close_connection()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()