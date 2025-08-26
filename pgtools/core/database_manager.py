"""
Database management utilities for PGTools.

This module provides high-level database operations including connection
management, table operations, and query execution.
"""

from typing import Dict, Any, List, Optional, Set
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from ..utils.db_config import DatabaseConfig


class DatabaseManager:
    """High-level database operations manager."""
    
    def __init__(self, env_path: Optional[str] = None, **connection_params):
        """Initialize database manager.
        
        Args:
            env_path: Path to .env file for configuration
            **connection_params: Direct connection parameters (dsn, host, port, etc.)
        """
        if connection_params:
            # Direct connection parameters provided
            self.db_config = None
            self._connection_params = connection_params
        else:
            # Use configuration from .env file
            self.db_config = DatabaseConfig(env_path)
            self._connection_params = None
        
        self._connection: Optional[psycopg.Connection] = None
    
    def get_connection(self) -> psycopg.Connection:
        """Get database connection, creating it if necessary.
        
        Returns:
            Active psycopg connection
        """
        if self._connection is None or self._connection.closed:
            if self._connection_params:
                conn_params = self._connection_params
            else:
                conn_params = self.db_config.get_connection_params()
            
            if "dsn" in conn_params:
                self._connection = psycopg.connect(dsn=conn_params["dsn"])
            else:
                self._connection = psycopg.connect(**conn_params)
            
            self._connection.row_factory = dict_row
        
        return self._connection
    
    def close_connection(self):
        """Close database connection if open."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None
    
    def table_exists(self, table_name: str, schema_name: str = "public") -> bool:
        """Check if table exists in the database.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (default: public)
            
        Returns:
            True if table exists, False otherwise
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                ) AS table_exists
            """, (schema_name, table_name))
            result = cur.fetchone()
            return result["table_exists"] if isinstance(result, dict) else result[0]
    
    def get_table_columns(self, table_name: str, schema_name: str = "public") -> Set[str]:
        """Get column names from existing table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (default: public)
            
        Returns:
            Set of column names
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (schema_name, table_name))
            return {row["column_name"] if isinstance(row, dict) else row[0] for row in cur.fetchall()}
    
    def get_table_schema(self, table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
        """Get complete schema information for a table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (default: public)
            
        Returns:
            List of column information dictionaries
        """
        conn = self.get_connection()
        
        with conn.cursor() as cur:
            # Get column information
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            
            col_info = cur.fetchall()
            if not col_info:
                raise ValueError(f"Table {schema_name}.{table_name} not found")
            
            # Get constraints
            cur.execute("""
                SELECT 
                    tc.constraint_type,
                    kcu.column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = %s AND tc.table_name = %s
            """, (schema_name, table_name))
            
            constraints_info = cur.fetchall()
        
        # Build constraints map
        constraints_map = {}
        for constraint in constraints_info:
            col_name = constraint["column_name"]
            if col_name not in constraints_map:
                constraints_map[col_name] = []
            constraints_map[col_name].append(constraint["constraint_type"])
        
        # Build schema list
        schema = []
        for col in col_info:
            name = col["column_name"]
            
            # Build type string
            data_type = col["data_type"].upper()
            if col["character_maximum_length"]:
                col_type = f"{data_type}({col['character_maximum_length']})"
            elif col["numeric_precision"] and col["numeric_scale"]:
                col_type = f"{data_type}({col['numeric_precision']},{col['numeric_scale']})"
            elif col["numeric_precision"]:
                col_type = f"{data_type}({col['numeric_precision']})"
            else:
                col_type = data_type
            
            # Handle special cases
            if data_type == "CHARACTER VARYING":
                col_type = col_type.replace("CHARACTER VARYING", "VARCHAR")
            
            # Build constraints
            constraints = []
            if name in constraints_map:
                if "PRIMARY KEY" in constraints_map[name]:
                    constraints.append("PRIMARY KEY")
                if "UNIQUE" in constraints_map[name]:
                    constraints.append("UNIQUE")
            
            if col["is_nullable"] == "NO":
                constraints.append("NOT NULL")
            
            if col["column_default"]:
                constraints.append(f"DEFAULT {col['column_default']}")
            
            schema.append({
                "name": name,
                "type": col_type,
                "constraints": constraints
            })
        
        return schema
    
    def create_table(self, table_name: str, schema: List[Dict[str, Any]], 
                    schema_name: str = "public", if_exists: str = "fail") -> bool:
        """Create table in database.
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions
            schema_name: Schema name (default: public)
            if_exists: Action if table exists ('fail', 'replace', 'skip')
            
        Returns:
            True if table was created, False if skipped
            
        Raises:
            SystemExit: If table exists and if_exists='fail'
        """
        exists = self.table_exists(table_name, schema_name)
        
        if exists:
            if if_exists == "skip":
                return False
            elif if_exists == "fail":
                raise SystemExit(f"Table {schema_name}.{table_name} already exists")
            elif if_exists == "replace":
                self.drop_table(table_name, schema_name)
        
        # Build CREATE TABLE SQL
        lines = [f"CREATE TABLE {schema_name}.{table_name} ("]
        column_lines = []
        
        for col in schema:
            parts = [f'    {col["name"]}', col["type"]]
            if col.get("constraints"):
                parts.extend(col["constraints"])
            column_lines.append(" ".join(parts))
        
        lines.append(",\n".join(column_lines))
        lines.append(");")
        
        create_sql = "\n".join(lines)
        
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        
        return True
    
    def drop_table(self, table_name: str, schema_name: str = "public", if_exists: bool = True):
        """Drop table from database.
        
        Args:
            table_name: Name of the table to drop
            schema_name: Schema name (default: public)
            if_exists: Don't error if table doesn't exist
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            if if_exists:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(schema_name), sql.Identifier(table_name)))
            else:
                cur.execute(sql.SQL("DROP TABLE {}.{}").format(
                    sql.Identifier(schema_name), sql.Identifier(table_name)))
        conn.commit()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an UPDATE/INSERT/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rowcount = cur.rowcount
        conn.commit()
        return rowcount
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_connection()
    
    def __repr__(self) -> str:
        """String representation."""
        if self.db_config:
            return f"DatabaseManager({self.db_config})"
        else:
            return f"DatabaseManager(direct_connection={bool(self._connection_params)})"