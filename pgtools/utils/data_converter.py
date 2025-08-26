"""
Data conversion utilities for PostgreSQL data types.

This module provides functions to convert string values from CSV files
to appropriate Python types for PostgreSQL insertion.
"""

import json
from datetime import datetime
from typing import Any, Optional


class DataConverter:
    """Converts string values to appropriate PostgreSQL types."""
    
    @staticmethod
    def convert_value(value: str, postgres_type: str) -> Any:
        """Convert string value to appropriate Python type for PostgreSQL.
        
        Args:
            value: String value to convert
            postgres_type: Target PostgreSQL data type
            
        Returns:
            Converted value suitable for psycopg insertion
        """
        if not value or value.strip() == "":
            return None
        
        value = value.strip()
        type_upper = postgres_type.upper()
        
        try:
            # Integer types
            if any(t in type_upper for t in ["INTEGER", "SERIAL", "BIGINT", "SMALLINT"]):
                return int(value)
            
            # Numeric/Decimal types
            elif any(t in type_upper for t in ["NUMERIC", "DECIMAL", "REAL", "DOUBLE"]):
                return float(value)
            
            # Boolean type
            elif "BOOLEAN" in type_upper:
                return DataConverter._convert_boolean(value)
            
            # Date type
            elif "DATE" in type_upper and "TIMESTAMP" not in type_upper:
                return DataConverter._convert_date(value)
            
            # Timestamp types
            elif "TIMESTAMP" in type_upper:
                return DataConverter._convert_timestamp(value)
            
            # Array types
            elif "[]" in type_upper:
                return DataConverter._convert_array(value)
            
            # JSON/JSONB types
            elif any(t in type_upper for t in ["JSON", "JSONB"]):
                return DataConverter._convert_json(value)
            
            # Default: return as string
            else:
                return value
                
        except (ValueError, TypeError):
            # If conversion fails, return None or original value based on type
            if any(t in type_upper for t in ["INTEGER", "NUMERIC", "DECIMAL", "REAL", "DOUBLE"]):
                return None  # Numeric types should be None if conversion fails
            return value  # Text types can keep original value
    
    @staticmethod
    def _convert_boolean(value: str) -> bool:
        """Convert string to boolean."""
        value_lower = value.lower()
        return value_lower in ('true', 't', 'yes', 'y', '1')
    
    @staticmethod
    def _convert_date(value: str) -> Optional[datetime]:
        """Convert string to date."""
        date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m", "%Y"]
        for fmt in date_formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None
    
    @staticmethod
    def _convert_timestamp(value: str) -> Optional[datetime]:
        """Convert string to timestamp."""
        timestamp_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d"
        ]
        for fmt in timestamp_formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None
    
    @staticmethod
    def _convert_array(value: str) -> list:
        """Convert string to array."""
        if ';' in value:
            items = [item.strip() for item in value.split(';') if item.strip()]
        elif ',' in value:
            items = [item.strip() for item in value.split(',') if item.strip()]
        else:
            items = [value]
        return items
    
    @staticmethod
    def _convert_json(value: str) -> str:
        """Convert string to JSON (returns JSON string for psycopg)."""
        try:
            # Parse to validate JSON, then return as string
            parsed = json.loads(value)
            return json.dumps(parsed)
        except json.JSONDecodeError:
            # If not valid JSON, wrap in a simple object
            return json.dumps({"raw_value": value})
    
    @staticmethod
    def prepare_record(record: dict, type_mapping: dict) -> dict:
        """Prepare a record for database insertion by converting all values.
        
        Args:
            record: Dictionary of column_name -> value
            type_mapping: Dictionary of column_name -> postgres_type
            
        Returns:
            Dictionary with converted values
        """
        converted_record = {}
        
        for column, value in record.items():
            if column in type_mapping:
                postgres_type = type_mapping[column]
                converted_record[column] = DataConverter.convert_value(value, postgres_type)
            else:
                # If no type mapping, keep as string
                converted_record[column] = value
        
        return converted_record