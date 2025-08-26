"""
Type inference utilities for PostgreSQL schema generation.

This module provides intelligent type inference for PostgreSQL columns
based on column names and sample data values.
"""

import json
import re
from datetime import datetime
from typing import List, Optional


class TypeInference:
    """PostgreSQL type inference engine."""
    
    @staticmethod
    def normalize_column_name(name: str) -> str:
        """Normalize column name for PostgreSQL compatibility.
        
        Args:
            name: Original column name
            
        Returns:
            Normalized column name (lowercase, underscores, PostgreSQL-safe)
        """
        # Replace non-alphanumeric with underscores, convert to lowercase
        normalized = re.sub(r'[^\w]+', '_', name.strip().lower())
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        # Ensure it doesn't start with a number
        if normalized and normalized[0].isdigit():
            normalized = f"col_{normalized}"
        return normalized or "unnamed_column"
    
    @staticmethod
    def infer_from_name(column_name: str) -> str:
        """Infer PostgreSQL type from column name patterns.
        
        Args:
            column_name: Normalized column name
            
        Returns:
            PostgreSQL data type
        """
        name_lower = column_name.lower()
        
        # Primary keys
        if name_lower in ("id", "pk") or name_lower.endswith("_id"):
            return "SERIAL PRIMARY KEY" if name_lower == "id" else "INTEGER"
        
        # Timestamps
        if any(x in name_lower for x in ["created_at", "updated_at", "timestamp", "_at"]):
            return "TIMESTAMPTZ"
        
        # Dates
        if any(x in name_lower for x in ["date", "_date", "birthday", "anniversary"]):
            return "DATE"
        
        # Boolean flags
        if any(x in name_lower for x in ["is_", "has_", "can_", "should_", "enabled", "active", "deleted"]):
            return "BOOLEAN"
        
        # Numeric fields
        if any(x in name_lower for x in ["price", "cost", "amount", "total", "count", "num", "quantity"]):
            return "NUMERIC(10,2)" if any(x in name_lower for x in ["price", "cost", "amount", "total"]) else "INTEGER"
        
        # Email, URL, phone
        if any(x in name_lower for x in ["email", "mail"]):
            return "TEXT"
        if any(x in name_lower for x in ["url", "link", "website"]):
            return "TEXT"
        if any(x in name_lower for x in ["phone", "mobile", "tel"]):
            return "VARCHAR(20)"
        
        # Arrays (plural forms)
        if name_lower.endswith("s") and not name_lower.endswith("ss"):
            singular = name_lower[:-1]
            if singular in ["tag", "category", "author", "keyword", "skill"]:
                return "TEXT[]"
        
        # JSON fields
        if any(x in name_lower for x in ["metadata", "config", "settings", "options", "data", "json"]):
            return "JSONB"
        
        # Default to TEXT for names, descriptions, etc.
        if any(x in name_lower for x in ["name", "title", "description", "comment", "note", "text", "content"]):
            return "TEXT"
        
        # Fallback
        return "TEXT"
    
    @staticmethod
    def infer_from_values(column_name: str, sample_values: List[str], max_samples: int = 20) -> str:
        """Infer PostgreSQL type from sample data values.
        
        Args:
            column_name: Column name for context
            sample_values: List of sample values from the column
            max_samples: Maximum number of values to analyze
            
        Returns:
            PostgreSQL data type
        """
        # Start with name-based inference
        name_type = TypeInference.infer_from_name(column_name)
        
        # Filter out empty values for analysis
        non_empty_values = [v.strip() for v in sample_values if v and v.strip()]
        if not non_empty_values:
            return name_type
        
        # Analyze sample values (limit to max_samples for performance)
        values_to_check = non_empty_values[:max_samples]
        
        # Count different types of values
        numeric_count = 0
        integer_count = 0
        float_count = 0
        boolean_count = 0
        date_count = 0
        json_count = 0
        
        for val in values_to_check:
            val_lower = val.lower()
            
            # Check for boolean values
            if val_lower in ('true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0'):
                boolean_count += 1
                continue
            
            # Check for numeric values
            try:
                if '.' in val or 'e' in val.lower():
                    float(val)
                    float_count += 1
                    numeric_count += 1
                else:
                    int(val)
                    integer_count += 1
                    numeric_count += 1
                continue
            except ValueError:
                pass
            
            # Check for date patterns
            if TypeInference._is_date_like(val):
                date_count += 1
                continue
            
            # Check for JSON-like content
            if TypeInference._is_json_like(val):
                json_count += 1
                continue
        
        total_samples = len(values_to_check)
        
        # Determine type based on sample analysis
        if boolean_count > total_samples * 0.7:
            return "BOOLEAN"
        elif integer_count > total_samples * 0.8:
            return "INTEGER"
        elif numeric_count > total_samples * 0.8:
            return "NUMERIC"
        elif date_count > total_samples * 0.7:
            return "DATE"
        elif json_count > total_samples * 0.7:
            return "JSONB"
        
        # Check for array-like content (semicolon or comma separated)
        name_lower = column_name.lower()
        if name_lower.endswith("s") and not name_lower.endswith("ss"):
            array_like = sum(1 for val in values_to_check[:10] if ';' in val or ',' in val)
            if array_like > len(values_to_check[:10]) * 0.5:
                return "TEXT[]"
        
        # Length-based TEXT vs VARCHAR decision
        max_length = max(len(val) for val in non_empty_values) if non_empty_values else 0
        if max_length <= 255:
            return f"VARCHAR({min(max_length * 2, 255)})"
        
        return "TEXT"
    
    @staticmethod
    def _is_date_like(value: str) -> bool:
        """Check if value looks like a date."""
        date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m", "%Y"]
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False
    
    @staticmethod
    def _is_json_like(value: str) -> bool:
        """Check if value looks like JSON."""
        if value.startswith(('{', '[')) and value.endswith(('}', ']')):
            try:
                json.loads(value)
                return True
            except (json.JSONDecodeError, ValueError):
                pass
        return False
    
    @staticmethod
    def infer_type(column_name: str, sample_values: Optional[List[str]] = None) -> str:
        """Main type inference method combining name and value analysis.
        
        Args:
            column_name: Column name to analyze
            sample_values: Optional sample values for analysis
            
        Returns:
            PostgreSQL data type
        """
        normalized_name = TypeInference.normalize_column_name(column_name)
        
        if sample_values:
            return TypeInference.infer_from_values(normalized_name, sample_values)
        else:
            return TypeInference.infer_from_name(normalized_name)