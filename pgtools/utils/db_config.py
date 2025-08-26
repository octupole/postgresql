"""
Database configuration utilities for PGTools.

This module handles database connection configuration from various sources
including .env files, environment variables, and direct parameters.
"""

import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class DatabaseConfig:
    """Database configuration manager for PostgreSQL connections."""
    
    def __init__(self, env_path: Optional[str] = None):
        """Initialize database configuration.
        
        Args:
            env_path: Path to .env file (default: .env)
        """
        self.env_path = env_path or ".env"
        self._config: Optional[Dict[str, Any]] = None
    
    def load_config(self, env_path: Optional[str] = None, override: bool = True) -> Dict[str, Any]:
        """Load database configuration from .env file and environment variables.
        
        Args:
            env_path: Path to .env file (overrides instance default)
            override: Whether to override existing env vars with .env values
            
        Returns:
            Dictionary with database configuration
            
        Raises:
            SystemExit: If required configuration is missing
        """
        if env_path:
            self.env_path = env_path
            
        # Load .env file if it exists
        if self.env_path and os.path.exists(self.env_path):
            load_dotenv(self.env_path, override=override)
        
        # Try DATABASE_URL first (most common in deployment)
        dsn = self._get_env_var("DATABASE_URL", "POSTGRES_URL", "DB_URL")
        if dsn:
            self._config = {"dsn": dsn}
            return self._config
        
        # Build configuration from individual components
        host = self._get_env_var("PGHOST", "DB_HOST", default="localhost")
        port = int(self._get_env_var("PGPORT", "DB_PORT", default="5432"))
        dbname = self._get_env_var("PGDATABASE", "DB_NAME")
        user = self._get_env_var("PGUSER", "DB_USER")
        password = self._get_env_var("PGPASSWORD", "DB_PASSWORD")
        
        # Validate required fields
        missing = []
        if not dbname:
            missing.append("PGDATABASE/DB_NAME")
        if not user:
            missing.append("PGUSER/DB_USER") 
        if not password:
            missing.append("PGPASSWORD/DB_PASSWORD")
            
        if missing:
            raise SystemExit(
                f"Missing required database configuration: {', '.join(missing)}\n"
                f"Provide either DATABASE_URL or individual variables:\n"
                f"  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD\n"
                f"  (or DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)\n"
                f"Configuration loaded from: {self.env_path}"
            )
        
        self._config = {
            "kwargs": {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password
            }
        }
        
        return self._config
    
    def _get_env_var(self, *names: str, default: Optional[str] = None) -> Optional[str]:
        """Get environment variable by trying multiple names."""
        for name in names:
            value = os.getenv(name)
            if value:
                return value
        return default
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get current configuration, loading it if necessary."""
        if self._config is None:
            self.load_config()
        return self._config
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters suitable for psycopg.connect()."""
        config = self.config
        if "dsn" in config:
            return {"dsn": config["dsn"]}
        else:
            return config["kwargs"]
    
    def __repr__(self) -> str:
        """String representation of database config."""
        config = self.config
        if "dsn" in config:
            # Hide password in DSN for security
            dsn = config["dsn"]
            if "@" in dsn:
                parts = dsn.split("@", 1)
                if ":" in parts[0]:
                    user_pass = parts[0].split(":", 1)[0]
                    masked_dsn = f"{user_pass}:***@{parts[1]}"
                else:
                    masked_dsn = dsn
            else:
                masked_dsn = dsn
            return f"DatabaseConfig(dsn='{masked_dsn}')"
        else:
            kwargs = config["kwargs"].copy()
            kwargs["password"] = "***"
            return f"DatabaseConfig({kwargs})"