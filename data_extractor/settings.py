"""
Enterprise-grade configuration and settings management using Pydantic.
Provides validation, type safety, and better error handling for configuration.
"""

import os
from typing import Optional
try:
    from pydantic import BaseSettings, validator, Field
    from pydantic_settings import BaseSettings as PydanticBaseSettings
except ImportError:
    # Fallback for environments without pydantic
    class BaseSettings:
        pass
    class PydanticBaseSettings:
        pass
    def validator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    def Field(default=None, **kwargs):
        return default


class DatabaseSettings(PydanticBaseSettings):
    """Database connection settings with validation."""
    
    oracle_host: str = Field(..., description="Oracle database host")
    oracle_port: int = Field(1521, description="Oracle database port", ge=1, le=65535)
    oracle_service: str = Field(..., description="Oracle service name")
    oracle_user: str = Field(..., description="Database username", min_length=1)
    oracle_password: str = Field(..., description="Database password", min_length=1)
    
    @validator('oracle_host')
    def validate_host(cls, v):
        if not v or v.isspace():
            raise ValueError('Oracle host cannot be empty or whitespace')
        return v.strip()
    
    @validator('oracle_service')
    def validate_service(cls, v):
        if not v or v.isspace():
            raise ValueError('Oracle service cannot be empty or whitespace')
        return v.strip()
    
    class Config:
        env_prefix = "ORACLE_"
        case_sensitive = False


class ExtractionSettings(PydanticBaseSettings):
    """Data extraction settings with validation."""
    
    output_base_path: str = Field("data", description="Base path for output files")
    max_workers: Optional[int] = Field(None, description="Maximum worker threads", ge=1, le=32)
    default_source: str = Field("default", description="Default source name")
    run_id: Optional[str] = Field(None, description="Unique run identifier")
    
    @validator('output_base_path')
    def validate_output_path(cls, v):
        if not v or v.isspace():
            raise ValueError('Output path cannot be empty or whitespace')
        return v.strip()
    
    @validator('max_workers')
    def validate_max_workers(cls, v):
        cpu_count = os.cpu_count() or 1
        if v is not None and v < 1:
            raise ValueError('Max workers must be at least 1')
        if v is not None and v > cpu_count * 2:
            raise ValueError(f'Max workers cannot exceed {cpu_count * 2}')
        return v or cpu_count
    
    class Config:
        case_sensitive = False


class DatabricksSettings(PydanticBaseSettings):
    """Databricks-specific settings."""
    
    output_base_path: str = Field("/dbfs/data", description="Databricks output path")
    max_workers: Optional[int] = Field(None, description="Max workers for Databricks", ge=1, le=16)
    use_existing_spark: bool = Field(True, description="Use existing Spark session")
    unity_catalog_volume: Optional[str] = Field(None, description="Unity Catalog volume path")
    cluster_mode: str = Field("shared", description="Databricks cluster mode")
    log_level: str = Field("INFO", description="Logging level")
    
    @validator('cluster_mode')
    def validate_cluster_mode(cls, v):
        allowed_modes = ['shared', 'single_user', 'high_concurrency']
        if v not in allowed_modes:
            raise ValueError(f'Cluster mode must be one of {allowed_modes}')
        return v
    
    @validator('log_level')
    def validate_log_level(cls, v):
        allowed_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in allowed_levels:
            raise ValueError(f'Log level must be one of {allowed_levels}')
        return v.upper()
    
    @validator('max_workers')
    def validate_databricks_workers(cls, v):
        cpu_count = os.cpu_count() or 1
        if v is not None and v < 1:
            raise ValueError('Max workers must be at least 1')
        if v is not None and v > 16:
            raise ValueError('Max workers for Databricks should not exceed 16 for stability')
        return v or max(1, cpu_count // 2)
    
    class Config:
        env_prefix = "DATABRICKS_"
        case_sensitive = False


class AppSettings(PydanticBaseSettings):
    """Main application settings combining all configuration sections."""
    
    database: DatabaseSettings
    extraction: ExtractionSettings
    databricks: Optional[DatabricksSettings] = None
    
    # Global settings
    environment: str = Field("development", description="Application environment")
    debug: bool = Field(False, description="Enable debug mode")
    log_level: str = Field("INFO", description="Global log level")
    
    @validator('environment')
    def validate_environment(cls, v):
        allowed_envs = ['development', 'staging', 'production']
        if v not in allowed_envs:
            raise ValueError(f'Environment must be one of {allowed_envs}')
        return v
    
    def __init__(self, **kwargs):
        # Initialize nested settings
        if 'database' not in kwargs:
            kwargs['database'] = DatabaseSettings()
        if 'extraction' not in kwargs:
            kwargs['extraction'] = ExtractionSettings()
        if 'databricks' not in kwargs and self._is_databricks_env():
            kwargs['databricks'] = DatabricksSettings()
        
        super().__init__(**kwargs)
    
    @staticmethod
    def _is_databricks_env() -> bool:
        """Check if running in Databricks environment."""
        return any([
            'DATABRICKS_RUNTIME_VERSION' in os.environ,
            'SPARK_HOME' in os.environ and 'databricks' in os.environ.get('SPARK_HOME', '').lower(),
            os.path.exists('/databricks')
        ])
    
    class Config:
        case_sensitive = False
        env_nested_delimiter = "__"  # Allow ORACLE__HOST style env vars


def load_settings() -> AppSettings:
    """
    Load and validate application settings from environment and config files.
    
    Returns:
        AppSettings: Validated application settings
        
    Raises:
        ValueError: If required settings are missing or invalid
        ValidationError: If settings don't pass validation
    """
    try:
        return AppSettings()
    except Exception as e:
        # Provide helpful error messages for common issues
        if "oracle_host" in str(e).lower():
            raise ValueError(
                "Database host is required. Set ORACLE_HOST environment variable or provide in config file."
            ) from e
        if "oracle_user" in str(e).lower():
            raise ValueError(
                "Database user is required. Set ORACLE_USER environment variable or provide in config file."
            ) from e
        if "oracle_password" in str(e).lower():
            raise ValueError(
                "Database password is required. Set ORACLE_PASSWORD environment variable or provide in config file."
            ) from e
        raise


def validate_database_connection(settings: DatabaseSettings) -> None:
    """
    Validate database connection settings without actually connecting.
    
    Args:
        settings: Database settings to validate
        
    Raises:
        ValueError: If connection parameters are invalid
    """
    # Additional business logic validation
    if settings.oracle_port not in range(1024, 65536):
        raise ValueError("Oracle port should typically be between 1024-65535")
    
    # Check for common misconfigurations
    if settings.oracle_host.lower() in ['localhost', '127.0.0.1'] and settings.oracle_port == 1521:
        import logging
        logging.getLogger(__name__).warning(
            "Using localhost:1521 - ensure Oracle database is running locally"
        )


def validate_extraction_config(settings: ExtractionSettings) -> None:
    """
    Validate extraction configuration settings.
    
    Args:
        settings: Extraction settings to validate
        
    Raises:
        ValueError: If extraction parameters are invalid
    """
    # Validate output path accessibility
    output_dir = os.path.dirname(settings.output_base_path) if not os.path.isdir(settings.output_base_path) else settings.output_base_path
    
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except PermissionError:
            raise ValueError(f"Cannot create output directory: {output_dir}. Check permissions.")
    
    if not os.access(output_dir, os.W_OK):
        raise ValueError(f"Output directory is not writable: {output_dir}")