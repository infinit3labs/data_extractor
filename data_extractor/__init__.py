"""
Data Extractor - Enterprise data extraction tool for Oracle databases.

This package provides high-performance data extraction capabilities with
support for both standalone and Databricks environments.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Main classes for public API
from .config import AppSettings, ConfigManager
from .core import DataExtractor
from .databricks import DatabricksConfigManager, DatabricksDataExtractor
from .health import HealthChecker, HealthStatus
from .logging_config import setup_logging
from .sqlserver import SqlServerDataExtractor

__all__ = [
    "ConfigManager",
    "AppSettings",
    "DataExtractor",
    "SqlServerDataExtractor",
    "DatabricksDataExtractor",
    "DatabricksConfigManager",
    "HealthChecker",
    "HealthStatus",
    "setup_logging",
]
