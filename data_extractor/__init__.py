"""
Data Extractor Package
A Spark JDBC data extraction module for Oracle databases with parallel processing capabilities.
Includes Databricks cluster support for optimized cloud-based data extraction.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from .core import DataExtractor
from .config import ConfigManager
from .databricks import DatabricksDataExtractor, DatabricksConfigManager

__all__ = [
    'DataExtractor',
    'ConfigManager', 
    'DatabricksDataExtractor',
    'DatabricksConfigManager'
]