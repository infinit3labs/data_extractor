"""
Configuration management module for data extraction.
Handles loading and parsing of extraction configurations.
"""

import os
import json
import configparser
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


class ConfigManager:
    """
    Manages configuration for data extraction including database connections
    and table extraction settings.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file (INI format)
        """
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        
        if config_file and os.path.exists(config_file):
            self.config.read(config_file)
            
    def get_database_config(self) -> Dict[str, str]:
        """
        Get database connection configuration.
        
        Returns:
            Dict containing database connection parameters
        """
        if 'database' in self.config:
            return dict(self.config['database'])
        else:
            # Fall back to environment variables
            return {
                'oracle_host': os.getenv('ORACLE_HOST', 'localhost'),
                'oracle_port': os.getenv('ORACLE_PORT', '1521'),
                'oracle_service': os.getenv('ORACLE_SERVICE', 'XE'),
                'oracle_user': os.getenv('ORACLE_USER', ''),
                'oracle_password': os.getenv('ORACLE_PASSWORD', ''),
                'output_base_path': os.getenv('OUTPUT_BASE_PATH', 'data')
            }
            
    def get_extraction_config(self) -> Dict[str, Any]:
        """
        Get general extraction configuration.
        
        Returns:
            Dict containing extraction parameters
        """
        config = {}
        
        if 'extraction' in self.config:
            section = self.config['extraction']
            config['max_workers'] = section.getint('max_workers', fallback=None)
            config['run_id'] = section.get('run_id', fallback=None)
            config['default_source'] = section.get('default_source', fallback='default')
            
        return config
        
    def load_table_configs_from_json(self, json_file: str) -> List[Dict]:
        """
        Load table extraction configurations from JSON file.
        
        Args:
            json_file: Path to JSON configuration file
            
        Returns:
            List of table configuration dictionaries
        """
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        table_configs = []
        
        for table_config in data.get('tables', []):
            # Parse extraction_date if provided
            if 'extraction_date' in table_config and table_config['extraction_date']:
                if isinstance(table_config['extraction_date'], str):
                    table_config['extraction_date'] = datetime.strptime(
                        table_config['extraction_date'], '%Y-%m-%d'
                    )
                    
            table_configs.append(table_config)
            
        return table_configs
        
    def create_sample_config_file(self, config_path: str):
        """
        Create a sample configuration file.
        
        Args:
            config_path: Path where to create the sample config file
        """
        sample_config = configparser.ConfigParser()
        
        # Database section
        sample_config['database'] = {
            'oracle_host': 'localhost',
            'oracle_port': '1521',
            'oracle_service': 'XE',
            'oracle_user': 'your_username',
            'oracle_password': 'your_password',
            'output_base_path': 'data'
        }
        
        # Extraction section
        sample_config['extraction'] = {
            'max_workers': '8',
            'default_source': 'oracle_db'
        }
        
        with open(config_path, 'w') as f:
            sample_config.write(f)
            
    def create_sample_tables_json(self, json_path: str):
        """
        Create a sample tables configuration JSON file.
        
        Args:
            json_path: Path where to create the sample JSON file
        """
        sample_tables = {
            "tables": [
                {
                    "source_name": "oracle_db",
                    "table_name": "employees",
                    "schema_name": "hr",
                    "incremental_column": "last_modified",
                    "extraction_date": "2023-12-01",
                    "is_full_extract": False
                },
                {
                    "source_name": "oracle_db", 
                    "table_name": "departments",
                    "schema_name": "hr",
                    "is_full_extract": True
                },
                {
                    "source_name": "oracle_db",
                    "table_name": "orders",
                    "schema_name": "sales",
                    "incremental_column": "order_date",
                    "is_full_extract": False
                }
            ]
        }
        
        with open(json_path, 'w') as f:
            json.dump(sample_tables, f, indent=2)
            
    def validate_table_config(self, table_config: Dict) -> List[str]:
        """
        Validate a table configuration.
        
        Args:
            table_config: Table configuration dictionary
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Required fields
        required_fields = ['source_name', 'table_name']
        for field in required_fields:
            if not table_config.get(field):
                errors.append(f"Missing required field: {field}")
                
        # Validate incremental extraction settings
        if not table_config.get('is_full_extract', False):
            if not table_config.get('incremental_column'):
                errors.append("incremental_column is required for incremental extraction")
                
        return errors
        
    def get_runtime_config(self, **kwargs) -> Dict[str, Any]:
        """
        Get runtime configuration by merging config file, environment variables, and kwargs.
        
        Args:
            **kwargs: Runtime configuration overrides
            
        Returns:
            Dict containing complete runtime configuration
        """
        # Start with config file
        db_config = self.get_database_config()
        extraction_config = self.get_extraction_config()
        
        # Merge with kwargs
        runtime_config = {**db_config, **extraction_config, **kwargs}
        
        return runtime_config
