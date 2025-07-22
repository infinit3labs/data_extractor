"""
Tests for the data extractor core functionality.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import tempfile
import os

from data_extractor.core import DataExtractor
from data_extractor.config import ConfigManager


class TestDataExtractor(unittest.TestCase):
    """Test cases for DataExtractor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_config = {
            'oracle_host': 'localhost',
            'oracle_port': '1521',
            'oracle_service': 'XE',
            'oracle_user': 'test_user',
            'oracle_password': 'test_password',
            'output_base_path': '/tmp/test_output'
        }
        
    def test_data_extractor_initialization(self):
        """Test DataExtractor initialization."""
        extractor = DataExtractor(**self.test_config)
        
        self.assertEqual(extractor.oracle_host, 'localhost')
        self.assertEqual(extractor.oracle_port, '1521')
        self.assertEqual(extractor.oracle_service, 'XE')
        self.assertEqual(extractor.oracle_user, 'test_user')
        self.assertEqual(extractor.oracle_password, 'test_password')
        self.assertEqual(extractor.output_base_path, '/tmp/test_output')
        self.assertTrue(extractor.max_workers > 0)
        
        # Test JDBC URL construction
        expected_url = "jdbc:oracle:thin:@localhost:1521:XE"
        self.assertEqual(extractor.jdbc_url, expected_url)
        
    def test_jdbc_connection_properties(self):
        """Test JDBC connection properties are properly set."""
        extractor = DataExtractor(**self.test_config)
        
        expected_properties = {
            "user": "test_user",
            "password": "test_password", 
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        self.assertEqual(extractor.connection_properties, expected_properties)
        
    @patch('data_extractor.core.SparkSession')
    def test_spark_session_creation(self, mock_spark_session):
        """Test Spark session creation with proper configuration."""
        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = Mock()
        
        extractor = DataExtractor(**self.test_config)
        spark_session = extractor._get_spark_session()
        
        # Verify Spark session configuration calls
        mock_builder.config.assert_any_call("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.7.0.0")
        mock_builder.config.assert_any_call("spark.sql.adaptive.enabled", "true")
        mock_builder.config.assert_any_call("spark.sql.adaptive.coalescePartitions.enabled", "true")
        mock_builder.config.assert_any_call("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
    def test_table_config_validation(self):
        """Test table configuration validation."""
        config_manager = ConfigManager()
        
        # Valid configuration
        valid_config = {
            'source_name': 'test_source',
            'table_name': 'test_table',
            'incremental_column': 'last_modified',
            'is_full_extract': False
        }
        errors = config_manager.validate_table_config(valid_config)
        self.assertEqual(len(errors), 0)
        
        # Missing required fields
        invalid_config = {
            'source_name': 'test_source'
            # missing table_name
        }
        errors = config_manager.validate_table_config(invalid_config)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any('table_name' in error for error in errors))
        
        # Missing incremental column for incremental extraction
        invalid_config2 = {
            'source_name': 'test_source',
            'table_name': 'test_table',
            'is_full_extract': False
            # missing incremental_column
        }
        errors = config_manager.validate_table_config(invalid_config2)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any('incremental_column' in error for error in errors))


class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager class."""
    
    def test_config_manager_initialization(self):
        """Test ConfigManager initialization."""
        config_manager = ConfigManager()
        self.assertIsNotNone(config_manager.config)
        
    def test_sample_config_creation(self):
        """Test creation of sample configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, 'test_config.ini')
            tables_path = os.path.join(temp_dir, 'test_tables.json')
            
            config_manager = ConfigManager()
            config_manager.create_sample_config_file(config_path)
            config_manager.create_sample_tables_json(tables_path)
            
            # Verify files were created
            self.assertTrue(os.path.exists(config_path))
            self.assertTrue(os.path.exists(tables_path))
            
            # Verify content
            with open(config_path, 'r') as f:
                config_content = f.read()
                self.assertIn('oracle_host', config_content)
                self.assertIn('oracle_port', config_content)
                
            with open(tables_path, 'r') as f:
                tables_content = f.read()
                self.assertIn('tables', tables_content)
                self.assertIn('employees', tables_content)
                
    def test_environment_variable_fallback(self):
        """Test fallback to environment variables."""
        with patch.dict(os.environ, {
            'ORACLE_HOST': 'env_host',
            'ORACLE_PORT': 'env_port',
            'ORACLE_SERVICE': 'env_service',
            'ORACLE_USER': 'env_user',
            'ORACLE_PASSWORD': 'env_password'
        }):
            config_manager = ConfigManager()
            db_config = config_manager.get_database_config()
            
            self.assertEqual(db_config['oracle_host'], 'env_host')
            self.assertEqual(db_config['oracle_port'], 'env_port')
            self.assertEqual(db_config['oracle_service'], 'env_service')
            self.assertEqual(db_config['oracle_user'], 'env_user')
            self.assertEqual(db_config['oracle_password'], 'env_password')


if __name__ == '__main__':
    unittest.main()
