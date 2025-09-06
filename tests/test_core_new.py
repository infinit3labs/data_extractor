"""
Tests for the data extractor core functionality.
"""

import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


class TestDataExtractor(unittest.TestCase):
    """Test cases for DataExtractor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_config = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "/tmp/test_output",
            "max_workers": 4,
        }

    def test_data_extractor_initialization(self):
        """Test DataExtractor initialization."""
        extractor = DataExtractor(**self.test_config)

        self.assertEqual(extractor.oracle_host, "localhost")
        self.assertEqual(extractor.oracle_port, "1521")
        self.assertEqual(extractor.oracle_service, "XE")
        self.assertEqual(extractor.oracle_user, "test_user")
        self.assertEqual(extractor.oracle_password, "test_password")
        self.assertEqual(extractor.output_base_path, "/tmp/test_output")
        self.assertEqual(extractor.max_workers, 4)

        # Test JDBC URL construction
        expected_url = "jdbc:oracle:thin:@localhost:1521:XE"
        self.assertEqual(extractor.jdbc_url, expected_url)

    def test_jdbc_connection_properties(self):
        """Test JDBC connection properties are properly set."""
        extractor = DataExtractor(**self.test_config)

        expected_properties = {
            "user": "test_user",
            "password": "test_password",
            "driver": "oracle.jdbc.driver.OracleDriver",
        }

        self.assertEqual(extractor.connection_properties, expected_properties)

    @patch("data_extractor.core.SparkSession")
    def test_spark_session_creation(self, mock_spark_session):
        """Test Spark session creation with proper configuration."""
        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = Mock()

        extractor = DataExtractor(**self.test_config)
        extractor._get_spark_session()

        # Verify Spark session configuration calls
        mock_builder.config.assert_any_call(
            "spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.7.0.0"
        )
        mock_builder.config.assert_any_call("spark.sql.adaptive.enabled", "true")
        mock_builder.config.assert_any_call(
            "spark.sql.adaptive.coalescePartitions.enabled", "true"
        )
        mock_builder.config.assert_any_call(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
        )

    @patch("data_extractor.core.SparkSession")
    def test_extract_table_success(self, mock_spark_session):
        """Test successful table extraction."""
        # Mock Spark session and DataFrame
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        extractor = DataExtractor(**self.test_config)

        with patch("pathlib.Path.mkdir"):
            result = extractor.extract_table(
                source_name="test_source",
                table_name="test_table",
                schema_name="test_schema",
                is_full_extract=True,
            )

        self.assertTrue(result)
        mock_df.count.assert_called_once()

    @patch("data_extractor.core.SparkSession")
    def test_extract_table_incremental(self, mock_spark_session):
        """Test incremental table extraction with date filtering."""
        # Mock Spark session and DataFrame
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 50
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        extractor = DataExtractor(**self.test_config)

        extraction_date = datetime(2023, 12, 1)

        with patch("pathlib.Path.mkdir"):
            result = extractor.extract_table(
                source_name="test_source",
                table_name="test_table",
                incremental_column="last_modified",
                extraction_date=extraction_date,
                is_full_extract=False,
            )

        self.assertTrue(result)
        # Verify the query was built for incremental extraction
        call_args = mock_read.option.call_args_list
        query_call = next((call for call in call_args if call[0][0] == "query"), None)
        self.assertIsNotNone(query_call)
        query_text = query_call[0][1]
        self.assertIn("last_modified", query_text)
        self.assertIn("TO_DATE", query_text)

    @patch("data_extractor.core.SparkSession")
    def test_extract_table_no_data(self, mock_spark_session):
        """Test table extraction when no data is found."""
        # Mock Spark session and DataFrame with zero count
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 0

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        extractor = DataExtractor(**self.test_config)

        result = extractor.extract_table(
            source_name="test_source", table_name="empty_table", is_full_extract=True
        )

        # Should return True even with no data (not an error condition)
        self.assertTrue(result)

    @patch("data_extractor.core.SparkSession")
    def test_extract_table_error_handling(self, mock_spark_session):
        """Test error handling in table extraction."""
        # Mock Spark session that raises an exception
        mock_spark = Mock()
        mock_read = Mock()
        mock_read.format.side_effect = Exception("Connection failed")
        mock_spark.read = mock_read

        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        extractor = DataExtractor(**self.test_config)

        result = extractor.extract_table(
            source_name="test_source", table_name="failing_table", is_full_extract=True
        )

        self.assertFalse(result)

    @patch("data_extractor.core.ThreadPoolExecutor")
    @patch("data_extractor.core.DataExtractor.extract_table")
    def test_extract_tables_parallel(self, mock_extract_table, mock_executor):
        """Test parallel extraction of multiple tables."""
        # Mock successful extraction
        mock_extract_table.return_value = True

        # Mock ThreadPoolExecutor
        mock_future = Mock()
        mock_future.result.return_value = True
        mock_executor_instance = MagicMock()
        mock_executor_instance.__enter__.return_value = mock_executor_instance
        mock_executor_instance.__exit__.return_value = None
        mock_executor_instance.submit.return_value = mock_future
        mock_executor.return_value = mock_executor_instance

        # Mock as_completed to return futures immediately
        with patch("data_extractor.core.as_completed", return_value=[mock_future]):
            extractor = DataExtractor(**self.test_config)

            # Create a future_to_table mapping
            future_to_table = {mock_future: "table1"}
            with patch.dict(
                "data_extractor.core.__dict__", {"future_to_table": future_to_table}
            ):
                table_configs = [
                    {
                        "source_name": "test_source",
                        "table_name": "table1",
                        "is_full_extract": True,
                    }
                ]

                results = extractor.extract_tables_parallel(table_configs)

                self.assertEqual(len(results), 1)
                self.assertTrue(results["table1"])

    @patch("data_extractor.core.ThreadPoolExecutor")
    def test_extract_tables_parallel_validation(self, mock_executor):
        """Test validation of table configs in parallel extraction."""
        mock_executor_instance = MagicMock()
        mock_executor_instance.__enter__.return_value = mock_executor_instance
        mock_executor_instance.__exit__.return_value = None
        mock_executor.return_value = mock_executor_instance

        with patch("data_extractor.core.as_completed", return_value=[]):
            extractor = DataExtractor(**self.test_config)

            # Invalid config missing required fields
            table_configs = [
                {
                    "source_name": "test_source",
                    # missing table_name
                },
                {
                    # missing source_name and table_name
                    "incremental_column": "updated_at"
                },
            ]

            results = extractor.extract_tables_parallel(table_configs)

            # Should skip invalid configs
            self.assertEqual(len(results), 0)
            mock_executor_instance.submit.assert_not_called()

    def test_cleanup_spark_sessions(self):
        """Test cleanup of Spark sessions."""
        extractor = DataExtractor(**self.test_config)

        # Mock a local spark session
        mock_spark = Mock()
        extractor._local.spark = mock_spark

        extractor.cleanup_spark_sessions()

        mock_spark.stop.assert_called_once()
        self.assertFalse(hasattr(extractor._local, "spark"))

    def test_table_config_validation(self):
        """Test table configuration validation."""
        config_manager = ConfigManager()

        # Valid configuration
        valid_config = {
            "source_name": "test_source",
            "table_name": "test_table",
            "incremental_column": "last_modified",
            "is_full_extract": False,
        }
        errors = config_manager.validate_table_config(valid_config)
        self.assertEqual(len(errors), 0)

        # Missing required fields
        invalid_config = {
            "source_name": "test_source"
            # missing table_name
        }
        errors = config_manager.validate_table_config(invalid_config)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("table_name" in error for error in errors))

        # Missing incremental column for incremental extraction
        invalid_config2 = {
            "source_name": "test_source",
            "table_name": "test_table",
            "is_full_extract": False,
            # missing incremental_column
        }
        errors = config_manager.validate_table_config(invalid_config2)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("incremental_column" in error for error in errors))


class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager class."""

    def test_config_manager_initialization(self):
        """Test ConfigManager initialization."""
        config_manager = ConfigManager()
        self.assertIsInstance(config_manager.config_data, dict)

    def test_sample_config_creation(self):
        """Test creation of sample configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yml")
            tables_path = os.path.join(temp_dir, "test_tables.json")

            config_manager = ConfigManager()
            config_manager.create_sample_config_file(config_path)
            config_manager.create_sample_tables_json(tables_path)

            # Verify files were created
            self.assertTrue(os.path.exists(config_path))
            self.assertTrue(os.path.exists(tables_path))

            # Verify content
            with open(config_path, "r") as f:
                config_content = f.read()
                self.assertIn("oracle_host", config_content)
                self.assertIn("oracle_port", config_content)

            with open(tables_path, "r") as f:
                tables_content = f.read()
                self.assertIn("tables", tables_content)
                self.assertIn("employees", tables_content)

    def test_environment_variable_fallback(self):
        """Test fallback to environment variables."""
        with patch.dict(
            os.environ,
            {
                "ORACLE_HOST": "env_host",
                "ORACLE_PORT": "env_port",
                "ORACLE_SERVICE": "env_service",
                "ORACLE_USER": "env_user",
                "ORACLE_PASSWORD": "env_password",
            },
        ):
            config_manager = ConfigManager()
            db_config = config_manager.get_database_config()

            self.assertEqual(db_config["oracle_host"], "env_host")
            self.assertEqual(db_config["oracle_port"], "env_port")
            self.assertEqual(db_config["oracle_service"], "env_service")
            self.assertEqual(db_config["oracle_user"], "env_user")
            self.assertEqual(db_config["oracle_password"], "env_password")

    def test_app_settings_integration(self):
        """Test AppSettings integration with ConfigManager."""
        config_manager = ConfigManager()
        app_settings = config_manager.get_app_settings()

        # Test that all required fields are present
        self.assertTrue(hasattr(app_settings, "oracle_host"))
        self.assertTrue(hasattr(app_settings, "oracle_port"))
        self.assertTrue(hasattr(app_settings, "oracle_service"))
        self.assertTrue(hasattr(app_settings, "oracle_user"))
        self.assertTrue(hasattr(app_settings, "oracle_password"))
        self.assertTrue(hasattr(app_settings, "output_base_path"))

    def test_get_extraction_config(self):
        """Test extraction configuration retrieval."""
        config_manager = ConfigManager()
        extraction_config = config_manager.get_extraction_config()

        # Should only contain non-None values
        for key, value in extraction_config.items():
            self.assertIsNotNone(value)

    def test_load_table_configs_from_json(self):
        """Test loading table configurations from JSON file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = os.path.join(temp_dir, "test_tables.json")
            config_manager = ConfigManager()
            config_manager.create_sample_tables_json(json_path)

            table_configs = config_manager.load_table_configs_from_json(json_path)

            self.assertIsInstance(table_configs, list)
            self.assertGreater(len(table_configs), 0)

            # Verify first table config has required fields
            first_config = table_configs[0]
            self.assertIn("source_name", first_config)
            self.assertIn("table_name", first_config)


if __name__ == "__main__":
    unittest.main()
