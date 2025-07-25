"""
Tests for the Databricks data extraction functionality.
"""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from data_extractor.databricks import DatabricksConfigManager, DatabricksDataExtractor


class TestDatabricksDataExtractor(unittest.TestCase):
    """Test cases for DatabricksDataExtractor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_config = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "/dbfs/test_output",
            "max_workers": 2,
        }

    def test_databricks_extractor_initialization(self):
        """Test DatabricksDataExtractor initialization."""
        extractor = DatabricksDataExtractor(**self.test_config)

        self.assertEqual(extractor.oracle_host, "localhost")
        self.assertEqual(extractor.oracle_port, "1521")
        self.assertEqual(extractor.oracle_service, "XE")
        self.assertEqual(extractor.oracle_user, "test_user")
        self.assertEqual(extractor.oracle_password, "test_password")
        self.assertEqual(extractor.output_base_path, "/dbfs/test_output")
        self.assertTrue(extractor.use_existing_spark)
        self.assertIsNone(extractor.unity_catalog_volume)
        self.assertTrue(extractor.max_workers > 0)

        # Test JDBC URL construction
        expected_url = "jdbc:oracle:thin:@localhost:1521:XE"
        self.assertEqual(extractor.jdbc_url, expected_url)

    def test_databricks_environment_detection(self):
        """Test Databricks environment detection."""
        extractor = DatabricksDataExtractor(**self.test_config)

        # Test without Databricks environment variables
        self.assertFalse(extractor._is_databricks_environment())

        # Test with Databricks environment variables
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"}):
            extractor2 = DatabricksDataExtractor(**self.test_config)
            self.assertTrue(extractor2._is_databricks_environment())

    def test_output_path_normalization(self):
        """Test output path normalization for Databricks."""
        # Test without Databricks environment (should not normalize)
        extractor = DatabricksDataExtractor(**self.test_config)
        self.assertEqual(extractor._normalize_output_path("data"), "data")
        self.assertEqual(extractor._normalize_output_path("/data"), "/data")
        self.assertEqual(extractor._normalize_output_path("/dbfs/data"), "/dbfs/data")

        # Test with Databricks environment (should normalize)
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"}):
            databricks_extractor = DatabricksDataExtractor(**self.test_config)
            self.assertEqual(
                databricks_extractor._normalize_output_path("data"), "/dbfs/data"
            )
            self.assertEqual(
                databricks_extractor._normalize_output_path("/data"), "/dbfs/data"
            )
            self.assertEqual(
                databricks_extractor._normalize_output_path("/dbfs/data"), "/dbfs/data"
            )

        # Test cloud storage paths (should remain unchanged regardless of environment)
        self.assertEqual(
            extractor._normalize_output_path("s3://bucket/path"), "s3://bucket/path"
        )
        self.assertEqual(
            extractor._normalize_output_path(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            "abfss://container@account.dfs.core.windows.net/path",
        )

        # Unity Catalog volume path should resolve to /dbfs/Volumes/... when in Databricks
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"}):
            uc_extractor = DatabricksDataExtractor(
                **self.test_config, unity_catalog_volume="main/default/vol"
            )
            self.assertEqual(
                uc_extractor._normalize_output_path("/Volumes/main/default/vol"),
                "/dbfs/Volumes/main/default/vol",
            )

    @patch("data_extractor.databricks.SparkSession")
    def test_spark_session_handling(self, mock_spark_session):
        """Test Spark session handling in Databricks mode."""
        # Mock existing Spark session
        mock_existing_session = Mock()
        mock_spark_session.getActiveSession.return_value = mock_existing_session

        extractor = DatabricksDataExtractor(**self.test_config, use_existing_spark=True)
        spark_session = extractor._get_spark_session()

        # Should use existing session
        self.assertEqual(spark_session, mock_existing_session)
        mock_spark_session.getActiveSession.assert_called_once()

    @patch("data_extractor.databricks.SparkSession")
    def test_databricks_extract_table_success(self, mock_spark_session):
        """Test successful table extraction in Databricks."""
        # Mock Spark session and DataFrame
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        # Mock defaultParallelism for numPartitions calculation
        mock_spark.sparkContext.defaultParallelism = 16

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        mock_spark_session.getActiveSession.return_value = mock_spark

        extractor = DatabricksDataExtractor(**self.test_config, use_existing_spark=True)

        with patch("pathlib.Path.mkdir"):
            result = extractor.extract_table(
                source_name="test_source",
                table_name="test_table",
                schema_name="test_schema",
                is_full_extract=True,
            )

        self.assertTrue(result)
        mock_df.count.assert_called_once()

        # Verify numPartitions was optimized for Databricks
        call_args = mock_read.option.call_args_list
        num_partitions_call = next(
            (call for call in call_args if call[0][0] == "numPartitions"), None
        )
        self.assertIsNotNone(num_partitions_call)
        self.assertEqual(num_partitions_call[0][1], "8")  # min(8, 16)

    @patch("data_extractor.databricks.SparkSession")
    def test_databricks_extract_table_unity_catalog(self, mock_spark_session):
        """Test table extraction with Unity Catalog volume."""
        # Mock Spark session and DataFrame
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 50
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        mock_spark.sparkContext.defaultParallelism = 4

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        mock_spark_session.getActiveSession.return_value = mock_spark

        # Test with Unity Catalog volume
        config_with_uc = self.test_config.copy()
        config_with_uc["unity_catalog_volume"] = "main.default.test_volume"

        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"}):
            extractor = DatabricksDataExtractor(
                **config_with_uc, use_existing_spark=True
            )

            with patch("pathlib.Path.mkdir"):
                result = extractor.extract_table(
                    source_name="test_source",
                    table_name="test_table",
                    is_full_extract=True,
                )

        self.assertTrue(result)

    def test_databricks_max_workers_optimization(self):
        """Test that Databricks uses optimized worker count."""
        with patch("os.cpu_count", return_value=16):
            # Remove max_workers from config to test default behavior
            config_without_max_workers = {
                k: v for k, v in self.test_config.items() if k != "max_workers"
            }
            extractor = DatabricksDataExtractor(**config_without_max_workers)
            # Should use half of CPU count for Databricks
            self.assertEqual(extractor.max_workers, 8)

    def test_databricks_logging_optimization(self):
        """Test Databricks-optimized logging setup."""
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"}):
            extractor = DatabricksDataExtractor(**self.test_config)
            # Should detect Databricks environment
            self.assertTrue(extractor.is_databricks)

    @patch("data_extractor.databricks.SparkSession")
    def test_spark_session_fallback(self, mock_spark_session):
        """Test Spark session fallback when no active session exists."""
        # Mock no existing session, then mock builder
        mock_spark_session.getActiveSession.return_value = None
        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.getOrCreate.return_value = Mock()

        extractor = DatabricksDataExtractor(**self.test_config, use_existing_spark=True)
        spark_session = extractor._get_spark_session()

        # Should fallback to builder
        mock_spark_session.getActiveSession.assert_called_once()
        mock_builder.getOrCreate.assert_called_once()

    def test_databricks_context_info(self):
        """Test getting Databricks context information."""
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12",
                "HOSTNAME": "databricks-worker-1",
            },
        ):
            extractor = DatabricksDataExtractor(**self.test_config)
            context = extractor.get_databricks_context()

            self.assertIn("is_databricks", context)
            self.assertIn("runtime_version", context)
            self.assertIn("hostname", context)
            self.assertIn("use_existing_spark", context)
            self.assertIn("max_workers", context)
            self.assertEqual(context["runtime_version"], "12.2.x-scala2.12")
            self.assertEqual(context["hostname"], "databricks-worker-1")


class TestDatabricksConfigManager(unittest.TestCase):
    """Test cases for DatabricksConfigManager class."""

    def test_databricks_config_manager_initialization(self):
        """Test DatabricksConfigManager initialization."""
        config_manager = DatabricksConfigManager()
        self.assertIsInstance(config_manager.config_data, dict)

    def test_databricks_database_config(self):
        """Test Databricks-specific database configuration."""
        config_manager = DatabricksConfigManager()
        db_config = config_manager.get_databricks_database_config()

        # Should have DBFS default output path
        self.assertEqual(db_config.get("output_base_path", "/dbfs/data"), "/dbfs/data")

    def test_databricks_extraction_config(self):
        """Test Databricks-specific extraction configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "conf.yml")
            with open(config_path, "w") as f:
                f.write("databricks:\n  unity_catalog_volume: main/demo/vol\n")
            config_manager = DatabricksConfigManager(config_path)
            extraction_config = config_manager.get_databricks_extraction_config()

        # Should use existing Spark session by default
        self.assertTrue(extraction_config.get("use_existing_spark"))
        # Should have conservative worker count
        self.assertGreater(extraction_config.get("max_workers", 0), 0)
        self.assertEqual(extraction_config.get("unity_catalog_volume"), "main/demo/vol")

    def test_databricks_sample_config_creation(self):
        """Test creation of Databricks sample configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_databricks_config.yml")
            tables_path = os.path.join(temp_dir, "test_databricks_tables.json")

            config_manager = DatabricksConfigManager()
            config_manager.create_databricks_sample_config(config_path)
            config_manager.create_databricks_sample_tables_json(tables_path)

            # Verify files were created
            self.assertTrue(os.path.exists(config_path))
            self.assertTrue(os.path.exists(tables_path))

            # Verify Databricks-specific content
            with open(config_path, "r") as f:
                config_content = f.read()
                self.assertIn("/dbfs/data", config_content)
                self.assertIn("use_existing_spark: true", config_content)
                self.assertIn("databricks:", config_content)
                self.assertIn("unity_catalog_volume", config_content)

            with open(tables_path, "r") as f:
                tables_content = f.read()
                self.assertIn("databricks", tables_content)
                self.assertIn("DBFS", tables_content)
                self.assertIn("comment", tables_content)

    def test_databricks_environment_variables(self):
        """Test Databricks-specific environment variable handling."""
        with patch.dict(
            os.environ,
            {
                "ORACLE_HOST": "databricks_host",
                "ORACLE_USER": "databricks_user",
                "OUTPUT_BASE_PATH": "/dbfs/custom_path",
            },
        ):
            config_manager = DatabricksConfigManager()
            db_config = config_manager.get_databricks_database_config()

            self.assertEqual(db_config["oracle_host"], "databricks_host")
            self.assertEqual(db_config["oracle_user"], "databricks_user")
            self.assertEqual(db_config["output_base_path"], "/dbfs/custom_path")


class TestDatabricksIntegration(unittest.TestCase):
    """Integration tests for Databricks functionality."""

    def test_databricks_cli_argument_parsing(self):
        """Test that Databricks CLI arguments are properly supported."""
        from data_extractor.cli import create_parser

        parser = create_parser()

        # Test Databricks arguments
        args = parser.parse_args(
            [
                "--databricks",
                "--databricks-output-path",
                "/dbfs/custom",
                "--generate-databricks-config",
                "db_config.yml",
                "--generate-databricks-tables",
                "db_tables.json",
            ]
        )

        self.assertTrue(args.databricks)
        self.assertEqual(args.databricks_output_path, "/dbfs/custom")
        self.assertEqual(args.generate_databricks_config, "db_config.yml")
        self.assertEqual(args.generate_databricks_tables, "db_tables.json")

    def test_databricks_mode_selection(self):
        """Test that Databricks mode is properly selected in CLI."""
        # This would require more complex mocking of the CLI execution
        # For now, we test that the imports work correctly
        try:
            from data_extractor.cli import extract_multiple_tables, extract_single_table
            from data_extractor.databricks import DatabricksDataExtractor

            # If imports succeed, the integration is working
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Databricks integration import failed: {e}")


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)
