"""
Tests for the Databricks job entry point functionality.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from data_extractor import databricks_job


class TestDatabricksJob(unittest.TestCase):
    """Test cases for Databricks job functionality."""

    def test_get_widget_fallback(self):
        """Test widget fallback to environment variables."""
        os.environ["FOO"] = "bar"
        self.assertEqual(databricks_job._get_widget("foo"), "bar")
        
        # Clean up
        del os.environ["FOO"]

    def test_get_widget_with_default(self):
        """Test widget fallback with default value."""
        # Widget that doesn't exist should return default
        result = databricks_job._get_widget("non_existent_widget", "default_value")
        self.assertEqual(result, "default_value")

    @patch("data_extractor.databricks_job.DataExtractor")
    @patch("data_extractor.databricks_job.ConfigManager")
    def test_run_from_widgets_success(self, mock_manager_cls, mock_extractor_cls):
        """Test successful execution from widgets."""
        # Set up environment variables for configuration
        os.environ.update({
            "CONFIG_PATH": "c.yml",
            "TABLES_PATH": "t.json"
        })
        
        # Mock ConfigManager
        mock_manager = MagicMock()
        settings_obj = MagicMock()
        settings_obj.model_dump.return_value = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "/dbfs/data",
            "max_workers": 2,
        }
        mock_manager.get_app_settings.return_value = settings_obj
        mock_manager.load_table_configs_from_json.return_value = [
            {
                "source_name": "test_source",
                "table_name": "test_table",
                "is_full_extract": True
            }
        ]
        mock_manager_cls.return_value = mock_manager

        # Mock DataExtractor
        mock_extractor = MagicMock()
        mock_extractor.extract_tables_parallel.return_value = {"test_table": True}
        mock_extractor_cls.return_value = mock_extractor

        try:
            result = databricks_job.run_from_widgets()
            
            self.assertEqual(result, {"test_table": True})
            mock_manager_cls.assert_called_with("c.yml")
            mock_extractor.extract_tables_parallel.assert_called_once()
            mock_extractor.cleanup_spark_sessions.assert_called_once()
            
        finally:
            # Clean up environment variables
            for key in ["CONFIG_PATH", "TABLES_PATH"]:
                if key in os.environ:
                    del os.environ[key]

    @patch("data_extractor.databricks_job.DataExtractor")
    @patch("data_extractor.databricks_job.ConfigManager")
    def test_run_from_widgets_with_failures(self, mock_manager_cls, mock_extractor_cls):
        """Test handling of extraction failures."""
        # Set up environment variables
        os.environ.update({
            "CONFIG_PATH": "c.yml",
            "TABLES_PATH": "t.json"
        })
        
        # Mock ConfigManager
        mock_manager = MagicMock()
        settings_obj = MagicMock()
        settings_obj.model_dump.return_value = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "/dbfs/data",
            "max_workers": 2,
        }
        mock_manager.get_app_settings.return_value = settings_obj
        mock_manager.load_table_configs_from_json.return_value = []
        mock_manager_cls.return_value = mock_manager

        # Mock DataExtractor with failures
        mock_extractor = MagicMock()
        mock_extractor.extract_tables_parallel.return_value = {
            "table1": True,
            "table2": False  # This table failed
        }
        mock_extractor_cls.return_value = mock_extractor

        try:
            with self.assertRaises(RuntimeError) as context:
                databricks_job.run_from_widgets()
            
            self.assertIn("Extraction failed for tables", str(context.exception))
            self.assertIn("table2", str(context.exception))
            
        finally:
            # Clean up environment variables
            for key in ["CONFIG_PATH", "TABLES_PATH"]:
                if key in os.environ:
                    del os.environ[key]

    @patch("data_extractor.databricks_job.DataExtractor")
    @patch("data_extractor.databricks_job.ConfigManager")
    def test_run_from_widgets_no_config_files(self, mock_manager_cls, mock_extractor_cls):
        """Test execution without config files."""
        # Ensure environment variables are clean
        env_vars_to_clean = ["CONFIG_PATH", "TABLES_PATH", "OUTPUT_PATH", "MAX_WORKERS", "RUN_ID"]
        for var in env_vars_to_clean:
            if var in os.environ:
                del os.environ[var]
        
        # Mock ConfigManager
        mock_manager = MagicMock()
        settings_obj = MagicMock()
        settings_obj.model_dump.return_value = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "data",  # Default value
            "max_workers": None,
        }
        mock_manager.get_app_settings.return_value = settings_obj
        mock_manager.load_table_configs_from_json.return_value = []
        mock_manager_cls.return_value = mock_manager

        # Mock DataExtractor
        mock_extractor = MagicMock()
        mock_extractor.extract_tables_parallel.return_value = {}
        mock_extractor_cls.return_value = mock_extractor

        result = databricks_job.run_from_widgets()
        
        self.assertEqual(result, {})
        # Should be called with None when no config file specified (no args)
        mock_manager_cls.assert_called_with()

    @patch("data_extractor.databricks_job.DataExtractor")
    @patch("data_extractor.databricks_job.ConfigManager")
    def test_run_from_widgets_with_overrides(self, mock_manager_cls, mock_extractor_cls):
        """Test widget parameter overrides."""
        # Set up environment variables with overrides
        os.environ.update({
            "OUTPUT_PATH": "/custom/path",
            "MAX_WORKERS": "4",
            "RUN_ID": "custom_run_123"
        })
        
        # Mock ConfigManager
        mock_manager = MagicMock()
        settings_obj = MagicMock()
        settings_obj.model_dump.return_value = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "/custom/path",
            "max_workers": 4,
            "run_id": "custom_run_123",
        }
        mock_manager.get_app_settings.return_value = settings_obj
        mock_manager.load_table_configs_from_json.return_value = []
        mock_manager_cls.return_value = mock_manager

        # Mock DataExtractor
        mock_extractor = MagicMock()
        mock_extractor.extract_tables_parallel.return_value = {}
        mock_extractor_cls.return_value = mock_extractor

        try:
            result = databricks_job.run_from_widgets()
            
            self.assertEqual(result, {})
            
            # Verify that the widget parameters were passed as overrides
            call_args = mock_manager.get_app_settings.call_args
            overrides = call_args[0][0] if call_args and call_args[0] else {}
            self.assertEqual(overrides.get("output_base_path"), "/custom/path")
            self.assertEqual(overrides.get("max_workers"), 4)
            self.assertEqual(overrides.get("run_id"), "custom_run_123")
            
        finally:
            # Clean up environment variables
            for key in ["OUTPUT_PATH", "MAX_WORKERS", "RUN_ID"]:
                if key in os.environ:
                    del os.environ[key]


if __name__ == "__main__":
    unittest.main()
