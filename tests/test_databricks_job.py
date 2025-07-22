import os
import unittest
from unittest.mock import patch, MagicMock

from data_extractor import databricks_job


class TestDatabricksJob(unittest.TestCase):
    def test_get_widget_fallback(self):
        os.environ["FOO"] = "bar"
        self.assertEqual(databricks_job._get_widget("foo"), "bar")

    @patch("data_extractor.databricks_job.DataExtractor")
    @patch("data_extractor.databricks_job.ConfigManager")
    def test_run_from_widgets(self, mock_manager_cls, mock_extractor_cls):
        os.environ["CONFIG_PATH"] = "c.ini"
        os.environ["TABLES_PATH"] = "t.json"
        mock_manager = MagicMock()
        mock_manager.get_runtime_config.return_value = {
            "oracle_host": "h",
            "oracle_port": "1521",
            "oracle_service": "s",
            "oracle_user": "u",
            "oracle_password": "p",
            "output_base_path": "o",
            "max_workers": 1,
        }
        mock_manager.load_table_configs_from_json.return_value = []
        mock_manager_cls.return_value = mock_manager

        mock_extractor = MagicMock()
        mock_extractor.extract_tables_parallel.return_value = {}
        mock_extractor_cls.return_value = mock_extractor

        result = databricks_job.run_from_widgets()
        self.assertEqual(result, {})
        mock_manager_cls.assert_called_with("c.ini")
        mock_extractor.extract_tables_parallel.assert_called_with([])


