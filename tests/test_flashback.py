import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from data_extractor.core import DataExtractor


class TestFlashbackQueries(unittest.TestCase):
    def setUp(self):
        self.test_config = {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_pass",
            "output_base_path": "/tmp/data",
            "max_workers": 2,
        }

    @patch("data_extractor.core.SparkSession")
    def test_flashback_query_clause(self, mock_spark_session):
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 1
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

        ts = datetime(2024, 1, 1, 12, 0, 0)

        with patch("pathlib.Path.mkdir"):
            result = extractor.extract_table(
                source_name="src",
                table_name="tbl",
                schema_name="sch",
                is_full_extract=True,
                flashback_enabled=True,
                flashback_timestamp=ts,
            )

        self.assertTrue(result)
        call_args = mock_read.option.call_args_list
        query_call = next(
            (c for c in call_args if c[0][0] == "query"),
            None,
        )
        self.assertIsNotNone(query_call)
        self.assertIn("AS OF TIMESTAMP", query_call[0][1])
        self.assertIn(ts.strftime("%Y-%m-%d %H:%M:%S"), query_call[0][1])


if __name__ == "__main__":
    unittest.main()

