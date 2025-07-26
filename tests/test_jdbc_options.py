import unittest
from unittest.mock import Mock, call, patch

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


class TestJdbcOptions(unittest.TestCase):
    def test_config_manager_returns_jdbc_options(self):
        manager = ConfigManager("config/config.yml")
        extraction = manager.get_extraction_config()
        self.assertIn("jdbc_fetch_size", extraction)
        self.assertIn("jdbc_num_partitions", extraction)

    @patch("data_extractor.core.SparkSession")
    def test_data_extractor_applies_jdbc_options(self, mock_session):
        mock_builder = Mock()
        mock_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = Mock(read=Mock())

        extractor = DataExtractor(
            oracle_host="h",
            oracle_port="1",
            oracle_service="s",
            oracle_user="u",
            oracle_password="p",
            jdbc_fetch_size=123,
            jdbc_num_partitions=7,
        )

        with patch.object(extractor, "_get_spark_session") as get_spark:
            spark = Mock()
            reader = Mock()
            reader.format.return_value = reader
            reader.option.return_value = reader
            reader.load.return_value = Mock(count=Mock(return_value=0))
            spark.read = reader
            get_spark.return_value = spark

            result = extractor.extract_table(
                source_name="src", table_name="tbl", is_full_extract=True
            )

        self.assertTrue(result)
        self.assertIn(call("fetchsize", "123"), reader.option.call_args_list)
        self.assertIn(call("numPartitions", "7"), reader.option.call_args_list)


if __name__ == "__main__":
    unittest.main()
