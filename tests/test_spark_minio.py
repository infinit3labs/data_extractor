import unittest
from unittest.mock import Mock, call, patch

from data_extractor.core import DataExtractor


class TestSparkAndS3Config(unittest.TestCase):
    @patch("data_extractor.core.SparkSession")
    def test_spark_master_and_s3_options(self, mock_session):
        builder = Mock()
        mock_session.builder = builder
        builder.appName.return_value = builder
        builder.config.return_value = builder
        builder.master.return_value = builder
        builder.getOrCreate.return_value = Mock()

        extractor = DataExtractor(
            oracle_host="h",
            oracle_port="1",
            oracle_service="s",
            oracle_user="u",
            oracle_password="p",
            spark_master="spark://spark-master:7077",
            s3_endpoint="http://minio:9000",
            s3_access_key="key",
            s3_secret_key="secret",
        )

        extractor._get_spark_session()

        builder.master.assert_called_with("spark://spark-master:7077")
        self.assertIn(
            call("spark.hadoop.fs.s3a.endpoint", "http://minio:9000"),
            builder.config.call_args_list,
        )
        self.assertIn(
            call("spark.hadoop.fs.s3a.access.key", "key"), builder.config.call_args_list
        )
        self.assertIn(
            call("spark.hadoop.fs.s3a.secret.key", "secret"),
            builder.config.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
