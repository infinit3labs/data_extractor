import unittest
from unittest.mock import Mock, patch

from data_extractor.sqlserver import SqlServerDataExtractor


class TestSqlServerExtractor(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = {
            "sqlserver_host": "localhost",
            "sqlserver_port": "1433",
            "sqlserver_database": "master",
            "sqlserver_user": "sa",
            "sqlserver_password": "pwd",
            "output_base_path": "/tmp/out",
        }

    def test_init(self) -> None:
        extractor = SqlServerDataExtractor(**self.cfg)
        self.assertEqual(extractor.sqlserver_host, "localhost")
        self.assertEqual(
            extractor.jdbc_url,
            "jdbc:sqlserver://localhost:1433;database=master",
        )
        props = {
            "user": "sa",
            "password": "pwd",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        self.assertEqual(extractor.connection_properties, props)

    @patch("data_extractor.sqlserver.SparkSession")
    def test_spark_session(self, mock_session: Mock) -> None:
        builder = Mock()
        mock_session.builder = builder
        builder.appName.return_value = builder
        builder.config.return_value = builder
        builder.getOrCreate.return_value = Mock()

        extractor = SqlServerDataExtractor(**self.cfg)
        extractor._get_spark_session()

        builder.config.assert_any_call(
            "spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11"
        )


if __name__ == "__main__":
    unittest.main()
