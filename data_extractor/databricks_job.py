"""Databricks job entry point for running data extraction."""
from typing import Optional
import os

from .config import ConfigManager
from .core import DataExtractor

try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - handled in tests
    SparkSession = None  # type: ignore


def run_job(config_path: str, tables_path: str, output_path: Optional[str] = None, use_global_spark_session: bool = True) -> None:
    """Run extraction inside a Databricks job.

    Parameters
    ----------
    config_path: str
        Path to INI configuration file.
    tables_path: str
        Path to tables JSON configuration file.
    output_path: Optional[str]
        Override output path. Defaults to value from config.
    use_global_spark_session: bool
        Whether to reuse the Databricks provided Spark session.
    """
    if SparkSession is None:
        raise RuntimeError("Spark is required to run in Databricks environment")

    dbutils = globals().get("dbutils")
    if dbutils is None:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
        except Exception:
            raise RuntimeError("dbutils is not available. This script must run on Databricks.")

    config_manager = ConfigManager(config_path)
    db_conf = config_manager.get_runtime_config(output_base_path=output_path, use_global_spark_session=use_global_spark_session)
    table_configs = config_manager.load_table_configs_from_json(tables_path)

    extractor = DataExtractor(
        oracle_host=db_conf['oracle_host'],
        oracle_port=db_conf.get('oracle_port', '1521'),
        oracle_service=db_conf['oracle_service'],
        oracle_user=db_conf['oracle_user'],
        oracle_password=db_conf['oracle_password'],
        output_base_path=db_conf.get('output_base_path', 'data'),
        max_workers=db_conf.get('max_workers'),
        use_global_spark_session=db_conf.get('use_global_spark_session', use_global_spark_session),
    )

    results = extractor.extract_tables_parallel(table_configs)
    extractor.cleanup_spark_sessions()

    failed = [name for name, success in results.items() if not success]
    if failed:
        dbutils.notebook.exit(f"Failed tables: {failed}")
    else:
        dbutils.notebook.exit("Success")

