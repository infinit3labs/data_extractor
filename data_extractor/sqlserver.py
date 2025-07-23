"""SQL Server data extraction module."""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import SparkSession


class SqlServerDataExtractor:
    """Data extractor for Microsoft SQL Server databases."""

    def __init__(
        self,
        sqlserver_host: str,
        sqlserver_port: str,
        sqlserver_database: str,
        sqlserver_user: str,
        sqlserver_password: str,
        output_base_path: str = "data",
        max_workers: Optional[int] = None,
        jdbc_fetch_size: int = 10000,
        jdbc_num_partitions: int = 4,
    ) -> None:
        self.sqlserver_host = sqlserver_host
        self.sqlserver_port = sqlserver_port
        self.sqlserver_database = sqlserver_database
        self.sqlserver_user = sqlserver_user
        self.sqlserver_password = sqlserver_password
        self.output_base_path = output_base_path
        self.max_workers = max_workers or os.cpu_count()
        self.jdbc_fetch_size = jdbc_fetch_size
        self.jdbc_num_partitions = jdbc_num_partitions

        self.jdbc_url = (
            f"jdbc:sqlserver://{sqlserver_host}:{sqlserver_port};database={sqlserver_database}"
        )
        self.connection_properties = {
            "user": sqlserver_user,
            "password": sqlserver_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        self._local = threading.local()
        self._setup_logging()

    def _setup_logging(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("data_extractor.log"),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def _get_spark_session(self) -> SparkSession:
        if not hasattr(self._local, "spark"):
            self._local.spark = (
                SparkSession.builder.appName(
                    f"SqlServerDataExtractor-{threading.current_thread().name}"
                )
                .config(
                    "spark.jars.packages",
                    "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11",
                )
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()
            )
        return self._local.spark

    def _is_first_run(self, source_name: str, table_name: str) -> bool:
        table_path = os.path.join(self.output_base_path, source_name, table_name)
        return not os.path.exists(table_path) or not any(Path(table_path).rglob("*.parquet"))

    def extract_table(
        self,
        source_name: str,
        table_name: str,
        schema_name: Optional[str] = None,
        incremental_column: Optional[str] = None,
        extraction_date: Optional[datetime] = None,
        is_full_extract: bool = False,
        custom_query: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> bool:
        thread_name = threading.current_thread().name

        if (
            not is_full_extract
            and incremental_column
            and self._is_first_run(source_name, table_name)
        ):
            self.logger.info(
                "[%s] First run detected for %s, switching to full extract",
                thread_name,
                table_name,
            )
            is_full_extract = True

        try:
            if not run_id:
                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            if not extraction_date:
                extraction_date = datetime.now() - timedelta(days=1)

            self.logger.info(
                "[%s] Starting extraction for table: %s",
                thread_name,
                table_name,
            )

            spark = self._get_spark_session()

            if custom_query:
                query = custom_query
            else:
                full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                if is_full_extract or not incremental_column:
                    query = f"SELECT * FROM {full_table_name}"
                else:
                    start_date = extraction_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    end_date = start_date + timedelta(days=1)
                    query = f"""
                    SELECT * FROM {full_table_name}
                    WHERE {incremental_column} >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND {incremental_column} < '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    """
            self.logger.info("[%s] Executing query: %s", thread_name, query)

            df = (
                spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("query", query)
                .option("user", self.sqlserver_user)
                .option("password", self.sqlserver_password)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("fetchsize", str(self.jdbc_fetch_size))
                .option("numPartitions", str(self.jdbc_num_partitions))
                .load()
            )

            record_count = df.count()
            self.logger.info(
                "[%s] Extracted %d records from %s",
                thread_name,
                record_count,
                table_name,
            )
            if record_count == 0:
                self.logger.warning("[%s] No data found for table %s", thread_name, table_name)
                return True

            year_month = extraction_date.strftime("%Y%m")
            day = extraction_date.strftime("%d")
            output_path = os.path.join(
                self.output_base_path,
                source_name,
                table_name,
                year_month,
                day,
                f"{run_id}.parquet",
            )

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            self.logger.info("[%s] Saving to: %s", thread_name, output_path)
            df.coalesce(1).write.mode("overwrite").parquet(output_path)

            self.logger.info(
                "[%s] Successfully extracted table: %s", thread_name, table_name
            )
            return True
        except (ConnectionError, ValueError, RuntimeError, 
                py4j.protocol.Py4JJavaError, pyspark.sql.utils.AnalysisException) as exc:
            self.logger.error(
                "[%s] Known error extracting table %s: %s", thread_name, table_name, str(exc)
            )
            return False
        except Exception as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            import traceback

            self.logger.error(
                "[%s] Unexpected error extracting table %s: %s\n%s",
                thread_name,
                table_name,
                str(exc),
                traceback.format_exc(),
            )
            return False

    def extract_tables_parallel(self, table_configs: List[Dict]) -> Dict[str, bool]:
        self.logger.info(
            "Starting parallel extraction of %d tables using %d workers",
            len(table_configs),
            self.max_workers,
        )
        results: Dict[str, bool] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_table = {}
            for config in table_configs:
                source_name = config.get("source_name")
                table_name = config.get("table_name")
                if not source_name or not table_name:
                    self.logger.error(
                        "Skipping table config missing required fields: source_name=%s, table_name=%s",
                        source_name,
                        table_name,
                    )
                    continue
                future = executor.submit(
                    self.extract_table,
                    source_name=source_name,
                    table_name=table_name,
                    schema_name=config.get("schema_name"),
                    incremental_column=config.get("incremental_column"),
                    extraction_date=config.get("extraction_date"),
                    is_full_extract=config.get("is_full_extract", False),
                    custom_query=config.get("custom_query"),
                    run_id=config.get("run_id"),
                )
                future_to_table[future] = table_name
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    success = future.result()
                    results[table_name] = success
                    if success:
                        self.logger.info(
                            "Successfully completed extraction for table: %s",
                            table_name,
                        )
                    else:
                        self.logger.error("Failed extraction for table: %s", table_name)
                except (ConnectionError, ValueError, RuntimeError) as exc:
                    self.logger.error(
                        "Known exception during extraction of table %s: %s",
                        table_name,
                        str(exc),
                    )
                    results[table_name] = False
                except Exception as exc:
                    self.logger.error(
                        "Unexpected exception during extraction of table %s: %s",
                        table_name,
                        str(exc),
                    )
                    results[table_name] = False
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        self.logger.info(
            "Parallel extraction completed: %d/%d tables successful",
            successful,
            total,
        )
        return results

    def cleanup_spark_sessions(self) -> None:
        if hasattr(self._local, "spark"):
            self._local.spark.stop()
            del self._local.spark
