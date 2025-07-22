"""
Core data extraction module using Spark JDBC for Oracle databases.
Supports parallel processing and both incremental and full table extraction.
"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import SparkSession


class DataExtractor:
    """
    Main data extraction class that handles Spark JDBC connections to Oracle
    and manages parallel extraction of multiple tables.
    """

    def __init__(
        self,
        oracle_host: str,
        oracle_port: str,
        oracle_service: str,
        oracle_user: str,
        oracle_password: str,
        output_base_path: str = "data",
        max_workers: Optional[int] = None,
    ):
        """
        Initialize the DataExtractor.

        Args:
            oracle_host: Oracle database host
            oracle_port: Oracle database port
            oracle_service: Oracle service name
            oracle_user: Database username
            oracle_password: Database password
            output_base_path: Base path for output files
            max_workers: Maximum number of worker threads (default: CPU count)
        """
        self.oracle_host = oracle_host
        self.oracle_port = oracle_port
        self.oracle_service = oracle_service
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
        self.output_base_path = output_base_path
        self.max_workers = max_workers or os.cpu_count()

        # JDBC connection properties
        self.jdbc_url = (
            f"jdbc:oracle:thin:@{oracle_host}:{oracle_port}:{oracle_service}"
        )
        self.connection_properties = {
            "user": oracle_user,
            "password": oracle_password,
            "driver": "oracle.jdbc.driver.OracleDriver",
        }

        # Thread-local storage for Spark sessions
        self._local = threading.local()

        # Setup logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Setup logging configuration."""
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
        """
        Get or create a thread-local Spark session.
        Each thread gets its own Spark session for true parallel processing.
        """
        if not hasattr(self._local, "spark"):
            self._local.spark = (
                SparkSession.builder.appName(
                    f"DataExtractor-{threading.current_thread().name}"
                )
                .config(
                    "spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.7.0.0"
                )
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config(
                    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                )
                .getOrCreate()
            )

        return self._local.spark

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
        """
        Extract a single table from Oracle database and save as Parquet.

        Args:
            source_name: Name of the data source
            table_name: Name of the table to extract
            schema_name: Schema name (optional)
            incremental_column: Column for incremental extraction
            extraction_date: Date for incremental extraction (default: yesterday)
            is_full_extract: Whether to perform full table extraction
            custom_query: Custom SQL query to use instead of table name
            run_id: Unique run identifier

        Returns:
            bool: True if extraction was successful, False otherwise
        """
        thread_name = threading.current_thread().name

        try:
            # Generate run_id if not provided
            if not run_id:
                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Set extraction date to yesterday if not provided
            if not extraction_date:
                extraction_date = datetime.now() - timedelta(days=1)

            self.logger.info(
                f"[{thread_name}] Starting extraction for table: {table_name}"
            )

            # Get Spark session for this thread
            spark = self._get_spark_session()

            # Build the query
            if custom_query:
                query = custom_query
            else:
                full_table_name = (
                    f"{schema_name}.{table_name}" if schema_name else table_name
                )

                if is_full_extract or not incremental_column:
                    query = f"SELECT * FROM {full_table_name}"
                else:
                    # Incremental extraction for 24-hour window
                    start_date = extraction_date.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    end_date = start_date + timedelta(days=1)

                    query = f"""
                    SELECT * FROM {full_table_name}
                    WHERE {incremental_column} >= TO_DATE('{start_date.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')
                    AND {incremental_column} < TO_DATE('{end_date.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')
                    """

            self.logger.info(f"[{thread_name}] Executing query: {query}")

            # Extract data using Spark JDBC
            df = (
                spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("query", query)
                .option("user", self.oracle_user)
                .option("password", self.oracle_password)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("fetchsize", "10000")
                .option("numPartitions", "4")
                .load()
            )

            # Check if data was extracted
            record_count = df.count()
            self.logger.info(
                f"[{thread_name}] Extracted {record_count} records from {table_name}"
            )

            if record_count == 0:
                self.logger.warning(
                    f"[{thread_name}] No data found for table {table_name}"
                )
                return True  # Not an error, just no data

            # Prepare output path: data/source_name/entity_name/yyyymm/dd/run_id.parquet
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

            # Create directory if it doesn't exist
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            # Save as Parquet
            self.logger.info(f"[{thread_name}] Saving to: {output_path}")
            df.coalesce(1).write.mode("overwrite").parquet(output_path)

            self.logger.info(
                f"[{thread_name}] Successfully extracted table: {table_name}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"[{thread_name}] Error extracting table {table_name}: {str(e)}"
            )
            return False

    def extract_tables_parallel(self, table_configs: List[Dict]) -> Dict[str, bool]:
        """
        Extract multiple tables in parallel using threading.

        Args:
            table_configs: List of table configuration dictionaries

        Returns:
            Dict mapping table names to extraction success status
        """
        self.logger.info(
            f"Starting parallel extraction of {len(table_configs)} tables using {self.max_workers} workers"
        )

        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all extraction tasks
            future_to_table = {}

            for config in table_configs:
                future = executor.submit(
                    self.extract_table,
                    source_name=config.get("source_name"),
                    table_name=config.get("table_name"),
                    schema_name=config.get("schema_name"),
                    incremental_column=config.get("incremental_column"),
                    extraction_date=config.get("extraction_date"),
                    is_full_extract=config.get("is_full_extract", False),
                    custom_query=config.get("custom_query"),
                    run_id=config.get("run_id"),
                )
                future_to_table[future] = config.get("table_name")

            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    success = future.result()
                    results[table_name] = success

                    if success:
                        self.logger.info(
                            f"Successfully completed extraction for table: {table_name}"
                        )
                    else:
                        self.logger.error(f"Failed extraction for table: {table_name}")

                except Exception as e:
                    self.logger.error(
                        f"Exception during extraction of table {table_name}: {str(e)}"
                    )
                    results[table_name] = False

        # Log summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        self.logger.info(
            f"Parallel extraction completed: {successful}/{total} tables successful"
        )

        return results

    def cleanup_spark_sessions(self) -> None:
        """Clean up Spark sessions for all threads."""
        if hasattr(self._local, "spark"):
            self._local.spark.stop()
            del self._local.spark
