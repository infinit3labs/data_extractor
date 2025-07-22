"""
Databricks-specific data extraction module.
Provides Databricks-optimized functionality for running in Databricks cluster context.
"""

import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pyspark.sql import SparkSession

from .config import ConfigManager


class DatabricksDataExtractor:
    """
    Databricks-optimized data extraction class.

    This class is designed to run efficiently in Databricks cluster context,
    utilizing the existing Spark session and Databricks-specific features.
    """

    def __init__(
        self,
        oracle_host: str,
        oracle_port: str,
        oracle_service: str,
        oracle_user: str,
        oracle_password: str,
        output_base_path: str = "/dbfs/data",
        max_workers: Optional[int] = None,
        use_existing_spark: bool = True,
        unity_catalog_volume: Optional[str] = None,
    ):
        """
        Initialize the DatabricksDataExtractor.

        Args:
            oracle_host: Oracle database host
            oracle_port: Oracle database port
            oracle_service: Oracle service name
            oracle_user: Database username
            oracle_password: Database password
            output_base_path: Base path for output files (default: /dbfs/data)
            max_workers: Maximum number of worker threads (default: CPU count / 2 for Databricks)
            use_existing_spark: Whether to use existing Spark session (recommended for Databricks)
            unity_catalog_volume: Optional Unity Catalog volume path in the form
                "catalog/schema/volume". When provided, output paths will be
                written to this volume instead of ``output_base_path``.
        """
        self.oracle_host = oracle_host
        self.oracle_port = oracle_port
        self.oracle_service = oracle_service
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
        self.output_base_path = output_base_path
        self.use_existing_spark = use_existing_spark
        self.unity_catalog_volume = unity_catalog_volume

        # In Databricks, use fewer workers to avoid overwhelming shared clusters
        cpu_count = os.cpu_count() or 2
        self.max_workers = max_workers or max(1, cpu_count // 2)

        # JDBC connection properties
        self.jdbc_url = (
            f"jdbc:oracle:thin:@{oracle_host}:{oracle_port}:{oracle_service}"
        )
        self.connection_properties = {
            "user": oracle_user,
            "password": oracle_password,
            "driver": "oracle.jdbc.driver.OracleDriver",
        }

        # Thread-local storage for Spark sessions (only used if use_existing_spark=False)
        self._local = threading.local()

        # Check if running in Databricks environment first
        self.is_databricks = self._is_databricks_environment()

        # Setup logging for Databricks
        self._setup_databricks_logging()

        if self.is_databricks:
            self.logger.info("Detected Databricks environment")

    def _is_databricks_environment(self) -> bool:
        """Check if running in Databricks environment."""
        return any(
            [
                "DATABRICKS_RUNTIME_VERSION" in os.environ,
                "SPARK_HOME" in os.environ
                and "databricks" in os.environ.get("SPARK_HOME", "").lower(),
                os.path.exists("/databricks"),
                "databricks" in os.environ.get("HOSTNAME", "").lower(),
            ]
        )

    def _setup_databricks_logging(self) -> None:
        """Setup logging optimized for Databricks."""
        # In Databricks, prefer console output over file logging
        if self.is_databricks or self._is_databricks_environment():
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
                handlers=[logging.StreamHandler()],  # Only console in Databricks
            )
        else:
            # Fallback to file + console for development/testing
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.StreamHandler(),
                    logging.FileHandler("data_extractor_databricks.log"),
                ],
            )
        self.logger = logging.getLogger(__name__)

    def _get_spark_session(self) -> SparkSession:
        """
        Get Spark session - use existing Databricks session by default.
        """
        if self.use_existing_spark:
            # In Databricks, use the existing Spark session
            spark = SparkSession.getActiveSession()
            if spark is None:
                # Fallback to getting or creating a session
                spark = SparkSession.builder.getOrCreate()
            return spark
        else:
            # Create thread-local sessions (not recommended for Databricks)
            if not hasattr(self._local, "spark"):
                self._local.spark = (
                    SparkSession.builder.appName(
                        f"DataExtractor-Databricks-{threading.current_thread().name}"
                    )
                    .config(
                        "spark.jars.packages",
                        "com.oracle.database.jdbc:ojdbc8:21.7.0.0",
                    )
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config(
                        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                    )
                    .getOrCreate()
                )
            return self._local.spark

    def _normalize_output_path(self, path: str) -> str:
        """
        Normalize output path for Databricks context.

        Args:
            path: Original path

        Returns:
            Normalized path for Databricks (e.g., /dbfs/, s3://, abfss://)
        """
        # If path already starts with a cloud storage prefix, return as-is
        cloud_prefixes = ["s3://", "abfss://", "adl://", "gs://", "/dbfs/"]
        if any(path.startswith(prefix) for prefix in cloud_prefixes):
            return path

        # If running in Databricks and path doesn't start with /dbfs/, add it
        if self.is_databricks and not path.startswith("/dbfs/"):
            # Convert relative paths to /dbfs/ paths
            if not path.startswith("/"):
                path = f"/dbfs/{path}"
            else:
                path = f"/dbfs{path}"

        return path

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
        Optimized for Databricks environment.

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
                f"[{thread_name}] Starting Databricks extraction for table: {table_name}"
            )

            # Get Spark session (use existing Databricks session)
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

            # Extract data using Spark JDBC with Databricks optimizations
            df = (
                spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("query", query)
                .option("user", self.oracle_user)
                .option("password", self.oracle_password)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("fetchsize", "10000")
                .option(
                    "numPartitions", str(min(8, spark.sparkContext.defaultParallelism))
                )
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

            # Prepare output path for Databricks: data/source_name/entity_name/yyyymm/dd/run_id.parquet
            year_month = extraction_date.strftime("%Y%m")
            day = extraction_date.strftime("%d")

            if self.unity_catalog_volume:
                uc_base = f"/Volumes/{self.unity_catalog_volume.replace('.', '/')}"
                base_output_path = self._normalize_output_path(uc_base)
            else:
                base_output_path = self._normalize_output_path(self.output_base_path)
            output_path = os.path.join(
                base_output_path,
                source_name,
                table_name,
                year_month,
                day,
                f"{run_id}.parquet",
            )

            # Create directory if it doesn't exist (for DBFS paths)
            if output_path.startswith("/dbfs/"):
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            # Save as Parquet with Databricks optimizations
            self.logger.info(f"[{thread_name}] Saving to: {output_path}")

            # Use coalesce based on data size for better performance in Databricks
            num_partitions = min(max(1, record_count // 100000), 10)
            df.coalesce(num_partitions).write.mode("overwrite").parquet(output_path)

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
        Extract multiple tables in parallel using threading, optimized for Databricks.

        Args:
            table_configs: List of table configuration dictionaries

        Returns:
            Dict mapping table names to extraction success status
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self.logger.info(
            f"Starting Databricks parallel extraction of {len(table_configs)} tables using {self.max_workers} workers"
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
                            f"Successfully completed Databricks extraction for table: {table_name}"
                        )
                    else:
                        self.logger.error(
                            f"Failed Databricks extraction for table: {table_name}"
                        )

                except Exception as e:
                    self.logger.error(
                        f"Exception during Databricks extraction of table {table_name}: {str(e)}"
                    )
                    results[table_name] = False

        # Log summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        self.logger.info(
            f"Databricks parallel extraction completed: {successful}/{total} tables successful"
        )

        return results

    def cleanup_spark_sessions(self):
        """Clean up Spark sessions if created locally (not recommended for Databricks)."""
        if not self.use_existing_spark and hasattr(self._local, "spark"):
            self._local.spark.stop()
            del self._local.spark

    def get_databricks_context(self) -> Dict[str, Any]:
        """
        Get Databricks context information for debugging and logging.

        Returns:
            Dict containing Databricks environment information
        """
        context = {
            "is_databricks": self.is_databricks,
            "runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
            "spark_home": os.environ.get("SPARK_HOME"),
            "hostname": os.environ.get("HOSTNAME"),
            "use_existing_spark": self.use_existing_spark,
            "max_workers": self.max_workers,
            "output_base_path": self.output_base_path,
            "unity_catalog_volume": self.unity_catalog_volume,
        }

        # Try to get Spark context information
        try:
            spark = self._get_spark_session()
            context.update(
                {
                    "spark_version": spark.version,
                    "spark_app_name": spark.sparkContext.appName,
                    "spark_master": spark.sparkContext.master,
                    "default_parallelism": spark.sparkContext.defaultParallelism,
                }
            )
        except Exception as e:
            context["spark_error"] = str(e)

        return context


class DatabricksConfigManager(ConfigManager):
    """
    Databricks-specific configuration manager.
    Extends the base ConfigManager with Databricks-specific functionality.
    """

    def __init__(self, config_file: Optional[str] = None) -> None:
        """Initialize Databricks configuration manager."""
        super().__init__(config_file)

    def get_databricks_database_config(self) -> Dict[str, str]:
        """
        Get database configuration with Databricks-specific defaults.

        Returns:
            Dict containing database connection parameters optimized for Databricks
        """
        config = self.get_database_config()
        if (
            not config.get("output_base_path")
            or config.get("output_base_path") == "data"
        ):
            config["output_base_path"] = "/dbfs/data"
        return config

    def get_databricks_extraction_config(self) -> Dict[str, Any]:
        """
        Get extraction configuration with Databricks-specific defaults.

        Returns:
            Dict containing extraction parameters optimized for Databricks
        """
        config = self.get_extraction_config()
        if not config.get("max_workers"):
            cpu_count = os.cpu_count() or 2
            config["max_workers"] = max(1, cpu_count // 2)
        config["use_existing_spark"] = True
        if (
            "databricks" in self.config_data
            and isinstance(self.config_data["databricks"], dict)
            and self.config_data["databricks"].get("unity_catalog_volume")
        ):
            config["unity_catalog_volume"] = self.config_data["databricks"].get(
                "unity_catalog_volume"
            )
        return config

    def create_databricks_sample_config(self, config_path: str):
        """
        Create a sample configuration file optimized for Databricks.

        Args:
            config_path: Path where to create the sample config file
        """
        sample_config = {
            "database": {
                "oracle_host": "your_oracle_host",
                "oracle_port": "1521",
                "oracle_service": "your_service_name",
                "oracle_user": "your_username",
                "oracle_password": "your_password",
                "output_base_path": "/dbfs/data",
            },
            "extraction": {
                "max_workers": 4,
                "default_source": "oracle_db",
                "use_existing_spark": True,
            },
            "databricks": {
                "cluster_mode": "shared",
                "log_level": "INFO",
                "output_format": "parquet",
                "unity_catalog_volume": "main/default/volume",
            },
        }

        with open(config_path, "w") as f:
            yaml.safe_dump(sample_config, f)

    def create_databricks_sample_tables_json(self, json_path: str):
        """
        Create a sample tables configuration JSON file for Databricks.

        Args:
            json_path: Path where to create the sample JSON file
        """
        import json

        sample_tables = {
            "tables": [
                {
                    "source_name": "oracle_db",
                    "table_name": "employees",
                    "schema_name": "hr",
                    "incremental_column": "last_modified",
                    "extraction_date": "2023-12-01",
                    "is_full_extract": False,
                    "comment": "Incremental extraction of employee data",
                },
                {
                    "source_name": "oracle_db",
                    "table_name": "departments",
                    "schema_name": "hr",
                    "is_full_extract": True,
                    "comment": "Full extraction of department reference data",
                },
                {
                    "source_name": "oracle_db",
                    "table_name": "orders",
                    "schema_name": "sales",
                    "incremental_column": "order_date",
                    "is_full_extract": False,
                    "comment": "Incremental extraction of daily orders",
                },
            ],
            "databricks": {
                "notes": [
                    "Output will be saved to DBFS (/dbfs/data) by default",
                    "Consider using shared clusters for development",
                    "Use single-user clusters for production workloads",
                    "Monitor Spark UI for performance optimization",
                ]
            },
        }

        with open(json_path, "w") as f:
            json.dump(sample_tables, f, indent=2)
