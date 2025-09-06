"""
Enhanced Databricks-specific data extraction module with state management.
Provides Databricks-optimized functionality with idempotent operations and restart capabilities.
"""

import hashlib
import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from .state_manager import StateManager


class StatefulDatabricksDataExtractor:
    """
    Databricks-optimized data extraction class with integrated state management.

    This class is designed to run efficiently in Databricks cluster context,
    utilizing the existing Spark session and Databricks-specific features
    while providing idempotent operations and restart capabilities.
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
        state_dir: str = "/dbfs/state",
        enable_checkpoints: bool = True,
        max_retry_attempts: int = 3,
    ):
        """
        Initialize the StatefulDatabricksDataExtractor.

        Args:
            oracle_host: Oracle database host
            oracle_port: Oracle database port
            oracle_service: Oracle service name
            oracle_user: Database username
            oracle_password: Database password
            output_base_path: Base path for output files (default: /dbfs/data)
            max_workers: Maximum number of worker threads (default: CPU count / 2 for Databricks)
            use_existing_spark: Whether to use existing Spark session (recommended for Databricks)
            unity_catalog_volume: Optional Unity Catalog volume path
            state_dir: Directory for state files (default: /dbfs/state)
            enable_checkpoints: Whether to enable periodic checkpoints
            max_retry_attempts: Maximum retry attempts for failed extractions
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

        # Ensure state directory exists on DBFS
        self._ensure_dbfs_path(state_dir)

        # State management
        self.state_manager = StateManager(
            state_dir=state_dir,
            enable_checkpoints=enable_checkpoints,
            max_retry_attempts=max_retry_attempts,
        )

        # Setup logging for Databricks
        self._setup_databricks_logging()

        if self.is_databricks:
            self.logger.info("Detected Databricks environment with state management")

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

    def _ensure_dbfs_path(self, path: str) -> None:
        """Ensure DBFS path exists."""
        try:
            if path.startswith("/dbfs/"):
                Path(path).mkdir(parents=True, exist_ok=True)
        except Exception:
            # Fallback to local path if DBFS not available
            local_path = path.replace("/dbfs/", "./")
            Path(local_path).mkdir(parents=True, exist_ok=True)

    def _setup_databricks_logging(self) -> None:
        """Setup Databricks-optimized logging."""
        # In Databricks, prefer console output for visibility in notebooks
        log_format = "%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"

        handlers = [logging.StreamHandler()]

        # Add file handler if not in notebook environment
        if not self.is_databricks or "DATABRICKS_NOTEBOOK" not in os.environ:
            try:
                handlers.append(
                    logging.FileHandler("/dbfs/logs/data_extractor_databricks.log")
                )
            except Exception:
                # Fallback to local logging
                handlers.append(logging.FileHandler("data_extractor_databricks.log"))

        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=handlers,
            force=True,  # Override any existing configuration
        )

        self.logger = logging.getLogger(__name__)

    def _get_spark_session(self) -> SparkSession:
        """
        Get Spark session - uses existing session in Databricks or creates new one.
        """
        if self.use_existing_spark:
            # Try to get existing Spark session (recommended for Databricks)
            spark = SparkSession.getActiveSession()
            if spark:
                return spark

            # Fallback to creating new session
            self.logger.warning("No active Spark session found, creating new one")

        # Create thread-local session if needed
        if not hasattr(self._local, "spark"):
            builder = SparkSession.builder.appName(
                f"DatabricksDataExtractor-{threading.current_thread().name}"
            )

            # Only add Oracle JDBC if not already available
            if not self.is_databricks:
                builder = builder.config(
                    "spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.7.0.0"
                )

            self._local.spark = (
                builder.config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config(
                    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                )
                .getOrCreate()
            )

        return self._local.spark

    def _normalize_output_path(self, path: str) -> str:
        """Normalize output path for Databricks/DBFS."""
        if self.unity_catalog_volume:
            # Use Unity Catalog volume
            base_path = f"/Volumes/{self.unity_catalog_volume}"
            relative_path = path.replace(self.output_base_path, "").lstrip("/")
            return f"{base_path}/{relative_path}"

        if (
            self.is_databricks
            and not path.startswith("/dbfs/")
            and not path.startswith("/Volumes/")
        ):
            # Convert to DBFS path
            return f"/dbfs/{path.lstrip('/')}"

        return path

    def _is_first_run(self, source_name: str, table_name: str) -> bool:
        """Check if this is the first extraction for a table."""
        table_path = os.path.join(self.output_base_path, source_name, table_name)
        normalized_path = self._normalize_output_path(table_path)

        try:
            return not os.path.exists(normalized_path) or not any(
                Path(normalized_path).rglob("*.parquet")
            )
        except Exception:
            return True

    def _calculate_file_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        try:
            # Handle DBFS paths
            if file_path.startswith("/dbfs/"):
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
            else:
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception:
            return ""

    def _get_file_size(self, file_path: str) -> int:
        """Get file size in bytes."""
        try:
            return os.path.getsize(file_path)
        except Exception:
            return 0

    def get_databricks_context(self) -> Dict[str, Any]:
        """Get Databricks-specific context information."""
        context = {
            "is_databricks": self.is_databricks,
            "runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
            "cluster_id": os.environ.get("DATABRICKS_CLUSTER_ID"),
            "workspace_url": os.environ.get("DATABRICKS_WORKSPACE_URL"),
            "use_existing_spark": self.use_existing_spark,
            "unity_catalog_volume": self.unity_catalog_volume,
            "max_workers": self.max_workers,
            "output_base_path": self.output_base_path,
        }

        # Add Spark session info
        try:
            spark = self._get_spark_session()
            context["spark_version"] = spark.version
            context["spark_app_name"] = spark.sparkContext.appName
            context["spark_app_id"] = spark.sparkContext.applicationId
        except Exception as e:
            context["spark_error"] = str(e)

        return context

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
        flashback_enabled: bool = False,
        flashback_timestamp: Optional[datetime] = None,
    ) -> bool:
        """
        Extract a single table from Oracle database and save as Parquet with state tracking.
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
            flashback_enabled: Whether to use Oracle Flashback feature
            flashback_timestamp: Timestamp for Flashback query (if enabled)

        Returns:
            bool: True if extraction was successful, False otherwise
        """
        thread_name = threading.current_thread().name

        # Generate table key for state tracking
        table_key = (
            f"{source_name}.{schema_name}.{table_name}"
            if schema_name
            else f"{source_name}.{table_name}"
        )

        # Check if extraction is needed (idempotent check)
        if not self.state_manager.is_extraction_needed(table_key):
            self.logger.info(
                f"[{thread_name}] Skipping {table_name} - already completed successfully"
            )
            return True

        # Start extraction in state manager
        if not self.state_manager.start_extraction(table_key, thread_name):
            self.logger.warning(
                f"[{thread_name}] Could not start extraction for {table_name}"
            )
            return False

        # Auto-detect first run
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
            # Generate run_id if not provided
            if not run_id:
                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Set extraction date to yesterday if not provided
            if not extraction_date:
                extraction_date = datetime.now() - timedelta(days=1)

            self.logger.info(
                "[%s] Starting Databricks extraction for table: %s",
                thread_name,
                table_name,
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

                # Add flashback clause if enabled
                flashback_clause = ""
                if flashback_enabled and flashback_timestamp:
                    flashback_clause = f" AS OF TIMESTAMP TO_TIMESTAMP('{flashback_timestamp.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"

                if is_full_extract or not incremental_column:
                    query = f"SELECT * FROM {full_table_name}{flashback_clause}"
                else:
                    # Incremental extraction for 24-hour window
                    start_date = extraction_date.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    end_date = start_date + timedelta(days=1)

                    query = f"""
                    SELECT * FROM {full_table_name}{flashback_clause}
                    WHERE {incremental_column} >= TO_DATE('{start_date.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')
                    AND {incremental_column} < TO_DATE('{end_date.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')
                    """

            self.logger.info("[%s] Executing query: %s", thread_name, query)

            # Extract data using Spark JDBC with Databricks optimizations
            df_reader = (
                spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("query", query)
                .option("user", self.oracle_user)
                .option("password", self.oracle_password)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("fetchsize", "50000")  # Larger fetch size for Databricks
            )

            # Dynamic partitioning based on cluster resources
            if self.is_databricks:
                # Use more partitions for Databricks clusters
                partition_count = max(4, self.max_workers * 2)
                df_reader = df_reader.option("numPartitions", str(partition_count))
            else:
                df_reader = df_reader.option("numPartitions", "4")

            df = df_reader.load()

            # Check if data was extracted
            record_count = df.count()
            self.logger.info(
                "[%s] Extracted %d records from %s",
                thread_name,
                record_count,
                table_name,
            )

            if record_count == 0:
                self.logger.warning(
                    "[%s] No data found for table %s", thread_name, table_name
                )
                # Mark as completed with 0 records
                self.state_manager.complete_extraction(
                    table_key=table_key,
                    success=True,
                    record_count=0,
                    output_path=None,
                    file_size_bytes=0,
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

            # Normalize path for Databricks/DBFS
            normalized_output_path = self._normalize_output_path(output_path)

            # Create directory if it doesn't exist
            Path(normalized_output_path).parent.mkdir(parents=True, exist_ok=True)

            # Save as Parquet with Databricks optimizations
            self.logger.info("[%s] Saving to: %s", thread_name, normalized_output_path)

            # Optimize coalesce based on data size and cluster resources
            if record_count > 1000000 and self.is_databricks:
                # For large datasets in Databricks, use multiple files
                coalesce_partitions = min(
                    max(1, record_count // 500000), self.max_workers
                )
            else:
                coalesce_partitions = 1

            df.coalesce(coalesce_partitions).write.mode("overwrite").parquet(
                normalized_output_path
            )

            # Calculate metadata for verification
            file_size = self._get_file_size(normalized_output_path)
            checksum = self._calculate_file_checksum(normalized_output_path)

            # Mark extraction as completed successfully
            self.state_manager.complete_extraction(
                table_key=table_key,
                success=True,
                record_count=record_count,
                output_path=normalized_output_path,
                file_size_bytes=file_size,
                checksum=checksum,
            )

            self.logger.info(
                "[%s] Successfully extracted table: %s to Databricks storage",
                thread_name,
                table_name,
            )
            return True

        except Exception as e:
            error_message = str(e)
            self.logger.error(
                "[%s] Error extracting table %s: %s",
                thread_name,
                table_name,
                error_message,
            )

            # Mark extraction as failed
            self.state_manager.complete_extraction(
                table_key=table_key, success=False, error_message=error_message
            )
            return False

    def extract_tables_parallel(self, table_configs: List[Dict]) -> Dict[str, bool]:
        """
        Extract multiple tables in parallel using threading with state management.
        Optimized for Databricks environment.

        Args:
            table_configs: List of table configuration dictionaries

        Returns:
            Dict mapping table names to extraction success status
        """
        # Start pipeline with state management
        pipeline_id = self.state_manager.start_pipeline(
            table_configs=table_configs,
            output_base_path=self.output_base_path,
            max_workers=self.max_workers,
        )

        self.logger.info(
            "Starting stateful Databricks parallel extraction of %d tables using %d workers (pipeline: %s)",
            len(table_configs),
            self.max_workers,
            pipeline_id,
        )

        # Log Databricks context
        context = self.get_databricks_context()
        self.logger.info(f"Databricks context: {context}")

        results = {}

        try:
            # Get only pending extractions
            pending_table_keys = self.state_manager.get_pending_extractions()
            pending_configs = []

            # Build config map for pending tables
            config_map = {}
            for config in table_configs:
                source_name = config.get("source_name", "")
                table_name = config.get("table_name", "")
                schema_name = config.get("schema_name")
                table_key = (
                    f"{source_name}.{schema_name}.{table_name}"
                    if schema_name
                    else f"{source_name}.{table_name}"
                )
                config_map[table_key] = config

                # Mark all tables as attempted for results tracking
                results[table_name] = False

            # Filter to only pending extractions
            for table_key in pending_table_keys:
                if table_key in config_map:
                    pending_configs.append(config_map[table_key])

            self.logger.info(
                f"Found {len(pending_configs)} pending extractions out of {len(table_configs)} total tables"
            )

            # Use conservative threading for Databricks shared clusters
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all extraction tasks
                future_to_table = {}

                for config in pending_configs:
                    # Validate required fields
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

                # Collect results as they complete
                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        success = future.result()
                        results[table_name] = success

                        if success:
                            self.logger.info(
                                "Successfully completed Databricks extraction for table: %s",
                                table_name,
                            )
                        else:
                            self.logger.error(
                                "Failed Databricks extraction for table: %s", table_name
                            )

                    except Exception as e:
                        self.logger.error(
                            "Exception during Databricks extraction of table %s: %s",
                            table_name,
                            str(e),
                        )
                        results[table_name] = False

            # Check for tables that were already completed
            for config in table_configs:
                table_name = config.get("table_name", "")
                source_name = config.get("source_name", "")
                schema_name = config.get("schema_name")
                table_key = (
                    f"{source_name}.{schema_name}.{table_name}"
                    if schema_name
                    else f"{source_name}.{table_name}"
                )

                if table_key not in pending_table_keys:
                    # Table was already completed
                    results[table_name] = True

            # Finish pipeline
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            overall_success = successful == total

            self.state_manager.finish_pipeline(success=overall_success)

            # Log summary
            self.logger.info(
                "Stateful Databricks parallel extraction completed: %d/%d tables successful",
                successful,
                total,
            )

            # Display progress summary
            progress = self.state_manager.get_pipeline_progress()
            self.logger.info("Pipeline Progress: %s", progress)

        except Exception as e:
            self.logger.error(f"Databricks pipeline execution failed: {e}")
            self.state_manager.finish_pipeline(success=False)
            # Mark all tables as failed if not already processed
            for config in table_configs:
                table_name = config.get("table_name", "")
                if table_name not in results:
                    results[table_name] = False

        return results

    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status and progress."""
        return self.state_manager.get_pipeline_progress()

    def list_recent_runs(self, limit: int = 10) -> List[Dict]:
        """List recent pipeline runs."""
        return self.state_manager.list_recent_pipelines(limit)

    def cleanup_old_state(self) -> int:
        """Clean up old state files."""
        return self.state_manager.cleanup_old_state_files()

    def force_reprocess_table(self, table_key: str) -> bool:
        """Force reprocessing of a specific table."""
        return self.state_manager.force_reprocess_table(table_key)

    def reset_failed_extractions(self) -> int:
        """Reset all failed extractions for retry."""
        return self.state_manager.reset_failed_extractions()

    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get detailed extraction summary."""
        return self.state_manager.get_extraction_summary()

    def create_extraction_report(self) -> Dict[str, Any]:
        """Create comprehensive extraction report."""
        return self.state_manager.create_extraction_report()

    def validate_extraction_window_consistency(self) -> Dict[str, Any]:
        """Validate extraction window consistency."""
        return self.state_manager.validate_extraction_window_consistency()

    def cleanup_spark_sessions(self) -> None:
        """Clean up Spark sessions for all threads."""
        if hasattr(self._local, "spark"):
            self._local.spark.stop()
            del self._local.spark
