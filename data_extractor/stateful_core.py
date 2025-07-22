"""
Enhanced core data extraction module with state management integration.
Provides idempotent operations and restart capabilities.
"""

import hashlib
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from .state_manager import StateManager


class StatefulDataExtractor:
    """
    Enhanced data extraction class with integrated state management.
    
    Provides:
    - Idempotent operations
    - Progress tracking
    - Restart capabilities
    - Automatic state persistence
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
        state_dir: str = "state",
        enable_checkpoints: bool = True,
        max_retry_attempts: int = 3,
    ):
        """
        Initialize the StatefulDataExtractor.

        Args:
            oracle_host: Oracle database host
            oracle_port: Oracle database port
            oracle_service: Oracle service name
            oracle_user: Database username
            oracle_password: Database password
            output_base_path: Base path for output files
            max_workers: Maximum number of worker threads (default: CPU count)
            state_dir: Directory for state files
            enable_checkpoints: Whether to enable periodic checkpoints
            max_retry_attempts: Maximum retry attempts for failed extractions
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

        # State management
        self.state_manager = StateManager(
            state_dir=state_dir,
            enable_checkpoints=enable_checkpoints,
            max_retry_attempts=max_retry_attempts
        )

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
    
    def _is_first_run(self, source_name: str, table_name: str) -> bool:
        """Check if this is the first extraction for a table."""
        table_path = os.path.join(self.output_base_path, source_name, table_name)
        return not os.path.exists(table_path) or not any(Path(table_path).rglob("*.parquet"))

    def _calculate_file_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        try:
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
        table_key = f"{source_name}.{schema_name}.{table_name}" if schema_name else f"{source_name}.{table_name}"

        # Check if extraction is needed (idempotent check)
        if not self.state_manager.is_extraction_needed(table_key):
            self.logger.info(f"[{thread_name}] Skipping {table_name} - already completed successfully")
            return True

        # Start extraction in state manager
        if not self.state_manager.start_extraction(table_key, thread_name):
            self.logger.warning(f"[{thread_name}] Could not start extraction for {table_name}")
            return False

        # Auto-detect first run
        if not is_full_extract and incremental_column and self._is_first_run(source_name, table_name):
            self.logger.info("[%s] First run detected for %s, switching to full extract", thread_name, table_name)
            is_full_extract = True

        try:
            # Generate run_id if not provided
            if not run_id:
                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Set extraction date to yesterday if not provided
            if not extraction_date:
                extraction_date = datetime.now() - timedelta(days=1)

            self.logger.info(
                "[%s] Starting extraction for table: %s", thread_name, table_name
            )

            # Get Spark session for this thread
            spark = self._get_spark_session()

            # Build the query
            if custom_query:
                query = custom_query
            else:
                full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                
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
                "[%s] Extracted %d records from %s", thread_name, record_count, table_name
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
                    file_size_bytes=0
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
            self.logger.info("[%s] Saving to: %s", thread_name, output_path)
            df.coalesce(1).write.mode("overwrite").parquet(output_path)

            # Calculate metadata for verification
            file_size = self._get_file_size(output_path)
            checksum = self._calculate_file_checksum(output_path)

            # Mark extraction as completed successfully
            self.state_manager.complete_extraction(
                table_key=table_key,
                success=True,
                record_count=record_count,
                output_path=output_path,
                file_size_bytes=file_size,
                checksum=checksum
            )

            self.logger.info(
                "[%s] Successfully extracted table: %s", thread_name, table_name
            )
            return True

        except Exception as e:
            error_message = str(e)
            self.logger.error(
                "[%s] Error extracting table %s: %s", thread_name, table_name, error_message
            )
            
            # Mark extraction as failed
            self.state_manager.complete_extraction(
                table_key=table_key,
                success=False,
                error_message=error_message
            )
            return False

    def extract_tables_parallel(self, table_configs: List[Dict]) -> Dict[str, bool]:
        """
        Extract multiple tables in parallel using threading with state management.

        Args:
            table_configs: List of table configuration dictionaries

        Returns:
            Dict mapping table names to extraction success status
        """
        # Start pipeline with state management
        pipeline_id = self.state_manager.start_pipeline(
            table_configs=table_configs,
            output_base_path=self.output_base_path,
            max_workers=self.max_workers or 1
        )

        self.logger.info(
            "Starting stateful parallel extraction of %d tables using %d workers (pipeline: %s)",
            len(table_configs), 
            self.max_workers,
            pipeline_id
        )

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
                table_key = f"{source_name}.{schema_name}.{table_name}" if schema_name else f"{source_name}.{table_name}"
                config_map[table_key] = config
                
                # Mark all tables as attempted for results tracking
                results[table_name] = False
            
            # Filter to only pending extractions
            for table_key in pending_table_keys:
                if table_key in config_map:
                    pending_configs.append(config_map[table_key])

            self.logger.info(f"Found {len(pending_configs)} pending extractions out of {len(table_configs)} total tables")

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
                            source_name, table_name
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
                                "Successfully completed extraction for table: %s", table_name
                            )
                        else:
                            self.logger.error("Failed extraction for table: %s", table_name)

                    except Exception as e:
                        self.logger.error(
                            "Exception during extraction of table %s: %s", table_name, str(e)
                        )
                        results[table_name] = False

            # Check for tables that were already completed
            for config in table_configs:
                table_name = config.get("table_name", "")
                source_name = config.get("source_name", "")
                schema_name = config.get("schema_name")
                table_key = f"{source_name}.{schema_name}.{table_name}" if schema_name else f"{source_name}.{table_name}"
                
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
                "Stateful parallel extraction completed: %d/%d tables successful", successful, total
            )

            # Display progress summary
            progress = self.state_manager.get_pipeline_progress()
            self.logger.info("Pipeline Progress: %s", progress)

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
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
