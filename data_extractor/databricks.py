"""
Databricks-specific data extraction module.
Provides Databricks-optimized functionality for running in Databricks cluster context.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import yaml
from pyspark.sql import SparkSession

from .config import ConfigManager
from .core import DataExtractor


class DatabricksDataExtractor(DataExtractor):
    """
    Databricks-optimized data extraction class.

    This class inherits from DataExtractor to get idempotency features
    and provides Databricks-specific optimizations.
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
        jdbc_fetch_size: int = 10000,
        jdbc_num_partitions: int = 4,
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
            jdbc_fetch_size: JDBC fetch size for Spark reads
            jdbc_num_partitions: Number of partitions for Spark JDBC reads
        """
        # In Databricks, use fewer workers to avoid overwhelming shared clusters
        cpu_count = os.cpu_count() or 2
        databricks_max_workers = max_workers or max(1, cpu_count // 2)

        # Call parent constructor to get idempotency features
        super().__init__(
            oracle_host=oracle_host,
            oracle_port=oracle_port,
            oracle_service=oracle_service,
            oracle_user=oracle_user,
            oracle_password=oracle_password,
            output_base_path=output_base_path,
            max_workers=databricks_max_workers,
            jdbc_fetch_size=jdbc_fetch_size,
            jdbc_num_partitions=jdbc_num_partitions,
        )

        # Databricks-specific attributes
        self.use_existing_spark = use_existing_spark
        self.unity_catalog_volume = unity_catalog_volume

        # Databricks environment detection
        self.is_databricks = self._is_databricks_environment()

        # Setup Databricks-optimized logging
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
            # Use parent implementation for thread-local sessions
            return super()._get_spark_session()

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

    def _get_output_path(
        self, source_name: str, table_name: str, extraction_date: datetime, run_id: str
    ) -> str:
        """Override to handle Unity Catalog volumes and Databricks paths."""
        year_month = extraction_date.strftime("%Y%m")
        day = extraction_date.strftime("%d")

        if self.unity_catalog_volume:
            # Use Unity Catalog volume path
            base_path = f"/Volumes/{self.unity_catalog_volume}"
        else:
            # Use regular output path, normalized for Databricks
            base_path = self._normalize_output_path(self.output_base_path)

        return os.path.join(
            base_path,
            source_name,
            table_name,
            year_month,
            day,
            f"{run_id}.parquet",
        )

    def cleanup_spark_sessions(self):
        """
        Clean up Spark sessions.
        In Databricks mode with existing sessions, this is typically not needed.
        """
        if not self.use_existing_spark:
            super().cleanup_spark_sessions()
        else:
            self.logger.info("Using existing Spark session - no cleanup needed")

    def get_databricks_context(self) -> Dict[str, Any]:
        """
        Get information about the current Databricks environment.

        Returns:
            Dictionary with Databricks context information
        """
        context = {
            "is_databricks": self.is_databricks,
            "use_existing_spark": self.use_existing_spark,
            "unity_catalog_volume": self.unity_catalog_volume,
            "output_base_path": self.output_base_path,
            "max_workers": self.max_workers,
            "environment_variables": {
                "databricks_runtime_version": os.environ.get(
                    "DATABRICKS_RUNTIME_VERSION"
                ),
                "spark_home": os.environ.get("SPARK_HOME"),
                "hostname": os.environ.get("HOSTNAME"),
            },
        }

        # Add Spark session information if available
        try:
            spark = self._get_spark_session()
            context["spark_version"] = spark.version
            context["spark_app_name"] = spark.sparkContext.appName
            context["spark_master"] = spark.sparkContext.master
        except Exception:
            context["spark_info"] = "Not available"

        return context

    # Configuration helper methods
    @staticmethod
    def get_databricks_database_config() -> Dict[str, str]:
        """
        Get database configuration optimized for Databricks.

        Returns:
            Dictionary with database configuration
        """
        return {
            "oracle_host": "your_oracle_host",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "your_username",
            "oracle_password": "your_password",
        }

    def get_databricks_extraction_config(self) -> Dict[str, Any]:
        """
        Get extraction configuration optimized for Databricks.

        Returns:
            Dictionary with extraction configuration
        """
        return {
            "output_base_path": "/dbfs/data",
            "max_workers": self.max_workers,
            "use_existing_spark": True,
            "unity_catalog_volume": None,  # Optional: "catalog/schema/volume"
            "jdbc_fetch_size": self.jdbc_fetch_size,
            "jdbc_num_partitions": self.jdbc_num_partitions,
        }

    @staticmethod
    def create_databricks_sample_config(config_path: str):
        """
        Create a sample configuration file optimized for Databricks.

        Args:
            config_path: Path where to save the sample config
        """
        config = {
            "database": {
                "oracle_host": "your_oracle_host",
                "oracle_port": 1521,
                "oracle_service": "XE",
                "oracle_user": "your_username",
                "oracle_password": "your_password",
            },
            "extraction": {
                "output_base_path": "/dbfs/data",
                "max_workers": 4,
                "default_source": "oracle_db",
            },
            "databricks": {
                "use_existing_spark": True,
                "unity_catalog_volume": None,  # Optional: "catalog/schema/volume"
                "databricks_output_path": "/dbfs/mnt/datalake/extracted_data",
            },
        }

        with open(config_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)

    @staticmethod
    def create_databricks_sample_tables_json(json_path: str):
        """
        Create a sample tables JSON file with Databricks-optimized settings.

        Args:
            json_path: Path where to save the sample JSON
        """
        tables_config = {
            "tables": [
                {
                    "source_name": "oracle_prod",
                    "table_name": "customers",
                    "schema_name": "sales",
                    "incremental_column": "last_modified",
                    "is_full_extract": False,
                },
                {
                    "source_name": "oracle_prod",
                    "table_name": "orders",
                    "schema_name": "sales",
                    "incremental_column": "order_date",
                    "is_full_extract": False,
                },
                {
                    "source_name": "oracle_prod",
                    "table_name": "products",
                    "schema_name": "inventory",
                    "is_full_extract": True,
                },
            ]
        }

        import json

        with open(json_path, "w") as f:
            json.dump(tables_config, f, indent=2)


class DatabricksConfigManager(ConfigManager):
    """
    Configuration manager optimized for Databricks environments.
    """

    def __init__(self, config_file: Optional[str] = None):
        super().__init__(config_file)
        self.databricks_settings = self._load_databricks_settings()

    def _load_databricks_settings(self) -> Dict[str, Any]:
        """Load Databricks-specific settings."""
        if self.config_data and "databricks" in self.config_data:
            return self.config_data["databricks"]
        return {}

    def get_databricks_output_path(self) -> str:
        """Get Databricks output path."""
        return self.databricks_settings.get("databricks_output_path", "/dbfs/data")

    def get_unity_catalog_volume(self) -> Optional[str]:
        """Get Unity Catalog volume if configured."""
        return self.databricks_settings.get("unity_catalog_volume")

    def should_use_existing_spark(self) -> bool:
        """Check if should use existing Spark session."""
        return self.databricks_settings.get("use_existing_spark", True)
