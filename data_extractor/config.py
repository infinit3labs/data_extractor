"""Configuration management using YAML and environment variables."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application settings loaded from environment variables."""

    oracle_host: str = Field(default="localhost", alias="ORACLE_HOST")
    oracle_port: str = Field(default="1521", alias="ORACLE_PORT")
    oracle_service: str = Field(default="XE", alias="ORACLE_SERVICE")
    oracle_user: str = Field(default="", alias="ORACLE_USER")
    oracle_password: str = Field(default="", alias="ORACLE_PASSWORD")

    sqlserver_host: str = Field(default="localhost", alias="SQLSERVER_HOST")
    sqlserver_port: str = Field(default="1433", alias="SQLSERVER_PORT")
    sqlserver_database: str = Field(default="master", alias="SQLSERVER_DATABASE")
    sqlserver_user: str = Field(default="", alias="SQLSERVER_USER")
    sqlserver_password: str = Field(default="", alias="SQLSERVER_PASSWORD")
    output_base_path: str = Field(default="data", alias="OUTPUT_BASE_PATH")

    max_workers: Optional[int] = Field(default=None, alias="MAX_WORKERS")
    run_id: Optional[str] = Field(default=None, alias="RUN_ID")
    default_source: str = Field(default="default", alias="DEFAULT_SOURCE")
    use_existing_spark: Optional[bool] = Field(default=None, alias="USE_EXISTING_SPARK")
    unity_catalog_volume: Optional[str] = Field(
        default=None, alias="UNITY_CATALOG_VOLUME"
    )
    jdbc_fetch_size: int = Field(default=10000, alias="JDBC_FETCH_SIZE")
    jdbc_num_partitions: int = Field(default=4, alias="JDBC_NUM_PARTITIONS")

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


# Settings are now defined in this file


class ConfigManager:
    """Load configuration from YAML with environment overrides."""

    def __init__(self, config_file: Optional[str] = None) -> None:
        self.config_file = config_file
        self.config_data: Dict[str, Any] = {}
        if config_file and Path(config_file).exists():
            with open(config_file, "r", encoding="utf-8") as f:
                self.config_data = yaml.safe_load(f) or {}

    # ------------------------------------------------------------------
    # YAML helpers
    # ------------------------------------------------------------------
    def _file_settings(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for section in ("database", "extraction", "databricks"):
            if section in self.config_data and isinstance(
                self.config_data[section], dict
            ):
                data.update(self.config_data[section])
        return data

    # ------------------------------------------------------------------
    def get_app_settings(
        self, overrides: Optional[Dict[str, Any]] = None
    ) -> AppSettings:
        """Return merged application settings."""

        env_settings = AppSettings()  # type: ignore
        result = env_settings.model_dump()
        env_fields = env_settings.model_fields_set

        for key, value in self._file_settings().items():
            if key not in env_fields and value is not None:
                result[key] = value

        if overrides:
            result.update({k: v for k, v in overrides.items() if v is not None})

        return AppSettings(**result)

    # ------------------------------------------------------------------
    def get_database_config(self) -> Dict[str, Any]:
        settings = self.get_app_settings()
        keys = [
            "oracle_host",
            "oracle_port",
            "oracle_service",
            "oracle_user",
            "oracle_password",
            "output_base_path",
        ]
        return {k: getattr(settings, k) for k in keys}

    # ------------------------------------------------------------------
    def get_sqlserver_database_config(self) -> Dict[str, Any]:
        settings = self.get_app_settings()
        keys = [
            "sqlserver_host",
            "sqlserver_port",
            "sqlserver_database",
            "sqlserver_user",
            "sqlserver_password",
            "output_base_path",
        ]
        return {k: getattr(settings, k) for k in keys}

    # ------------------------------------------------------------------
    def get_extraction_config(self) -> Dict[str, Any]:
        settings = self.get_app_settings()
        keys = [
            "max_workers",
            "run_id",
            "default_source",
            "use_existing_spark",
            "unity_catalog_volume",
            "jdbc_fetch_size",
            "jdbc_num_partitions",
        ]
        return {
            k: getattr(settings, k) for k in keys if getattr(settings, k) is not None
        }

    # ------------------------------------------------------------------
    def load_table_configs_from_json(self, json_file: str) -> List[Dict[str, Any]]:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        table_configs = []
        for table_config in data.get("tables", []):
            if table_config.get("extraction_date"):
                if isinstance(table_config["extraction_date"], str):
                    table_config["extraction_date"] = datetime.strptime(
                        table_config["extraction_date"], "%Y-%m-%d"
                    )
            table_configs.append(table_config)
        return table_configs

    # ------------------------------------------------------------------
    def create_sample_config_file(self, config_path: str) -> None:
        sample_config = {
            "database": {
                "oracle_host": "localhost",
                "oracle_port": "1521",
                "oracle_service": "XE",
                "oracle_user": "your_username",
                "oracle_password": "your_password",
                "sqlserver_host": "localhost",
                "sqlserver_port": "1433",
                "sqlserver_database": "master",
                "sqlserver_user": "your_username",
                "sqlserver_password": "your_password",
                "output_base_path": "data",
            },
            "extraction": {
                "max_workers": 8,
                "default_source": "oracle_db",
                "jdbc_fetch_size": 10000,
                "jdbc_num_partitions": 4,
            },
        }
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(sample_config, f)

    # ------------------------------------------------------------------
    def create_sample_tables_json(self, json_path: str) -> None:
        sample_tables = {
            "tables": [
                {
                    "source_name": "oracle_db",
                    "table_name": "employees",
                    "schema_name": "hr",
                    "incremental_column": "last_modified",
                    "extraction_date": "2023-12-01",
                    "is_full_extract": False,
                },
                {
                    "source_name": "oracle_db",
                    "table_name": "departments",
                    "schema_name": "hr",
                    "is_full_extract": True,
                },
                {
                    "source_name": "oracle_db",
                    "table_name": "orders",
                    "schema_name": "sales",
                    "incremental_column": "order_date",
                    "is_full_extract": False,
                },
            ]
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(sample_tables, f, indent=2)

    # ------------------------------------------------------------------
    def validate_table_config(self, table_config: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        for field in ["source_name", "table_name"]:
            if not table_config.get(field):
                errors.append(f"Missing required field: {field}")
        is_incremental_extract = not table_config.get("is_full_extract", False)
        missing_incremental_column = not table_config.get("incremental_column")
        if is_incremental_extract and missing_incremental_column:
            errors.append("incremental_column is required for incremental extraction")
        return errors

    # ------------------------------------------------------------------
    def get_runtime_config(self, **kwargs: Any) -> Dict[str, Any]:
        return self.get_app_settings(kwargs).model_dump()
