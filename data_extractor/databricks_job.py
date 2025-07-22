"""Databricks job entry point for the data extractor."""
from __future__ import annotations

import os
from typing import Optional, Dict

from pyspark.sql import SparkSession

from .config import ConfigManager
from .core import DataExtractor


def _get_widget(name: str, default: Optional[str] = None) -> Optional[str]:
    """Retrieve a Databricks widget value or fall back to environment variables."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.widgets.get(name)
    except Exception:
        return os.getenv(name.upper(), default)


def run_from_widgets() -> Dict[str, bool]:
    """Run extraction using Databricks widgets for configuration."""
    config_path = _get_widget("config_path")
    tables_path = _get_widget("tables_path")
    widget_params = {
        "output_base_path": _get_widget("output_path", "data"),
        "max_workers": int(max_workers_widget) if (max_workers_widget := _get_widget("max_workers")) else None,
        "run_id": _get_widget("run_id"),
    }

    manager = ConfigManager(config_path)
    runtime = manager.get_app_settings(widget_params).model_dump()

    table_configs = manager.load_table_configs_from_json(tables_path)

    extractor = DataExtractor(
        oracle_host=runtime["oracle_host"],
        oracle_port=runtime.get("oracle_port", "1521"),
        oracle_service=runtime["oracle_service"],
        oracle_user=runtime["oracle_user"],
        oracle_password=runtime["oracle_password"],
        output_base_path=runtime.get("output_base_path", "data"),
        max_workers=runtime.get("max_workers"),
    )

    results = extractor.extract_tables_parallel(table_configs)
    extractor.cleanup_spark_sessions()
    failed = [tbl for tbl, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(f"Extraction failed for tables: {failed}")
    return results


if __name__ == "__main__":
    run_from_widgets()
