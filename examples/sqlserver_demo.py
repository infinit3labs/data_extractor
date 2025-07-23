#!/usr/bin/env python3
"""Demo extraction script for SQL Server."""

import argparse

from data_extractor.config import ConfigManager
from data_extractor.sqlserver import SqlServerDataExtractor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL Server demo")
    parser.add_argument("--config", required=True, help="Path to YAML configuration file")
    parser.add_argument("--tables", required=True, help="Path to tables JSON file")
    return parser.parse_args()


def run_demo(config_path: str, tables_path: str) -> None:
    cfg = ConfigManager(config_path)
    params = cfg.get_runtime_config()

    extractor = SqlServerDataExtractor(
        sqlserver_host=params["sqlserver_host"],
        sqlserver_port=params.get("sqlserver_port", "1433"),
        sqlserver_database=params["sqlserver_database"],
        sqlserver_user=params["sqlserver_user"],
        sqlserver_password=params["sqlserver_password"],
        output_base_path=params.get("output_base_path", "data"),
        max_workers=params.get("max_workers"),
    )

    tables = cfg.load_table_configs_from_json(tables_path)
    results = extractor.extract_tables_parallel(tables)
    extractor.cleanup_spark_sessions()

    successful = sum(1 for ok in results.values() if ok)
    total = len(results)
    print(f"Demo completed: {successful}/{total} tables successful")

    if successful < total:
        print("Failed tables:")
        for name, ok in results.items():
            if not ok:
                print(f"  - {name}")


def main() -> None:
    args = parse_args()
    run_demo(args.config, args.tables)


if __name__ == "__main__":
    main()
