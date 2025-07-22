#!/usr/bin/env python3
"""Demo extraction script using configuration files."""

import argparse

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run demo extraction")
    parser.add_argument(
        "--config", required=True, help="Path to YAML configuration file"
    )
    parser.add_argument(
        "--tables", required=True, help="Path to JSON tables configuration"
    )
    return parser.parse_args()


def run_demo(config_path: str, tables_path: str) -> None:
    config = ConfigManager(config_path)
    params = config.get_runtime_config()

    extractor = DataExtractor(
        oracle_host=params["oracle_host"],
        oracle_port=params.get("oracle_port", "1521"),
        oracle_service=params["oracle_service"],
        oracle_user=params["oracle_user"],
        oracle_password=params["oracle_password"],
        output_base_path=params.get("output_base_path", "data"),
        max_workers=params.get("max_workers"),
    )

    table_configs = config.load_table_configs_from_json(tables_path)
    results = extractor.extract_tables_parallel(table_configs)
    extractor.cleanup_spark_sessions()

    successful = sum(1 for success in results.values() if success)
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
