#!/usr/bin/env python3
"""Demo for writing to Unity Catalog volumes."""

import argparse

from data_extractor.databricks import DatabricksConfigManager, DatabricksDataExtractor


def parse_args():
    parser = argparse.ArgumentParser(description="Unity Catalog demo")
    parser.add_argument("--config", required=True, help="Config file")
    parser.add_argument("--tables", required=True, help="Tables JSON")
    return parser.parse_args()


def main():
    args = parse_args()
    config = DatabricksConfigManager(args.config)
    params = config.get_databricks_database_config()
    extraction_params = config.get_databricks_extraction_config()

    extractor = DatabricksDataExtractor(
        oracle_host=params["oracle_host"],
        oracle_port=params.get("oracle_port", "1521"),
        oracle_service=params["oracle_service"],
        oracle_user=params["oracle_user"],
        oracle_password=params["oracle_password"],
        output_base_path=params.get("output_base_path", "/dbfs/data"),
        max_workers=extraction_params.get("max_workers"),
        unity_catalog_volume=extraction_params.get("unity_catalog_volume"),
    )

    table_configs = config.load_table_configs_from_json(args.tables)
    results = extractor.extract_tables_parallel(table_configs)
    extractor.cleanup_spark_sessions()

    successful = sum(1 for s in results.values() if s)
    print(f"Unity Catalog demo completed: {successful}/{len(results)} tables")


if __name__ == "__main__":
    main()
