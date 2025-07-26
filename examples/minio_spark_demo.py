#!/usr/bin/env python3
"""Demo extraction writing to MinIO using Spark cluster."""

import argparse

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MinIO demo extraction")
    parser.add_argument(
        "--config", default="config/minio_config.yml", help="Path to YAML config"
    )
    parser.add_argument(
        "--tables", default="config/tables.json", help="Path to tables JSON"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = ConfigManager(args.config)
    params = cfg.get_runtime_config()

    extractor = DataExtractor(
        oracle_host=params["oracle_host"],
        oracle_port=params.get("oracle_port", "1521"),
        oracle_service=params["oracle_service"],
        oracle_user=params["oracle_user"],
        oracle_password=params["oracle_password"],
        output_base_path=params.get("output_base_path", "data"),
        max_workers=params.get("max_workers"),
        jdbc_fetch_size=params.get("jdbc_fetch_size", 10000),
        jdbc_num_partitions=params.get("jdbc_num_partitions", 4),
        spark_master=params.get("spark_master"),
        s3_endpoint=params.get("s3_endpoint"),
        s3_access_key=params.get("s3_access_key"),
        s3_secret_key=params.get("s3_secret_key"),
    )

    tables = cfg.load_table_configs_from_json(args.tables)
    results = extractor.extract_tables_parallel(tables)
    extractor.cleanup_spark_sessions()

    success = sum(1 for ok in results.values() if ok)
    total = len(results)
    print(f"MinIO demo completed: {success}/{total} tables successful")


if __name__ == "__main__":
    main()
