"""Flashback demo script."""

import argparse
from datetime import datetime

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Oracle Flashback demo")
    parser.add_argument("--config", required=True, help="YAML configuration file")
    parser.add_argument("--tables", required=True, help="Tables JSON file")
    parser.add_argument(
        "--timestamp",
        required=True,
        help="Flashback timestamp in YYYY-MM-DDTHH:MM:SS format",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = ConfigManager(args.config)
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

    try:
        ts = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        print(
            "Error: Invalid timestamp format. Please use the format 'YYYY-MM-DDTHH:MM:SS'."
        )
        exit(1)

    table_configs = config.load_table_configs_from_json(args.tables)
    for table in table_configs:
        extractor.extract_table(
            source_name=table["source_name"],
            table_name=table["table_name"],
            schema_name=table.get("schema_name"),
            is_full_extract=True,
            flashback_enabled=True,
            flashback_timestamp=ts,
            run_id="flashback_demo",
        )

    extractor.cleanup_spark_sessions()


if __name__ == "__main__":
    main()
