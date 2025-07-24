"""Demonstration of configurable JDBC options."""

import sys
from pathlib import Path

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


def main() -> None:
    config_path = Path(__file__).parent / "config" / "config.yml"
    if not config_path.is_file():
        print(f"Error: Configuration file not found at {config_path}", file=sys.stderr)
        sys.exit(1)

    manager = ConfigManager(str(config_path))
    db_config = manager.get_database_config()
    extraction_config = manager.get_extraction_config()

    extractor = DataExtractor(
        **db_config,
        max_workers=extraction_config.get("max_workers"),
        jdbc_fetch_size=extraction_config.get(
            "jdbc_fetch_size", AppSettings.DEFAULT_FETCH_SIZE
        ),
        jdbc_num_partitions=extraction_config.get(
            "jdbc_num_partitions", AppSettings.DEFAULT_NUM_PARTITIONS
        ),
    )

    print(
        f"Configured fetch size: {extractor.jdbc_fetch_size}, partitions: {extractor.jdbc_num_partitions}"
    )


if __name__ == "__main__":
    main()
