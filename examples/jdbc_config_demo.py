"""Demonstration of configurable JDBC options."""

from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


def main() -> None:
    manager = ConfigManager("config/config.yml")
    db_config = manager.get_database_config()
    extraction_config = manager.get_extraction_config()

    extractor = DataExtractor(
        **db_config,
        max_workers=extraction_config.get("max_workers"),
        jdbc_fetch_size=extraction_config.get("jdbc_fetch_size", 10000),
        jdbc_num_partitions=extraction_config.get("jdbc_num_partitions", 4),
    )

    print(
        f"Configured fetch size: {extractor.jdbc_fetch_size}, partitions: {extractor.jdbc_num_partitions}"
    )


if __name__ == "__main__":
    main()
