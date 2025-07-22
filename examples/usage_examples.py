"""Example usage of the data extractor module.

This script demonstrates how to use the data extractor programmatically
and via the command line interface.
"""


from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor


from typing import Dict


def example_config_file_usage() -> Dict[str, bool]:
    """Demonstrate loading configs from files."""
    config_manager = ConfigManager("config/config.yml")
    db_config = config_manager.get_database_config()
    table_configs = config_manager.load_table_configs_from_json("config/tables.json")
    extractor = DataExtractor(
        oracle_host=db_config["oracle_host"],
        oracle_port=db_config.get("oracle_port", "1521"),
        oracle_service=db_config["oracle_service"],
        oracle_user=db_config["oracle_user"],
        oracle_password=db_config["oracle_password"],
        output_base_path=db_config.get("output_base_path", "data"),
    )
    results = extractor.extract_tables_parallel(table_configs)
    extractor.cleanup_spark_sessions()
    return results


if __name__ == "__main__":
    print("Running example configuration file extraction…")
    try:
        example_config_file_usage()
    except Exception as exc:
        print(f"Example failed: {exc}")
        print("This is expected if no Oracle database is available.")
