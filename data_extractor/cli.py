"""
Command Line Interface for the data extractor.
"""

import argparse
import os
import sys
from datetime import datetime
from typing import List, cast

from .config import ConfigManager
from .core import DataExtractor
from .databricks import DatabricksConfigManager, DatabricksDataExtractor
from .sqlserver import SqlServerDataExtractor


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Extract data from Oracle databases using Spark JDBC with parallel processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract using configuration files
  data-extractor --config config.yml --tables tables.json
  
  # Extract using Databricks mode
  data-extractor --databricks --config config.yml --tables tables.json
  
  # Extract single table with incremental extraction
  data-extractor --host localhost --port 1521 --service XE --user hr --password secret \\
                 --source-name oracle_db --table-name employees --schema hr \\
                 --incremental-column last_modified
  
  # Extract single table in Databricks mode
  data-extractor --databricks --host localhost --port 1521 --service XE --user hr --password secret \\
                 --source-name oracle_db --table-name employees --schema hr \\
                 --incremental-column last_modified
  
  # Extract single table with full extraction
  data-extractor --host localhost --port 1521 --service XE --user hr --password secret \\
                 --source-name oracle_db --table-name departments --schema hr --full-extract
  
  # Generate sample configuration files
  data-extractor --generate-config config.yml --generate-tables tables.json
  
  # Generate Databricks-specific configuration files
  data-extractor --generate-databricks-config databricks_config.yml --generate-databricks-tables databricks_tables.json
        """,
    )

    # Configuration file arguments
    parser.add_argument(
        "--config", "-c", help="Path to configuration file (YAML format)"
    )
    parser.add_argument(
        "--tables", "-t", help="Path to tables configuration file (JSON format)"
    )

    # Database connection arguments
    parser.add_argument("--host", help="Oracle database host")
    parser.add_argument(
        "--port", help="Oracle database port (default: 1521)", default="1521"
    )
    parser.add_argument("--service", help="Oracle service name")
    parser.add_argument("--user", help="Database username")
    parser.add_argument("--password", help="Database password")

    parser.add_argument(
        "--sqlserver",
        action="store_true",
        help="Use SQL Server instead of Oracle",
    )
    parser.add_argument("--sqlserver-host", help="SQL Server host")
    parser.add_argument(
        "--sqlserver-port", help="SQL Server port (default: 1433)", default="1433"
    )
    parser.add_argument("--sqlserver-database", help="SQL Server database name")
    parser.add_argument("--sqlserver-user", help="SQL Server username")
    parser.add_argument("--sqlserver-password", help="SQL Server password")

    # Single table extraction arguments
    parser.add_argument("--source-name", help="Name of the data source")
    parser.add_argument("--table-name", help="Name of the table to extract")
    parser.add_argument("--schema", help="Schema name")
    parser.add_argument(
        "--incremental-column", help="Column name for incremental extraction"
    )
    parser.add_argument(
        "--extraction-date",
        help="Date for incremental extraction (YYYY-MM-DD format, default: yesterday)",
    )
    parser.add_argument(
        "--full-extract",
        action="store_true",
        help="Perform full table extraction instead of incremental",
    )
    parser.add_argument(
        "--custom-query", help="Custom SQL query to execute instead of table extraction"
    )

    # Databricks mode arguments
    parser.add_argument(
        "--databricks",
        action="store_true",
        help="Run in Databricks cluster mode (uses existing Spark session)",
    )
    parser.add_argument(
        "--databricks-output-path",
        help="Databricks output path (default: /dbfs/data)",
        default="/dbfs/data",
    )

    # General arguments
    parser.add_argument(
        "--output-path",
        help="Base output path for extracted files (default: data)",
        default="data",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of worker threads (default: number of CPU cores)",
    )
    parser.add_argument("--run-id", help="Unique run identifier (default: timestamp)")

    # Utility arguments
    parser.add_argument(
        "--generate-config", help="Generate sample configuration file at specified path"
    )
    parser.add_argument(
        "--generate-tables",
        help="Generate sample tables configuration file at specified path",
    )
    parser.add_argument(
        "--generate-databricks-config",
        help="Generate sample Databricks configuration file at specified path",
    )
    parser.add_argument(
        "--generate-databricks-tables",
        help="Generate sample Databricks tables configuration file at specified path",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    return parser


def validate_single_table_args(args: argparse.Namespace) -> List[str]:
    """Validate arguments for single table extraction."""
    errors = []
    if args.sqlserver:
        required = [
            "sqlserver_host",
            "sqlserver_database",
            "sqlserver_user",
            "sqlserver_password",
            "source_name",
            "table_name",
        ]
    else:
        required = ["host", "service", "user", "password", "source_name", "table_name"]
    for arg in required:
        if not getattr(args, arg.replace("-", "_")):
            errors.append(f"--{arg} is required for single table extraction")

    if not args.full_extract and not args.incremental_column:
        errors.append(
            "--incremental-column is required unless --full-extract is specified"
        )

    return errors


def extract_single_table(args: argparse.Namespace) -> bool:
    """Extract a single table based on command line arguments."""
    # Parse extraction date
    extraction_date = None
    if args.extraction_date:
        try:
            extraction_date = datetime.strptime(args.extraction_date, "%Y-%m-%d")
        except ValueError:
            print(
                f"Error: Invalid date format: {args.extraction_date}. Use YYYY-MM-DD format."
            )
            return False

    # Determine output path
    output_path = args.databricks_output_path if args.databricks else args.output_path

    # Create data extractor (Databricks or standard)
    if args.sqlserver:
        extractor = SqlServerDataExtractor(
            sqlserver_host=args.sqlserver_host,
            sqlserver_port=args.sqlserver_port,
            sqlserver_database=args.sqlserver_database,
            sqlserver_user=args.sqlserver_user,
            sqlserver_password=args.sqlserver_password,
            output_base_path=output_path,
            max_workers=args.max_workers,
        )
    elif args.databricks:
        extractor = DatabricksDataExtractor(
            oracle_host=args.host,
            oracle_port=args.port,
            oracle_service=args.service,
            oracle_user=args.user,
            oracle_password=args.password,
            output_base_path=output_path,
            max_workers=args.max_workers,
        )
    else:
        extractor = DataExtractor(
            oracle_host=args.host,
            oracle_port=args.port,
            oracle_service=args.service,
            oracle_user=args.user,
            oracle_password=args.password,
            output_base_path=output_path,
            max_workers=args.max_workers,
        )

    # Extract table
    success = extractor.extract_table(
        source_name=args.source_name,
        table_name=args.table_name,
        schema_name=args.schema,
        incremental_column=args.incremental_column,
        extraction_date=extraction_date,
        is_full_extract=args.full_extract,
        custom_query=args.custom_query,
        run_id=args.run_id,
    )

    # Cleanup
    extractor.cleanup_spark_sessions()

    return success


def extract_multiple_tables(args: argparse.Namespace) -> bool:
    """Extract multiple tables based on configuration files."""
    # Load configuration (Databricks or standard)
    if args.sqlserver:
        config_manager = ConfigManager(args.config)
        db_config = config_manager.get_sqlserver_database_config()
        extraction_config = {}
    elif args.databricks:
        config_manager = DatabricksConfigManager(args.config)
        db_config = config_manager.get_databricks_database_config()
        extraction_config = config_manager.get_databricks_extraction_config()
    else:
        config_manager = ConfigManager(args.config)
        db_config = config_manager.get_runtime_config(
            output_base_path=args.output_path, max_workers=args.max_workers
        )
        extraction_config = {}

    # Override with command line arguments if provided
    if args.sqlserver:
        if args.sqlserver_host:
            db_config["sqlserver_host"] = args.sqlserver_host
        if args.sqlserver_port:
            db_config["sqlserver_port"] = args.sqlserver_port
        if args.sqlserver_database:
            db_config["sqlserver_database"] = args.sqlserver_database
        if args.sqlserver_user:
            db_config["sqlserver_user"] = args.sqlserver_user
        if args.sqlserver_password:
            db_config["sqlserver_password"] = args.sqlserver_password
    else:
        if args.host:
            db_config["oracle_host"] = args.host
        if args.port:
            db_config["oracle_port"] = args.port
        if args.service:
            db_config["oracle_service"] = args.service
        if args.user:
            db_config["oracle_user"] = args.user
        if args.password:
            db_config["oracle_password"] = args.password
    if args.databricks:
        db_config["output_base_path"] = args.databricks_output_path
    elif args.output_path:
        db_config["output_base_path"] = args.output_path

    # Validate required database configuration
    if args.sqlserver:
        required_db_fields = [
            "sqlserver_host",
            "sqlserver_database",
            "sqlserver_user",
            "sqlserver_password",
        ]
    else:
        required_db_fields = [
            "oracle_host",
            "oracle_service",
            "oracle_user",
            "oracle_password",
        ]
    for field in required_db_fields:
        if not db_config.get(field):
            print(
                f"Error: {field} is required. Provide it via config file or command line argument."
            )
            return False

    # Load table configurations
    if not args.tables:
        print(
            "Error: --tables configuration file is required for multiple table extraction."
        )
        return False

    if not os.path.exists(args.tables):
        print(f"Error: Tables configuration file not found: {args.tables}")
        return False

    try:
        table_configs = config_manager.load_table_configs_from_json(args.tables)
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error loading tables configuration: {e}")
        return False

    if not table_configs:
        print("Error: No table configurations found in the tables file.")
        return False

    # Validate table configurations
    for i, table_config in enumerate(table_configs):
        errors = config_manager.validate_table_config(table_config)
        if errors:
            print(f"Error in table configuration {i}: {', '.join(errors)}")
            return False

    # Create data extractor (Databricks or standard)
    if args.sqlserver:
        extractor = SqlServerDataExtractor(
            sqlserver_host=db_config["sqlserver_host"],
            sqlserver_port=db_config.get("sqlserver_port", "1433"),
            sqlserver_database=db_config["sqlserver_database"],
            sqlserver_user=db_config["sqlserver_user"],
            sqlserver_password=db_config["sqlserver_password"],
            output_base_path=db_config.get("output_base_path", "data"),
            max_workers=cast(int, db_config.get("max_workers")),
        )
    elif args.databricks:
        extractor = DatabricksDataExtractor(
            oracle_host=db_config["oracle_host"],
            oracle_port=db_config.get("oracle_port", "1521"),
            oracle_service=db_config["oracle_service"],
            oracle_user=db_config["oracle_user"],
            oracle_password=db_config["oracle_password"],
            output_base_path=db_config.get("output_base_path", "/dbfs/data"),
            max_workers=extraction_config.get("max_workers") or args.max_workers,
        )
    else:
        extractor = DataExtractor(
            oracle_host=db_config["oracle_host"],
            oracle_port=db_config.get("oracle_port", "1521"),
            oracle_service=db_config["oracle_service"],
            oracle_user=db_config["oracle_user"],
            oracle_password=db_config["oracle_password"],
            output_base_path=db_config.get("output_base_path", "data"),
            max_workers=cast(int, db_config.get("max_workers")),
        )

    # Extract tables in parallel
    results = extractor.extract_tables_parallel(table_configs)

    # Cleanup
    extractor.cleanup_spark_sessions()

    # Check results
    successful = sum(1 for success in results.values() if success)
    total = len(results)

    print(f"Extraction completed: {successful}/{total} tables successful")

    if successful < total:
        print("Failed tables:")
        for table_name, success in results.items():
            if not success:
                print(f"  - {table_name}")

    return successful == total


def main() -> None:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Handle utility commands
    if args.generate_config:
        config_manager = ConfigManager()
        config_manager.create_sample_config_file(args.generate_config)
        print(f"Sample configuration file created: {args.generate_config}")

        if args.generate_tables:
            config_manager.create_sample_tables_json(args.generate_tables)
            print(f"Sample tables configuration file created: {args.generate_tables}")

        return

    if args.generate_tables:
        config_manager = ConfigManager()
        config_manager.create_sample_tables_json(args.generate_tables)
        print(f"Sample tables configuration file created: {args.generate_tables}")
        return

    if args.generate_databricks_config:
        config_manager = DatabricksConfigManager()
        config_manager.create_databricks_sample_config(args.generate_databricks_config)
        print(
            f"Sample Databricks configuration file created: {args.generate_databricks_config}"
        )

        if args.generate_databricks_tables:
            config_manager.create_databricks_sample_tables_json(
                args.generate_databricks_tables
            )
            print(
                f"Sample Databricks tables configuration file created: {args.generate_databricks_tables}"
            )

        return

    if args.generate_databricks_tables:
        config_manager = DatabricksConfigManager()
        config_manager.create_databricks_sample_tables_json(
            args.generate_databricks_tables
        )
        print(
            f"Sample Databricks tables configuration file created: {args.generate_databricks_tables}"
        )
        return

    # Determine extraction mode
    if args.tables or args.config:
        # Multiple table extraction using configuration files
        success = extract_multiple_tables(args)
    else:
        # Single table extraction using command line arguments
        errors = validate_single_table_args(args)
        if errors:
            print("Error: " + ", ".join(errors))
            parser.print_help()
            sys.exit(1)

        success = extract_single_table(args)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
