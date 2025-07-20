#!/usr/bin/env python3
"""
Example usage of the data extractor module.

This script demonstrates how to use the data extractor programmatically
to extract data from Oracle databases using Spark JDBC.
"""

from datetime import datetime, timedelta
from data_extractor.core import DataExtractor
from data_extractor.config import ConfigManager


def example_single_table_extraction():
    """Example of extracting a single table."""
    print("=== Single Table Extraction Example ===")
    
    # Initialize extractor
    extractor = DataExtractor(
        oracle_host="localhost",
        oracle_port="1521",
        oracle_service="XE",
        oracle_user="hr",
        oracle_password="password",
        output_base_path="data",
        max_workers=4
    )
    
    # Extract single table with incremental extraction
    success = extractor.extract_table(
        source_name="oracle_db",
        table_name="employees",
        schema_name="hr",
        incremental_column="last_modified",
        extraction_date=datetime(2023, 12, 1),
        is_full_extract=False
    )
    
    print(f"Single table extraction {'succeeded' if success else 'failed'}")
    
    # Cleanup
    extractor.cleanup_spark_sessions()


def example_parallel_extraction():
    """Example of extracting multiple tables in parallel."""
    print("\n=== Parallel Multiple Tables Extraction Example ===")
    
    # Initialize extractor
    extractor = DataExtractor(
        oracle_host="localhost",
        oracle_port="1521",
        oracle_service="XE",
        oracle_user="hr",
        oracle_password="password",
        output_base_path="data",
        max_workers=8  # Use 8 worker threads
    )
    
    # Define table configurations
    table_configs = [
        {
            "source_name": "oracle_db",
            "table_name": "employees",
            "schema_name": "hr",
            "incremental_column": "last_modified",
            "extraction_date": datetime(2023, 12, 1),
            "is_full_extract": False
        },
        {
            "source_name": "oracle_db",
            "table_name": "departments",
            "schema_name": "hr",
            "is_full_extract": True  # Full extraction
        },
        {
            "source_name": "oracle_db",
            "table_name": "orders",
            "schema_name": "sales",
            "incremental_column": "order_date",
            "extraction_date": datetime(2023, 12, 1),
            "is_full_extract": False
        },
        {
            "source_name": "oracle_db",
            "table_name": "customers",
            "schema_name": "sales",
            "incremental_column": "updated_timestamp",
            "extraction_date": datetime(2023, 12, 1),
            "is_full_extract": False
        },
        {
            "source_name": "oracle_db",
            "table_name": "products",
            "schema_name": "inventory",
            "is_full_extract": True  # Full extraction
        }
    ]
    
    # Extract all tables in parallel
    results = extractor.extract_tables_parallel(table_configs)
    
    # Print results
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    print(f"Parallel extraction completed: {successful}/{total} tables successful")
    
    if successful < total:
        print("Failed tables:")
        for table_name, success in results.items():
            if not success:
                print(f"  - {table_name}")
    
    # Cleanup
    extractor.cleanup_spark_sessions()


def example_config_file_usage():
    """Example of using configuration files."""
    print("\n=== Configuration File Usage Example ===")
    
    # Create configuration manager
    config_manager = ConfigManager("examples/config.ini")
    
    # Load database configuration
    db_config = config_manager.get_database_config()
    print(f"Database host: {db_config.get('oracle_host')}")
    
    # Load table configurations from JSON
    table_configs = config_manager.load_table_configs_from_json("examples/tables.json")
    print(f"Loaded {len(table_configs)} table configurations")
    
    # Validate configurations
    for i, config in enumerate(table_configs):
        errors = config_manager.validate_table_config(config)
        if errors:
            print(f"Validation errors in table config {i}: {errors}")
        else:
            print(f"Table config {i} is valid: {config['table_name']}")


def example_custom_query():
    """Example of using custom SQL queries."""
    print("\n=== Custom Query Example ===")
    
    extractor = DataExtractor(
        oracle_host="localhost",
        oracle_port="1521",
        oracle_service="XE",
        oracle_user="hr",
        oracle_password="password",
        output_base_path="data"
    )
    
    # Custom query with joins and filters
    custom_query = """
    SELECT e.employee_id, e.first_name, e.last_name, d.department_name, e.salary
    FROM hr.employees e
    JOIN hr.departments d ON e.department_id = d.department_id
    WHERE e.hire_date >= TO_DATE('2023-01-01', 'YYYY-MM-DD')
    AND e.salary > 50000
    """
    
    success = extractor.extract_table(
        source_name="oracle_db",
        table_name="employee_department_report",  # Output table name
        custom_query=custom_query,
        run_id="custom_report_001"
    )
    
    print(f"Custom query extraction {'succeeded' if success else 'failed'}")
    
    # Cleanup
    extractor.cleanup_spark_sessions()


def main():
    """Run all examples."""
    print("Data Extractor Examples")
    print("=======================")
    print("Note: These examples require a running Oracle database")
    print("with appropriate credentials and sample data.\n")
    
    try:
        # Run examples (commented out to avoid connection errors)
        # example_single_table_extraction()
        # example_parallel_extraction()
        example_config_file_usage()
        # example_custom_query()
        
        print("\n=== CLI Usage Examples ===")
        print("# Generate sample configuration files:")
        print("data-extractor --generate-config config.ini --generate-tables tables.json")
        print()
        print("# Extract single table:")
        print("data-extractor --host localhost --service XE --user hr --password secret \\")
        print("               --source-name oracle_db --table-name employees --schema hr \\")
        print("               --incremental-column last_modified")
        print()
        print("# Extract multiple tables using config files:")
        print("data-extractor --config config.ini --tables tables.json")
        print()
        print("# Extract with custom settings:")
        print("data-extractor --config config.ini --tables tables.json \\")
        print("               --max-workers 16 --output-path /data/extracts")
        
    except Exception as e:
        print(f"Example execution failed: {e}")
        print("This is expected if no Oracle database is available.")


if __name__ == "__main__":
    main()