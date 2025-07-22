#!/usr/bin/env python3
"""
Databricks usage examples for the data extractor module.

This script demonstrates how to use the data extractor in Databricks cluster context
to extract data from Oracle databases using Spark JDBC with optimized Databricks patterns.
"""

from datetime import datetime

from data_extractor.databricks import DatabricksConfigManager, DatabricksDataExtractor


def databricks_single_table_example():
    """Example of extracting a single table in Databricks."""
    print("=== Databricks Single Table Extraction Example ===")

    # Initialize Databricks-optimized extractor
    extractor = DatabricksDataExtractor(
        oracle_host="your_oracle_host",
        oracle_port="1521",
        oracle_service="your_service",
        oracle_user="your_username",
        oracle_password="your_password",
        output_base_path="/dbfs/data",  # DBFS path
        max_workers=4,  # Conservative for shared clusters
        use_existing_spark=True,  # Use Databricks Spark session
    )

    # Extract single table with incremental extraction
    success = extractor.extract_table(
        source_name="oracle_db",
        table_name="employees",
        schema_name="hr",
        incremental_column="last_modified",
        extraction_date=datetime(2023, 12, 1),
        is_full_extract=False,
    )

    print(f"Databricks single table extraction {'succeeded' if success else 'failed'}")

    # Get Databricks context information
    context = extractor.get_databricks_context()
    print(f"Databricks context: {context}")

    # Cleanup (though not necessary when using existing Spark session)
    extractor.cleanup_spark_sessions()


def databricks_parallel_extraction_example():
    """Example of extracting multiple tables in parallel in Databricks."""
    print("\n=== Databricks Parallel Multiple Tables Extraction Example ===")

    # Initialize Databricks-optimized extractor
    extractor = DatabricksDataExtractor(
        oracle_host="your_oracle_host",
        oracle_port="1521",
        oracle_service="your_service",
        oracle_user="your_username",
        oracle_password="your_password",
        output_base_path="/dbfs/data",
        max_workers=6,  # Moderate for shared clusters
        use_existing_spark=True,
    )

    # Define table configurations
    table_configs = [
        {
            "source_name": "oracle_db",
            "table_name": "employees",
            "schema_name": "hr",
            "incremental_column": "last_modified",
            "extraction_date": datetime(2023, 12, 1),
            "is_full_extract": False,
        },
        {
            "source_name": "oracle_db",
            "table_name": "departments",
            "schema_name": "hr",
            "is_full_extract": True,  # Full extraction
        },
        {
            "source_name": "oracle_db",
            "table_name": "orders",
            "schema_name": "sales",
            "incremental_column": "order_date",
            "extraction_date": datetime(2023, 12, 1),
            "is_full_extract": False,
        },
    ]

    # Extract all tables in parallel
    results = extractor.extract_tables_parallel(table_configs)

    # Print results
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    print(
        f"Databricks parallel extraction completed: {successful}/{total} tables successful"
    )

    if successful < total:
        print("Failed tables:")
        for table_name, success in results.items():
            if not success:
                print(f"  - {table_name}")

    # Cleanup
    extractor.cleanup_spark_sessions()


def databricks_config_file_example():
    """Example of using Databricks configuration files."""
    print("\n=== Databricks Configuration File Usage Example ===")

    # Create Databricks configuration manager
    config_manager = DatabricksConfigManager("examples/databricks_config.yml")

    # Load Databricks-specific database configuration
    db_config = config_manager.get_databricks_database_config()
    print(f"Database host: {db_config.get('oracle_host')}")
    print(f"Output path: {db_config.get('output_base_path')}")

    # Load extraction configuration
    extraction_config = config_manager.get_databricks_extraction_config()
    print(f"Max workers: {extraction_config.get('max_workers')}")
    print(f"Use existing Spark: {extraction_config.get('use_existing_spark')}")

    # Load table configurations from JSON
    try:
        table_configs = config_manager.load_table_configs_from_json(
            "examples/databricks_tables.json"
        )
        print(f"Loaded {len(table_configs)} table configurations")

        # Validate configurations
        for i, config in enumerate(table_configs):
            errors = config_manager.validate_table_config(config)
            if errors:
                print(f"Validation errors in table config {i}: {errors}")
            else:
                print(f"Table config {i} is valid: {config['table_name']}")
    except FileNotFoundError:
        print(
            "Databricks tables configuration file not found (this is expected for the example)"
        )


def databricks_notebook_example():
    """
    Example code for running in a Databricks notebook.
    This would typically be in a separate notebook cell.
    """
    print("\n=== Databricks Notebook Usage Example ===")

    # Notebook cell example code
    notebook_code = """
# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Oracle Data Extraction in Databricks
# MAGIC 
# MAGIC This notebook demonstrates how to extract data from Oracle using the data_extractor module in Databricks.

# COMMAND ----------

# Install the data_extractor package (if not already available)
# %pip install git+https://github.com/infinit3labs/data_extractor.git

# COMMAND ----------

from data_extractor.databricks import DatabricksDataExtractor
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC Set up Oracle connection parameters. In production, use Databricks secrets.

# COMMAND ----------

# Configuration using Databricks widgets or environment variables
oracle_host = dbutils.widgets.get("oracle_host") if "dbutils" in globals() else "your_host"
oracle_user = dbutils.secrets.get("oracle", "username") if "dbutils" in globals() else "your_user"
oracle_password = dbutils.secrets.get("oracle", "password") if "dbutils" in globals() else "your_password"

# COMMAND ----------

# Initialize Databricks data extractor
extractor = DatabricksDataExtractor(
    oracle_host=oracle_host,
    oracle_port="1521",
    oracle_service="XE",
    oracle_user=oracle_user,
    oracle_password=oracle_password,
    output_base_path="/dbfs/mnt/datalake/extracted_data",  # Mount point or DBFS path
    max_workers=4
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Table Extraction

# COMMAND ----------

# Extract a single table
success = extractor.extract_table(
    source_name="oracle_prod",
    table_name="customers",
    schema_name="sales",
    incremental_column="last_modified",
    extraction_date=datetime(2023, 12, 1),
    is_full_extract=False
)

print(f"Extraction {'succeeded' if success else 'failed'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple Tables Extraction

# COMMAND ----------

# Define tables to extract
tables_config = [
    {
        "source_name": "oracle_prod",
        "table_name": "orders",
        "schema_name": "sales",
        "incremental_column": "order_date",
        "is_full_extract": False
    },
    {
        "source_name": "oracle_prod",
        "table_name": "products",
        "schema_name": "inventory",
        "is_full_extract": True
    }
]

# Extract multiple tables in parallel
results = extractor.extract_tables_parallel(tables_config)
print(f"Results: {results}")

# COMMAND ----------

# Get Databricks context information
context = extractor.get_databricks_context()
print("Databricks Context:")
for key, value in context.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Output Files

# COMMAND ----------

# List extracted files
if "dbutils" in globals():
    files = dbutils.fs.ls("/dbfs/mnt/datalake/extracted_data/")
    for file in files:
        print(f"  {file.path}")
"""

    print("Example notebook code (see comments for actual notebook usage):")
    print("This code would be used in Databricks notebook cells.")
    print("Use dbutils.widgets for parameters and dbutils.secrets for credentials.")


def databricks_job_example():
    """Example of using the extractor in a Databricks job."""
    print("\n=== Databricks Job Usage Example ===")

    job_script = """
# databricks_extraction_job.py
# This script can be run as a Databricks job

import sys
import json
from data_extractor.databricks import DatabricksDataExtractor

def main():
    # Get job parameters (passed via Databricks job configuration)
    oracle_host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    oracle_user = sys.argv[2] if len(sys.argv) > 2 else "user"
    oracle_password = sys.argv[3] if len(sys.argv) > 3 else "password"
    tables_json = sys.argv[4] if len(sys.argv) > 4 else "tables.json"
    
    print(f"Starting Databricks extraction job...")
    print(f"Oracle host: {oracle_host}")
    print(f"Tables config: {tables_json}")
    
    # Initialize extractor
    extractor = DatabricksDataExtractor(
        oracle_host=oracle_host,
        oracle_port="1521",
        oracle_service="XE",
        oracle_user=oracle_user,
        oracle_password=oracle_password,
        output_base_path="/dbfs/mnt/datalake/daily_extracts"
    )
    
    # Load table configurations
    with open(tables_json, 'r') as f:
        config_data = json.load(f)
        table_configs = config_data.get('tables', [])
    
    # Extract tables
    results = extractor.extract_tables_parallel(table_configs)
    
    # Report results
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    
    print(f"Job completed: {successful}/{total} tables successful")
    
    if successful < total:
        print("Failed tables:")
        for table_name, success in results.items():
            if not success:
                print(f"  - {table_name}")
        sys.exit(1)  # Exit with error code for failed extractions
    
    print("All extractions completed successfully")

if __name__ == "__main__":
    main()
"""

    print("Example Databricks job script:")
    print("This script can be scheduled as a Databricks job with parameters.")


def main():
    """Run all Databricks examples."""
    print("Data Extractor Databricks Examples")
    print("==================================")
    print("Note: These examples show Databricks-specific usage patterns.")
    print(
        "Ensure you have Oracle connectivity and appropriate Databricks permissions.\n"
    )

    try:
        # Run examples that don't require actual connections
        databricks_config_file_example()
        databricks_notebook_example()
        databricks_job_example()

        # Connection-based examples (commented out to avoid connection errors)
        # databricks_single_table_example()
        # databricks_parallel_extraction_example()

        print("\n=== CLI Usage Examples for Databricks ===")
        print("# Generate Databricks configuration files:")
        print(
            "data-extractor --generate-databricks-config databricks_config.yml --generate-databricks-tables databricks_tables.json"
        )
        print()
        print("# Extract single table in Databricks mode:")
        print(
            "data-extractor --databricks --host your_host --service XE --user hr --password secret \\"
        )
        print(
            "               --source-name oracle_db --table-name employees --schema hr \\"
        )
        print("               --incremental-column last_modified")
        print()
        print("# Extract multiple tables using Databricks config files:")
        print(
            "data-extractor --databricks --config databricks_config.yml --tables databricks_tables.json"
        )
        print()
        print("# Extract to custom DBFS path:")
        print(
            "data-extractor --databricks --databricks-output-path /dbfs/mnt/datalake/extracts \\"
        )
        print(
            "               --config databricks_config.yml --tables databricks_tables.json"
        )

    except Exception as e:
        print(f"Example execution failed: {e}")
        print("This is expected if no Oracle database is available.")


if __name__ == "__main__":
    main()
