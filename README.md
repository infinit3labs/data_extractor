# Data Extractor

A high-performance data extraction module that uses Spark JDBC to efficiently extract data from Oracle databases and save as Parquet files. The application is designed to process hundreds of tables efficiently using all available CPU cores with true parallel processing via threading.

## ✨ New: Databricks Support

The data extractor now includes **Databricks cluster mode** for optimized data extraction in Databricks environments. This mode leverages the existing Databricks Spark session and follows Databricks best practices for cloud-based data processing.

### Unity Catalog Volumes

You can also write extracted tables directly to Unity Catalog volumes by setting
`unity_catalog_volume` in the `[databricks]` section of your INI file. See
[`docs/UNITY_CATALOG.md`](docs/UNITY_CATALOG.md) for details and run the demo in
`examples/unity_catalog_demo.py`.

## Features

- **Parallel Processing**: Utilizes all available CPU cores with thread-based parallel extraction
- **Incremental Extraction**: Supports incremental data extraction using specified columns with 24-hour daily windows
- **Full Table Extraction**: Supports complete table extraction when needed
- **Spark JDBC Integration**: Leverages Apache Spark for efficient database connectivity and data processing
- **Parquet Output**: Saves data in optimized Parquet format with organized directory structure
- **Flexible Configuration**: Supports both configuration files and command-line arguments
- **Oracle Database Support**: Optimized for Oracle database connectivity
- **Asynchronous Processing**: True asynchronous processing with dedicated Spark sessions per thread
- **🚀 Databricks Integration**: Native support for Databricks cluster environments with optimized resource utilization

## Directory Structure

Extracted files are saved with the following structure:
```
data/source_name/entity_name/yyyymm/dd/run_id.parquet
```

For example:
```
data/oracle_db/employees/202312/01/20231201_143022.parquet
data/oracle_db/orders/202312/01/20231201_143022.parquet
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/infinit3labs/data_extractor.git
cd data_extractor
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Activate the virtual environment:
```bash
poetry shell
```

## Prerequisites

- Python 3.8+
- Apache Spark 3.4+
- Oracle JDBC Driver (automatically downloaded via Spark packages)
- Oracle Database connectivity
- Sufficient disk space for Parquet output files

## Configuration

### Configuration File (config.ini)

Create a configuration file with database connection details:

```ini
[database]
oracle_host = localhost
oracle_port = 1521
oracle_service = XE
oracle_user = your_username
oracle_password = your_password
output_base_path = data

[extraction]
max_workers = 8
default_source = oracle_db
```

### Tables Configuration (tables.json)

Define tables to extract in JSON format:

```json
{
  "tables": [
    {
      "source_name": "oracle_db",
      "table_name": "employees",
      "schema_name": "hr",
      "incremental_column": "last_modified",
      "extraction_date": "2023-12-01",
      "is_full_extract": false
    },
    {
      "source_name": "oracle_db",
      "table_name": "departments",
      "schema_name": "hr",
      "is_full_extract": true
    }
  ]
}
```

## Usage

### Command Line Interface

The data extractor provides a flexible CLI for various extraction scenarios:

#### Generate Sample Configuration Files

```bash
data-extractor --generate-config config.ini --generate-tables tables.json
```

#### Extract Multiple Tables (Recommended for Production)

```bash
data-extractor --config config.ini --tables tables.json
```

### Databricks Mode

The data extractor includes a specialized Databricks mode for running in Databricks cluster environments:

```bash
# Extract using Databricks mode
data-extractor --databricks --config config.ini --tables tables.json

# Extract single table in Databricks mode
data-extractor --databricks --host localhost --service XE --user hr --password secret \
               --source-name oracle_db --table-name employees --schema hr \
               --incremental-column last_modified

# Extract to custom DBFS path
data-extractor --databricks --databricks-output-path /dbfs/mnt/datalake/extracts \
               --config config.ini --tables tables.json
```

#### Databricks Configuration

Generate Databricks-specific configuration files:

```bash
data-extractor --generate-databricks-config databricks_config.ini --generate-databricks-tables databricks_tables.json
```

#### Extract Single Table with Incremental Extraction

```bash
data-extractor --host localhost --port 1521 --service XE --user hr --password secret \
               --source-name oracle_db --table-name employees --schema hr \
               --incremental-column last_modified
```

#### Extract Single Table with Full Extraction

```bash
data-extractor --host localhost --port 1521 --service XE --user hr --password secret \
               --source-name oracle_db --table-name departments --schema hr --full-extract
```

#### Using Environment Variables

Set database connection via environment variables:

```bash
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_SERVICE=XE
export ORACLE_USER=your_username
export ORACLE_PASSWORD=your_password
export OUTPUT_BASE_PATH=data

data-extractor --tables tables.json
```

### Programmatic Usage

#### Standard Mode

```python
from data_extractor.core import DataExtractor
from data_extractor.config import ConfigManager
from datetime import datetime

# Initialize extractor
extractor = DataExtractor(
    oracle_host="localhost",
    oracle_port="1521", 
    oracle_service="XE",
    oracle_user="hr",
    oracle_password="password",
    output_base_path="data",
    max_workers=8
)

# Extract single table
success = extractor.extract_table(
    source_name="oracle_db",
    table_name="employees",
    schema_name="hr",
    incremental_column="last_modified",
    extraction_date=datetime(2023, 12, 1),
    is_full_extract=False
)

# Extract multiple tables in parallel
table_configs = [
    {
        "source_name": "oracle_db",
        "table_name": "employees", 
        "schema_name": "hr",
        "incremental_column": "last_modified",
        "is_full_extract": False
    },
    {
        "source_name": "oracle_db",
        "table_name": "departments",
        "schema_name": "hr", 
        "is_full_extract": True
    }
]

results = extractor.extract_tables_parallel(table_configs)

# Cleanup
extractor.cleanup_spark_sessions()
```

#### Databricks Mode

```python
from data_extractor.databricks import DatabricksDataExtractor
from datetime import datetime

# Initialize Databricks-optimized extractor
extractor = DatabricksDataExtractor(
    oracle_host="localhost",
    oracle_port="1521", 
    oracle_service="XE",
    oracle_user="hr",
    oracle_password="password",
    output_base_path="/dbfs/data",  # DBFS path
    max_workers=4,  # Conservative for shared clusters
    use_existing_spark=True  # Use Databricks Spark session
)

# Extract single table (same API as standard mode)
success = extractor.extract_table(
    source_name="oracle_db",
    table_name="employees",
    schema_name="hr",
    incremental_column="last_modified",
    extraction_date=datetime(2023, 12, 1),
    is_full_extract=False
)

# Get Databricks context information
context = extractor.get_databricks_context()
print(f"Running in Databricks: {context['is_databricks']}")
print(f"Spark version: {context.get('spark_version')}")

# Extract multiple tables in parallel
results = extractor.extract_tables_parallel(table_configs)

# Cleanup (though not necessary when using existing Spark session)
extractor.cleanup_spark_sessions()
```

#### Databricks Notebook Usage

```python
# Databricks notebook cell
from data_extractor.databricks import DatabricksDataExtractor

# Use Databricks widgets for parameters
oracle_host = dbutils.widgets.get("oracle_host")
oracle_user = dbutils.secrets.get("oracle", "username")
oracle_password = dbutils.secrets.get("oracle", "password")

# Initialize extractor (automatically detects Databricks environment)
extractor = DatabricksDataExtractor(
    oracle_host=oracle_host,
    oracle_port="1521",
    oracle_service="XE",
    oracle_user=oracle_user,
    oracle_password=oracle_password,
    output_base_path="/dbfs/mnt/datalake/extracted_data"
)

# Extract data
success = extractor.extract_table(
    source_name="oracle_prod",
    table_name="customers",
    schema_name="sales",
    incremental_column="last_modified",
    is_full_extract=False
)

# Verify output
files = dbutils.fs.ls("/dbfs/mnt/datalake/extracted_data/")
for file in files:
    print(f"  {file.path}")
```

## Configuration Options

### Table Configuration Parameters

- `source_name`: Name of the data source (required)
- `table_name`: Name of the table to extract (required)
- `schema_name`: Schema name (optional)
- `incremental_column`: Column for incremental extraction (required for incremental)
- `extraction_date`: Specific date for extraction (optional, defaults to yesterday)
- `is_full_extract`: Whether to perform full table extraction (default: false)
- `custom_query`: Custom SQL query instead of table name (optional)
- `run_id`: Unique run identifier (optional, defaults to timestamp)

### Extraction Modes

1. **Incremental Extraction**: Extracts data based on an incremental column within a 24-hour window
2. **Full Extraction**: Extracts all data from the specified table
3. **Custom Query**: Executes a custom SQL query for complex extraction logic

## Performance Optimization

### Parallel Processing

- The application uses threading to achieve true parallel processing
- Each thread gets its own Spark session for optimal resource utilization (standard mode)
- In Databricks mode, threads share the existing Spark session for better resource efficiency
- Default worker count equals the number of CPU cores (standard) or CPU cores / 2 (Databricks)
- Configurable via `max_workers` parameter

### Spark Configuration

#### Standard Mode
The application automatically configures Spark with optimizations:

- Adaptive Query Execution enabled
- Automatic partition coalescing
- Kryo serialization for better performance
- Oracle JDBC driver auto-loading

#### Databricks Mode
Leverages Databricks-optimized Spark configuration:

- Uses existing Databricks Spark session
- Optimized partition count based on cluster resources
- DBFS and cloud storage path handling
- Conservative worker threading for shared clusters

### JDBC Optimization

- Fetch size optimized for Oracle (10,000 rows)
- Default 4 partitions per table for parallel reading (standard mode)
- Dynamic partition count in Databricks based on cluster parallelism
- Connection pooling via Spark JDBC

### Databricks-Specific Optimizations

- **Resource Management**: Conservative threading for shared clusters
- **Path Handling**: Automatic DBFS path normalization
- **Session Management**: Reuses existing Spark session
- **Output Optimization**: Dynamic partition coalescing based on data size
- **Environment Detection**: Automatic Databricks environment detection

## Logging

The application provides comprehensive logging:

- Thread-specific logging for parallel operations
- File logging to `data_extractor.log`
- Console output for real-time monitoring
- Detailed extraction progress and timing information

## Error Handling

- Graceful handling of connection failures
- Individual table extraction failures don't affect other tables
- Detailed error logging with thread context
- Cleanup of Spark sessions on completion

## Examples

See the `examples/` directory for:
- Sample configuration files
- Example table definitions
- Common usage patterns
- **Databricks-specific examples** (`databricks_usage_examples.py`)
- **Databricks configuration templates** (`databricks_config.ini`, `databricks_tables.json`)

## Databricks Deployment Guide

### Cluster Requirements

- **Runtime Version**: Databricks Runtime 12.2 LTS or higher
- **Cluster Type**: Single-user or shared clusters supported
- **Libraries**: The data_extractor package (install via PyPI or upload wheel)

### Installation in Databricks

1. **Via PyPI** (if published):
   ```bash
   %pip install data_extractor
   ```

2. **Via Git** (development):
   ```bash
   %pip install git+https://github.com/infinit3labs/data_extractor.git
   ```

3. **Via Wheel Upload**:
   - Build wheel: `python setup.py bdist_wheel`
   - Upload to Databricks workspace
   - Install on cluster

### Configuration in Databricks

1. **Use Databricks Secrets** for database credentials:
   ```python
   oracle_user = dbutils.secrets.get("oracle", "username")
   oracle_password = dbutils.secrets.get("oracle", "password")
   ```

2. **Configure Storage Paths**:
   - DBFS paths: `/dbfs/data/` or `/dbfs/mnt/your_mount/`
   - S3 paths: `s3://your-bucket/path/`
   - Azure paths: `abfss://container@account.dfs.core.windows.net/path/`

3. **Cluster Configuration**:
   ```python
   # For shared clusters
   max_workers = 4
   
   # For single-user clusters
   max_workers = 8
   ```

### Running as Databricks Job

1. **Create Job Script**:
   ```python
   # databricks_extraction_job.py
   from data_extractor.databricks import DatabricksDataExtractor
   import json
   
   # Job parameters from Databricks job configuration
   oracle_host = dbutils.widgets.get("oracle_host")
   tables_config_path = dbutils.widgets.get("tables_config")
   
   # Initialize extractor
   extractor = DatabricksDataExtractor(
       oracle_host=oracle_host,
       oracle_port="1521",
       oracle_service="XE",
       oracle_user=dbutils.secrets.get("oracle", "username"),
       oracle_password=dbutils.secrets.get("oracle", "password"),
       output_base_path="/dbfs/mnt/datalake/daily_extracts"
   )
   
   # Load and execute extraction
   with open(tables_config_path, 'r') as f:
       table_configs = json.load(f)['tables']
   
   results = extractor.extract_tables_parallel(table_configs)
   print(f"Extraction completed: {sum(results.values())}/{len(results)} successful")
   ```

2. **Schedule Job**:
   - Use Databricks Jobs UI
   - Set parameters via job configuration
   - Schedule for daily/hourly runs

## Troubleshooting

### Common Issues

1. **Oracle JDBC Driver**: The driver is automatically downloaded via Spark packages
2. **Memory Issues**: Adjust Spark memory settings via environment variables
3. **Connection Timeouts**: Check Oracle database connectivity and credentials
4. **Permission Issues**: Ensure the user has read access to specified tables

### Environment Variables

Set Spark configuration via environment variables:

```bash
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
export PYSPARK_PYTHON=python3
```

## Development

### Testing

Run tests using pytest:

```bash
pytest tests/
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Documentation

Additional guides can be found in the [docs](docs/) directory:

- [Getting Started](docs/GETTING_STARTED.md)
- [Demo Script](docs/DEMO.md)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
