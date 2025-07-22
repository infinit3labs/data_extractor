# Data Extractor

A high-performance data extraction module that uses Spark JDBC to efficiently extract data from Oracle databases and save as Parquet files. The application is designed to process hundreds of tables efficiently using all available CPU cores with true parallel processing via threading.

## Features

- **Parallel Processing**: Utilizes all available CPU cores with thread-based parallel extraction
- **Incremental Extraction**: Supports incremental data extraction using specified columns with 24-hour daily windows
4. Submit a pull request
- **Full Table Extraction**: Supports complete table extraction when needed
- **Spark JDBC Integration**: Leverages Apache Spark for efficient database connectivity and data processing
- **Parquet Output**: Saves data in optimized Parquet format with organized directory structure
- **Flexible Configuration**: Supports both configuration files and command-line arguments
- **Oracle Database Support**: Optimized for Oracle database connectivity
- **Asynchronous Processing**: True asynchronous processing with dedicated Spark sessions per thread

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

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package:
```bash
pip install -e .
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
- Each thread gets its own Spark session for optimal resource utilization
- Default worker count equals the number of CPU cores
- Configurable via `max_workers` parameter

### Spark Configuration

The application automatically configures Spark with optimizations:

- Adaptive Query Execution enabled
- Automatic partition coalescing
- Kryo serialization for better performance
- Oracle JDBC driver auto-loading

### JDBC Optimization

- Fetch size optimized for Oracle (10,000 rows)
- Default 4 partitions per table for parallel reading
- Connection pooling via Spark JDBC

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

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Databricks Usage
See `docs/DATABRICKS.md` for running this module on a Databricks cluster.
