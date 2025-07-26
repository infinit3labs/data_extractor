# Idempotent Operations and Pipeline Recovery

## Overview

All data extraction operations are now idempotent and the pipeline is fully recoverable at any stage. This ensures that operations can be safely rerun without causing duplication or inconsistency.

## Key Features

### 1. Idempotent Operations

- **File Existence Checks**: Before starting extraction, the system checks if output files already exist
- **File Integrity Validation**: Parquet files are validated to ensure they are readable and complete
- **Metadata Tracking**: Each extraction saves metadata including record count, timestamps, file size, and checksum
- **Automatic Skip Logic**: Completed extractions are automatically skipped unless forced to reprocess

### 2. Recovery Capabilities

- **Force Reprocessing**: Use `--force-reprocess` flag to override idempotency checks
- **Partial Pipeline Recovery**: Failed extractions can be rerun while skipping successful ones
- **Consistent Output Paths**: Deterministic file naming ensures recovery operations target the same files
- **Metadata Persistence**: Extraction state is preserved in JSON metadata files alongside Parquet outputs

### 3. Enhanced CLI Options

```bash
# Standard extraction with idempotency
data-extractor --config config.yml --tables tables.json

# Force reprocessing all tables (useful for recovery)
data-extractor --config config.yml --tables tables.json --force-reprocess

# Databricks mode with idempotency
data-extractor --databricks --config config.yml --tables tables.json

# Single table with force reprocess
data-extractor --host localhost --user hr --password secret \
               --source-name oracle_db --table-name employees \
               --force-reprocess
```

## Implementation Details

### File Structure

Each extraction creates two files:
```
data/source_name/table_name/yyyymm/dd/
├── run_id.parquet           # Main data file
└── run_id_metadata.json     # Extraction metadata
```

### Metadata Schema

```json
{
  "output_path": "path/to/output.parquet",
  "record_count": 1000,
  "start_time": "2023-12-01T10:00:00",
  "end_time": "2023-12-01T10:05:00",
  "duration_seconds": 300.0,
  "file_size_bytes": 52428800,
  "checksum": "md5_hash_of_file",
  "table_config": {
    "source_name": "oracle_db",
    "table_name": "employees",
    "is_full_extract": false,
    ...
  },
  "extraction_complete": true
}
```

### Idempotency Check Logic

1. **Output File Exists**: Check if target Parquet file exists
2. **File Validity**: Verify the file can be read by Spark
3. **Metadata Completeness**: Ensure metadata indicates successful completion
4. **Integrity Verification**: Validate file checksum matches metadata
5. **Force Override**: Skip all checks if `force_reprocess=True`

### Databricks Integration

The `DatabricksDataExtractor` inherits all idempotency features from the base `DataExtractor`:

- Unity Catalog volume support with idempotent paths
- DBFS path normalization for consistent outputs
- Existing Spark session reuse for efficiency
- All recovery capabilities available in Databricks mode

## Usage Examples

### Basic Recovery Scenario

```python
from data_extractor.core import DataExtractor

extractor = DataExtractor(
    oracle_host="localhost",
    oracle_port="1521",
    oracle_service="XE",
    oracle_user="hr",
    oracle_password="password"
)

# First run - extracts data
result1 = extractor.extract_table(
    source_name="oracle_db",
    table_name="employees",
    is_full_extract=True
)

# Second run - skips extraction (idempotent)
result2 = extractor.extract_table(
    source_name="oracle_db",
    table_name="employees",
    is_full_extract=True
)

# Force reprocessing
result3 = extractor.extract_table(
    source_name="oracle_db",
    table_name="employees",
    is_full_extract=True,
    force_reprocess=True  # Will extract again
)
```

### Parallel Recovery

```python
table_configs = [
    {"source_name": "oracle_db", "table_name": "employees", "is_full_extract": True},
    {"source_name": "oracle_db", "table_name": "departments", "is_full_extract": True},
    {"source_name": "oracle_db", "table_name": "orders", "is_full_extract": True},
]

# Initial run - some tables may fail
results1 = extractor.extract_tables_parallel(table_configs)

# Recovery run - only failed tables will be reprocessed
results2 = extractor.extract_tables_parallel(table_configs)

# Force reprocess all tables
results3 = extractor.extract_tables_parallel(table_configs, force_reprocess=True)
```

### Databricks Recovery

```python
from data_extractor.databricks import DatabricksDataExtractor

extractor = DatabricksDataExtractor(
    oracle_host="localhost",
    oracle_port="1521",
    oracle_service="XE",
    oracle_user="hr",
    oracle_password="password",
    output_base_path="/dbfs/data",
    unity_catalog_volume="catalog/schema/volume"  # Optional
)

# All idempotency features work in Databricks mode
results = extractor.extract_tables_parallel(table_configs)
```

## Benefits

1. **Data Consistency**: No duplicate data from rerunning operations
2. **Time Efficiency**: Skip completed extractions automatically
3. **Fault Tolerance**: Recover from failures without starting over
4. **Resource Optimization**: Don't waste compute on unnecessary reprocessing
5. **Operational Safety**: Safe to rerun pipelines at any time
6. **Debugging Support**: Force reprocessing for troubleshooting

## State Management Integration

This implementation provides the foundation for more advanced state management features:

- The existing `StatefulDataExtractor` and `StatefulDatabricksDataExtractor` classes build on these idempotency features
- Progress tracking and pipeline state management are available for complex workflows
- Integration with external orchestration tools is supported through consistent state interfaces

## Testing

Comprehensive test suite validates:

- File integrity validation
- Checksum calculation and verification
- Metadata save/load operations
- Idempotent extraction skipping
- Force reprocess override behavior
- Parallel extraction recovery
- Output path consistency
- Databricks inheritance and integration

Run tests with:
```bash
pytest tests/test_idempotency.py -v
```
