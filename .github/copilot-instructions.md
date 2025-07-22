# Data Extractor AI Coding Instructions

## Architecture Overview

This is a high-performance data extraction tool with **dual execution modes**: standard Spark mode and Databricks-optimized mode. The core architecture centers around parallel Oracle-to-Parquet extraction using Spark JDBC.

### Key Components

- **`core.py`**: Standard DataExtractor with thread-local Spark sessions for true parallelism
- **`databricks.py`**: DatabricksDataExtractor that reuses existing Spark sessions and handles DBFS paths
- **`cli.py`**: Unified CLI supporting both modes via `--databricks` flag
- **`config.py`**: YAML-based configuration with environment variable fallbacks and Pydantic validation

## Critical Patterns

### Thread-Local Spark Sessions (Standard Mode)
```python
# Each worker thread gets its own Spark session for true parallelism
self._local = threading.local()
self._local.spark = SparkSession.builder.appName(f"DataExtractor-{threading.current_thread().name}")
```

### Databricks Session Reuse
```python
# Databricks mode reuses existing session to avoid resource conflicts
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
```

### Path Handling Strategy
- Standard mode: Local filesystem paths (`data/source/table/`)
- Databricks mode: Auto-converts to DBFS (`/dbfs/data/source/table/`)
- Unity Catalog: Optional volume paths (`/Volumes/catalog/schema/volume/`)

## Development Workflows

### Local Development
```bash
poetry install              # Install dependencies
poetry shell               # Activate environment
make install-dev           # Install with dev tools
make lint                  # Run all linting
make test                  # Run test suite
```

### Testing Both Modes
```bash
# Standard mode test
data-extractor --host localhost --user hr --password secret --table-name employees

# Databricks mode test (uses existing Spark session)
data-extractor --databricks --host localhost --user hr --password secret --table-name employees
```

## Configuration Patterns

### INI Configuration Structure
```ini
[database]           # Oracle connection params
[extraction]         # max_workers, default_source
[databricks]         # databricks_output_path, unity_catalog_volume
```

### Environment Variable Mapping
- Database: `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD`
- Output: `OUTPUT_BASE_PATH`
- All configs support env var override

## Integration Points

### Databricks Environment Detection
The system auto-detects Databricks via multiple signals:
- `DATABRICKS_RUNTIME_VERSION` environment variable
- `/databricks` directory existence  
- `databricks` in hostname or `SPARK_HOME`

### Spark Package Dependencies
- Oracle JDBC: `com.oracle.database.jdbc:ojdbc8:21.7.0.0`
- Adaptive query execution enabled by default
- Kryo serialization for performance

### Output Format
```
data/source_name/table_name/yyyymm/dd/run_id.parquet
```

## Error Handling Conventions

### Database Connection
- Validate connection params via Pydantic models in `config.py`
- Graceful fallback to environment variables
- Thread-safe connection pooling per worker

### Databricks Compatibility
- Auto-detect and adapt to Databricks environment
- Conservative resource usage (CPU_count/2 workers)
- DBFS path normalization for cloud storage

## Testing Strategy

- Mock Spark sessions in tests to avoid actual cluster dependencies
- Separate test classes for `DataExtractor` vs `DatabricksDataExtractor`
- Configuration validation tests use Pydantic model validation
- Integration tests via `examples/` directory demos

## Key Files for Reference

- `examples/databricks_demo.py`: Shows Databricks widget integration
- `examples/config.ini`: Standard configuration template
- `examples/databricks_config.ini`: Databricks-specific template
- `DATABRICKS_IMPLEMENTATION.md`: Detailed Databricks architecture decisions
- `docs/UNITY_CATALOG.md`: Unity Catalog volume integration guide
