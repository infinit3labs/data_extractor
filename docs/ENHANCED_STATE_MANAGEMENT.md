# Enhanced State Management Documentation

## Overview

The enhanced state management module provides comprehensive pipeline progress tracking, idempotent operations, and restart capabilities for the data extractor. This ensures reliable, resumable data extraction pipelines with detailed monitoring and recovery features.

## Key Features

### 🔄 Idempotent Operations
- **Automatic duplicate detection**: Prevents re-processing of already completed extractions
- **File integrity verification**: Validates output files exist and match expected checksums
- **Window consistency checks**: Ensures extractions use consistent time windows
- **Force reprocessing**: Allows manual override of idempotency checks when needed

### 🚀 Restart Capabilities  
- **Persistent state storage**: JSON-based state files with atomic updates
- **Pipeline resumption**: Automatically resumes incomplete pipelines based on run_id
- **Progress preservation**: Maintains extraction status across application restarts
- **Restart counting**: Tracks number of pipeline restarts for monitoring

### 📅 24-Hour Window Processing
- **Fixed window extraction**: Uses consistent 24-hour windows from start of run date to end
- **Window validation**: Verifies all extractions use consistent time boundaries
- **Date-based organization**: Organizes output files by year/month/day structure
- **Incremental processing**: Supports incremental extraction within defined windows

### 📊 Progress Tracking
- **Real-time monitoring**: Track pipeline and individual extraction progress
- **Detailed metrics**: Record counts, file sizes, durations, and error messages
- **Status reporting**: Comprehensive status breakdown by extraction state
- **Historical tracking**: Maintain history of pipeline runs with completion rates

## Core Components

### StateManager Class

The `StateManager` class is the central component providing:

```python
from data_extractor.state_manager import StateManager

# Initialize with custom settings
state_manager = StateManager(
    state_dir="state",
    enable_checkpoints=True,
    checkpoint_interval_seconds=30,
    max_retry_attempts=3,
    cleanup_after_days=7
)
```

#### Key Methods

**Pipeline Management:**
- `start_pipeline()` - Start new or resume existing pipeline
- `finish_pipeline()` - Mark pipeline as completed
- `get_pipeline_progress()` - Get current pipeline status and metrics

**Extraction Tracking:**
- `is_extraction_needed()` - Idempotent check for extraction necessity
- `start_extraction()` - Mark extraction as started
- `complete_extraction()` - Mark extraction as completed (success/failure)

**Progress Monitoring:**
- `get_extraction_summary()` - Detailed extraction statistics
- `create_extraction_report()` - Comprehensive report with recommendations
- `validate_extraction_window_consistency()` - Verify time window consistency

**Recovery Operations:**
- `force_reprocess_table()` - Force reprocessing of specific table
- `reset_failed_extractions()` - Reset failed extractions for retry
- `get_pending_extractions()` - Get list of tables needing extraction

### StatefulDataExtractor Class

Enhanced data extractor with integrated state management:

```python
from data_extractor.stateful_core import StatefulDataExtractor

extractor = StatefulDataExtractor(
    oracle_host="localhost",
    oracle_port="1521",
    oracle_service="XE",
    oracle_user="user",
    oracle_password="password",
    output_base_path="data",
    state_dir="state",
    enable_checkpoints=True,
    max_retry_attempts=3
)
```

### StatefulDatabricksDataExtractor Class

Databricks-optimized version with state management:

```python
from data_extractor.stateful_databricks import StatefulDatabricksDataExtractor

extractor = StatefulDatabricksDataExtractor(
    oracle_host="localhost",
    oracle_port="1521", 
    oracle_service="XE",
    oracle_user="user",
    oracle_password="password",
    output_base_path="/dbfs/data",
    state_dir="/dbfs/state",
    enable_checkpoints=True
)
```

## Usage Patterns

### 1. Basic Pipeline with State Management

```python
# Define table configurations
table_configs = [
    {
        "source_name": "sales_db",
        "table_name": "transactions",
        "schema_name": "public",
        "incremental_column": "created_at",
        "is_full_extract": False
    }
]

# Extract with automatic state tracking
results = extractor.extract_tables_parallel(table_configs)

# Check pipeline status
status = extractor.get_pipeline_status()
print(f"Completion rate: {status['completion_rate']:.1f}%")
```

### 2. Idempotent Re-execution

```python
# First run - extracts all tables
results1 = extractor.extract_tables_parallel(table_configs)

# Second run - skips already completed tables  
results2 = extractor.extract_tables_parallel(table_configs)

# Check what was skipped
summary = extractor.get_extraction_summary()
print(f"Completed: {summary['by_status']['completed']}")
print(f"Skipped: {summary['by_status']['skipped']}")
```

### 3. Force Reprocessing

```python
# Force reprocessing of specific table
table_key = "sales_db.public.transactions"
extractor.force_reprocess_table(table_key)

# Re-run pipeline to process forced table
results = extractor.extract_tables_parallel(table_configs)
```

### 4. Failure Recovery

```python
# Reset all failed extractions for retry
reset_count = extractor.reset_failed_extractions()
print(f"Reset {reset_count} failed extractions")

# Re-run pipeline to retry failed extractions
results = extractor.extract_tables_parallel(table_configs)
```

### 5. Window Validation

```python
# Validate 24-hour window consistency
validation = extractor.validate_extraction_window_consistency()

if not validation['is_consistent']:
    print("Window inconsistencies found:")
    for issue in validation['inconsistent_extractions']:
        print(f"- {issue['table_key']}: {issue['extraction_date']}")
```

### 6. Comprehensive Reporting

```python
# Generate detailed report
report = extractor.create_extraction_report()

print(f"Pipeline Status: {report['pipeline_info']['status']}")
print(f"Completion Rate: {report['pipeline_info']['completion_rate']:.1f}%")

# Show recommendations
for rec in report['recommendations']:
    print(f"- {rec['type']}: {rec['message']}")
```

### 7. Pipeline Restart

```python
# Application restart scenario
extractor = StatefulDataExtractor(
    # ... same config ...
    state_dir="state"  # Same state directory
)

# Resume with same run_id - automatically resumes from last state
results = extractor.extract_tables_parallel(table_configs)

status = extractor.get_pipeline_status()
print(f"Restart count: {status['restart_count']}")
```

## State File Structure

State files are stored as JSON with the following structure:

```json
{
  "pipeline": {
    "pipeline_id": "uuid",
    "run_id": "20250123_120000",
    "status": "running",
    "extraction_date": "2025-01-22T00:00:00",
    "total_tables": 5,
    "completed_tables": 3,
    "failed_tables": 1,
    "restart_count": 0
  },
  "extractions": {
    "sales_db.public.transactions": {
      "status": "completed",
      "record_count": 15000,
      "output_path": "/data/sales_db/transactions/202501/22/daily.parquet",
      "file_size_bytes": 2048000,
      "checksum": "abc123def456",
      "start_time": "2025-01-23T12:00:00",
      "end_time": "2025-01-23T12:05:30"
    }
  },
  "metadata": {
    "saved_at": "2025-01-23T12:05:30",
    "version": "1.0"
  }
}
```

## Best Practices

### 1. State Directory Management
- Use persistent storage for state directory
- Ensure proper file permissions for state files
- Consider backup strategies for critical state data

### 2. Idempotency Design
- Design extraction logic to be naturally idempotent
- Use consistent output file naming conventions
- Implement proper file integrity checks

### 3. Error Handling
- Set appropriate retry limits for failed extractions
- Monitor failed extraction patterns
- Implement alerting for persistent failures

### 4. Performance Optimization
- Use checkpoints for long-running pipelines
- Configure appropriate cleanup intervals
- Monitor state file sizes and cleanup old files

### 5. Window Processing
- Ensure consistent time zone handling
- Validate extraction windows across pipeline restarts
- Document time window boundaries clearly

## Configuration Options

### StateManager Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `state_dir` | "state" | Directory for state files |
| `enable_checkpoints` | `True` | Enable periodic state checkpoints |
| `checkpoint_interval_seconds` | 30 | Interval between checkpoints |
| `max_retry_attempts` | 3 | Maximum retry attempts for failures |
| `cleanup_after_days` | 7 | Days to keep old state files |

### Extraction Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `force_reprocess` | `False` | Force reprocessing regardless of state |
| `include_failed_retries` | `True` | Include failed extractions in pending list |
| `window_hours` | 24 | Extraction window size in hours |

## Monitoring and Observability

### Key Metrics to Monitor

1. **Pipeline Completion Rate** - Percentage of successful extractions
2. **Restart Frequency** - How often pipelines restart
3. **Failure Patterns** - Common causes of extraction failures
4. **Processing Duration** - Time to complete extractions
5. **State File Growth** - Size and growth of state files

### Recommended Alerting

- Pipeline completion rate below threshold (e.g., < 95%)
- High restart frequency (e.g., > 3 restarts/day)
- Persistent extraction failures
- State directory disk usage
- Window consistency violations

## Troubleshooting

### Common Issues

**Issue: Extractions not being skipped (idempotency not working)**
- Check output file paths and permissions
- Verify file integrity with checksums
- Review extraction window consistency

**Issue: Pipeline not resuming after restart**
- Confirm state directory persistence
- Check state file permissions and corruption
- Verify run_id consistency

**Issue: High memory usage from state files**
- Enable cleanup of old state files
- Reduce checkpoint frequency
- Monitor state file sizes

**Issue: Window inconsistency errors**
- Validate time zone handling
- Check extraction date configuration
- Review incremental column processing

## Integration Examples

### With Airflow
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def run_stateful_extraction(**context):
    extractor = StatefulDataExtractor(...)
    results = extractor.extract_tables_parallel(table_configs)
    
    # Log results to Airflow
    for table, success in results.items():
        if not success:
            raise Exception(f"Extraction failed for {table}")

dag = DAG('stateful_extraction', ...)
task = PythonOperator(
    task_id='extract',
    python_callable=run_stateful_extraction,
    dag=dag
)
```

### With Databricks Jobs
```python
# In Databricks notebook
from data_extractor.stateful_databricks import StatefulDatabricksDataExtractor

# Use existing Spark session and DBFS paths
extractor = StatefulDatabricksDataExtractor(
    oracle_host=dbutils.widgets.get("oracle_host"),
    oracle_user=dbutils.secrets.get("oracle", "username"),
    oracle_password=dbutils.secrets.get("oracle", "password"),
    output_base_path="/dbfs/mnt/datalake/extracts",
    state_dir="/dbfs/mnt/datalake/state"
)

results = extractor.extract_tables_parallel(table_configs)

# Display results in notebook
display(extractor.create_extraction_report())
```

## Conclusion

The enhanced state management system provides enterprise-grade reliability and observability for data extraction pipelines. It ensures idempotent operations, enables graceful recovery from failures, and provides comprehensive progress tracking suitable for production environments.

For additional examples and advanced usage patterns, see:
- `test_enhanced_state_management.py` - Comprehensive test scenarios
- `example_enhanced_state_management.py` - Complete usage example
