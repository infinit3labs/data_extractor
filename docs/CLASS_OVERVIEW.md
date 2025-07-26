# Codebase Class Overview

This document summarises all public classes in the `data_extractor` package and highlights how they interact.

## Configuration

### `AppSettings`
Pydantic settings model providing environment variable driven configuration for database connection parameters, Spark options and runtime flags. Defined in `data_extractor/config.py`.

### `ConfigManager`
Utility class used to load YAML configuration files and merge them with environment variables via `AppSettings`. It also provides helper methods to produce dictionaries for database connections and to create sample configuration files.

### `DatabricksConfigManager`
Subclass of `ConfigManager` located in `data_extractor/databricks.py`. Adds convenience helpers for running in Databricks, e.g. generating Unity Catalog paths and adjusting worker counts.

## Extraction Engines

### `DataExtractor`
Core extraction engine implemented in `data_extractor/core.py`. Manages per-thread Spark sessions and performs table extraction from Oracle databases using JDBC. Supports incremental or full extraction and multi-table parallel execution via `ThreadPoolExecutor`.

### `DatabricksDataExtractor`
Databricks optimised variant defined in `data_extractor/databricks.py`. Reuses an existing Spark session when running inside Databricks and normalises output paths (e.g. `/dbfs`). Shares the basic extraction logic with `DataExtractor` but tunes defaults for a cluster environment.

### `SqlServerDataExtractor`
Located in `data_extractor/sqlserver.py`, this extractor uses the same JDBC based approach but targets Microsoft SQL Server. Configuration and Spark session handling mirror those in `DataExtractor`.

### `StatefulDataExtractor`
Implemented in `data_extractor/stateful_core.py`. Wraps `DataExtractor` and integrates the `StateManager` to provide idempotent extraction, progress tracking and pipeline restart capabilities.

### `StatefulDatabricksDataExtractor`
Found in `data_extractor/stateful_databricks.py`. Combines Databricks specific behaviour with the state management features from `StatefulDataExtractor`.

## State Management

### `ExtractionStatus` & `PipelineStatus`
Enums defined in `data_extractor/state_manager.py` describing extraction and pipeline lifecycle states.

### `ExtractionState`
Dataclass representing the state of a single table extraction (counts, timestamps, errors).

### `PipelineState`
Dataclass storing pipeline level metadata such as run id, start/end times and table counters.

### `StateManager`
Central class for tracking and persisting pipeline state. Provides methods to start pipelines, mark extractions as completed, resume previous runs and generate progress reports. Used by the stateful extractors above.

## Logging

### `StructuredFormatter` & `ThreadSafeFileHandler`
Helpers in `data_extractor/logging_config.py` providing JSON logging and safe file handling.

### `setup_logging`
Factory function returning a configured `logging.Logger` using the above helpers. Other modules import this to configure logging consistently.

## Health Monitoring

### `HealthStatus`, `HealthCheckResult` & `HealthChecker`
Defined in `data_extractor/health.py`. They implement database, filesystem and system resource checks which can be used for operational monitoring.

## Command Line Interface

### `cli.py`
Exposes a flexible command line interface. Functions include `create_parser`, `extract_single_table` and `extract_multiple_tables` which wire up configuration objects and the appropriate extractor class based on user options.

### `databricks_job.py`
Provides a `run_from_widgets` helper used when executing inside a Databricks job. It reads configuration via widgets/environment variables and runs `DataExtractor` accordingly.

## Interactions

1. **Configuration**: `ConfigManager` (or `DatabricksConfigManager`) loads settings which are passed to the extractor classes upon instantiation.
2. **Extraction**: `DataExtractor` and its variants perform the Spark based JDBC extraction. The stateful variants delegate state tracking to `StateManager`.
3. **Logging and Health**: All modules use helpers from `logging_config.py` for consistent logging. The `HealthChecker` can be employed to validate database connections or filesystem health before running extractions.
4. **CLI & Job Runner**: `cli.py` and `databricks_job.py` tie everything together, parsing user input and launching the appropriate extractor with configuration from `ConfigManager`.

This overview should aid new contributors in understanding the high level responsibilities of each class and how data flows through the system.
