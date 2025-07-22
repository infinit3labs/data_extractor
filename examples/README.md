# Examples Directory

This directory contains practical examples and sample configurations for the data extractor.

## Configuration Examples

- `databricks_config.yml` - Databricks environment configuration
- `databricks_tables.json` - Sample table definitions for Databricks

## Code Examples

- `demo.py` - Basic usage demonstration
- `databricks_demo.py` - Databricks-specific usage
- `databricks_usage_examples.py` - Comprehensive Databricks examples
- `flashback_demo.py` - Flashback query example
- `unity_catalog_demo.py` - Unity Catalog integration example
- `usage_examples.py` - General usage patterns

## Running Examples

```bash
# Run basic demo
cd examples && python demo.py

# Run Databricks examples (requires Databricks environment)
cd examples && python databricks_demo.py

# Run Flashback demo
cd examples && python flashback_demo.py --config ../config/config.yml --tables ../config/tables.json --timestamp 2024-01-01T12:00:00
```

