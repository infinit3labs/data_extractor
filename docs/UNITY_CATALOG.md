# Unity Catalog Usage

This guide explains how to extract Oracle tables directly into Unity Catalog volumes using `DatabricksDataExtractor`.

## Configuration

Add a `unity_catalog_volume` entry in the `[databricks]` section of your INI file:

```ini
[databricks]
unity_catalog_volume = main/default/volume
```

This path uses the format `catalog/schema/volume`.

## Running the Extractor

```bash
poetry run python examples/unity_catalog_demo.py --config examples/databricks_config.yml --tables examples/databricks_tables.json
```

The demo shows how the extractor writes Parquet files to `/Volumes/<catalog>/<schema>/<volume>`.
