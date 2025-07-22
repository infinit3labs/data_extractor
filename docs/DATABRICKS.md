# Running on Databricks

This module can be executed on a Databricks cluster as a scheduled job.
The entry point `data_extractor.databricks_job` reads configuration from
Databricks widgets or environment variables and runs the extraction using
`DataExtractor`.

## Widgets
- `config_path`  – path to the YAML configuration file in DBFS
- `tables_path`  – path to the table configuration JSON file in DBFS
- `output_path`  – base output directory (optional)
- `max_workers`  – maximum worker threads (optional)
- `run_id`       – optional run identifier

## Example Job Configuration
1. Create widgets in a notebook:
```python
dbutils.widgets.text("config_path", "/dbfs/config.yml")
dbutils.widgets.text("tables_path", "/dbfs/tables.json")
```
2. Set the notebook to run `python -m data_extractor.databricks_job`.
3. Schedule the notebook as a Databricks job and provide widget values.

The script will load configurations, execute parallel extractions and stop the
Spark session when complete.
