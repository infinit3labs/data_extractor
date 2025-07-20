# Running on Databricks

This module can run inside a Databricks job by reusing the Spark session provided by Databricks.

1. Upload your configuration files to DBFS.
2. Create a new job with a Python task pointing to `examples/databricks_job_demo.py` or use `data_extractor.databricks_job.run_job` in your own script.
3. Pass the paths to the configuration files as parameters using Databricks widgets.

```python
# Example within a Databricks notebook
dbutils.widgets.text("config_path", "/dbfs/config.ini")
dbutils.widgets.text("tables_path", "/dbfs/tables.json")

from data_extractor.databricks_job import run_job
run_job(dbutils.widgets.get("config_path"), dbutils.widgets.get("tables_path"))
```

The `use_global_spark_session` option ensures that the existing Databricks `spark` session is reused for all extraction tasks.
