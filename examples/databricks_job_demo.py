#!/usr/bin/env python3
"""Demo for running the extractor as a Databricks job."""
from data_extractor.databricks_job import run_job

if __name__ == "__main__":
    run_job(
        config_path="examples/config.ini",
        tables_path="examples/tables.json",
        output_path="/tmp/databricks_demo",
    )
