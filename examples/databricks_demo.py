"""Demonstration of running the Databricks job runner locally."""

import os

from data_extractor.databricks_job import run_from_widgets

# Emulate widget values using environment variables
os.environ["CONFIG_PATH"] = "examples/config.yml"
os.environ["TABLES_PATH"] = "examples/tables.json"

if __name__ == "__main__":
    try:
        run_from_widgets()
    except Exception as exc:
        print(f"Demo failed: {exc}")
        print("This is expected if no Oracle database is available.")
