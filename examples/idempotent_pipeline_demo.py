#!/usr/bin/env python3
"""Run the idempotent pipeline using configuration."""

import argparse
from pathlib import Path

import yaml

from data_extractor.idempotent_pipeline import DataPipelineOrchestrator


def run_from_config(path: str) -> None:
    with open(path) as fh:
        cfg = yaml.safe_load(fh)
    base_path = Path(cfg["base_path"])
    pipeline_name = cfg["pipeline_name"]
    tables = cfg["tables"]
    business_date = cfg["business_date"]

    orchestrator = DataPipelineOrchestrator(base_path)
    orchestrator.run_pipeline(pipeline_name, business_date, tables)
    orchestrator.run_pipeline(pipeline_name, business_date, tables)
    orchestrator.run_pipeline(pipeline_name, business_date, tables, force_rerun=True)
    dataset = orchestrator.complete_dataset(business_date)
    for table, path in dataset.items():
        print(f"{table}: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Idempotent pipeline demo")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    run_from_config(args.config)


if __name__ == "__main__":
    main()
