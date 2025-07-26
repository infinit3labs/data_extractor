# Idempotent Pipeline Demo

This example demonstrates an idempotent data pipeline implemented in `data_extractor.idempotent_pipeline`.

## Running the demo

```bash
poetry install
poetry run python examples/idempotent_pipeline_demo.py --config config/idempotent_pipeline.yml
```

The script runs the pipeline three times:
1. initial extraction
2. idempotent replay (no changes)
3. forced rerun

After execution the console shows the dataset locations written under the configured base path.
