# Demo Script

The repository includes a demo script located in `examples/demo.py`. It demonstrates how to run an extraction using configuration files.

## Running the Demo

1. Ensure dependencies are installed:

```bash
poetry install
```

2. Generate sample configuration files (if you don't have them):

```bash
poetry run data-extractor --generate-config config/config.yml --generate-tables config/tables.json
```

3. Execute the demo script:

```bash
poetry run python examples/demo.py --config config/config.yml --tables config/tables.json
```

The script loads the configuration, runs parallel extractions and prints a summary of successes and failures.

For Databricks widget usage run:
```bash
poetry run python examples/databricks_demo.py
```

To try Oracle Flashback queries use:
```bash
poetry run python examples/flashback_demo.py --config config/config.yml --tables config/tables.json --timestamp 2024-01-01T12:00:00
```

### JDBC Options Demo

You can check the configured JDBC fetch size and partition count with:

```bash
poetry run python examples/jdbc_config_demo.py
```
