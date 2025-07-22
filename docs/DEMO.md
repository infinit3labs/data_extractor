# Demo Script

The repository includes a demo script located in `examples/demo.py`. It demonstrates how to run an extraction using configuration files.

## Running the Demo

1. Ensure dependencies are installed:

```bash
poetry install
```

2. Generate sample configuration files (if you don't have them):

```bash
poetry run data-extractor --generate-config examples/demo_config.yml --generate-tables examples/demo_tables.json
```

3. Execute the demo script:

```bash
poetry run python examples/demo.py --config examples/demo_config.yml --tables examples/demo_tables.json
```

The script loads the configuration, runs parallel extractions and prints a summary of successes and failures.
