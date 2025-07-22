# Getting Started

This guide walks through installing **data_extractor** using Poetry and running basic extractions.

## Installation with Poetry

```bash
# Clone the repository
git clone https://github.com/infinit3labs/data_extractor.git
cd data_extractor

# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

## Generating Configuration Files

Create sample configuration and tables files using the built in CLI:

```bash
poetry run data-extractor --generate-config config.yml --generate-tables tables.json
```

Edit `config.yml` and `tables.json` to match your environment.

## Running an Extraction

Once configured, run the extractor using your configuration files:

```bash
poetry run data-extractor --config config.yml --tables tables.json
```

This command processes all tables defined in `tables.json` in parallel and saves Parquet files under the configured output path.

## Programmatic Usage

The module can also be used from Python code:

```python
from data_extractor.config import ConfigManager
from data_extractor.core import DataExtractor

config = ConfigManager('config.yml')
params = config.get_runtime_config()

extractor = DataExtractor(
    oracle_host=params['oracle_host'],
    oracle_port=params['oracle_port'],
    oracle_service=params['oracle_service'],
    oracle_user=params['oracle_user'],
    oracle_password=params['oracle_password'],
    output_base_path=params.get('output_base_path', 'data'),
    max_workers=params.get('max_workers')
)

table_configs = config.load_table_configs_from_json('tables.json')
results = extractor.extract_tables_parallel(table_configs)

extractor.cleanup_spark_sessions()
```

Consult `examples/` for additional scripts and configuration samples. For historical snapshots using Oracle Flashback see [docs/FLASHBACK.md](FLASHBACK.md).
