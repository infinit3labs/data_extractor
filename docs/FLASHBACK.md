# Oracle Flashback Queries

This project supports Oracle Flashback to read historical snapshots of your tables. Use the optional `flashback_enabled` and `flashback_timestamp` parameters when calling `extract_table`.

## Usage

```python
from datetime import datetime
from data_extractor.core import DataExtractor

extractor = DataExtractor(
    oracle_host="localhost",
    oracle_port="1521",
    oracle_service="XE",
    oracle_user="hr",
    oracle_password="password",
)

snapshot_time = datetime(2024, 1, 1, 12, 0, 0)

extractor.extract_table(
    source_name="oracle_db",
    table_name="employees",
    schema_name="hr",
    is_full_extract=True,
    flashback_enabled=True,
    flashback_timestamp=snapshot_time,
)
```

Run the example script:

```bash
poetry run python examples/flashback_demo.py \
  --config config/config.yml \
  --tables config/tables.json \
  --timestamp 2024-01-01T12:00:00
```

This extracts each table at the specified point in time using Oracle Flashback.
