# SQL Server Support

The data extractor can connect to Microsoft SQL Server databases using Spark JDBC.

## Configuration

Add SQL Server settings to your YAML configuration file:

```yaml
database:
  sqlserver_host: localhost
  sqlserver_port: 1433
  sqlserver_database: master
  sqlserver_user: your_username
  sqlserver_password: your_password
  output_base_path: ./data
```

Table configurations are the same as for Oracle.

## Running the Demo

A demo script is available at `examples/sqlserver_demo.py`:

```bash
poetry run python examples/sqlserver_demo.py --config config/sqlserver_config.yml --tables config/sqlserver_tables.json
```

This loads the configuration, extracts the tables in parallel and prints a summary.
