# Configurable JDBC Options

The extractor now allows customization of JDBC fetch size and the number of partitions used for Spark reads.

These settings help tune performance for different workloads and are controlled via the configuration file or environment variables:

```yaml
extraction:
  jdbc_fetch_size: 10000
  jdbc_num_partitions: 4
```

Environment variable overrides:

- `JDBC_FETCH_SIZE`
- `JDBC_NUM_PARTITIONS`

Increasing these values can improve throughput when extracting large tables. Be sure to test different settings based on your cluster resources.
