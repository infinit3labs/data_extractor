# Docker Mock Environment with MinIO and Spark

This guide explains how to run the provided `docker-compose.yml` configuration to
create a local environment containing:

- Oracle XE with test data and Flashback enabled
- A Spark master and worker
- MinIO for S3-compatible storage
- The data extractor container

## Running the Environment

```bash
# Build images and start all services
docker-compose up --build
```

The compose file initializes an Oracle database with sample tables and enables
Flashback. A MinIO bucket named `test-bucket` is created automatically.
Spark is started in standalone mode with one worker.

## Writing to MinIO

The configuration file `config/minio_config.yml` sets the output path to
` s3a://test-bucket/data`. The data extractor container is already configured
with the required S3 credentials and Spark master URL.

To run an extraction inside the environment:

```bash
docker-compose run --rm data-extractor \
  python examples/minio_spark_demo.py \
  --config config/minio_config.yml \
  --tables config/tables.json
```

Parquet files will be written to the MinIO bucket and can be inspected via the
MinIO console at [http://localhost:9001](http://localhost:9001).
