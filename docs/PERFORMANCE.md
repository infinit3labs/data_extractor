# Performance Tuning and Best Practices

## Performance Optimization

### Parallel Processing Settings

```bash
# Optimize worker threads based on your system
data-extractor --max-workers 16 --config config.ini --tables tables.json
```

### Spark Configuration

Set environment variables to optimize Spark performance:

```bash
# Memory settings
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MAX_RESULT_SIZE=2g

# Parallelism settings
export SPARK_SQL_ADAPTIVE_ENABLED=true
export SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED=true

# Serialization
export SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer
```

### JDBC Optimization

Modify `core.py` for custom JDBC settings:

```python
# Increase fetch size for larger tables
.option("fetchsize", "50000")  # Default: 10000

# Increase partitions for very large tables
.option("numPartitions", "8")  # Default: 4

# Add partition column for better performance
.option("partitionColumn", "id")
.option("lowerBound", "1")
.option("upperBound", "1000000")
```

## Best Practices

### Table Extraction Strategy

1. **Incremental Tables**: Use timestamp or sequence columns
2. **Full Extraction**: For small reference tables only
3. **Large Tables**: Consider partitioning strategies
4. **Custom Queries**: For complex transformations

### Error Handling

```python
# Example with retry logic
def extract_with_retry(extractor, config, max_retries=3):
    for attempt in range(max_retries):
        if extractor.extract_table(**config):
            return True
        print(f"Attempt {attempt + 1} failed, retrying...")
    return False
```

### Memory Management

1. **Process tables in batches**: Don't extract hundreds simultaneously
2. **Monitor memory usage**: Use system monitoring tools
3. **Clean up resources**: Always call `cleanup_spark_sessions()`

## Troubleshooting

### Common Issues

#### 1. Oracle JDBC Driver Issues

```bash
# Error: No suitable driver found
# Solution: Ensure Spark packages are properly configured
export SPARK_JARS_PACKAGES=com.oracle.database.jdbc:ojdbc8:21.7.0.0
```

#### 2. Memory Issues

```bash
# Error: OutOfMemoryError
# Solution: Increase driver memory
export SPARK_DRIVER_MEMORY=8g
export SPARK_DRIVER_MAX_RESULT_SIZE=4g
```

#### 3. Connection Timeouts

```python
# Add connection properties
.option("connectionProperties", "oracle.net.CONNECT_TIMEOUT=60000")
```

#### 4. Large Result Sets

```python
# Use streaming for large results
.option("fetchsize", "1000")
.option("numPartitions", "16")
```

### Performance Monitoring

```python
import time
import psutil

def monitor_extraction():
    start_time = time.time()
    start_memory = psutil.virtual_memory().used
    
    # Your extraction code here
    
    end_time = time.time()
    end_memory = psutil.virtual_memory().used
    
    print(f"Execution time: {end_time - start_time:.2f} seconds")
    print(f"Memory used: {(end_memory - start_memory) / 1024**2:.2f} MB")
```

### Debugging

Enable verbose logging:

```bash
data-extractor --verbose --config config.ini --tables tables.json
```

Check Spark UI for query execution plans:
- Usually available at: http://localhost:4040
- Monitor SQL tab for query performance
- Check executors tab for resource utilization

## Production Deployment

### Environment Setup

```bash
#!/bin/bash
# production_setup.sh

# Set Java home
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Spark configuration
export SPARK_HOME=/opt/spark
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g
export SPARK_DRIVER_MAX_RESULT_SIZE=4g

# Oracle settings
export ORACLE_HOME=/opt/oracle/instantclient
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH

# Run extraction
data-extractor --config /etc/data-extractor/config.ini \
               --tables /etc/data-extractor/tables.json \
               --max-workers 32 \
               --output-path /data/extracts
```

### Scheduling

```bash
# Crontab example for daily incremental extraction
0 2 * * * /opt/data-extractor/run_daily_extraction.sh >> /var/log/data-extractor.log 2>&1
```

### Monitoring and Alerting

```python
def send_alert(message):
    # Implement your alerting mechanism
    # Email, Slack, PagerDuty, etc.
    pass

# In your extraction script
results = extractor.extract_tables_parallel(table_configs)
failed_tables = [table for table, success in results.items() if not success]

if failed_tables:
    send_alert(f"Data extraction failed for tables: {failed_tables}")
```

## Scale Considerations

### For 100+ Tables

1. **Batch Processing**: Process tables in groups of 20-50
2. **Resource Limits**: Monitor CPU, memory, and network
3. **Storage**: Ensure sufficient disk space for Parquet files
4. **Network**: Consider Oracle connection pool limits

### Sample Batch Processing

```python
def extract_tables_in_batches(extractor, table_configs, batch_size=25):
    total_results = {}
    
    for i in range(0, len(table_configs), batch_size):
        batch = table_configs[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(table_configs) + batch_size - 1)//batch_size}")
        
        batch_results = extractor.extract_tables_parallel(batch)
        total_results.update(batch_results)
        
        # Brief pause between batches
        time.sleep(5)
    
    return total_results
```

## Security

### Credentials Management

```bash
# Use environment variables instead of config files
export ORACLE_PASSWORD=$(vault kv get -field=password secret/oracle/prod)

# Or use encrypted config files
gpg --decrypt config.ini.gpg | data-extractor --config /dev/stdin --tables tables.json
```

### Network Security

1. **VPN/Private Networks**: Use secure connections to Oracle
2. **Firewall Rules**: Restrict access to Oracle ports
3. **SSL/TLS**: Enable encrypted Oracle connections

```python
# SSL connection example
.option("oracle.net.ssl_client_authentication", "false")
.option("oracle.net.ssl_server_dn_match", "true")
```