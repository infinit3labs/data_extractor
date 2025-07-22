# Data Extractor Testing Guide

This document provides comprehensive instructions for testing the data extractor platform capabilities.

## Quick Start Testing

### 1. Platform Validation
```bash
# Run comprehensive platform tests
poetry run pytest -v
```

### 2. Development Environment Setup
```bash
# Setup development environment
make setup-env

# Install all dependencies
make install-dev
```

### 3. Code Quality Tests
```bash
# Run all code quality checks
make ci

# Individual checks
make lint        # Code formatting and style
make type-check  # Type checking with mypy
make security    # Security scanning
make test        # Unit tests
```

## Docker Testing

### 1. Build and Test Container
```bash
# Build Docker image
make docker-build

# Test container functionality
make docker-run
```

### 2. Full Environment with Docker Compose
```bash
# Start complete environment (with test Oracle DB)
make docker-compose-up

# Stop environment
make docker-compose-down
```

## Testing Components

### 1. Configuration System
- ✅ YAML configuration loading
- ✅ Environment variable override
- ✅ Pydantic validation
- ✅ Sample config generation

### 2. Data Extraction Core
- ✅ Oracle JDBC connectivity
- ✅ Parallel processing with threading
- ✅ Incremental and full extraction modes
- ✅ Parquet output format

### 3. Databricks Integration
- ✅ Databricks environment detection
- ✅ Unity Catalog volume support
- ✅ Optimized resource usage
- ✅ DBFS path handling

### 4. Enterprise Features
- ✅ Structured JSON logging
- ✅ Health check monitoring
- ✅ Security scanning (Bandit, Safety)
- ✅ Code quality (Black, isort, MyPy)
- ✅ Pre-commit hooks

### 5. Observability
- ✅ Application health monitoring
- ✅ System resource monitoring
- ✅ Database connectivity checks
- ✅ File system accessibility

## Test Scenarios

### Basic Functionality Tests
```bash
# Test CLI help
python -m data_extractor.cli --help

# Generate sample configs
python -m data_extractor.cli --generate-config test_config.yml --generate-tables test_tables.json

# Test configuration loading
python -c "
from data_extractor.config import ConfigManager
config = ConfigManager('test_config.yml')
print('Configuration loaded successfully')
"
```

### Health Check Tests
```bash
# Test health checks
python -c "
from data_extractor.health import HealthChecker
hc = HealthChecker()
results, status = hc.run_all_checks()
print(f'Health Status: {status.value}')
"
```

### Logging Tests
```bash
# Test structured logging
python -c "
from data_extractor.logging_config import setup_logging
logger = setup_logging(structured_logging=True)
logger.info('Test structured log message', extra={'component': 'test'})
"
```

## Testing with Real Oracle Database

### 1. Setup Test Database
```bash
# Using Docker Oracle
docker run -d --name test-oracle \
  -p 1521:1521 \
  -e ORACLE_PASSWORD=testpass \
  -e APP_USER=testuser \
  -e APP_USER_PASSWORD=testpass \
  gvenzl/oracle-xe:21-slim
```

### 2. Configure Environment
```bash
# Copy and edit .env file
cp .env.example .env

# Edit .env with your database settings
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=XE
ORACLE_USER=testuser
ORACLE_PASSWORD=testpass
```

### 3. Run Extraction Tests
```bash
# Test single table extraction
python -m data_extractor.cli \
  --host localhost \
  --service XE \
  --user testuser \
  --password testpass \
  --source-name test_db \
  --table-name employees \
  --schema testuser \
  --full-extract

# Test batch extraction
python -m data_extractor.cli \
  --config config/test_config.yml \
  --tables config/test_tables.json
```

## Testing in Databricks

### 1. Databricks Notebook Testing
```python
# Install the package in Databricks
%pip install -e .

# Test Databricks mode
from data_extractor.databricks import DatabricksDataExtractor

# Initialize with Databricks settings
extractor = DatabricksDataExtractor(
    oracle_host="your_oracle_host",
    oracle_port="1521",
    oracle_service="XE",
    oracle_user=dbutils.secrets.get("oracle", "username"),
    oracle_password=dbutils.secrets.get("oracle", "password"),
    output_base_path="/dbfs/data/extracts"
)

# Test Databricks context
context = extractor.get_databricks_context()
print(f"Running in Databricks: {context['is_databricks']}")
```

### 2. Unity Catalog Testing
```python
# Test Unity Catalog volume integration
extractor = DatabricksDataExtractor(
    oracle_host="your_oracle_host",
    oracle_port="1521",
    oracle_service="XE",
    oracle_user="user",
    oracle_password="pass",
    unity_catalog_volume="main/default/data_extracts"
)
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Install all dependencies with `make install-dev`
2. **Docker Build Fails**: Ensure Docker is running and check Dockerfile syntax
3. **Oracle Connection**: Verify database credentials and network connectivity
4. **Permission Issues**: Check file system permissions for output directories

### Debug Mode
```bash
# Enable verbose logging
LOG_LEVEL=DEBUG python -m data_extractor.cli --help

# Run with Python debugger
python -m pdb -m data_extractor.cli --help
```

### Health Check Debugging
```bash
# Check system health
python -c "
from data_extractor.health import HealthChecker
hc = HealthChecker()
results, status = hc.run_all_checks(output_path='./data')
for name, result in results.items():
    print(f'{name}: {result.status.value} - {result.message}')
"
```

## Performance Testing

### Resource Monitoring
```bash
# Monitor resource usage during extraction
python -c "
import psutil
import time
from data_extractor.health import HealthChecker

hc = HealthChecker()
for i in range(10):
    result = hc.check_system_resources()
    print(f'CPU: {result.details[\"cpu_percent\"]}%, Memory: {result.details[\"memory_percent\"]}%')
    time.sleep(1)
"
```

### Parallel Processing Test
```bash
# Test with different worker counts
MAX_WORKERS=2 python -m data_extractor.cli --config config/test_config.yml --tables config/test_tables.json
MAX_WORKERS=4 python -m data_extractor.cli --config config/test_config.yml --tables config/test_tables.json
MAX_WORKERS=8 python -m data_extractor.cli --config config/test_config.yml --tables config/test_tables.json
```

## Validation Checklist

- [ ] All imports working correctly
- [ ] Configuration system functional
- [ ] Health checks operational
- [ ] Logging system working
- [ ] CLI commands functional
- [ ] Docker build successful
- [ ] Docker container runs
- [ ] Database connectivity (if available)
- [ ] File system permissions correct
- [ ] Security scans passing
- [ ] Unit tests passing
- [ ] Code quality checks passing

## Next Steps After Testing

1. **Production Setup**: Configure production database credentials
2. **Scheduling**: Set up job scheduling (cron, Airflow, Databricks Jobs)
3. **Monitoring**: Implement production monitoring and alerting
4. **Scaling**: Configure appropriate worker counts and resource limits
5. **Security**: Review and implement additional security measures