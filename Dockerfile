# Multi-stage Docker build for data extractor
# Production-ready container with security hardening

# Build stage
FROM python:3.10-slim as builder

# Set build arguments
ARG POETRY_VERSION=1.6.1

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry==$POETRY_VERSION

# Set environment variables
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VENV_IN_PROJECT=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Set work directory
WORKDIR /app

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Configure poetry and install dependencies
RUN poetry config virtualenvs.in-project true && \
    poetry install --only=main --no-root && \
    rm -rf $POETRY_CACHE_DIR

# Production stage
FROM python:3.10-slim as production

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    # Oracle client dependencies
    libaio1 \
    # System monitoring tools
    procps \
    # Network tools (for health checks)
    iputils-ping \
    netcat-openbsd \
    # Clean up
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install Oracle Instant Client
ARG ORACLE_CLIENT_VERSION=21.8.0.0.0
RUN mkdir -p /opt/oracle && \
    cd /opt/oracle && \
    curl -o instantclient.zip https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-basic-linux.x64-${ORACLE_CLIENT_VERSION}dbru.zip && \
    unzip instantclient.zip && \
    rm instantclient.zip && \
    echo /opt/oracle/instantclient_21_8 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

# Set Oracle environment variables
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_8:$LD_LIBRARY_PATH
ENV TNS_ADMIN=/opt/oracle/instantclient_21_8
ENV ORACLE_HOME=/opt/oracle/instantclient_21_8

# Create non-root user for security
RUN groupadd -r dataextractor && useradd -r -g dataextractor dataextractor

# Set work directory
WORKDIR /app

# Copy virtual environment from builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY data_extractor/ ./data_extractor/
COPY examples/ ./examples/
COPY README.md CHECKLIST.md ./

# Create necessary directories
RUN mkdir -p /app/data /app/logs && \
    chown -R dataextractor:dataextractor /app

# Set Python path
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:$PYTHONPATH"

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "from data_extractor.health import HealthChecker; hc = HealthChecker(); results, status = hc.run_all_checks(); exit(0 if status.value == 'healthy' else 1)"

# Switch to non-root user
USER dataextractor

# Set default command
CMD ["python", "-m", "data_extractor.cli", "--help"]

# Labels for metadata
LABEL maintainer="Data Engineering Team" \
      version="1.0.0" \
      description="Enterprise data extractor for Oracle databases with Spark" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="data-extractor" \
      org.label-schema.description="Parallel data extraction from Oracle databases" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"