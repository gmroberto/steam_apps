# ============================================================================
# Steam Games Data Processor - Dockerfile
# ============================================================================
# Multi-stage build for optimized production image

# ============================================================================
# BASE STAGE - Python and system dependencies
# ============================================================================
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libpq-dev \
        pkg-config \
        libffi-dev \
        libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r airflow && useradd -r -g airflow airflow

# ============================================================================
# DEPENDENCIES STAGE - Install Python packages
# ============================================================================
FROM base as dependencies

# Set working directory
WORKDIR /app

# Copy requirements file
COPY config/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# ============================================================================
# APPLICATION STAGE - Copy application code
# ============================================================================
FROM dependencies as application

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY airflow/ ./airflow/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY docs/ ./docs/

# Create data directories
RUN mkdir -p data/raw data/processed data/logs data/test_output \
    && chown -R airflow:airflow /app

# Switch to non-root user
USER airflow

# Set working directory
WORKDIR /app

# ============================================================================
# AIRFLOW STAGE - Airflow-specific configuration
# ============================================================================
FROM application as airflow

# Set Airflow environment variables
ENV AIRFLOW_HOME=/app/airflow \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DEBUG=False \
    AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key-here \
    AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080 \
    AIRFLOW__LOGGING__LOGGING_LEVEL=INFO \
    AIRFLOW__LOGGING__BASE_LOG_FOLDER=/app/airflow/logs

# Create Airflow directories
RUN mkdir -p /app/airflow/logs /app/airflow/dags /app/airflow/plugins \
    && chown -R airflow:airflow /app/airflow

# Copy Airflow configuration
COPY --chown=airflow:airflow airflow/ /app/airflow/

# Expose Airflow webserver port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command (can be overridden in docker-compose)
CMD ["airflow", "webserver"]
