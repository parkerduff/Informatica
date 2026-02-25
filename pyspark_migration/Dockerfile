# PySpark Migration - Informatica to PySpark
# Replaces Informatica PowerCenter 9.6.1 Integration Service
#
# Base image with PySpark and Oracle client support
FROM python:3.11-slim

LABEL maintainer="HHS BIIS Team"
LABEL description="PySpark migration of Informatica PowerCenter ETL workflows"

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app/pyspark_migration/

# Set Python path
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Default environment variables
ENV ENVIRONMENT="Prod"
ENV REPOSITORY_SERVICE="Prd_Repo_Srvc"
ENV LOG_LEVEL="INFO"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import pyspark; print('OK')" || exit 1

# Default entrypoint - run specific job via environment variable
ENTRYPOINT ["python", "-m", "pyspark_migration.scripts.run_job"]
