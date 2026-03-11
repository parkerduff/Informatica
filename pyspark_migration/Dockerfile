# Dockerfile for PySpark Migration testing environment.
# Provides PySpark + Oracle JDBC driver for running migration jobs
# against a local Oracle XE container.
FROM python:3.11-slim

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        wget \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Oracle JDBC driver
RUN mkdir -p /opt/oracle/jdbc && \
    wget -q -O /opt/oracle/jdbc/ojdbc11.jar \
    https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.3.0.23.09/ojdbc11-23.3.0.23.09.jar

ENV ORACLE_JDBC_PATH=/opt/oracle/jdbc/ojdbc11.jar

# Copy application code
COPY . .

# Default command: run tests
CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]
