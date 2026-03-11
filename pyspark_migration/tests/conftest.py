"""
Shared pytest fixtures for PySpark migration tests.

Provides a local SparkSession, mock database manager, and test configurations
used across all unit tests. No Oracle dependency required for unit tests.
"""

import os
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from pyspark_migration.common.config import (
    EmailConfig,
    MigrationConfig,
    OracleConnectionConfig,
    PathConfig,
    SparkConfig,
)
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.counter_error import CounterErrorManager


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.
    
    Includes Oracle JDBC driver on classpath if available,
    enabling integration tests against Oracle XE.
    """
    jdbc_jar = None
    # Resolve paths relative to the conftest.py file location
    _tests_dir = os.path.dirname(os.path.abspath(__file__))
    _migration_dir = os.path.dirname(_tests_dir)
    for path in [
        os.path.join(_migration_dir, "ojdbc11.jar"),
        "/opt/oracle/ojdbc11.jar",
        os.environ.get("ORACLE_JDBC_DRIVER_PATH", ""),
    ]:
        if path and os.path.exists(path):
            jdbc_jar = path
            break

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("pyspark_migration_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby-test")
    )
    if jdbc_jar:
        builder = (
            builder
            .config("spark.jars", jdbc_jar)
            .config("spark.driver.extraClassPath", jdbc_jar)
            .config("spark.executor.extraClassPath", jdbc_jar)
        )

    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def test_config():
    """Create a test MigrationConfig with defaults."""
    return MigrationConfig(
        oracle=OracleConnectionConfig(
            host="localhost",
            port=1521,
            service_name="XEPDB1",
            username="test_user",
            password="test_pass",
        ),
        spark=SparkConfig(
            executor_memory="1g",
            driver_memory="1g",
            shuffle_partitions=2,
        ),
        email=EmailConfig(
            smtp_host="localhost",
            smtp_port=25,
            sender="test@test.com",
            default_recipients="test@test.com",
            environment="Test",
        ),
        paths=PathConfig(
            source_dir="/tmp/test_source",
            target_dir="/tmp/test_target",
            reject_dir="/tmp/test_reject",
        ),
    )


@pytest.fixture
def mock_db_manager(spark):
    """Create a mock DatabaseManager that uses in-memory DataFrames."""
    manager = MagicMock(spec=DatabaseManager)

    def mock_read_jdbc(query, **kwargs):
        """Return empty DataFrame by default."""
        return spark.createDataFrame([], schema="col1 string")

    manager.read_jdbc.side_effect = mock_read_jdbc
    manager.write_jdbc.return_value = 0
    manager.execute_sql.return_value = (True, "OK")
    manager.execute_stored_procedure.return_value = (True, "OK")
    return manager


@pytest.fixture
def mock_email_service():
    """Create a mock EmailService."""
    service = MagicMock(spec=EmailService)
    service.send_job_success.return_value = True
    service.send_job_failure.return_value = True
    return service


@pytest.fixture
def mock_counter_manager(spark, mock_db_manager):
    """Create a mock CounterErrorManager."""
    manager = MagicMock(spec=CounterErrorManager)
    manager.write_counter.return_value = True
    manager.write_error.return_value = True
    return manager
