import os
import sys

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE not in sys.path:
    sys.path.insert(0, BASE)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("biis-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def spark_jdbc():
    """Spark session with the SQL Server JDBC driver (functional tests)."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("biis-functional")
        .master("local[2]")
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def config():
    from utils.secrets import load_config

    return load_config("test")


@pytest.fixture(scope="session")
def secret(config):
    from utils.secrets import get_secret

    return get_secret("biis", config)


@pytest.fixture(scope="session")
def golden_dir():
    return os.path.join(BASE, "tests", "fixtures", "golden")


@pytest.fixture()
def db_connection(config, secret):
    from utils.db import pyodbc_connection

    with pyodbc_connection(secret, config) as conn:
        yield conn


@pytest.fixture(scope="session")
def seed_tables(config, secret):
    """Ensure golden fixtures have been seeded (functional/regression runs)."""
    from utils.db import pyodbc_connection

    with pyodbc_connection(secret, config) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM PAY_PERIOD")
        if cur.fetchone()[0] == 0:
            pytest.skip("Database not seeded; run `make db-seed` first")
    return True
