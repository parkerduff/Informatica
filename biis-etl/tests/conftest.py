"""Shared pytest fixtures for the BIIS ETL migration test suite."""
from __future__ import annotations

import os

import pytest

from utils import config as cfg
from utils import db

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DDL_PATH = os.path.join(ROOT, "sql", "ddl.sql")
SEED_PATH = os.path.join(ROOT, "tests", "fixtures", "pay_calendar_seed.sql")
COMPTIME_CSV = os.path.join(ROOT, "tests", "fixtures", "comptime_input.csv")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[*]")
        .appName("biis-etl-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def db_config():
    return cfg.load_config("test")


@pytest.fixture(scope="session")
def db_schema(db_config):
    """Create the database/schema once per test session."""
    with open(DDL_PATH, "r", encoding="utf-8") as fh:
        db.execute_sql(db_config, fh.read(), database="master")
    return True


@pytest.fixture()
def db_connection(db_config, db_schema):
    """A pyodbc connection with a freshly seeded PAY_PERIOD and empty targets."""
    _seed_pay_period(db_config)
    _truncate_targets(db_config)
    conn = db.get_pyodbc_connection(db_config)
    yield conn
    conn.close()


@pytest.fixture()
def clean_tables(db_config, db_schema):
    """Truncate target tables (and reseed PAY_PERIOD) between tests."""
    _seed_pay_period(db_config)
    _truncate_targets(db_config)
    yield


@pytest.fixture()
def seeded_pay_calendar(db_config, db_schema):
    _seed_pay_period(db_config)
    return db_config


@pytest.fixture()
def comptime_csv_path():
    return COMPTIME_CSV


def _seed_pay_period(config):
    with open(SEED_PATH, "r", encoding="utf-8") as fh:
        db.execute_sql(config, fh.read())


def _truncate_targets(config):
    db.execute_sql(config, "DELETE FROM COMP_TIME_DAILY_TBL")
    db.execute_sql(config, "DELETE FROM COUNTER_TBL")
