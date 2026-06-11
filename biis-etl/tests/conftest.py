"""Shared pytest fixtures for the BIIS ETL test suite."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from utils.secrets import load_config

GOLDEN_DIR = Path(__file__).resolve().parent / "fixtures" / "golden"
RUN_DATE = dt.date(2026, 6, 11)


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession shared across the whole test session.

    JDBC is enabled so the same cached session works for both pure-logic unit
    tests and the DB-backed functional/regression tests.
    """
    from utils.spark import get_spark

    session = get_spark("biis-tests", jdbc=True)
    yield session


@pytest.fixture(scope="session")
def config():
    return load_config("test")


@pytest.fixture(scope="session")
def golden_dir() -> Path:
    return GOLDEN_DIR


@pytest.fixture(scope="session")
def run_date() -> dt.date:
    return RUN_DATE


def _db_available(config) -> bool:
    try:
        import pyodbc  # noqa: F401

        from utils.db import get_odbc_connstr
        from utils.secrets import get_db_secret

        conn = pyodbc.connect(get_odbc_connstr(config, get_db_secret(config)), timeout=3)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def db_ready(config) -> bool:
    if not _db_available(config):
        pytest.skip("SQL Server not reachable (start it with `make db-up`)")
    return True


@pytest.fixture()
def db_connection(config, db_ready):
    """A pyodbc connection to the Docker SQL Server (autocommit)."""
    import pyodbc

    from utils.db import get_odbc_connstr
    from utils.secrets import get_db_secret

    conn = pyodbc.connect(get_odbc_connstr(config, get_db_secret(config)), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="session")
def seeded(config, db_ready):
    """Apply DDL and seed golden data once for DB-backed tests."""
    import subprocess
    import sys

    root = Path(__file__).resolve().parent.parent
    env = {"PYTHONPATH": str(root)}
    import os

    full_env = {**os.environ, **env}
    subprocess.run([sys.executable, "scripts/apply_ddl.py", "--env", "test"],
                   cwd=root, check=True, env=full_env)
    subprocess.run([sys.executable, "scripts/seed_golden_data.py",
                    "--fixtures-dir", str(GOLDEN_DIR), "--env", "test"],
                   cwd=root, check=True, env=full_env)
    return True


@pytest.fixture(scope="session")
def pipeline(config, spark, seeded):
    """Run the full ETL pipeline once (in-process) against the seeded DB.

    Functional and regression tests consume the resulting target tables.
    """
    from jobs import comptime, fda_leave, pay_calendar, pseudossn
    from jobs.cpm import cpm_common
    from jobs.ehrp2biis import afterload, etl, preload

    pay_calendar.run(config, RUN_DATE)
    comptime.run(config, str(GOLDEN_DIR / "comptime_input.csv"))
    pseudossn.run(config, str(GOLDEN_DIR / "pseudossn_input.dat"))
    fda_leave.run(config, RUN_DATE)
    preload.run(config, RUN_DATE)
    etl.run(config, RUN_DATE)
    afterload.run(config, RUN_DATE)
    for agency in ("nih", "oig", "cdc"):
        cpm_common.run_agency(agency, config)
    return True
