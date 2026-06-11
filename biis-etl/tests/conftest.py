"""Shared pytest fixtures.

The default ``test`` environment uses the self-contained SQLite backend, so the
whole suite (unit + functional + regression) runs with no external services.
The ``spark`` and ``cfg`` fixtures are session-scoped; ``fresh_db`` /
``seeded_db`` give each test a clean, isolated schema.
"""
from __future__ import annotations

import os
import sys
import tempfile

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

GOLDEN_DIR = os.path.join(REPO_ROOT, "tests", "fixtures", "golden")

# Point the test backend at an isolated scratch dir before any job imports
# resolve their config.
_TEST_ROOT = tempfile.mkdtemp(prefix="biis-pytest-")
os.environ["BIIS_TEST_ROOT"] = _TEST_ROOT
os.environ.setdefault("SETUPTOOLS_USE_DISTUTILS", "local")


@pytest.fixture(scope="session")
def spark():
    from utils.spark import get_spark

    session = get_spark("biis-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cfg():
    from utils.config import get_config

    return get_config("test")


@pytest.fixture
def fresh_db(cfg):
    """Apply the full DDL to a clean database and return the config."""
    from scripts import apply_ddl

    apply_ddl.apply("test")
    return cfg


@pytest.fixture
def seeded_db(fresh_db):
    """Clean schema + golden input fixtures loaded."""
    from scripts import seed_golden_data

    seed_golden_data.seed("test", GOLDEN_DIR)
    return fresh_db


@pytest.fixture
def golden_dir():
    return GOLDEN_DIR


def run_full_pipeline(spark, run_date="2026-06-11"):
    """Apply DDL, seed inputs and run every job against the test DB (in-process)."""
    import datetime as dt

    from scripts import apply_ddl, seed_golden_data
    from jobs import comptime, fda_leave, pay_calendar, pseudossn
    from jobs.cpm import cpm_cdc, cpm_nih, cpm_oig
    from jobs.ehrp2biis import afterload, etl, preload

    apply_ddl.apply("test")
    seed_golden_data.seed("test", GOLDEN_DIR)
    rd = dt.date.fromisoformat(run_date)

    pay_calendar.run(env="test", run_date=rd, spark=spark)
    comptime.run(env="test", file_path=os.path.join(GOLDEN_DIR, "comptime_input.csv"),
                 run_date=run_date, spark=spark)
    pseudossn.run(env="test", file_path=os.path.join(GOLDEN_DIR, "pseudossn_input.dat"),
                  run_date=run_date, spark=spark)
    fda_leave.run(env="test", run_date=run_date, spark=spark)
    preload.run(env="test", run_date=rd)
    etl.run(env="test", run_date=rd, spark=spark)
    afterload.run(env="test", run_date=rd)
    cpm_nih.run(env="test", run_date=run_date, spark=spark)
    cpm_oig.run(env="test", run_date=run_date, spark=spark)
    cpm_cdc.run(env="test", run_date=run_date, spark=spark)


@pytest.fixture(scope="session")
def pipeline(spark):
    """Run the entire pipeline once per session for regression/read-only checks."""
    run_full_pipeline(spark)
    return spark
