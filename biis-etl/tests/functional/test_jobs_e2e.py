"""End-to-end functional tests against the Docker SQL Server.

The session-scoped ``pipeline`` fixture seeds golden inputs and runs every job
in dependency order; each test asserts on the resulting target tables.
"""
import pytest

pytestmark = pytest.mark.functional


def _count(conn, table, where=""):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM dbo.{table} {where}")
    return cur.fetchone()[0]


def test_pay_calendar_exactly_one_current(pipeline, db_connection):
    assert _count(db_connection, "PAY_PERIOD", "WHERE CURR_PP_FLAG = 'Y'") == 1


def test_comptime_loaded_all_rows(pipeline, db_connection):
    assert _count(db_connection, "COMP_TIME_DAILY_TBL") == 100
    assert _count(db_connection, "COUNTER_TBL", "WHERE PROCESS_NAME = 'COMPTIME'") >= 1


def test_pseudossn_targets(pipeline, db_connection):
    assert _count(db_connection, "PSEUDOSSN_FROM_SDA_TBL") == 50
    assert _count(db_connection, "PSEUDOSSN_TBL") == 45


def test_fda_leave_errors(pipeline, db_connection):
    assert _count(db_connection, "ERROR_TBL") == 5


def test_ehrp2biis_promoted(pipeline, db_connection):
    assert _count(db_connection, "ACTION_PRIMARY_ALL") == 10
    assert _count(db_connection, "ACTION_SECONDARY_ALL") == 10
    assert _count(db_connection, "ACTION_REMARKS_ALL") == 10


def test_cpm_staging(pipeline, db_connection):
    for code in ("NIH", "OIG", "CDC"):
        assert _count(db_connection, f"CPM_{code}_STAGING_TBL") == 5
