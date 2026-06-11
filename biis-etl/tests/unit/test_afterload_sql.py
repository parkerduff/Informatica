"""Unit tests for the EHRP2BIIS afterload Oracle->SQL Server conversions."""
import datetime as dt

import pytest

from jobs.ehrp2biis import afterload

pytestmark = pytest.mark.unit


def test_process_table_uses_top_not_rownum():
    assert "TOP 1" in afterload.SQL_PROCESS_TABLE
    assert "ROWNUM" not in afterload.SQL_PROCESS_TABLE
    assert "ORDER BY" in afterload.SQL_PROCESS_TABLE


def test_reset_retained_is_run_date_parameterised():
    rendered = afterload.SQL_RESET_RETAINED.format(run_date="2026-06-11")
    assert "2026-06-11" in rendered
    assert "SYSDATE" not in rendered


def test_promotions_target_all_tables():
    targets = [p[0] for p in afterload.PROMOTIONS]
    assert targets == ["ACTION_PRIMARY_ALL", "ACTION_SECONDARY_ALL", "ACTION_REMARKS_ALL"]


class FakeCursor:
    def __init__(self):
        self.rowcount = 0

    def execute(self, sql, params=None):
        return self

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.statements = []

    def cursor(self):
        return FakeCursor()

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_run_commits_in_single_transaction(monkeypatch):
    conn = FakeConn()

    import contextlib

    @contextlib.contextmanager
    def fake_conn(secret, config, autocommit=True):
        assert autocommit is False  # delete/insert must be transactional
        yield conn

    monkeypatch.setattr(afterload, "pyodbc_connection", fake_conn)
    monkeypatch.setattr(afterload, "get_db_secret", lambda c: None)
    monkeypatch.setattr(afterload, "send_notification", lambda *a: None)

    rc = afterload.run(config=None, run_date=dt.date(2026, 6, 11))
    assert rc == 0
    assert conn.committed and not conn.rolled_back
