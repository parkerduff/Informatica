import sys
import types

import pytest

from utils import db


@pytest.fixture()
def cfg():
    return {"database": {"host": "h", "port": 1433, "name": "biis_test",
                         "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                         "schema": "dbo"}}


def test_get_jdbc_url(cfg):
    url = db.get_jdbc_url(cfg)
    assert url.startswith("jdbc:sqlserver://h:1433;databaseName=biis_test")
    assert "trustServerCertificate=true" in url


def test_get_jdbc_properties(cfg):
    props = db.get_jdbc_properties(cfg, {"username": "u", "password": "p"})
    assert props == {"user": "u", "password": "p",
                     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


def test_odbc_connstr(cfg):
    s = db._odbc_connstr({"username": "u", "password": "p"}, cfg)
    assert "ODBC Driver 18 for SQL Server" in s
    assert "TrustServerCertificate=yes" in s
    assert "DATABASE=biis_test" in s
    s2 = db._odbc_connstr({"username": "u", "password": "p"}, cfg, database="master")
    assert "DATABASE=master" in s2


def test_pyodbc_connection(monkeypatch, cfg):
    closed = []

    class FakeConn:
        def close(self):
            closed.append(True)

    captured = {}

    def fake_connect(connstr, autocommit):
        captured["connstr"] = connstr
        captured["autocommit"] = autocommit
        return FakeConn()

    monkeypatch.setitem(sys.modules, "pyodbc", types.SimpleNamespace(connect=fake_connect))
    with db.pyodbc_connection({"username": "u", "password": "p"}, cfg, autocommit=False) as conn:
        assert isinstance(conn, FakeConn)
    assert captured["autocommit"] is False
    assert closed == [True]


def test_execute_sql():
    class FakeCursor:
        def __init__(self):
            self.calls = []

        def execute(self, sql, *params):
            self.calls.append((sql, params))

    class FakeConn:
        def __init__(self):
            self.c = FakeCursor()

        def cursor(self):
            return self.c

    conn = FakeConn()
    db.execute_sql(conn, "SELECT 1")
    db.execute_sql(conn, "SELECT ?", (1,))
    assert len(conn.c.calls) == 2


def test_execute_sql_failure():
    class BadCursor:
        def execute(self, *a):
            raise RuntimeError("boom")

    class FakeConn:
        def cursor(self):
            return BadCursor()

    with pytest.raises(RuntimeError):
        db.execute_sql(FakeConn(), "SELECT 1")


def test_get_current_pay_period(monkeypatch, spark, cfg):
    df = spark.createDataFrame(
        [(12, 2026, "Y"), (13, 2026, None)],
        "PP_NUM int, PP_END_YEAR int, CURR_PP_FLAG string",
    )
    monkeypatch.setattr(db, "read_table", lambda *a, **k: df)
    pp = db.get_current_pay_period(spark, cfg, {})
    assert pp["pp_num"] == 12 and pp["pp_end_year"] == 2026


def test_get_current_pay_period_invalid(monkeypatch, spark, cfg):
    df = spark.createDataFrame(
        [(12, 2026, None)], "PP_NUM int, PP_END_YEAR int, CURR_PP_FLAG string")
    monkeypatch.setattr(db, "read_table", lambda *a, **k: df)
    with pytest.raises(ValueError):
        db.get_current_pay_period(spark, cfg, {})
