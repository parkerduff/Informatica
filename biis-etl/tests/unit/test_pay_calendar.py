import datetime

import pytest

from jobs import pay_calendar
from utils.validation import ValidationError


class FakeCursor:
    def __init__(self, rows=None, rowcount=1):
        self.rows = rows or []
        self.rowcount = rowcount
        self.calls = []

    def execute(self, sql, *params):
        self.calls.append((sql, params))
        return self

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_session_reset():
    cur = FakeCursor(rowcount=3)
    assert pay_calendar.session_reset(FakeConn(cur)) == 3
    assert "SET CURR_PP_FLAG = NULL" in cur.calls[0][0]


def test_session_set():
    cur = FakeCursor(rowcount=1)
    n = pay_calendar.session_set(FakeConn(cur), datetime.date(2026, 6, 11))
    assert n == 1
    assert cur.calls[0][1] == (datetime.date(2026, 6, 11), datetime.date(2026, 6, 11))


def test_session_verify_ok():
    cur = FakeCursor(rows=[(12, 2026, "s", "e", "p")])
    pp = pay_calendar.session_verify(FakeConn(cur))
    assert pp == {"pp_num": 12, "pp_end_year": 2026, "pp_start_dte": "s",
                  "pp_end_dte": "e", "pay_dte": "p"}


def test_session_verify_fails_on_zero_or_many():
    with pytest.raises(ValidationError):
        pay_calendar.session_verify(FakeConn(FakeCursor(rows=[])))
    with pytest.raises(ValidationError):
        pay_calendar.session_verify(FakeConn(FakeCursor(rows=[(1,) * 5, (2,) * 5])))


def test_session_notify(monkeypatch):
    sent = []
    monkeypatch.setattr(pay_calendar, "send_notification",
                        lambda s, b, c: sent.append((s, b)))
    pay_calendar.session_notify(
        {"pp_num": 12, "pp_end_year": 2026, "pp_start_dte": "s",
         "pp_end_dte": "e", "pay_dte": "p"}, {})
    assert "PP 12 / 2026" in sent[0][1]


def test_run(monkeypatch):
    import contextlib

    cur = FakeCursor(rows=[(12, 2026, "s", "e", "p")], rowcount=1)

    @contextlib.contextmanager
    def fake_conn(*a, **k):
        yield FakeConn(cur)

    monkeypatch.setattr(pay_calendar, "pyodbc_connection", fake_conn)
    monkeypatch.setattr(pay_calendar, "send_notification", lambda *a: None)
    pp = pay_calendar.run("test", "2026-06-11")
    assert pp["pp_num"] == 12


def test_run_failure_notifies(monkeypatch):
    import contextlib

    cur = FakeCursor(rows=[], rowcount=0)
    sent = []

    @contextlib.contextmanager
    def fake_conn(*a, **k):
        yield FakeConn(cur)

    monkeypatch.setattr(pay_calendar, "pyodbc_connection", fake_conn)
    monkeypatch.setattr(pay_calendar, "send_notification",
                        lambda s, b, c: sent.append(s))
    with pytest.raises(ValidationError):
        pay_calendar.run("test", "2026-06-11")
    assert any("FAILED" in s for s in sent)


def test_main(monkeypatch):
    monkeypatch.setattr(pay_calendar, "run", lambda env, rd: {"pp_num": 1})
    monkeypatch.setattr("sys.argv", ["pay_calendar.py", "--env", "test", "--run-date", "2026-06-11"])
    assert pay_calendar.main() == 0
