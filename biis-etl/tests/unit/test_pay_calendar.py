"""Unit tests for the pay_calendar sessions using a fake DB connection."""
import datetime as dt

import pytest

from jobs import pay_calendar
from utils.validation import ValidationError

pytestmark = pytest.mark.unit


class FakeCursor:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_session_reset_clears_flag():
    cur = FakeCursor(rowcount=3)
    assert pay_calendar.session_reset(FakeConn(cur)) == 3
    assert "CURR_PP_FLAG = NULL" in cur.executed[0][0]


def test_session_set_uses_run_date_param():
    cur = FakeCursor(rowcount=1)
    rd = dt.date(2026, 6, 11)
    assert pay_calendar.session_set(FakeConn(cur), rd) == 1
    sql, params = cur.executed[0]
    assert "CURR_PP_FLAG = 'Y'" in sql
    assert params == [rd]


def test_session_verify_passes_with_one_row():
    cur = FakeCursor(rows=[(12, 2026, "2026-06-01", "2026-06-14", "2026-06-19")])
    pp = pay_calendar.session_verify(FakeConn(cur))
    assert pp["pp_num"] == 12 and pp["pp_end_year"] == 2026


@pytest.mark.parametrize("rows", [[], [(1, 2026, "", "", ""), (2, 2026, "", "", "")]])
def test_session_verify_fails_when_not_exactly_one(rows):
    cur = FakeCursor(rows=rows)
    with pytest.raises(ValidationError):
        pay_calendar.session_verify(FakeConn(cur))


def test_session_notify_sends(monkeypatch):
    captured = {}

    def fake_send(subject, body, config):
        captured["subject"], captured["body"] = subject, body

    monkeypatch.setattr(pay_calendar, "send_notification", fake_send)
    pp = {
        "pp_num": 12, "pp_end_year": 2026,
        "pp_start_dte": "2026-06-01", "pp_end_dte": "2026-06-14", "pay_dte": "2026-06-19",
    }
    pay_calendar.session_notify(pp, config=None)
    assert "PP 12 of 2026" in captured["body"]
