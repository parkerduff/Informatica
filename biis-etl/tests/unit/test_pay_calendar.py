"""Unit tests for jobs/pay_calendar.py (transformations only, no DB)."""
import datetime as dt

import pytest

from jobs import pay_calendar
from utils import notifications


def _pay_period_df(spark):
    rows = []
    start = dt.date(2026, 1, 4)
    for i in range(26):
        s = start + dt.timedelta(days=14 * i)
        e = s + dt.timedelta(days=13)
        flag = "Y" if i == 0 else None
        rows.append((i + 1, 2026, s.isoformat(), e.isoformat(), flag))
    return spark.createDataFrame(
        rows, ["PP_NUM", "PP_END_YEAR", "PP_START_DTE", "PP_END_DTE", "CURR_PP_FLAG"])


def test_reset_sets_all_flags_null(spark):
    df = pay_calendar.reset_flags(_pay_period_df(spark))
    assert df.filter(df.CURR_PP_FLAG == "Y").count() == 0


def test_set_by_date_picks_correct_period(spark):
    df = _pay_period_df(spark)
    found = pay_calendar.find_current_by_date(df, dt.date(2026, 6, 11))
    assert found is not None
    pp_num, year = found
    assert year == 2026
    # 2026-06-11 is in the 12th biweekly period starting 2026-01-04
    assert pp_num == 12


def test_set_current_marks_exactly_one(spark):
    df = pay_calendar.set_current(_pay_period_df(spark), 12, 2026)
    assert pay_calendar.count_current(df) == 1
    only = df.filter(df.CURR_PP_FLAG == "Y").collect()[0]
    assert only["PP_NUM"] == 12


def test_set_by_parameter_overrides_date(spark):
    # an explicit pp_num different from the date-derived one wins
    df = pay_calendar.set_current(_pay_period_df(spark), 5, 2026)
    current = df.filter(df.CURR_PP_FLAG == "Y").collect()
    assert len(current) == 1 and current[0]["PP_NUM"] == 5


def test_verify_exactly_one_current(spark):
    df = pay_calendar.set_current(_pay_period_df(spark), 12, 2026)
    assert pay_calendar.count_current(df) == 1
    none_df = pay_calendar.reset_flags(df)
    assert pay_calendar.count_current(none_df) == 0


def test_no_period_brackets_date_returns_none(spark):
    df = _pay_period_df(spark)
    assert pay_calendar.find_current_by_date(df, dt.date(2030, 1, 1)) is None


def test_notification_contains_period_info():
    notifications.reset()
    subject, body = pay_calendar.build_notification(12, 2026)
    assert "12" in subject and "2026" in subject
    assert "PP_NUM=12" in body and "PP_END_YEAR=2026" in body


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
