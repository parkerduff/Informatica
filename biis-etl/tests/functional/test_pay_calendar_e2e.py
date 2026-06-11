"""End-to-end functional tests for the Pay Calendar job against the test DB."""
import datetime as dt

import pytest

from jobs import pay_calendar
from utils import db

RD = dt.date(2026, 6, 11)


def _current_count(cfg):
    return db.fetch_all(cfg, "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG='Y'")[0][0]


def test_sets_exactly_one_current(seeded_db, spark):
    pay_calendar.run(env="test", run_date=RD, spark=spark)
    assert _current_count(seeded_db) == 1
    row = db.fetch_all(seeded_db, "SELECT PP_NUM FROM PAY_PERIOD WHERE CURR_PP_FLAG='Y'")
    assert row[0][0] == 12


def test_idempotent_second_run(seeded_db, spark):
    pay_calendar.run(env="test", run_date=RD, spark=spark)
    pay_calendar.run(env="test", run_date=RD, spark=spark)
    assert _current_count(seeded_db) == 1


def test_corrects_multiple_current_flags(seeded_db, spark):
    db.execute(seeded_db, "UPDATE PAY_PERIOD SET CURR_PP_FLAG='Y' WHERE PP_NUM IN (1,2)")
    assert _current_count(seeded_db) == 2
    pay_calendar.run(env="test", run_date=RD, spark=spark)
    assert _current_count(seeded_db) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
