"""End-to-end functional tests for the COMPTIME job."""
import datetime as dt
import os

import pytest

from jobs import comptime, pay_calendar
from utils import db

RD = dt.date(2026, 6, 11)


def test_loads_all_rows_with_pay_period(seeded_db, spark, golden_dir):
    pay_calendar.run(env="test", run_date=RD, spark=spark)
    comptime.run(env="test", file_path=os.path.join(golden_dir, "comptime_input.csv"),
                 run_date="2026-06-11", spark=spark)

    assert db.count(seeded_db, "COMP_TIME_DAILY_TBL") == 100
    distinct_pp = db.fetch_all(
        seeded_db, "SELECT DISTINCT PP_END_YEAR, PP_NUM FROM COMP_TIME_DAILY_TBL")
    assert len(distinct_pp) == 1 and distinct_pp[0] == (2026, 12)


def test_counter_row_matches_load(seeded_db, spark, golden_dir):
    pay_calendar.run(env="test", run_date=RD, spark=spark)
    comptime.run(env="test", file_path=os.path.join(golden_dir, "comptime_input.csv"),
                 run_date="2026-06-11", spark=spark)
    counter = db.fetch_all(seeded_db, "SELECT COUNTER_VALUE FROM COUNTER_TBL")
    assert len(counter) == 1 and counter[0][0] == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
