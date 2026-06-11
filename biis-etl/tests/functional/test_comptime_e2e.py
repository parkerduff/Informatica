"""End-to-end functional test for the COMPTIME job against SQL Server."""
from __future__ import annotations

import datetime as dt
import json
import os

from jobs import comptime, pay_calendar
from utils import db

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXPECTED = os.path.join(ROOT, "tests", "fixtures", "comptime_expected.json")
CSV = os.path.join(ROOT, "tests", "fixtures", "comptime_input.csv")


def _load_expected():
    with open(EXPECTED, "r", encoding="utf-8") as fh:
        return json.load(fh)


def test_comptime_end_to_end(spark, clean_tables, db_config):
    expected = _load_expected()
    # Ensure a current pay period exists (PP 12 / 2026) for the lookup.
    pay_calendar.set_pay_calendar(spark, db_config, "2026-06-11")

    result = comptime.run(spark, db_config, file_path=CSV)

    daily_count = int(db.execute_scalar(db_config, "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL"))
    counter_count = int(db.execute_scalar(db_config, "SELECT COUNT(*) FROM COUNTER_TBL"))
    counter_value = int(db.execute_scalar(db_config, "SELECT COUNTER_VALUE FROM COUNTER_TBL"))

    assert daily_count == expected["comp_time_daily_tbl_rows"]
    assert counter_count == expected["counter_tbl_rows"]
    assert counter_value == expected["counter_value"]
    assert result["detail_count"] == expected["detail_record_count"]

    # Verify a specific row's derived values + date conversions.
    columns, rows = db.fetch_all(
        db_config,
        "SELECT PP_END_YEAR, PP_NUM, PP_YEAR_NUM, COMP_TIME_CUR_BAL, PP_END_DATE, "
        "DAILY_DATE_EARNED FROM COMP_TIME_DAILY_TBL WHERE SSN = '999000001'",
    )
    assert len(rows) == 1
    pp_end_year, pp_num, pp_year_num, cur_bal, pp_end_date, daily_date = rows[0]
    sample = expected["sample_row_ssn_999000001"]
    assert int(pp_end_year) == sample["PP_END_YEAR"]
    assert int(pp_num) == sample["PP_NUM"]
    assert int(pp_year_num) == sample["PP_YEAR_NUM"]
    assert float(cur_bal) == sample["COMP_TIME_CUR_BAL"]
    assert pp_end_date.date() == dt.date(2026, 6, 13)
    assert daily_date.date() == dt.date(2026, 6, 10)


def test_comptime_header_trailer_filtered(spark, clean_tables, db_config):
    pay_calendar.set_pay_calendar(spark, db_config, "2026-06-11")
    comptime.run(spark, db_config, file_path=CSV)
    bad = int(db.execute_scalar(
        db_config,
        "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL WHERE SSN IN ('HEADER','TRAILER')",
    ))
    assert bad == 0
