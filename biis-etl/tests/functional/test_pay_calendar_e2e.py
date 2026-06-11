"""End-to-end functional test for the Pay Calendar job against SQL Server."""
from __future__ import annotations

import json
import os

from jobs import pay_calendar
from utils import db

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXPECTED = os.path.join(ROOT, "tests", "fixtures", "pay_calendar_expected.json")


def _load_expected():
    with open(EXPECTED, "r", encoding="utf-8") as fh:
        return json.load(fh)


def test_pay_calendar_end_to_end(spark, clean_tables, db_config):
    expected = _load_expected()

    result = pay_calendar.run(spark, db_config, run_date="2026-06-11")

    # Exactly one current pay period, and it is PP 12 / 2026.
    columns, rows = db.fetch_all(
        db_config, "SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
    )
    assert len(rows) == 1
    assert rows[0] == (12, 2026)

    assert result["pp_num"] == expected["after_set"]["pp_num"]
    assert result["pp_end_year"] == expected["after_set"]["pp_end_year"]
    assert expected["message"]["subject_contains"] in result["message_subject"]
    assert expected["message"]["message_contains"] in result["message_body"]

    # Capture the full PAY_PERIOD table for the HTML report.
    total = db.execute_scalar(db_config, "SELECT COUNT(*) FROM PAY_PERIOD")
    assert int(total) == 26


def test_pay_calendar_reset_then_verify(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    current = db.execute_scalar(
        db_config, "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
    )
    assert int(current) == 0
