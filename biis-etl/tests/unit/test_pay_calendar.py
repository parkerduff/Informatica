"""Unit tests for the Pay Calendar PySpark job (jobs/pay_calendar.py).

These exercise each migrated session function against the Dockerised SQL Server
seeded by the shared fixtures.
"""
from __future__ import annotations

import pytest

from jobs import pay_calendar
from utils import db

RUN_DATE = "2026-06-11"


def _count_current(config):
    return int(db.execute_scalar(config, "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"))


def test_reset_clears_current_flag(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    assert _count_current(db_config) == 0


def test_reset_with_no_current_flag_does_nothing(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    # Second reset must not error and must keep zero current rows.
    pay_calendar.reset_pay_calendar(spark, db_config)
    assert _count_current(db_config) == 0


def test_set_by_system_date(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    result = pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE)
    assert result["path"] == "PARAMETERS_NOT_EXIST"
    assert result["pp_num"] == 12 and result["pp_end_year"] == 2026


def test_set_by_parameter(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    result = pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE, pp_num_param="5", pp_end_year_param="2026")
    assert result["path"] == "PARAMETERS_EXIST"
    assert result["pp_num"] == 5
    flag = db.execute_scalar(db_config, "SELECT CURR_PP_FLAG FROM PAY_PERIOD WHERE PP_NUM=5 AND PP_END_YEAR=2026")
    assert flag == "Y"


def test_set_by_invalid_parameter_falls_back_to_date(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    result = pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE, pp_num_param="ABC", pp_end_year_param="XYZ")
    assert result["path"] == "PARAMETERS_NOT_EXIST"
    assert result["pp_num"] == 12


def test_set_by_nonexistent_parameter_falls_back_to_date(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    result = pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE, pp_num_param="99", pp_end_year_param="2099")
    assert result["path"] == "PARAMETERS_NOT_EXIST"
    assert result["pp_num"] == 12


def test_verify_exactly_one_current(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE)
    result = pay_calendar.verify_pay_calendar(spark, db_config)
    assert result["current_count"] == 1 and result["status"] == "pass"


def test_verify_zero_current_raises(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    with pytest.raises(ValueError, match="no pay period set"):
        pay_calendar.verify_pay_calendar(spark, db_config)


def test_verify_multiple_current_raises(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    db.execute_sql(db_config, "UPDATE PAY_PERIOD SET CURR_PP_FLAG='Y' WHERE PP_NUM IN (11,12)")
    with pytest.raises(ValueError, match="more than one"):
        pay_calendar.verify_pay_calendar(spark, db_config)


def test_build_message_subject_format(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE)
    msg = pay_calendar.build_message(spark, db_config)
    assert "Pay Calendar Process Completed Successfully for: 2026-12" in msg["subject"]


def test_build_message_body_contains_dates(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    pay_calendar.set_pay_calendar(spark, db_config, RUN_DATE)
    msg = pay_calendar.build_message(spark, db_config)
    assert "Current Pay Period = 12" in msg["message"]
    assert "05/31/2026" in msg["message"]
    assert "06/13/2026" in msg["message"]


def test_pp_num_single_digit_padded():
    assert pay_calendar._pad_pp_num(3) == "03"


def test_pp_num_double_digit_not_padded():
    assert pay_calendar._pad_pp_num(12) == "12"


def test_is_number_helper():
    assert pay_calendar._is_number("2026") is True
    assert pay_calendar._is_number("ABC") is False
    assert pay_calendar._is_number(None) is False


def test_full_workflow_runs_without_error(spark, clean_tables, db_config):
    result = pay_calendar.run(spark, db_config, RUN_DATE)
    assert result["status"] == "SUCCESS"
    assert result["pp_num"] == 12
    assert result["pp_end_year"] == 2026
    assert _count_current(db_config) == 1


def test_coerce_date_variants():
    import datetime as _dt

    assert pay_calendar._coerce_date("2026-06-11").date() == _dt.date(2026, 6, 11)
    assert pay_calendar._coerce_date(_dt.date(2026, 6, 11)).date() == _dt.date(2026, 6, 11)
    assert pay_calendar._coerce_date(_dt.datetime(2026, 6, 11, 9)).date() == _dt.date(2026, 6, 11)


def test_set_pay_calendar_no_match_raises(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    with pytest.raises(ValueError, match="No pay period contains"):
        pay_calendar.set_pay_calendar(spark, db_config, "2099-01-01")


def test_build_message_no_current_raises(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    with pytest.raises(ValueError, match="No current pay period"):
        pay_calendar.build_message(spark, db_config)


def test_schema_matches_target_definition(spark, clean_tables, db_config):
    df = db.spark_df_from_query(
        spark, db_config,
        "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, CURR_PP_FLAG FROM PAY_PERIOD",
    )
    for col in ["PP_NUM", "PP_END_YEAR", "PP_START_DTE", "PP_END_DTE", "CURR_PP_FLAG"]:
        assert col in df.columns
