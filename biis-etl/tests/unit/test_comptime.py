"""Unit tests for the COMPTIME PySpark job (jobs/comptime.py)."""
from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import functions as F

from jobs import comptime
from utils import db

PAY_PERIOD = {"pp_num": 12, "pp_end_year": 2026, "pp_year_num": 202612}


def _detail_rows(spark, csv_path):
    source = comptime.read_source_csv(spark, csv_path)
    return comptime.transform_comp_time_daily(spark, source, PAY_PERIOD).collect()


def test_get_current_pay_period_returns_correct_values(spark, clean_tables, db_config):
    pp = comptime.get_current_pay_period(spark, db_config)
    assert pp == {"pp_num": 12, "pp_end_year": 2026, "pp_year_num": 202612}


def test_get_current_pay_period_no_current_raises(spark, clean_tables, db_config):
    db.execute_sql(db_config, "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL")
    with pytest.raises(ValueError, match="No current pay period"):
        comptime.get_current_pay_period(spark, db_config)


def test_csv_parsing_correct_field_count(spark, comptime_csv_path):
    df = comptime.read_source_csv(spark, comptime_csv_path)
    assert len(df.columns) == 12


def test_header_row_filtered_out(spark, comptime_csv_path):
    ssns = {r["SSN"] for r in _detail_rows(spark, comptime_csv_path)}
    assert "HEADER" not in ssns


def test_trailer_row_filtered_out(spark, comptime_csv_path):
    ssns = {r["SSN"] for r in _detail_rows(spark, comptime_csv_path)}
    assert "TRAILER" not in ssns


def test_valid_records_count_is_10(spark, comptime_csv_path):
    source = comptime.read_source_csv(spark, comptime_csv_path)
    assert comptime.count_detail_records(spark, source) == 10


def test_date_conversion_yyyymmdd(spark, comptime_csv_path):
    rows = {r["SSN"]: r for r in _detail_rows(spark, comptime_csv_path)}
    row = rows["999000001"]
    assert row["PP_END_DATE"].date() == dt.date(2026, 6, 13)
    assert row["DAILY_DATE_EARNED"].date() == dt.date(2026, 6, 10)


def test_invalid_date_returns_null(spark):
    df = spark.createDataFrame([("99999999",)], ["d"])
    out = df.withColumn("conv", comptime._to_date_yyyymmdd(F.col("d"))).collect()
    assert out[0]["conv"] is None


def test_pp_year_num_derivation(spark, clean_tables, db_config):
    pp = comptime.get_current_pay_period(spark, db_config)
    assert pp["pp_year_num"] == 202612


def test_pp_year_num_with_single_digit_pp(spark, clean_tables, db_config):
    db.execute_sql(db_config, "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL")
    db.execute_sql(db_config, "UPDATE PAY_PERIOD SET CURR_PP_FLAG='Y' WHERE PP_NUM=3 AND PP_END_YEAR=2026")
    pp = comptime.get_current_pay_period(spark, db_config)
    assert pp["pp_year_num"] == 202603


def test_counter_value_equals_detail_count(spark, clean_tables, db_config, comptime_csv_path):
    result = comptime.build_message_counters(spark, db_config, comptime_csv_path, PAY_PERIOD)
    value = db.execute_scalar(db_config, "SELECT COUNTER_VALUE FROM COUNTER_TBL")
    assert int(value) == result["detail_count"] == 10


def test_counter_description_exact_text(spark, clean_tables, db_config, comptime_csv_path):
    comptime.build_message_counters(spark, db_config, comptime_csv_path, PAY_PERIOD)
    desc = db.execute_scalar(db_config, "SELECT COUNTER_DESCRIPTION FROM COUNTER_TBL")
    assert desc == "Number of detail records from the COMP TIME file."


def test_process_name_set_to_mapping_name(spark, clean_tables, db_config, comptime_csv_path):
    comptime.build_message_counters(spark, db_config, comptime_csv_path, PAY_PERIOD)
    name = db.execute_scalar(db_config, "SELECT PROCESS_NAME FROM COUNTER_TBL")
    assert name == comptime.MAPPING_NAME


def test_run_date_is_session_start_time(spark, clean_tables, db_config, comptime_csv_path):
    comptime.build_message_counters(spark, db_config, comptime_csv_path, PAY_PERIOD)
    run_date = db.execute_scalar(db_config, "SELECT RUN_DATE FROM COUNTER_TBL")
    assert run_date is not None


def test_message_subject_contains_pay_period(spark, clean_tables, db_config, comptime_csv_path):
    result = comptime.build_message_counters(spark, db_config, comptime_csv_path, PAY_PERIOD)
    assert "2026-12" in result["subject"]


def test_all_12_source_fields_preserved_in_target(spark, comptime_csv_path):
    rows = _detail_rows(spark, comptime_csv_path)
    row = {r["SSN"]: r for r in rows}["999000001"]
    for col in ["SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
                "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
                "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF"]:
        assert col in row.asDict()


def test_empty_file_raises_error(spark, clean_tables, db_config, tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    with pytest.raises(ValueError, match="Empty source file"):
        comptime.load_comp_time_daily(spark, db_config, str(empty), PAY_PERIOD)


def test_comp_time_cur_bal_precision(spark, comptime_csv_path):
    rows = {r["SSN"]: r for r in _detail_rows(spark, comptime_csv_path)}
    assert float(rows["999000001"]["COMP_TIME_CUR_BAL"]) == 120.50


def test_load_comp_time_daily_inserts_rows(spark, clean_tables, db_config, comptime_csv_path):
    loaded = comptime.load_comp_time_daily(spark, db_config, comptime_csv_path, PAY_PERIOD)
    assert loaded == 10
    count = int(db.execute_scalar(db_config, "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL"))
    assert count == 10


def test_run_full_workflow(spark, clean_tables, db_config, comptime_csv_path):
    result = comptime.run(spark, db_config, comptime_csv_path)
    assert result["status"] == "SUCCESS"
    assert result["records_loaded"] == 10
    assert result["detail_count"] == 10
    assert result["pp_year_num"] == 202612
