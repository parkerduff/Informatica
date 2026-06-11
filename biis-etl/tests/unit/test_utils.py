"""Unit tests for utils (config, db, validation)."""
from __future__ import annotations

import pytest

from jobs import pay_calendar
from utils import config as cfg
from utils import db, validation


# --------------------------- config -----------------------------------------
def test_load_config_returns_database_block(db_config):
    assert db_config["database"]["name"] == "biis_test"


def test_load_config_missing_raises():
    with pytest.raises(FileNotFoundError):
        cfg.load_config("does_not_exist")


def test_jdbc_url_and_properties(db_config):
    url = cfg.get_jdbc_url(db_config)
    assert url.startswith("jdbc:sqlserver://")
    assert "databaseName=biis_test" in url
    props = cfg.get_jdbc_properties(db_config)
    assert props["user"] == "sa"
    assert "SQLServerDriver" in props["driver"]


def test_pyodbc_and_master_connection_strings(db_config):
    assert "DATABASE=biis_test" in cfg.get_pyodbc_connection_string(db_config)
    assert "DATABASE=master" in cfg.get_master_connection_string(db_config)


# --------------------------- db ---------------------------------------------
def test_spark_df_from_query_empty_returns_empty_df(spark, clean_tables, db_config):
    df = db.spark_df_from_query(
        spark, db_config, "SELECT PP_NUM FROM PAY_PERIOD WHERE PP_NUM = -1"
    )
    assert df.count() == 0


def test_fetch_all_returns_columns_and_rows(clean_tables, db_config):
    cols, rows = db.fetch_all(db_config, "SELECT PP_NUM FROM PAY_PERIOD WHERE PP_NUM=12")
    assert cols == ["PP_NUM"]
    assert rows[0][0] == 12


def test_execute_scalar(clean_tables, db_config):
    assert int(db.execute_scalar(db_config, "SELECT COUNT(*) FROM PAY_PERIOD")) == 26


def test_jdbc_helpers_via_db_module(db_config):
    assert db.get_jdbc_url(db_config).startswith("jdbc:sqlserver://")
    assert db.get_jdbc_properties(db_config)["user"] == "sa"


# --------------------------- validation -------------------------------------
def test_validate_row_count_ok(spark, clean_tables, db_config):
    df = db.spark_df_from_query(spark, db_config, "SELECT PP_NUM FROM PAY_PERIOD")
    validation.validate_row_count(df, 26, "PAY_PERIOD")


def test_validate_row_count_mismatch_raises(spark, clean_tables, db_config):
    df = db.spark_df_from_query(spark, db_config, "SELECT PP_NUM FROM PAY_PERIOD")
    with pytest.raises(AssertionError):
        validation.validate_row_count(df, 1, "PAY_PERIOD")


def test_validate_schema_ok_and_missing(spark, clean_tables, db_config):
    df = db.spark_df_from_query(spark, db_config, "SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD")
    validation.validate_schema(df, {"PP_NUM": "", "PP_END_YEAR": ""})
    with pytest.raises(AssertionError):
        validation.validate_schema(df, {"NOPE": ""})


def test_validate_schema_type_mismatch_raises(spark, clean_tables, db_config):
    df = db.spark_df_from_query(spark, db_config, "SELECT PP_NUM FROM PAY_PERIOD")
    with pytest.raises(AssertionError):
        validation.validate_schema(df, {"PP_NUM": "string"})


def test_validate_no_nulls(spark, clean_tables, db_config):
    df = db.spark_df_from_query(spark, db_config, "SELECT PP_NUM FROM PAY_PERIOD")
    validation.validate_no_nulls(df, ["PP_NUM"])


def test_validate_no_nulls_raises(spark, clean_tables, db_config):
    df = db.spark_df_from_query(
        spark, db_config, "SELECT CURR_PP_FLAG FROM PAY_PERIOD")
    with pytest.raises(AssertionError):
        validation.validate_no_nulls(df, ["CURR_PP_FLAG"])


def test_validate_single_current_pp(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    pay_calendar.set_pay_calendar(spark, db_config, "2026-06-11")
    assert validation.validate_single_current_pp(spark, db_config) == 1


def test_validate_single_current_pp_raises(spark, clean_tables, db_config):
    pay_calendar.reset_pay_calendar(spark, db_config)
    with pytest.raises(AssertionError):
        validation.validate_single_current_pp(spark, db_config)


def test_compare_dataframes(spark):
    a = spark.createDataFrame([(1,), (2,)], ["k"])
    b = spark.createDataFrame([(1,), (2,)], ["k"])
    result = validation.compare_dataframes(a, b, ["k"])
    assert result["matches"] is True
    c = spark.createDataFrame([(1,), (3,)], ["k"])
    result2 = validation.compare_dataframes(a, c, ["k"])
    assert result2["matches"] is False
