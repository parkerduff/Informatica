"""
Unit tests for Job 1: Pay Calendar.

Tests filter logic, parameter validation, session sequencing,
email building, and error handling using local Spark mode.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType

from pyspark_migration.jobs.job1_pay_calendar import PayCalendarJob


class TestPayCalendarParameterValidation:
    """Tests for parameter validation (IS_NUMBER replacement)."""

    def test_valid_integer(self):
        assert PayCalendarJob._safe_parse_int("2025") == 2025

    def test_valid_zero(self):
        assert PayCalendarJob._safe_parse_int("0") is None

    def test_negative(self):
        assert PayCalendarJob._safe_parse_int("-1") is None

    def test_non_numeric(self):
        assert PayCalendarJob._safe_parse_int("abc") is None

    def test_none(self):
        assert PayCalendarJob._safe_parse_int(None) is None

    def test_empty_string(self):
        assert PayCalendarJob._safe_parse_int("") is None

    def test_float_string(self):
        assert PayCalendarJob._safe_parse_int("3.14") is None


class TestPayCalendarSessionSequencing:
    """Tests for session execution order and failure handling."""

    def test_successful_run(self, spark, test_config, mock_db_manager, mock_email_service):
        """Verify all 4 sessions execute in order on success."""
        from datetime import date
        # Mock PAY_PERIOD lookups - full row for reset and build_message sessions
        pp_df = spark.createDataFrame(
            [(13, 2025, date(2025, 6, 15), date(2025, 6, 28), 13, 2025, date(2025, 7, 3), "Y")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", DateType()),
                StructField("PP_END_DTE", DateType()),
                StructField("LV_NUM", IntegerType()),
                StructField("LV_YEAR", IntegerType()),
                StructField("PAY_DTE", DateType()),
                StructField("CURR_PP_FLAG", StringType()),
            ]),
        )
        count_df = spark.createDataFrame(
            [(1,)],
            schema=StructType([
                StructField("COUNT_CURRENT", LongType()),
            ]),
        )

        # Non-param path: reset(1 read) + set(0 reads, uses execute_sql) + verify(1 read) + build(1 read)
        mock_db_manager.read_jdbc.side_effect = [
            pp_df,    # Session 1: reset
            count_df, # Session 3: verify (COUNT_CURRENT=1)
            pp_df,    # Session 4: build message
        ]

        job = PayCalendarJob(spark, test_config, mock_db_manager, mock_email_service)
        metrics = job.run()

        assert metrics.status == "SUCCEEDED"
        assert len(metrics.sessions) == 4
        mock_email_service.send_job_success.assert_called_once()

    def test_verify_fails_aborts(self, spark, test_config, mock_db_manager, mock_email_service):
        """Verify ABORT when CURR_PP_FLAG='Y' count != 1."""
        pp_df = spark.createDataFrame(
            [(13, 2025, None, None, 13, 2025, None, "Y")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", DateType()),
                StructField("PP_END_DTE", DateType()),
                StructField("LV_NUM", IntegerType()),
                StructField("LV_YEAR", IntegerType()),
                StructField("PAY_DTE", DateType()),
                StructField("CURR_PP_FLAG", StringType()),
            ]),
        )
        # COUNT = 0 -> should ABORT
        count_df = spark.createDataFrame(
            [(0,)],
            schema=StructType([
                StructField("COUNT_CURRENT", LongType()),
            ]),
        )

        # Non-param path: reset(1 read) + set(0 reads) + verify(1 read) -> ABORT before build
        mock_db_manager.read_jdbc.side_effect = [pp_df, count_df]

        job = PayCalendarJob(spark, test_config, mock_db_manager, mock_email_service)

        with pytest.raises(RuntimeError, match="ABORT"):
            job.run()

        mock_email_service.send_job_failure.assert_called_once()

    def test_with_explicit_params(self, spark, test_config, mock_db_manager, mock_email_service):
        """Test with explicit PP_END_YEAR and PP_NUM parameters."""
        from datetime import date
        pp_df = spark.createDataFrame(
            [(12, 2025, date(2025, 6, 1), date(2025, 6, 14), 12, 2025, date(2025, 6, 20), "Y")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", DateType()),
                StructField("PP_END_DTE", DateType()),
                StructField("LV_NUM", IntegerType()),
                StructField("LV_YEAR", IntegerType()),
                StructField("PAY_DTE", DateType()),
                StructField("CURR_PP_FLAG", StringType()),
            ]),
        )
        count_df = spark.createDataFrame(
            [(1,)],
            schema=StructType([
                StructField("COUNT_CURRENT", LongType()),
            ]),
        )

        # Param path: reset(1 read) + set(1 read: check_df) + verify(1 read) + build(1 read)
        mock_db_manager.read_jdbc.side_effect = [
            pp_df,    # Session 1: reset
            pp_df,    # Session 2: set (check_df for param branch)
            count_df, # Session 3: verify
            pp_df,    # Session 4: build message
        ]

        job = PayCalendarJob(spark, test_config, mock_db_manager, mock_email_service)
        metrics = job.run(pp_end_year="2025", pp_num="12")

        assert metrics.status == "SUCCEEDED"
        assert job.pp_end_year == 2025
        assert job.pp_num == 12


class TestPayCalendarEmailBuilding:
    """Tests for email subject/message construction."""

    def test_email_prefix(self, spark, test_config, mock_db_manager, mock_email_service):
        """Verify email includes environment prefix."""
        from datetime import date
        pp_df = spark.createDataFrame(
            [(13, 2025, date(2025, 6, 15), date(2025, 6, 28), 13, 2025, date(2025, 7, 3), "Y")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", DateType()),
                StructField("PP_END_DTE", DateType()),
                StructField("LV_NUM", IntegerType()),
                StructField("LV_YEAR", IntegerType()),
                StructField("PAY_DTE", DateType()),
                StructField("CURR_PP_FLAG", StringType()),
            ]),
        )
        count_df = spark.createDataFrame(
            [(1,)],
            schema=StructType([
                StructField("COUNT_CURRENT", LongType()),
            ]),
        )

        # Non-param path: reset(1 read) + set(0 reads) + verify(1 read) + build(1 read)
        mock_db_manager.read_jdbc.side_effect = [
            pp_df,    # Session 1: reset
            count_df, # Session 3: verify
            pp_df,    # Session 4: build message
        ]

        job = PayCalendarJob(spark, test_config, mock_db_manager, mock_email_service)
        job.run()

        # conftest sets environment="Test" -> prefix is "Test: "
        assert "Test: " in job.wf_subject
        assert "2025" in job.wf_subject
