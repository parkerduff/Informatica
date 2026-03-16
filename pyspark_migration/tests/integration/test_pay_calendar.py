"""
Integration Tests for Pay Calendar Job

Tests the full Reset-Set-Verify-Message-Email workflow.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestPayCalendarReset:
    """Tests for pay calendar reset step."""

    @patch("pyspark_migration.jobs.pay_calendar.pay_calendar_job.execute_sql")
    def test_reset_clears_all_flags(self, mock_execute):
        """Set 2 rows to 'Y', run reset, verify UPDATE is called."""
        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import step_reset

        mock_execute.return_value = 2
        step_reset()

        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert "UPDATE" in call_args[0][1]
        assert "CURR_PP_FLAG" in call_args[0][1]
        assert "NULL" in call_args[0][1]


class TestPayCalendarSet:
    """Tests for pay calendar set step."""

    @patch("pyspark_migration.jobs.pay_calendar.pay_calendar_job.execute_sql")
    def test_set_with_parameters(self, mock_execute):
        """Set PP 5/2024, verify parameterized UPDATE."""
        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import step_set

        mock_execute.return_value = 1
        step_set(pp_num=5, pp_end_year=2024)

        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        sql = call_args[0][1]
        assert "PP_NUM" in sql
        assert "PP_END_YEAR" in sql

    @patch("pyspark_migration.jobs.pay_calendar.pay_calendar_job.execute_sql")
    def test_set_with_date_range(self, mock_execute):
        """Run with SYSDATE, verify date-based UPDATE."""
        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import step_set

        mock_execute.return_value = 1
        step_set()

        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        sql = call_args[0][1]
        assert "SYSDATE" in sql


class TestPayCalendarVerify:
    """Tests for pay calendar verify step."""

    @patch("pyspark_migration.jobs.pay_calendar.pay_calendar_job.read_oracle_query")
    def test_verify_passes_with_one(self, mock_read):
        """Expect success when exactly 1 current pay period."""
        from pyspark.sql import SparkSession

        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import step_verify

        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            mock_df = spark.createDataFrame([(1,)], ["CNT"])
            mock_read.return_value = mock_df

            result = step_verify(spark)
            assert result is True
        finally:
            spark.stop()

    @patch("pyspark_migration.jobs.pay_calendar.pay_calendar_job.read_oracle_query")
    def test_verify_aborts_with_zero(self, mock_read):
        """Expect failure when 0 current pay periods."""
        from pyspark.sql import SparkSession

        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import step_verify

        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            mock_df = spark.createDataFrame([(0,)], ["CNT"])
            mock_read.return_value = mock_df

            with pytest.raises(SystemExit):
                step_verify(spark)
        finally:
            spark.stop()

    @patch("pyspark_migration.jobs.pay_calendar.pay_calendar_job.read_oracle_query")
    def test_verify_aborts_with_multiple(self, mock_read):
        """Expect failure when multiple current pay periods."""
        from pyspark.sql import SparkSession

        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import step_verify

        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            mock_df = spark.createDataFrame([(3,)], ["CNT"])
            mock_read.return_value = mock_df

            with pytest.raises(SystemExit):
                step_verify(spark)
        finally:
            spark.stop()


class TestPayCalendarEmail:
    """Tests for pay calendar email notification."""

    def test_build_message_contains_period(self):
        """Verify notification message contains period details."""
        from pyspark_migration.jobs.pay_calendar.pay_calendar_job import (
            step_build_message,
        )

        pp_details = {
            "PP_NUM": 5,
            "PP_END_YEAR": 2024,
            "PP_START_DTE": "2024-03-03",
            "PP_END_DTE": "2024-03-16",
        }
        subject, message = step_build_message(pp_details)

        assert "5" in message
        assert "2024" in message
        assert "Pay Period" in subject or "Pay Calendar" in subject
